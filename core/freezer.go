package core

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
)

var ChunkStat []byte = []byte("ChunkStat")

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{filename: name}
}

type FrozenRefImp struct {
	filename string
	offset   int64
}

func (w *FrozenRefImp) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		panic("unimp")
	}
	w.offset = offset
	return w.offset, nil
}

func (w *FrozenRefImp) Read(dest []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(w.offset, 0)
	if err != nil {
		return 0, err
	}

	n, err := f.Read(dest)
	if err != nil {
		return n, err
	}
	w.offset += int64(n)

	return n, nil
}

func (w *FrozenRefImp) Release() {

}

type FreezerImp struct {
	path string
	db   KVStore
	//	blocks map[BlockID]string
}

func NewFreezer(path string, db KVStore) *FreezerImp {
	chunkPath := path + "/chunks"
	err := os.MkdirAll(chunkPath, 0700)
	if err != nil {
		log.Fatal(err)
	}

	return &FreezerImp{path: chunkPath, db: db}
}

func (f *FreezerImp) Close() error {
	return f.db.Close()
}

func (f *FreezerImp) getPath(BID BlockID) string {
	filename := fmt.Sprintf("%s/%s", f.path, base64.URLEncoding.EncodeToString(BID[:]))
	return filename
}

func (f *FreezerImp) GetRef(BID BlockID) (FrozenRef, error) {
	filename := f.getPath(BID)
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		fmt.Printf("Path %s does not exists\n", filename)
		return nil, nil
	}
	fmt.Printf("Path %s exists\n", filename)

	return &FrozenRefImp{filename: filename}, nil
}

func computeHash(path string) (BlockID, error) {
	hash := sha256.New()

	fi, err := os.Open(path)
	defer fi.Close()
	if err != nil {
		return NABlock, err
	}

	_, err = io.Copy(hash, fi)
	if err != nil {
		return NABlock, err
	}

	var BID BlockID
	copy(BID[:], hash.Sum(nil))

	return BID, err
}

func (f *FreezerImp) hasChunk(BID BlockID) (bool, error) {
	var value []byte
	err := f.db.View(func(tx RTx) error {
		chunkStat := tx.RBucket(ChunkStat)
		value = chunkStat.Get(BID[:])
		return nil
	})
	if err != nil {
		return false, err
	}

	return value != nil, nil
}

func (f *FreezerImp) writeChunkStatus(BID BlockID, isRemote bool) error {
	err := f.db.Update(func(tx RWTx) error {
		chunkStat := tx.WBucket(ChunkStat)
		value := make([]byte, 1)
		if isRemote {
			value[0] = 1
		}
		return chunkStat.Put(BID[:], value)
	})
	return err
}

func (f *FreezerImp) IsPushed(BID BlockID) (bool, error) {
	var pushed bool
	err := f.db.View(func(tx RTx) error {
		chunkStat := tx.RBucket(ChunkStat)
		value := chunkStat.Get(BID[:])
		pushed = value[0] != 0
		return nil
	})
	return pushed, err
}

func (f *FreezerImp) AddFile(path string) (BlockID, error) {
	BID, err := computeHash(path)
	if err != nil {
		return NABlock, err
	}

	// find the path in the freezer for this block
	destPath := f.getPath(BID)
	// and move this file there
	err = os.Rename(path, destPath)
	if err != nil {
		return NABlock, err
	}

	err = f.writeChunkStatus(BID, false)
	if err != nil {
		return NABlock, err
	}

	return BID, nil
}

func (f *FreezerImp) AddBlock(BID BlockID, remoteRef RemoteRef) error {
	hasChunk, err := f.hasChunk(BID)
	if err != nil {
		return err
	}
	if hasChunk {
		return nil
	}

	filename := f.getPath(BID)

	_, err = os.Stat(filename)
	fmt.Printf("attempting to create %s (%s)\n", filename, err)
	fi, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	fmt.Printf("err (%s)\n", err)
	if err != nil {
		return err
	}

	defer fi.Close()

	fmt.Printf("Performing copy of 0-%d\n", remoteRef.GetSize())
	err = remoteRef.Copy(0, remoteRef.GetSize(), fi)
	fmt.Printf("err from copy %s\n", err)
	if err != nil {
		return err
	}
	// s, err = os.Stat(filename)

	err = f.writeChunkStatus(BID, true)
	if err != nil {
		return err
	}

	return nil
}
