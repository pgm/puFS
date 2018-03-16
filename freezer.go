package sply2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"

	bolt "github.com/coreos/bbolt"
)

var ChunkStat []byte = []byte("ChunkStat")

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{name}
}

type FrozenRefImp struct {
	filename string
}

func (w *FrozenRefImp) Read(offset int64, dest []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return f.Read(dest)
}

func (w *FrozenRefImp) Release() {

}

type FreezerImp struct {
	path string
	db   *bolt.DB
	//	blocks map[BlockID]string
}

func NewFreezer(path string) *FreezerImp {
	chunkPath := path + "/chunks"
	err := os.MkdirAll(chunkPath, 0700)
	if err != nil {
		log.Fatal(err)
	}

	db, err := bolt.Open(path+"/chunkstat.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(ChunkStat)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	return &FreezerImp{path: chunkPath, db: db}
}

func (f *FreezerImp) Close() error {
	return f.db.Close()
}

func (f *FreezerImp) getPath(BID BlockID) string {
	filename := fmt.Sprintf("%s/%s", f.path, hex.EncodeToString(BID[:]))
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

	return &FrozenRefImp{filename}, nil
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

func (f *FreezerImp) writeChunkStatus(BID BlockID, isRemote bool) error {
	err := f.db.Update(func(tx *bolt.Tx) error {
		chunkStat := tx.Bucket(ChunkStat)
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
	err := f.db.View(func(tx *bolt.Tx) error {
		chunkStat := tx.Bucket(ChunkStat)
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
	filename := f.getPath(BID)

	fi, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		return err
	}

	defer fi.Close()

	err = remoteRef.Copy(0, remoteRef.GetSize(), fi)
	if err != nil {
		return err
	}

	err = f.writeChunkStatus(BID, true)
	if err != nil {
		return err
	}

	return nil
}
