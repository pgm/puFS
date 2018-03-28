package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/pgm/sply2/region"
)

var ChunkStat []byte = []byte("ChunkStat")

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{filename: name}
}

type FrozenRefImp struct {
	remote    RemoteRef
	mask      *region.Mask
	filename  string
	offset    int64
	chunkSize int
}

func (w *FrozenRefImp) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		panic("unimp")
	}
	w.offset = offset
	return w.offset, nil
}

func divideIntoChunks(chunkSize int, x []region.Region) []region.Region {
	// TODO: make each region at most ChunkSize bytes long
	return x
}

func (w *FrozenRefImp) ensurePulled(ctx context.Context, start int64, end int64) error {
	// align the read region with ChunkSize
	start = (start / int64(w.chunkSize)) * int64(w.chunkSize)
	if end > w.remote.GetSize() {
		end = w.remote.GetSize()
	}
	missingRegions := w.mask.GetMissing(start, end)
	missingRegions = divideIntoChunks(w.chunkSize, missingRegions)

	f, err := os.OpenFile(w.filename, os.O_RDWR, 0755)
	if err != nil {
		return err
	}

	for _, r := range missingRegions {
		_, err = f.Seek(r.Start, 0)
		if err != nil {
			return err
		}

		err = w.remote.Copy(ctx, r.Start, r.End-r.Start, f)
		if err != nil {
			return err
		}
		w.mask.Add(r.Start, r.End)
	}

	return nil
}

func (w *FrozenRefImp) Read(ctx context.Context, dest []byte) (int, error) {
	err := w.ensurePulled(ctx, w.offset, w.offset+int64(len(dest)))
	if err != nil {
		return 0, err
	}

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
	path      string
	db        KVStore
	chunkSize int
}

func NewFreezer(path string, db KVStore, chunkSize int) *FreezerImp {
	chunkPath := path + "/chunks"
	err := os.MkdirAll(chunkPath, 0700)
	if err != nil {
		log.Fatal(err)
	}

	return &FreezerImp{path: chunkPath, db: db, chunkSize: chunkSize}
}

func (f *FreezerImp) Close() error {
	return f.db.Close()
}

func (f *FreezerImp) getPath(BID BlockID) string {
	filename := fmt.Sprintf("%s/%s", f.path, base64.URLEncoding.EncodeToString(BID[:]))
	return filename
}
func (f *FreezerImp) getRemote(BID BlockID) RemoteRef {
	panic("unimp")
}
func (f *FreezerImp) GetRef(BID BlockID) (FrozenRef, error) {
	filename := f.getPath(BID)
	remote := f.getRemote(BID)
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// fmt.Printf("Path %s does not exists\n", filename)
		return nil, nil
	}
	// fmt.Printf("Path %s exists\n", filename)

	return &FrozenRefImp{remote: remote,
		mask:      region.New(),
		filename:  filename,
		chunkSize: f.chunkSize}, nil
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

type NewBlock struct {
	BID     BlockID
	Size    int64
	ModTime time.Time
}

func (f *FreezerImp) AddFile(path string) (*NewBlock, error) {
	BID, err := computeHash(path)
	if err != nil {
		return nil, err
	}

	// find the path in the freezer for this block
	destPath := f.getPath(BID)
	// and move this file there
	err = os.Rename(path, destPath)
	if err != nil {
		return nil, err
	}

	st, err := os.Stat(destPath)
	if err != nil {
		panic("File disappeared after move or could not stat")
	}

	err = f.writeChunkStatus(BID, false)
	if err != nil {
		return nil, err
	}

	return &NewBlock{BID: BID, Size: st.Size(), ModTime: st.ModTime()}, nil
}

func (f *FreezerImp) AddBlock(ctx context.Context, BID BlockID, remoteRef RemoteRef) error {
	hasChunk, err := f.hasChunk(BID)
	if err != nil {
		return err
	}
	if hasChunk {
		return nil
	}

	filename := f.getPath(BID)

	// st, err = os.Stat(filename)
	// fmt.Printf("attempting to create %s (%s)\n", filename, err)
	fi, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	// fmt.Printf("err (%s)\n", err)
	if err != nil {
		return err
	}

	defer fi.Close()

	// fmt.Printf("Performing copy of 0-%d\n", remoteRef.GetSize())
	// copy with size = -1 to copy entire contents
	err = remoteRef.Copy(ctx, 0, -1, fi)
	// fmt.Printf("err from copy %s\n", err)
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
