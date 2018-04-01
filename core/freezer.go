package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pgm/sply2/region"
)

var ChunkStat []byte = []byte("ChunkStat")

// func NewFrozenRef(name string) FrozenRef {
// 	return &FrozenRefImp{filename: name}
// }

type FrozenRefImp struct {
	BID      BlockID
	remote   RemoteRef
	filename string
	offset   int64
	size     int64
	owner    *FreezerImp
}

type BlockInfo struct {
	Source interface{}
}

func (w *FrozenRefImp) Seek(offset int64, whence int) (int64, error) {
	if whence == os.SEEK_SET {
		w.offset = offset
	} else if whence == os.SEEK_CUR {
		w.offset += offset
	} else if whence == os.SEEK_END {
		w.offset = w.size + offset
	} else {
		panic("unknown value of whence")
	}
	return w.offset, nil
}

func divideIntoChunks(chunkSize int, x []region.Region) []region.Region {
	// TODO: make each region at most ChunkSize bytes long
	return x
}

func (w *FrozenRefImp) ensurePulled(ctx context.Context, start int64, end int64) error {
	// align the read region with ChunkSize
	chunkSize := w.owner.chunkSize

	start = (start / int64(chunkSize)) * int64(chunkSize)
	end = ((end + int64(chunkSize) - 1) / int64(chunkSize)) * int64(chunkSize)
	if end > w.size {
		end = w.size
	}
	missingRegions, err := w.owner.getInvalidRegions(w.BID, start, end)
	if err != nil {
		return err
	}
	missingRegions = divideIntoChunks(chunkSize, missingRegions)

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
		w.owner.addValidRegion(w.BID, r.Start, r.End)
	}

	return nil
}

// // fmt.Printf("Performing copy of 0-%d\n", remoteRef.GetSize())
// // copy with size = -1 to copy entire contents
// err = remoteRef.Copy(ctx, 0, -1, fi)
// // fmt.Printf("err from copy %s\n", err)
// if err != nil {
// 	return err
// }
// // s, err = os.Stat(filename)

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

	refFactory RemoteRefFactory2

	mutex        sync.Mutex
	blockRegions map[BlockID]*region.Mask
}

func NewFreezer(path string, db KVStore, refFactory RemoteRefFactory2, chunkSize int) *FreezerImp {
	chunkPath := path + "/chunks"
	err := os.MkdirAll(chunkPath, 0700)
	if err != nil {
		log.Fatal(err)
	}

	return &FreezerImp{path: chunkPath, db: db, chunkSize: chunkSize,
		blockRegions: make(map[BlockID]*region.Mask), refFactory: refFactory}
}

func (f *FreezerImp) Close() error {
	return f.db.Close()
}

func (f *FreezerImp) getPath(BID BlockID) string {
	filename := fmt.Sprintf("%s/%s", f.path, base64.URLEncoding.EncodeToString(BID[:]))
	return filename
}
func (f *FreezerImp) getRemote(BID BlockID) (RemoteRef, error) {
	var source interface{}

	err := f.db.View(func(tx RTx) error {
		info, err := f.readChunkInfo(BID, tx)
		if err != nil {
			return err
		}
		source = info.Source
		return nil
	})

	if err != nil {
		return nil, err
	}

	if source == nil {
		return nil, nil
	}

	return f.refFactory.GetRef(source), nil
}
func (f *FreezerImp) GetRef(BID BlockID) (FrozenRef, error) {
	if BID == NABlock {
		panic("Cannot get ref for NA block")
	}

	filename := f.getPath(BID)
	remote, err := f.getRemote(BID)
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// fmt.Printf("Path %s does not exists\n", filename)
		return nil, nil
	}
	// fmt.Printf("Path %s exists\n", filename)

	var size int64
	if remote != nil {
		size = remote.GetSize()
	} else {
		size = st.Size()
	}

	return &FrozenRefImp{BID: BID,
		remote:   remote,
		owner:    f,
		filename: filename,
		size:     size}, nil
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

func (f *FreezerImp) ensureRegionsCached(BID BlockID) (*region.Mask, error) {
	mask := f.blockRegions[BID]
	if mask == nil {
		regionLog := f.getPath(BID) + ".regions"

		mask = region.New()

		fp, err := os.Open(regionLog)

		readInt := func() (int64, error) {
			b := make([]byte, 8)
			_, err := fp.Read(b)
			if err != nil {
				return 0, err
			}
			return int64(binary.LittleEndian.Uint64(b)), nil
		}

		if err == nil {
			defer fp.Close()

			for {
				start, err := readInt()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				end, err := readInt()
				if err != nil {
					return nil, err
				}
				mask.Add(start, end)
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}

		f.blockRegions[BID] = mask
	}

	return mask, nil
}

func (f *FreezerImp) addValidRegion(BID BlockID, start int64, end int64) error {
	regionLog := f.getPath(BID) + ".regions"

	f.mutex.Lock()
	defer f.mutex.Unlock()

	mask, err := f.ensureRegionsCached(BID)
	if err != nil {
		return err
	}

	fp, err := os.OpenFile(regionLog, os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	defer fp.Close()

	writeInt := func(v int64) error {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		_, err := fp.Write(b)
		if err != nil {
			return err
		}
		return nil
	}

	writeInt(start)
	writeInt(end)
	mask.Add(start, end)

	return nil
}

func (f *FreezerImp) getInvalidRegions(BID BlockID, start int64, end int64) ([]region.Region, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	mask, err := f.ensureRegionsCached(BID)
	if err != nil {
		return nil, err
	}

	return mask.GetMissing(start, end), nil
}

// func (f *FreezerImp) getRemoteRef(BID BlockID) RemoteRef {
// }

func (f *FreezerImp) writeChunkInfo(BID BlockID, info *BlockInfo) error {
	buffer := bytes.NewBuffer(make([]byte, 0, 1000))
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(info)
	if err != nil {
		return err
	}

	infoBytes := buffer.Bytes()

	err = f.db.Update(func(tx RWTx) error {
		chunkStat := tx.WBucket(ChunkStat)
		return chunkStat.Put(BID[:], infoBytes)
	})
	return err
}

func (f *FreezerImp) readChunkInfo(BID BlockID, tx RTx) (*BlockInfo, error) {
	chunkStat := tx.RBucket(ChunkStat)
	buffer := chunkStat.Get(BID[:])
	if buffer == nil {
		return nil, UnknownBlockID
	}
	dec := gob.NewDecoder(bytes.NewReader(buffer))
	var info BlockInfo
	err := dec.Decode(&info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (f *FreezerImp) IsPushed(BID BlockID) (bool, error) {
	if BID == NABlock {
		panic("Asked if NA block was pushed")
	}

	var pushed bool
	var err error
	err = f.db.View(func(tx RTx) error {
		info, err := f.readChunkInfo(BID, tx)
		if err != nil {
			return err
		}
		pushed = info.Source != nil
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

	err = f.addValidRegion(BID, 0, st.Size())
	if err != nil {
		panic("Could not mark file as fully valid")
	}

	// TODO: Change "status" to include remote definition and path to chunklist (?)
	err = f.writeChunkInfo(BID, &BlockInfo{})
	if err != nil {
		return nil, err
	}

	return &NewBlock{BID: BID, Size: st.Size(), ModTime: st.ModTime()}, nil
}

func (f *FreezerImp) AddBlock(ctx context.Context, BID BlockID, remoteRef RemoteRef) error {
	if BID == NABlock {
		panic("Attempted to add NA block")
	}
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

	err = f.writeChunkInfo(BID, &BlockInfo{Source: remoteRef.GetSource()})
	if err != nil {
		return err
	}

	return nil
}
