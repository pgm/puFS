package sply2

import (
	"io"
	"time"
)

type INode uint32
type BlockID [32]byte

type Freezer interface {
	GetRef(BlockID) (error, FrozenRef)
	AddBlock(BlockID, remoteRef RemoteRef) error
}

// type MemFreezer struct {
// 	blocks map[BlockID][]byte
// }

// func (f *MemFreezer) GetRef(id BlockID) (error, FrozenRef) {
// 	buffer, ok := f.blocks[id]
// 	if !ok {
// 		return nil, nil
// 	}
// 	return nil, &FrozenRefImp{buffer}
// }

// func (f *MemFreezer) AddBlock(id BlockID, remoteRef RemoteRef) (error, FrozenRef) {
// 	buffer := f.blocks[id]
// 	return nil, &FrozenRefImp{buffer}
// }

type FrozenRef interface {
	Read(offset int64, dest []byte) (int, error)
	Release()
}

// type FrozenRefImp struct {
// 	buffer []byte
// }

// func (f *FrozenRefImp) Read(offset int64, len int64, dest []byte) (error, int64) {
// 	copy(dest, f.buffer[offset:offset+len])
// 	return nil, len
// }

// func (f *FrozenRefImp) Release() {
// 	// noop
// }

type RemoteRef interface {
	// GetLength() int64
	Copy(offset int64, len int64, writer io.Writer) error
	// Release()
}

type WritableRef interface {
	Read(offset int64, dest []byte) (int, error)
	Write(offset int64, buffer []byte) (int, error)
	Release()
}

type ReadableRef interface {
	Read(offset int64, dest []byte) (int, error)
	Release()
}

type Node struct {
	IsDir   bool
	Size    int64
	ModTime time.Time

	BID  *BlockID
	URL  string
	ETag string

	names map[string]INode // only populated when IsDir set

	// only populated when IsDir is false
	Remote   RemoteRef
	Frozen   FrozenRef
	Writable WritableRef
}
