package sply2

import (
	"io"
	"time"
)

type INode uint32
type BlockID [32]byte

type Freezer interface {
	GetRef(BID BlockID) (FrozenRef, error)
	AddBlock(BID BlockID, remoteRef RemoteRef) error
	AddFile(path string) (BlockID, error)
	IsPushed(BID BlockID) (bool, error)
}

type FrozenRef interface {
	io.ReadSeeker
	Release()
}

type RemoteRef interface {
	GetSize() int64
	Copy(offset int64, len int64, writer io.Writer) error
	// Release()
}

type WritableRef interface {
	io.ReadSeeker
	io.Writer
	Release()
}

type DirEntry struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time

	BID BlockID // maybe lift this up to header block as previously considered. Would allow GC to trace references without reading/parsing whole block

	URL  string
	ETag string

	Bucket     string
	Key        string
	Generation int64
}

type Dir struct {
	Entries []DirEntry
}
