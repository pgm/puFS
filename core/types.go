package core

import (
	"context"
	"io"
	"time"
)

type INode uint32
type BlockID [32]byte

type Freezer interface {
	GetRef(BID BlockID) (FrozenRef, error)
	AddBlock(ctx context.Context, BID BlockID, remoteRef RemoteRef) error
	AddFile(path string) (*NewBlock, error)
	IsPushed(BID BlockID) (bool, error)
}

type Reader interface {
	io.Seeker
	Read(ctx context.Context, p []byte) (n int, err error)
}

type FrozenRef interface {
	Reader
	Release()
}

type RemoteRef interface {
	GetSize() int64
	Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error
	GetSource() interface{}
	GetChildNodes(ctx context.Context) ([]*RemoteFile, error)
	// Release()
}

type HasPrintStats interface {
	PrintStats()
}

type WritableRef interface {
	Reader
	io.Writer
	Release()
}

type DirEntry struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time

	BID BlockID // maybe lift this up to header block as previously considered. Would allow GC to trace references without reading/parsing whole block

	RemoteSource interface{}
}

type DirEntryWithID struct {
	DirEntry
	ID INode
}

type Dir struct {
	Entries []DirEntry
}

type RTx interface {
	RBucket(name []byte) RBucket
}

type RWTx interface {
	RTx
	WBucket(name []byte) WBucket
}

type KVStore interface {
	Update(func(RWTx) error) error
	View(func(RTx) error) error
	Close() error
}

type RBucket interface {
	Get(key []byte) []byte
	ForEachWithPrefix(prefix []byte, callback func(key []byte, value []byte) error) error
}

type WBucket interface {
	RBucket
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

func generateUniqueString() string {
	return time.Now().Format(time.RFC3339Nano)
}

type GCSAttrs struct {
	Generation int64
	Size       int64
	ModTime    time.Time
	IsDir      bool
}

type NetworkClient interface {
	GetGCSAttr(ctx context.Context, bucket string, key string) (*GCSAttrs, error)
	GetHTTPAttr(ctx context.Context, url string) (*HTTPAttrs, error)
}

type HTTPAttrs struct {
	ETag string
	Size int64
}
