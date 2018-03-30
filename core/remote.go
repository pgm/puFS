package core

import (
	"context"
	"encoding/base64"
	"time"
)

type GCSObjectSource struct {
	Bucket     string
	Key        string
	Generation int64
	Size       int64
}

type URLSource struct {
	URL  string
	ETag string
	Size int64
}

type RemoteFile struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time

	RemoteSource interface{}
}

//TODO: Rename this "RemoteRepo"
// Change all references to Bucket/Key/URL to pointer to RemoteSource interface{}
type RemoteRefFactory interface {
	Push(ctx context.Context, BID BlockID, rr FrozenRef) error
	SetLease(ctx context.Context, name string, expiry time.Time, BID BlockID) error
	SetRoot(ctx context.Context, name string, BID BlockID) error
	GetRoot(ctx context.Context, name string) (BlockID, error)
	GetChildNodes(ctx context.Context, node *NodeRepr) ([]*RemoteFile, error)
}

type RemoteRefFactory2 interface {
	GetRef(source interface{}) RemoteRef
}

func GetBlockKey(CASKeyPrefix string, BID BlockID) string {
	return CASKeyPrefix + base64.URLEncoding.EncodeToString(BID[:])
}
