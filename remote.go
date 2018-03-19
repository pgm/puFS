package sply2

import (
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

type RemoteFile struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time

	// Fields for Remote GCS
	Bucket     string
	Key        string
	Generation int64
}

type RemoteRefFactory interface {
	GetRef(node *NodeRepr) (RemoteRef, error)
	Push(BID BlockID, rr ReadableRef) error
	SetLease(name string, expiry time.Time, BID BlockID) error
	SetRoot(name string, BID BlockID) error
	GetRoot(name string) (BlockID, error)
	GetChildNodes(node *NodeRepr) ([]*RemoteFile, error)
}

type RemoteRefFactoryImp struct {
	CASBucket    string
	CASKeyPrefix string
	GCSClient    *storage.Client
}

func (rrf *RemoteRefFactoryImp) Push(BID BlockID, rr ReadableRef) error {
	ctx := context.Background()
	// upload only if generation == 0, which means this upload will fail if any object exists with the key
	// TODO: need to add a check for that case
	key := rrf.getBlockKey(BID)
	fmt.Println("bucket " + rrf.CASBucket + " " + key)
	CASBucketRef := rrf.GCSClient.Bucket(rrf.CASBucket)
	objHandle := CASBucketRef.Object(key).If(storage.Conditions{DoesNotExist: true})
	writer := objHandle.NewWriter(ctx)
	defer writer.Close()

	n, err := io.Copy(writer, &ReadableRefAdapter{rr: rr})
	if err != nil {
		return err
	}

	fmt.Printf("Bytes copied: %d\n", n)

	return nil
}

type ReadableRefAdapter struct {
	rr     ReadableRef
	offset int64
}

func (rra *ReadableRefAdapter) Read(buffer []byte) (int, error) {
	n, err := rra.rr.Read(rra.offset, buffer)
	rra.offset += int64(n)
	if err != nil {
		return n, err
	}
	return n, err
}

func NewRemoteRefFactory(client *storage.Client, CASBucket string, CASKeyPrefix string) *RemoteRefFactoryImp {
	return &RemoteRefFactoryImp{GCSClient: client, CASBucket: CASBucket, CASKeyPrefix: CASKeyPrefix}
}

func (rrf *RemoteRefFactoryImp) getBlockKey(BID BlockID) string {
	return rrf.CASKeyPrefix + base64.URLEncoding.EncodeToString(BID[:])
}

func (rrf *RemoteRefFactoryImp) GetRef(node *NodeRepr) (RemoteRef, error) {
	var remote RemoteRef

	if node.URL != "" {
		remote = &RemoteURL{URL: node.URL, ETag: node.ETag, Length: node.Size}
	} else {
		CASBucketRef := rrf.GCSClient.Bucket(rrf.CASBucket)

		remote = &RemoteGCS{Bucket: CASBucketRef, Key: rrf.getBlockKey(node.BID), Size: node.Size}
	}

	return remote, nil
}
