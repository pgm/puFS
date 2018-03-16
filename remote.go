package sply2

import (
	"encoding/hex"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

type RemoteRefFactory interface {
	GetRef(node *NodeRepr) (RemoteRef, error)
	Push(BID BlockID, rr ReadableRef) error
}

type RemoteRefFactoryImp struct {
	CASBucket    string
	CASKeyPrefix string
	CASBucketRef *storage.BucketHandle // has ref to client. Good/bad idea? Not threadsafe
	GCSClient    *storage.Client
}

func (rrf *RemoteRefFactoryImp) Push(BID BlockID, rr ReadableRef) error {
	ctx := context.Background()
	// upload only if generation == 0, which means this upload will fail if any object exists with the key
	// TODO: need to add a check for that case
	objHandle := rrf.CASBucketRef.Object(rrf.getBlockKey(BID)).If(storage.Conditions{GenerationMatch: 0})
	writer := objHandle.NewWriter(ctx)
	defer writer.Close()

	_, err := io.Copy(writer, &ReadableRefAdapter{rr: rr})
	if err != nil {
		return err
	}

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

func NewRemoteRefFactory(projectID string, CASBucket string, CASKeyPrefix string) (*RemoteRefFactoryImp, error) {
	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return &RemoteRefFactoryImp{GCSClient: client}, nil
}

func (rrf *RemoteRefFactoryImp) getBlockKey(BID BlockID) string {
	return rrf.CASKeyPrefix + hex.EncodeToString(BID[:])
}

func (rrf *RemoteRefFactoryImp) GetRef(node *NodeRepr) (RemoteRef, error) {
	var remote RemoteRef

	if node.URL != "" {
		remote = &RemoteURL{URL: node.URL, ETag: node.ETag, Length: node.Size}
	} else {
		remote = &RemoteGCS{Bucket: rrf.CASBucketRef, Key: rrf.getBlockKey(node.BID), Size: node.Size}
	}

	return remote, nil
}
