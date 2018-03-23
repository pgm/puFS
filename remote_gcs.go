package sply2

import (
	"errors"
	"fmt"
	"io"
	"time"

	// Imports the Google Cloud Storage client package.

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2/core"
	"golang.org/x/net/context"
)

type RemoteGCS struct {
	Bucket     *storage.BucketHandle // has ref to client
	Key        string
	Generation int64
	Size       int64
}

func (r *RemoteGCS) Copy(offset int64, len int64, writer io.Writer) error {
	ctx := context.Background()
	objHandle := r.Bucket.Object(r.Key)
	if r.Generation != 0 {
		objHandle = objHandle.If(storage.Conditions{GenerationMatch: r.Generation})
	}

	var reader io.ReadCloser
	var err error
	if offset != 0 || len != r.Size {
		reader, err = objHandle.NewRangeReader(ctx, offset, len)
	} else {
		reader, err = objHandle.NewReader(ctx)
	}
	if err != nil {
		return err
	}
	defer reader.Close()

	n, err := io.Copy(writer, reader)
	if err != nil {
		return err
	}

	if n != len {
		return errors.New("Did not copy full requested length")
	}

	return nil
}

func (r *RemoteGCS) GetSize() int64 {
	return r.Size
}

func NewRemoteObject(client *storage.Client, bucketName string, key string) (*RemoteGCS, error) {
	ctx := context.Background()
	bucket := client.Bucket(bucketName)
	objHandle := bucket.Object(key)
	attr, err := objHandle.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	return &RemoteGCS{Bucket: bucket, Key: key, Generation: attr.Generation, Size: attr.Size}, nil
}

type RemoteRefFactoryImp struct {
	CASBucket    string
	CASKeyPrefix string
	GCSClient    *storage.Client
}

func (rrf *RemoteRefFactoryImp) GetChildNodes(node *core.NodeRepr) ([]*core.RemoteFile, error) {
	panic("unimp")
}

func (rrf *RemoteRefFactoryImp) SetLease(name string, expiry time.Time, BID core.BlockID) error {
	panic("unimp")
}

func (rrf *RemoteRefFactoryImp) SetRoot(name string, BID core.BlockID) error {
	panic("unimp")
}

func (rrf *RemoteRefFactoryImp) GetRoot(name string) (core.BlockID, error) {
	panic("unimp")
}

func (rrf *RemoteRefFactoryImp) GetGCSAttr(bucket string, key string) (*core.GCSAttrs, error) {
	ctx := context.Background()
	b := rrf.GCSClient.Bucket(bucket)
	o := b.Object(key)
	attrs, err := o.Attrs(ctx)

	if err != nil {
		return nil, err
	}

	return &core.GCSAttrs{Generation: attrs.Generation, IsDir: false, ModTime: attrs.Updated, Size: attrs.Size}, nil
}

func (rrf *RemoteRefFactoryImp) Push(BID core.BlockID, rr io.Reader) error {
	ctx := context.Background()
	// upload only if generation == 0, which means this upload will fail if any object exists with the key
	// TODO: need to add a check for that case
	key := core.GetBlockKey(rrf.CASKeyPrefix, BID)
	fmt.Println("bucket " + rrf.CASBucket + " " + key)
	CASBucketRef := rrf.GCSClient.Bucket(rrf.CASBucket)
	objHandle := CASBucketRef.Object(key).If(storage.Conditions{DoesNotExist: true})
	writer := objHandle.NewWriter(ctx)
	defer writer.Close()

	n, err := io.Copy(writer, rr)
	if err != nil {
		return err
	}

	fmt.Printf("Bytes copied: %d\n", n)

	return nil
}

func NewRemoteRefFactory(client *storage.Client, CASBucket string, CASKeyPrefix string) *RemoteRefFactoryImp {
	return &RemoteRefFactoryImp{GCSClient: client, CASBucket: CASBucket, CASKeyPrefix: CASKeyPrefix}
}

func (rrf *RemoteRefFactoryImp) GetRef(node *core.NodeRepr) (core.RemoteRef, error) {
	var remote core.RemoteRef

	if node.URL != "" {
		remote = &RemoteURL{URL: node.URL, ETag: node.ETag, Length: node.Size}
	} else {
		if node.Key != "" {
			CASBucketRef := rrf.GCSClient.Bucket(node.Bucket)
			remote = &RemoteGCS{Bucket: CASBucketRef, Key: node.Key, Size: node.Size}
		} else {
			CASBucketRef := rrf.GCSClient.Bucket(rrf.CASBucket)
			remote = &RemoteGCS{Bucket: CASBucketRef, Key: core.GetBlockKey(rrf.CASKeyPrefix, node.BID), Size: node.Size}
		}
	}
	fmt.Printf("remote=%v\n", remote)

	return remote, nil
}
