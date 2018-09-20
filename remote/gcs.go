package remote

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"runtime/trace"
	"strings"
	"time"

	// Imports the Google Cloud Storage client package.

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2/core"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type RemoteGCS struct {
	Owner  *RemoteRefFactoryImp
	Source *core.GCSObjectSource
}

func (r *RemoteGCS) GetSize() int64 {
	return r.Source.Size
}

// func NewRemoteObject(client *storage.Client, bucketName string, key string) (*RemoteGCS, error) {
// 	ctx := context.Background()
// 	bucket := client.Bucket(bucketName)
// 	objHandle := bucket.Object(key)
// 	attr, err := objHandle.Attrs(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &RemoteGCS{Bucket: bucket, Key: key, Generation: attr.Generation, Size: attr.Size}, nil
// }

type RemoteRefFactoryImp struct {
	Bucket         string
	RootKeyPrefix  string
	LeaseKeyPrefix string
	CASKeyPrefix   string
	GCSClient      *storage.Client
}

// func (rrf *RemoteRefFactoryImp) GetChildNodes(ctx context.Context, remoteSource interface{}) ([]*core.RemoteFile, error) {
// 	gcsSource := remoteSource.(*core.GCSObjectSource)

// 	b := rrf.GCSClient.Bucket(gcsSource.Bucket)
// 	it := b.Objects(ctx, &storage.Query{Delimiter: "/", Prefix: gcsSource.Key, Versions: false})
// 	result := make([]*core.RemoteFile, 0, 100)
// 	for {
// 		next, err := it.Next()
// 		if err == iterator.Done {
// 			break
// 		}
// 		if err != nil {
// 			return nil, err
// 		}

// 		var file *core.RemoteFile
// 		if next.Prefix != "" {
// 			// if we really want mtime we could get the mtime of the prefix because it's usually an empty object via
// 			// an explicit attr fetch of the prefix
// 			file = &core.RemoteFile{Name: next.Prefix[len(gcsSource.Key) : len(next.Prefix)-1],
// 				IsDir: true,
// 				RemoteSource: &core.GCSObjectSource{
// 					Bucket: gcsSource.Bucket,
// 					Key:    next.Prefix}}
// 		} else {
// 			name := next.Name[len(gcsSource.Key):]
// 			if name == "" {
// 				continue
// 			}
// 			file = &core.RemoteFile{Name: name,
// 				IsDir:   false,
// 				Size:    next.Size,
// 				ModTime: next.Updated,
// 				RemoteSource: &core.GCSObjectSource{
// 					Bucket:     gcsSource.Bucket,
// 					Key:        next.Name,
// 					Generation: next.Generation}}
// 		}

// 		result = append(result, file)
// 	}

// 	return result, nil
// }

type Lease struct {
	Expiry time.Time
	BID    core.BlockID
}

func (rrf *RemoteRefFactoryImp) SetLease(ctx context.Context, name string, expiry time.Time, BID core.BlockID) error {
	b := rrf.GCSClient.Bucket(rrf.Bucket)
	o := b.Object(rrf.LeaseKeyPrefix + name)
	w := o.NewWriter(ctx)
	defer w.Close()
	enc := gob.NewEncoder(w)
	err := enc.Encode(&Lease{Expiry: expiry, BID: BID})
	if err != nil {
		return err
	}
	return nil
}

func (rrf *RemoteRefFactoryImp) SetRoot(ctx context.Context, name string, BID core.BlockID) error {
	b := rrf.GCSClient.Bucket(rrf.Bucket)
	o := b.Object(rrf.RootKeyPrefix + name)
	w := o.NewWriter(ctx)
	defer w.Close()

	BIDStr := base64.URLEncoding.EncodeToString(BID[:])
	_, err := w.Write([]byte(BIDStr))
	if err != nil {
		return err
	}

	return nil
}

func (rrf *RemoteRefFactoryImp) GetRoot(ctx context.Context, name string) (core.BlockID, error) {
	b := rrf.GCSClient.Bucket(rrf.Bucket)
	o := b.Object(rrf.RootKeyPrefix + name)
	r, err := o.NewReader(ctx)
	if err != nil {
		return core.NABlock, err
	}
	defer r.Close()

	buffer, err := ioutil.ReadAll(r)
	if err != nil {
		return core.NABlock, err
	}

	var BID core.BlockID
	dbuffer := make([]byte, 1000)
	n, err := base64.URLEncoding.Decode(dbuffer, buffer)
	copy(BID[:], dbuffer[:n])
	if err != nil {
		return core.NABlock, err
	}
	return BID, nil
}

func (rrf *RemoteRefFactoryImp) GetGCSAttr(ctx context.Context, bucket string, key string) (*core.GCSAttrs, error) {
	if strings.HasSuffix(key, "/") || key == "" {
		// should we do some sanity check. For the moment, assuming bucket/key is always good
		return &core.GCSAttrs{IsDir: true}, nil
	}

	b := rrf.GCSClient.Bucket(bucket)
	o := b.Object(key)
	attrs, err := o.Attrs(ctx)

	if err != nil {
		return nil, err
	}

	return &core.GCSAttrs{Generation: attrs.Generation, IsDir: false, ModTime: attrs.Updated, Size: attrs.Size}, nil
}

func (rrf *RemoteRefFactoryImp) GetBlockSource(ctx context.Context, BID core.BlockID) (interface{}, error) {
	key := core.GetBlockKey(rrf.CASKeyPrefix, BID)
	attr, err := rrf.GetGCSAttr(ctx, rrf.Bucket, key)
	if err != nil {
		return nil, err
	}
	return &core.GCSObjectSource{Bucket: rrf.Bucket, Key: key, Size: attr.Size}, nil
}

func (rrf *RemoteRefFactoryImp) Push(ctx context.Context, BID core.BlockID, rr core.FrozenRef) error {
	// upload only if generation == 0, which means this upload will fail if any object exists with the key
	// TODO: need to add a check for that case
	key := core.GetBlockKey(rrf.CASKeyPrefix, BID)
	// fmt.Println("bucket " + rrf.CASBucket + " " + key)
	CASBucketRef := rrf.GCSClient.Bucket(rrf.Bucket)
	objHandle := CASBucketRef.Object(key).If(storage.Conditions{DoesNotExist: true})
	writer := objHandle.NewWriter(ctx)
	defer writer.Close()

	_, err := io.Copy(writer, &core.FrozenReader{ctx, rr})
	if err != nil {
		return err
	}

	// fmt.Printf("Bytes copied: %d\n", n)

	return nil
}

func NewRemoteRefFactory(client *storage.Client, Bucket string, KeyPrefix string) *RemoteRefFactoryImp {
	if !strings.HasSuffix(KeyPrefix, "/") && KeyPrefix != "" {
		panic("Prefix must end in /")
	}
	return &RemoteRefFactoryImp{GCSClient: client, Bucket: Bucket, CASKeyPrefix: KeyPrefix + "CAS/",
		RootKeyPrefix:  KeyPrefix + "root/",
		LeaseKeyPrefix: KeyPrefix + "lease/"}
}

type GCSRef struct {
	Owner  *RemoteRefFactoryImp
	Source *core.GCSObjectSource
}

func (r *GCSRef) GetSize() int64 {
	return r.Source.Size
}

func (r *GCSRef) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	defer trace.StartRegion(ctx, "GCSCopy").End()
	return copyRegion(ctx, r.Owner.GCSClient, r.Source.Bucket, r.Source.Key, r.Source.Generation, offset, len, writer)
}

func (r *GCSRef) GetSource() interface{} {
	return r.Source
}

func (r *GCSRef) GetChildNodes(ctx context.Context) ([]*core.RemoteFile, error) {
	return getChildNodes(ctx, r.Owner.GCSClient, r.Source.Bucket, r.Source.Key)
}

func (rf *RemoteRefFactoryImp) GetRef(source interface{}) core.RemoteRef {
	// switch r := r.(type) {
	// default:
	// 	// Note: To FUSE, ENOSYS means "this server never implements this request."
	// 	// It would be inappropriate to return ENOSYS for other operations in this
	// 	// switch that might only be unavailable in some contexts, not all.
	// 	return fuse.ENOSYS

	// // Node operations.
	// case *fuse.GetattrRequest:

	switch source := source.(type) {
	default:
		panic(fmt.Sprintf("unknown type: %v", source))

	case *core.URLSource:
		return &URLRef{Owner: rf, Source: source}
	case *core.GCSObjectSource:
		return &GCSRef{Owner: rf, Source: source}
	}
}

func randomBlockID() core.BlockID {
	var BID core.BlockID
	rand.Read(BID[:])
	return BID
}

func getChildNodes(ctx context.Context, GCSClient *storage.Client, Bucket string, Key string) ([]*core.RemoteFile, error) {
	b := GCSClient.Bucket(Bucket)
	it := b.Objects(ctx, &storage.Query{Delimiter: "/", Prefix: Key, Versions: false})
	result := make([]*core.RemoteFile, 0, 100)
	for {
		next, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var file *core.RemoteFile
		if next.Prefix != "" {
			// if we really want mtime we could get the mtime of the prefix because it's usually an empty object via
			// an explicit attr fetch of the prefix
			file = &core.RemoteFile{Name: next.Prefix[len(Key) : len(next.Prefix)-1],
				IsDir: true,
				RemoteSource: &core.GCSObjectSource{
					Bucket: Bucket,
					Key:    next.Prefix}}
		} else {
			name := next.Name[len(Key):]
			if name == "" {
				continue
			}
			file = &core.RemoteFile{Name: name,
				IsDir:   false,
				Size:    next.Size,
				ModTime: next.Updated,
				BID:     randomBlockID(), // not computed based on content. Useful to be able to reuse freezer without colliding any real content
				RemoteSource: &core.GCSObjectSource{Bucket: Bucket,
					Key:        next.Name,
					Generation: next.Generation,
					Size:       next.Size}}
		}

		result = append(result, file)
	}

	return result, nil
}

func copyRegion(ctx context.Context, GCSClient *storage.Client, Bucket string, Key string, Generation int64, offset int64, len int64, writer io.Writer) error {
	b := GCSClient.Bucket(Bucket)
	objHandle := b.Object(Key)
	if Generation != 0 {
		objHandle = objHandle.If(storage.Conditions{GenerationMatch: Generation})
	}

	var reader io.ReadCloser
	var err error
	// if offset != 0 || len != r.Size {
	reader, err = objHandle.NewRangeReader(ctx, offset, len)
	// } else {
	// 	reader, err = objHandle.NewReader(ctx)
	// }
	if err != nil {
		return err
	}
	defer reader.Close()

	n, err := io.Copy(writer, reader)
	if err != nil {
		return err
	}

	if len >= 0 && n != len {
		return fmt.Errorf("Expected to copy to copy %d bytes but copied %d", len, n)
	}

	return nil
}
