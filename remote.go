package sply2

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
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
	Push(BID BlockID, rr io.Reader) error
	SetLease(name string, expiry time.Time, BID BlockID) error
	SetRoot(name string, BID BlockID) error
	GetRoot(name string) (BlockID, error)
	GetChildNodes(node *NodeRepr) ([]*RemoteFile, error)
}

type RemoteRefFactoryMem struct {
	leases  map[string]BlockID
	roots   map[string]BlockID
	objects map[string][]byte
	prefix  string
}

type MemCopy struct {
	buffer []byte
}

func (m *MemCopy) GetSize() int64 {
	return int64(len(m.buffer))
}

func (m *MemCopy) Copy(offset int64, len int64, writer io.Writer) error {
	n, err := writer.Write(m.buffer[offset : offset+len])
	if n != int(len) {
		panic(fmt.Sprintf("%d != %d", n, len))
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func NewRemoteRefFactoryMem() *RemoteRefFactoryMem {
	return &RemoteRefFactoryMem{roots: make(map[string]BlockID),
		objects: make(map[string][]byte),
		prefix:  "blocks/",
		leases:  make(map[string]BlockID)}
}

func (r *RemoteRefFactoryMem) GetRef(node *NodeRepr) (RemoteRef, error) {
	key := getBlockKey(r.prefix, node.BID)
	b, ok := r.objects[key]
	if !ok {
		panic("missing block")
	}
	return &MemCopy{b}, nil
}

func (r *RemoteRefFactoryMem) Push(BID BlockID, rr io.Reader) error {
	b, err := ioutil.ReadAll(rr)
	if err != nil {
		panic(err)
	}
	key := getBlockKey(r.prefix, BID)
	r.objects[key] = b
	return nil
}

func (r *RemoteRefFactoryMem) SetLease(name string, expiry time.Time, BID BlockID) error {
	r.leases[name] = BID
	return nil
}

func (r *RemoteRefFactoryMem) SetRoot(name string, BID BlockID) error {
	r.roots[name] = BID
	return nil
}

func (r *RemoteRefFactoryMem) GetRoot(name string) (BlockID, error) {
	return r.roots[name], nil
}

func (r *RemoteRefFactoryMem) GetChildNodes(node *NodeRepr) ([]*RemoteFile, error) {
	prefix := node.Key + "/"
	dirs := make(map[string]bool)
	result := make([]*RemoteFile, 0, 100)
	now := time.Now()

	for key, value := range r.objects {
		if strings.HasPrefix(key, prefix) {
			name := key[len(prefix):]
			nextSlash := strings.Index(name, "/")
			if nextSlash >= 0 {
				name = name[:nextSlash]
				dirs[name] = true
			} else {
				rec := &RemoteFile{
					Name:       name,
					IsDir:      false,
					Size:       int64(len(value)),
					ModTime:    now,
					Bucket:     node.Bucket,
					Key:        key,
					Generation: 1}
				result = append(result, rec)
			}
		}
	}

	for name, _ := range dirs {
		rec := &RemoteFile{
			Name:       name,
			IsDir:      true,
			Size:       0,
			ModTime:    now,
			Bucket:     node.Bucket,
			Key:        node.Key + "/" + name,
			Generation: 0}
		result = append(result, rec)
	}

	return nil, nil
}

type RemoteRefFactoryImp struct {
	CASBucket    string
	CASKeyPrefix string
	GCSClient    *storage.Client
}

func (rrf *RemoteRefFactoryImp) Push(BID BlockID, rr io.Reader) error {
	ctx := context.Background()
	// upload only if generation == 0, which means this upload will fail if any object exists with the key
	// TODO: need to add a check for that case
	key := getBlockKey(rrf.CASKeyPrefix, BID)
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

func getBlockKey(CASKeyPrefix string, BID BlockID) string {
	return CASKeyPrefix + base64.URLEncoding.EncodeToString(BID[:])
}

func (rrf *RemoteRefFactoryImp) GetRef(node *NodeRepr) (RemoteRef, error) {
	var remote RemoteRef

	if node.URL != "" {
		remote = &RemoteURL{URL: node.URL, ETag: node.ETag, Length: node.Size}
	} else {
		CASBucketRef := rrf.GCSClient.Bucket(rrf.CASBucket)

		remote = &RemoteGCS{Bucket: CASBucketRef, Key: getBlockKey(rrf.CASKeyPrefix, node.BID), Size: node.Size}
	}

	return remote, nil
}
