package sply2

import (
	"bytes"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/pgm/sply2/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

func testClient() *storage.Client {
	ctx := context.Background()
	//	projectID := "gcs-test-1136"

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile("/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	return client
}

const BucketName = "gcs-test-1136"

func generateUniqueString() string {
	return time.Now().Format(time.RFC3339Nano)
}

func TestBlockPushPull(t *testing.T) {
	require := require.New(t)
	client := testClient()

	body := generateUniqueString()
	b := client.Bucket(BucketName)
	o := b.Object("test/AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	ctx := context.Background()
	o.Delete(ctx)

	BID := core.BlockID{1}
	f := NewRemoteRefFactory(client, BucketName, "test/")
	err := f.Push(BID, bytes.NewReader([]byte(body)))

	r, err := o.NewReader(ctx)
	require.Nil(err)

	buffer := make([]byte, len(body))
	_, err = r.Read(buffer)
	require.Nil(err)
	require.Equal(body, string(buffer))

	bb := bytes.NewBuffer(make([]byte, 0, 100))
	ref, err := f.GetRef(&core.NodeRepr{BID: BID, Size: int64(len(body))})
	require.Nil(err)
	err = ref.Copy(0, int64(len(body)), bb)
	require.Nil(err)
	require.Equal(body, string(bb.Bytes()))
}

func TestGCSClient(t *testing.T) {
	require := require.New(t)
	client := testClient()

	ctx := context.Background()

	b := client.Bucket("gcs-test-1136")
	o := b.Object("sample")
	w := o.NewWriter(ctx)
	_, err := w.Write([]byte("hello"))
	require.Nil(err)
	w.Close()
}

// func newDataStore(dir string) *DataStore {
// 	return
// }

// func testDataStore() *core.DataStore {
// 	dir, err := ioutil.TempDir("", "test")
// 	if err != nil {
// 		panic(err)
// 	}
// 	return newDataStore(dir)
// }

func TestDatastoreWithGCSRemote(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile("/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := NewRemoteRefFactory(client, bucketName, "blocks/")
	//		sply2.NewBoltDB(path.Join(dir, "nodes.db"), [][]byte{ChildNodeBucket, NodeBucket}))
	dir, err := ioutil.TempDir("", "test")
	require.Nil(err)

	ds := core.NewDataStore(dir, f, core.NewMemStore([][]byte{core.ChunkStat}), core.NewMemStore([][]byte{core.ChildNodeBucket, core.NodeBucket}))
	// ds := core.NewDataStore(dir, f, sply2.NewBoltDB(path.Join(dir, "freezer.db"), [][]byte{core.ChunkStat}),
	// 	sply2.NewBoltDB(path.Join(dir, "nodes.db"), [][]byte{core.ChildNodeBucket, core.NodeBucket}))
	ds.SetClients(f, f)

	inode, err := ds.AddRemoteGCS(core.RootINode, "gcs", bucketName, "sample")
	r, err := ds.GetReadRef(inode)
	require.Nil(err)

	b := make([]byte, 100)
	n, err := r.Read(b)
	require.Equal(5, n)
	require.Equal("hello", string(b[:n]))
}
