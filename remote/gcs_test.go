package remote

import (
	"bytes"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pgm/sply2/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

const ServiceFile = "/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"

func needsServiceFile(t *testing.T) {
	if _, err := os.Stat(ServiceFile); os.IsNotExist(err) {
		t.Skipf("Service cred file is missing: %s", ServiceFile)
	}
}

func testClient(t *testing.T) *storage.Client {
	needsServiceFile(t)

	ctx := context.Background()
	//	projectID := "gcs-test-1136"

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(ServiceFile))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	return client
}

const BucketName = "gcs-test-1136"

func generateUniqueString() string {
	return time.Now().Format(time.RFC3339Nano)
}

func TestListChildren(t *testing.T) {
	needsServiceFile(t)

	require := require.New(t)
	client := testClient(t)

	f := NewRemoteRefFactory(client, BucketName, "test/")
	ref := f.GetRef(&core.GCSObjectSource{Bucket: BucketName, Key: "test-data/"})
	ctx := context.Background()
	files, err := ref.GetChildNodes(ctx)
	require.Nil(err)

	require.Equal(4, len(files))
	fileChecked := false
	folderChecked := false
	for _, f := range files {
		s := f.RemoteSource.(*core.GCSObjectSource)
		if f.Name == "file1" {
			require.Equal("test-data/file1", s.Key)
			fileChecked = true
		} else if f.Name == "folder1" {
			require.Equal("test-data/folder1/", s.Key)
			folderChecked = true
		}
	}
	require.True(fileChecked)
	require.True(folderChecked)
}

type mockFrozenReader struct {
	reader io.ReadSeeker
}

func (m *mockFrozenReader) Read(ctx context.Context, p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *mockFrozenReader) Seek(offset int64, b int) (n int64, err error) {
	return m.reader.Seek(offset, b)
}

func (m *mockFrozenReader) Release() {
}

func TestBlockPushPull(t *testing.T) {
	needsServiceFile(t)

	require := require.New(t)
	client := testClient(t)

	body := generateUniqueString()
	b := client.Bucket(BucketName)
	o := b.Object("test/CAS/AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	ctx := context.Background()
	o.Delete(ctx)

	BID := core.BlockID{1}
	f := NewRemoteRefFactory(client, BucketName, "test/")
	mfr := &mockFrozenReader{bytes.NewReader([]byte(body))}
	err := f.Push(ctx, BID, mfr)
	require.Nil(err)

	r, err := o.NewReader(ctx)
	require.Nil(err)

	buffer := make([]byte, len(body))
	_, err = r.Read(buffer)
	require.Nil(err)
	require.Equal(body, string(buffer))

	bb := bytes.NewBuffer(make([]byte, 0, 100))
	s, err := f.GetBlockSource(ctx, BID)
	require.Nil(err)
	ref := f.GetRef(s)
	require.Nil(err)
	err = ref.Copy(ctx, 0, int64(len(body)), bb)
	require.Nil(err)
	require.Equal(body, string(bb.Bytes()))
}

func TestGCSClient(t *testing.T) {
	needsServiceFile(t)

	require := require.New(t)
	client := testClient(t)

	ctx := context.Background()

	b := client.Bucket("gcs-test-1136")
	o := b.Object("sample")
	w := o.NewWriter(ctx)
	_, err := w.Write([]byte("hello"))
	require.Nil(err)
	w.Close()
}

func TestDatastoreWithGCSRemote(t *testing.T) {
	needsServiceFile(t)

	var x *core.GCSObjectSource
	gob.Register(x)
	require := require.New(t)
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(ServiceFile))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := NewRemoteRefFactory(client, bucketName, "blocks/")
	dir, err := ioutil.TempDir("", "gcs_test")
	require.Nil(err)

	ds, err := core.NewDataStore(dir, f, f, core.NewMemStore([][]byte{core.ChunkStat}), core.NewMemStore([][]byte{core.ChildNodeBucket, core.NodeBucket}))
	require.Nil(err)
	ds.SetClients(f)

	inode, err := ds.AddRemoteGCS(ctx, core.RootINode, "gcs", bucketName, "sample")
	r, err := ds.GetReadRef(ctx, inode)
	require.Nil(err)

	b := make([]byte, 100)
	n, err := r.Read(ctx, b)
	require.Nil(err)
	require.Equal(5, n)
	require.Equal("hello", string(b[:n]))

	// test child directories
	inode, err = ds.AddRemoteGCS(ctx, core.RootINode, "gcs", bucketName, "test-data/")
	children, err := ds.GetDirContents(ctx, inode)
	require.Nil(err)
	require.Equal(6, len(children))
}

func newFullDataStore() *core.DataStore {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(ServiceFile))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := NewRemoteRefFactory(client, bucketName, "blocks/")
	dir, err := ioutil.TempDir("", "gcs_test")
	if err != nil {
		panic(err)
	}

	ds, err := core.NewDataStore(dir, f, f, core.NewMemStore([][]byte{core.ChunkStat}), core.NewMemStore([][]byte{core.ChildNodeBucket, core.NodeBucket}))
	if err != nil {
		panic(err)
	}
	ds.SetClients(f)

	return ds
}

func TestImportPublicData(t *testing.T) {
	needsServiceFile(t)

	var x *core.GCSObjectSource
	gob.Register(x)
	require := require.New(t)

	ds := newFullDataStore()
	ctx := context.Background()
	inode, err := ds.AddRemoteGCS(ctx, core.RootINode, "gcs", "gcp-public-data-sentinel-2", "")
	require.Nil(err)

	entries, err := ds.GetDirContents(ctx, inode)
	require.True(len(entries) > 3)

	fileInode, err := ds.GetNodeID(ctx, inode, "index.csv.gz")
	require.Nil(err)

	ref, err := ds.GetReadRef(ctx, fileInode)
	require.Nil(err)

	buffer := make([]byte, 100)
	offset, err := ref.Seek(-100, os.SEEK_END)
	require.True(offset > 0)
	require.Nil(err)

	n, err := ref.Read(ctx, buffer)
	require.Nil(err)
	require.Equal(100, n)

}

func TestMount(t *testing.T) {
	needsServiceFile(t)

	var x *core.GCSObjectSource
	gob.Register(x)
	require := require.New(t)
	ctx := context.Background()

	ds1 := newFullDataStore()
	ds2 := newFullDataStore()

	folderID, err := ds1.MakeDir(ctx, core.RootINode, "folder")
	require.Nil(err)

	_, w, err := ds1.CreateWritable(ctx, folderID, "file")
	require.Nil(err)

	body := generateUniqueString()
	_, err = w.Write([]byte(body))
	require.Nil(err)
	w.Release()

	err = ds1.Push(ctx, core.RootINode, "test-mount")
	require.Nil(err)

	err = ds2.MountByLabel(ctx, core.RootINode, "mounted", "test-mount")
	require.Nil(err)

	// TODO: fix here

	folderID2, err := ds2.GetNodeID(ctx, core.RootINode, "folder")
	require.Nil(err)

	fileID2, err := ds2.GetNodeID(ctx, folderID2, "file")
	require.Nil(err)

	r, err := ds2.GetReadRef(ctx, fileID2)
	require.Nil(err)

	buffer, err := ioutil.ReadAll(&core.FrozenReader{ctx, r})
	require.Nil(err)

	require.Equal(body, string(buffer))
}
