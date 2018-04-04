package core

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func createFile(require *require.Assertions, d *DataStore, parent INode, name string, content string) INode {
	ctx := context.Background()

	aID, w, err := d.CreateWritable(ctx, parent, name)
	// d.db.db.View(func(tx RTx) error {
	// 	fmt.Println("Right after CreateWrtiable")
	// 	printDbStats(tx)
	// 	return nil
	// })
	require.Nil(err)
	_, err = w.Write([]byte(content))
	require.Nil(err)
	w.Release()
	return aID
}

func newDataStore(dir string) *DataStore {
	repo := NewRemoteRefFactoryMem()
	rrf2 := NewMemRemoteRefFactory2(repo)
	return NewDataStore(dir, repo, rrf2, NewMemStore([][]byte{ChunkStat}), NewMemStore([][]byte{ChildNodeBucket, NodeBucket}))
}

func testDataStore() *DataStore {
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	return newDataStore(dir)
}

func TestPersistence(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	freezerStore := NewMemStore([][]byte{ChunkStat})
	nodeStore := NewMemStore([][]byte{ChildNodeBucket, NodeBucket})
	ds1 := NewDataStore(dir, nil, nil, freezerStore, nodeStore)
	aID := createFile(require, ds1, RootINode, "a", "data")
	ds1.Close()

	ds2 := NewDataStore(dir, nil, nil, freezerStore, nodeStore)

	r, err := ds2.GetReadRef(ctx, aID)
	require.Nil(err)

	buffer := make([]byte, 4)
	_, err = r.Read(ctx, buffer)
	require.Nil(err)

	require.Equal("data", string(buffer))
}

func TestRename(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	d := testDataStore()

	createFile(require, d, RootINode, "a", "data")
	d.PrintDebug()
	fmt.Println("-----")
	err := d.Rename(ctx, RootINode, "a", RootINode, "b")
	require.Nil(err)
	d.PrintDebug()

	_, err = d.GetNodeID(ctx, RootINode, "a")
	require.Equal(NoSuchNodeErr, err)
	_, err = d.GetNodeID(ctx, RootINode, "b")
	require.Nil(err)
}

func TestMultipleWrite(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	d := testDataStore()

	aID := createFile(require, d, RootINode, "a", "data")

	r, err := d.GetReadRef(ctx, aID)
	require.Nil(err)

	buffer := make([]byte, 100)
	n, err := r.Read(ctx, buffer)
	require.Nil(err)
	require.Equal("data", string(buffer[:n]))

	// now update the file
	w, err := d.GetWritableRef(ctx, aID, false)
	offset, err := w.Seek(2, os.SEEK_SET)
	require.Nil(err)
	require.Equal(int64(2), offset)

	w.Write([]byte("TA..."))
	offset, err = w.Seek(0, os.SEEK_SET)
	require.Nil(err)
	require.Equal(int64(0), offset)

	// read it back with writable handle
	n, err = w.Read(ctx, buffer)
	require.Nil(err)
	require.Equal("daTA...", string(buffer[:n]))

	// get a reader and read back
	r, err = d.GetReadRef(ctx, aID)
	require.Nil(err)
	n, err = r.Read(ctx, buffer)
	require.Nil(err)
	require.Equal("daTA...", string(buffer[:n]))

	// test truncate
	w, err = d.GetWritableRef(ctx, aID, true)
	w.Write([]byte("da"))

	_, err = r.Seek(0, os.SEEK_SET)
	require.Nil(err)
	n, err = r.Read(ctx, buffer)
	require.Nil(err)
	require.Equal("da", string(buffer[:n]))

}

func TestWriteRead(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	d := testDataStore()

	aID := createFile(require, d, RootINode, "a", "data")

	r, err := d.GetReadRef(ctx, aID)
	require.Nil(err)

	buffer := make([]byte, 4)
	_, err = r.Read(ctx, buffer)
	require.Nil(err)

	require.Equal("data", string(buffer))
}

func TestFreezeFile(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	d := testDataStore()

	aID := createFile(require, d, RootINode, "a", "data")

	BID, err := d.Freeze(aID)
	require.Nil(err)

	r, err := d.GetReadRef(ctx, aID)
	require.Nil(err)

	buffer := make([]byte, 4)
	_, err = r.Read(ctx, buffer)
	require.Nil(err)

	require.Equal("data", string(buffer))

	// now verify this is the same thing that's in the freezer
	r, err = d.freezer.GetRef(BID)
	require.Nil(err)
	_, err = r.Read(ctx, buffer)
	require.Equal("data", string(buffer))
}

func TestFreezeDir(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	d := testDataStore()

	aID, err := d.MakeDir(ctx, RootINode, "a")
	require.Nil(err)

	aBID1, err := d.Freeze(aID)
	require.Nil(err)

	// calling freeze twice should yield same blockID
	aBID2, err := d.Freeze(aID)
	require.Nil(err)
	require.Equal(aBID1, aBID2)

	// now mutate a, which should clear the blockID
	_, err = d.MakeDir(ctx, aID, "b")
	require.Nil(err)

	// and freezing should yield a different id
	aBID3, err := d.Freeze(aID)
	require.Nil(err)
	require.NotEqual(aBID1, aBID3)
}

func extractNames(entries []*DirEntryWithID) []string {
	names := make([]string, 0, 100)
	for _, c := range entries {
		names = append(names, c.Name)
	}
	return names
}

func TestRootDirListing(t *testing.T) {
	require := require.New(t)
	d := testDataStore()
	ctx := context.Background()

	createFile(require, d, RootINode, "a", "data")
	contents, err := d.GetDirContents(ctx, RootINode)
	require.Nil(err)
	require.Equal([]string{".", "..", "a"}, extractNames(contents))
}

func TestSubDirListing(t *testing.T) {
	require := require.New(t)
	d := testDataStore()
	ctx := context.Background()

	aID, err := d.MakeDir(ctx, RootINode, "a")
	require.Nil(err)

	createFile(require, d, aID, "b", "data")

	contents, err := d.GetDirContents(ctx, RootINode)
	require.Nil(err)
	require.Equal([]string{".", "..", "a"}, extractNames(contents))

	contents, err = d.GetDirContents(ctx, aID)
	require.Nil(err)
	require.Equal([]string{".", "..", "b"}, extractNames(contents))
}

func TestMkdirErrors(t *testing.T) {
	require := require.New(t)
	d := testDataStore()
	ctx := context.Background()

	_, err := d.MakeDir(ctx, RootINode, "a")
	require.Nil(err)

	_, err = d.MakeDir(ctx, RootINode, "a")
	require.Equal(ExistsErr, err)

	_, err = d.MakeDir(ctx, 100, "a")
	require.Equal(NoSuchNodeErr, err)
}

func TestRmdir(t *testing.T) {
	require := require.New(t)
	d := testDataStore()
	ctx := context.Background()

	aID, err := d.MakeDir(ctx, RootINode, "a")
	require.Nil(err)

	_, err = d.MakeDir(ctx, aID, "b")
	require.Nil(err)

	err = d.Remove(ctx, RootINode, "a")
	require.Equal(DirNotEmptyErr, err)

	err = d.Remove(ctx, aID, "b")
	require.Nil(err)

	err = d.Remove(ctx, RootINode, "a")
	require.Nil(err)
}

func TestCreateFileErrors(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)
	d := testDataStore()

	_, err := d.MakeDir(ctx, RootINode, "a")
	require.Nil(err)

	_, _, err = d.CreateWritable(ctx, RootINode, "a")
	require.Equal(ExistsErr, err)

	_, _, err = d.CreateWritable(ctx, 100, "a")
	require.Equal(NoSuchNodeErr, err)
}

type NetworkClientImp struct {
}

func (n *NetworkClientImp) GetGCSAttr(ctx context.Context, bucket string, key string) (*GCSAttrs, error) {
	panic("unimp")
}

func (n *NetworkClientImp) GetHTTPAttr(ctx context.Context, url string) (*HTTPAttrs, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}

	return &HTTPAttrs{ETag: resp.Header.Get("etag"), Size: resp.ContentLength}, nil
}
