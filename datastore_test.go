package sply2

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func createFile(require *require.Assertions, d *DataStore, parent INode, name string, content string) INode {
	aID, w, err := d.CreateWritable(parent, name)
	require.Nil(err)
	_, err = w.Write(0, []byte(content))
	require.Nil(err)
	w.Release()
	return aID
}

func testDataStore() *DataStore {
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	ds := NewDataStore(dir)
	return ds
}

func TestPersistence(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	ds1 := NewDataStore(dir)
	aID := createFile(require, ds1, RootINode, "a", "data")
	ds1.Close()

	ds2 := NewDataStore(dir)

	r, err := ds2.GetReadRef(aID)
	require.Nil(err)

	buffer := make([]byte, 4)
	_, err = r.Read(0, buffer)
	require.Nil(err)

	r.Release()

	require.Equal("data", string(buffer))
}

func TestWriteRead(t *testing.T) {
	require := require.New(t)

	d := testDataStore()

	aID := createFile(require, d, RootINode, "a", "data")

	r, err := d.GetReadRef(aID)
	require.Nil(err)

	buffer := make([]byte, 4)
	_, err = r.Read(0, buffer)
	require.Nil(err)

	r.Release()

	require.Equal("data", string(buffer))
}

func TestRootDirListing(t *testing.T) {
	require := require.New(t)
	d := testDataStore()

	createFile(require, d, RootINode, "a", "data")
	names, err := d.GetDirContents(RootINode)
	require.Nil(err)
	require.Equal(1, len(names))
	require.Equal("a", names[0])
}

func TestSubDirListing(t *testing.T) {
	require := require.New(t)
	d := testDataStore()

	aID, err := d.MakeDir(RootINode, "a")
	require.Nil(err)

	createFile(require, d, aID, "b", "data")

	names, err := d.GetDirContents(RootINode)
	require.Nil(err)
	require.Equal(1, len(names))
	require.Equal("a", names[0])

	names, err = d.GetDirContents(aID)
	require.Nil(err)
	require.Equal(1, len(names))
	require.Equal("b", names[0])
}

// func TestMkdirErrors(t *testing.T) {
// 	require := require.New(t)
// 	d := testDataStore()

// 	_, err := d.MakeDir(RootINode, "a")
// 	require.Nil(err)

// 	_, err = d.MakeDir(RootINode, "a")
// 	require.Equal(NoSuchNodeErr, err)

// 	_, err = d.MakeDir(100, "a")
// 	require.Equal(ParentMissingErr, err)
// }

func TestRmdir(t *testing.T) {
	require := require.New(t)
	d := testDataStore()

	aID, err := d.MakeDir(RootINode, "a")
	require.Nil(err)

	_, err = d.MakeDir(aID, "b")
	require.Nil(err)

	err = d.Remove(RootINode, "a")
	require.Equal(DirNotEmptyErr, err)

	err = d.Remove(aID, "b")
	require.Nil(err)

	err = d.Remove(RootINode, "a")
	require.Nil(err)
}

// func TestCreateFileErrors(t *testing.T) {
// 	require := require.New(t)
// 	d := testDataStore()

// 	_, err := d.MakeDir(RootINode, "a")
// 	require.Nil(err)

// 	_, _, err = d.CreateWritable(RootINode, "a")
// 	require.Equal(ExistsErr, err)

// 	_, _, err = d.CreateWritable(100, "a")
// 	require.Equal(NoSuchNodeErr, err)
// }

// func TestRemote(t *testing.T) {
// 	url := "https://developer.mozilla.org/en-US/"

// 	require := require.New(t)
// 	d := testDataStore()

// 	fileID, err := d.AddRemoteURL(RootINode, "remote", url)
// 	require.Nil(err)

// 	_, r, err := d.GetReadRef(fileID)
// 	require.Nil(err)

// 	buffer := make([]byte, 500)
// 	_, err = r.Read(0, buffer)
// 	require.Nil(err)

// 	r.Release()

// 	fmt.Printf("%s", string(buffer))
// }
