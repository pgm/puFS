package core

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreezePush(t *testing.T) {
	require := require.New(t)

	content := generateUniqueString()

	dir1, err := ioutil.TempDir("", "test")
	require.Nil(err)

	f := NewRemoteRefFactoryMem()
	ds1 := NewDataStore(dir1, f)
	aID := createFile(require, ds1, RootINode, "a", content)
	err = ds1.Push(RootINode, "sample-label")
	require.Nil(err)
	ds1.Close()

	dir2, err := ioutil.TempDir("", "test")
	require.Nil(err)
	ds2 := NewDataStore(dir2, f)

	err = ds2.MountByLabel(RootINode, "sample-label")
	require.Nil(err)

	aID, err = ds2.GetNodeID(RootINode, "a")
	require.Nil(err)
	r, err := ds2.GetReadRef(aID)
	require.Nil(err)

	buffer, err := ioutil.ReadAll(r)
	require.Nil(err)

	require.Equal(content, string(buffer))
}
