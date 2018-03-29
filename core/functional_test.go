package core

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreezePush(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	content := generateUniqueString()

	dir1, err := ioutil.TempDir("", "test")
	require.Nil(err)

	f := NewRemoteRefFactoryMem()
	ds1 := NewDataStore(dir1, f, &RemoteRefFactory2Mock{}, NewMemStore([][]byte{ChunkStat}), NewMemStore([][]byte{ChildNodeBucket, NodeBucket}))

	aID := createFile(require, ds1, RootINode, "a", content)
	err = ds1.Push(ctx, RootINode, "sample-label")
	require.Nil(err)
	ds1.Close()

	dir2, err := ioutil.TempDir("", "test")
	require.Nil(err)
	ds2 := NewDataStore(dir2, f, &RemoteRefFactory2Mock{}, NewMemStore([][]byte{ChunkStat}), NewMemStore([][]byte{ChildNodeBucket, NodeBucket}))

	err = ds2.MountByLabel(ctx, RootINode, "sample-label")
	require.Nil(err)

	aID, err = ds2.GetNodeID(ctx, RootINode, "a")
	require.Nil(err)
	r, err := ds2.GetReadRef(ctx, aID)
	require.Nil(err)

	buffer, err := ioutil.ReadAll(&FrozenReader{ctx, r})
	require.Nil(err)

	require.Equal(content, string(buffer))
}
