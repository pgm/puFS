package core

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

type PullCountRefFactoryMock struct {
	bytesRead int
}

type PullCountRefFactoryMockRef struct {
	owner *PullCountRefFactoryMock
	id    string
}

func (rf *PullCountRefFactoryMock) GetRef(source interface{}) RemoteRef {
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

	case string:
		return &PullCountRefFactoryMockRef{rf, source}
		// case *URLSource:
		// 	return &URLRemoteRef{source.URL, source.ETag}
		// case *GCSObjectSource:
		// 	return &GCSRemoteRef{source.URL, source.ETag}
	}
}

func (rr *PullCountRefFactoryMockRef) GetSize() int64 {
	return 2000
}

func (rr *PullCountRefFactoryMockRef) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	b := []byte(rr.id)[0]
	buffer := make([]byte, len)
	for i := 0; i < int(len); i++ {
		buffer[i] = b
	}
	_, err := writer.Write(buffer)
	if err != nil {
		return err
	}
	rr.owner.bytesRead += int(len)
	return nil
}

func (rr *PullCountRefFactoryMockRef) GetSource() interface{} {
	return rr.id
}

func (rf *PullCountRefFactoryMockRef) GetChildNodes(ctx context.Context) ([]*RemoteFile, error) {
	panic("unimp")
}

func TestPersistedPartialReads(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "test")
	require.Nil(err)

	rf := &PullCountRefFactoryMock{}

	kv := NewMemStore([][]byte{ChunkStat})
	f := NewFreezer(dir, kv, rf, 2)
	BID := BlockID{2}

	rr := rf.GetRef("x")
	ctx := context.Background()

	err = f.AddBlock(ctx, BID, rr)
	require.Nil(err)

	fr, err := f.GetRef(BID)
	require.Nil(err)

	_, err = fr.Seek(10, 0)
	require.Nil(err)

	dest := make([]byte, 3)
	fr.Read(ctx, dest)

	// confirm we read data from the remote
	require.Equal(4, rf.bytesRead)
	rf.bytesRead = 0

	// make a new freezer
	f2 := NewFreezer(dir, kv, rf, 2)

	fr, err = f2.GetRef(BID)
	require.Nil(err)

	_, err = fr.Seek(10, 0)
	require.Nil(err)

	dest = make([]byte, 3)
	fr.Read(ctx, dest)

	// confirm we didn't need to read any additional data
	require.Equal(0, rf.bytesRead)
}

func TestPartialReads(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "test")
	require.Nil(err)

	rf := &PullCountRefFactoryMock{}

	f := NewFreezer(dir, NewMemStore([][]byte{ChunkStat}), rf, 2)
	BID := BlockID{2}

	rr := rf.GetRef("x")
	ctx := context.Background()

	err = f.AddBlock(ctx, BID, rr)
	require.Nil(err)

	fr, err := f.GetRef(BID)
	require.Nil(err)

	_, err = fr.Seek(10, 0)
	require.Nil(err)

	dest := make([]byte, 3)
	fr.Read(ctx, dest)

	// confirm we rounded up to an even number of chunks
	require.Equal(4, rf.bytesRead)

	// and confirm the data was correctly written
	require.Equal([]byte{'x', 'x', 'x'}, dest)

	_, err = fr.Seek(12, 0)
	require.Nil(err)

	dest = make([]byte, 4)
	fr.Read(ctx, dest)
	// confirm we only needed to read two more bytes
	require.Equal(6, rf.bytesRead)

	require.Equal([]byte{'x', 'x', 'x', 'x'}, dest)
}
