package core

import (
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

type RemoteRefFactory2Mock struct {
	bytesRead int
}

type RemoteRefFactory2MockRef struct {
	owner *RemoteRefFactory2Mock
	id    string
}

func NewRemoteRefFactory2Mock() *RemoteRefFactory2Mock {
	return &RemoteRefFactory2Mock{}
}

func (rf *RemoteRefFactory2Mock) GetRef(source interface{}) RemoteRef {
	s := source.(string)
	return &RemoteRefFactory2MockRef{rf, s}
}

func (rr *RemoteRefFactory2MockRef) GetSize() int64 {
	return 2000
}

func (rr *RemoteRefFactory2MockRef) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	b := []byte(rr.id)[0]
	buffer := make([]byte, len)
	for i := 0; i < int(len); i++ {
		buffer[i] = b
	}
	writer.Write(buffer)
	rr.owner.bytesRead += int(len)
	return nil
}

func (rr *RemoteRefFactory2MockRef) GetSource() interface{} {
	return rr.id
}

// func (rf)

func TestPartialReads(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir("", "test")
	require.Nil(err)

	rf := NewRemoteRefFactory2Mock()

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
