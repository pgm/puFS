package sply2

import (
	"bytes"
	"io"
	"log"
	"testing"
	"time"

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

type MockReadable struct {
	value string
}

func (m *MockReadable) Read(offset int64, dest []byte) (int, error) {
	end := int(offset) + len(dest)
	if end > len(m.value) {
		end = len(m.value)
	}
	copy(dest, m.value[int(offset):end])
	copiedLen := end - int(offset)
	if copiedLen == 0 {
		return 0, io.EOF
	}
	return copiedLen, nil
}

func (m *MockReadable) Release() {

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

	BID := BlockID{1}
	f := NewRemoteRefFactory(client, BucketName, "test/")
	err := f.Push(BID, &MockReadable{body})

	r, err := o.NewReader(ctx)
	require.Nil(err)

	buffer := make([]byte, len(body))
	_, err = r.Read(buffer)
	require.Nil(err)
	require.Equal(body, string(buffer))

	bb := bytes.NewBuffer(make([]byte, 0, 100))
	ref, err := f.GetRef(&NodeRepr{BID: BID, Size: int64(len(body))})
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
