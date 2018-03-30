package remote

import (
	"context"
	"fmt"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2/core"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type CopyFromGCSRequest struct {
	Ctx      context.Context
	Bucket   string
	Key      string
	Start    int64
	End      int64
	Filename string
	DoneChan chan error
}

type GetChildNodesRequest struct {
	Bucket string
	Key    string
}

type GetChildNodesResponse struct {
	Files []*core.RemoteFile
	Err   error
}

type GCSPool struct {
	serviceAccountFile string // "/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"
	queue              chan interface{}
	client *storage.Client
}

func NewGCSPool(serviceAccountFile string) *GCSPool {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(p.serviceAccountFile))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return &GCSPool{serviceAccountFile: serviceAccountFile,
		queue: make(chan interface{}, 10),
	client: client}
}

func (p *GCSPool) Pull(ctx context.Context, bucket string, key string, start int64, end int64, filename string) chan error {
	done := make(chan error)
	p.queue <- &CopyFromGCSRequest{Ctx: ctx, Bucket: bucket, Key: key, Start: start, End: end, Filename: filename, DoneChan: done}
	return done
}

func getChildNodes(ctx context.Context, GCSClient *storage.Client, Bucket string, Key string) ([]*core.RemoteFile, error) {
	b := GCSClient.Bucket(Bucket)
	it := b.Objects(ctx, &storage.Query{Delimiter: "/", Prefix: Key, Versions: false})
	result := make([]*core.RemoteFile, 0, 100)
	for {
		next, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var file *core.RemoteFile
		if next.Prefix != "" {
			// if we really want mtime we could get the mtime of the prefix because it's usually an empty object via
			// an explicit attr fetch of the prefix
			file = &core.RemoteFile{Name: next.Prefix[len(Key) : len(next.Prefix)-1],
				IsDir:  true,
				Bucket: Bucket,
				Key:    next.Prefix}
		} else {
			name := next.Name[len(Key):]
			if name == "" {
				continue
			}
			file = &core.RemoteFile{Name: name,
				IsDir:      false,
				Size:       next.Size,
				ModTime:    next.Updated,
				Bucket:     Bucket,
				Key:        next.Name,
				Generation: next.Generation}
		}

		result = append(result, file)
	}

	return result, nil
}

func copyRegion(ctx context.Context, GCSClient *storage.Client, Bucket string, Key string, Generation int64, offset int64, len int64, writer io.Writer) error {
	b := GCSClient.Bucket(Bucket)
	objHandle := b.Object(Key)
	if Generation != 0 {
		objHandle = objHandle.If(storage.Conditions{GenerationMatch: Generation})
	}

	var reader io.ReadCloser
	var err error
	// if offset != 0 || len != r.Size {
	reader, err = objHandle.NewRangeReader(ctx, offset, len)
	// } else {
	// 	reader, err = objHandle.NewReader(ctx)
	// }
	if err != nil {
		return err
	}
	defer reader.Close()

	n, err := io.Copy(writer, reader)
	if err != nil {
		return err
	}

	if len >= 0 && n != len {
		return fmt.Errorf("Expected to copy to copy %d bytes but copied %d", len, n)
	}

	return nil
}

func (p *GCSPool) mainLoop() {
	for {
		req, ok := <-p.queue
		if !ok {
			break
		}

		switch req := req.(type) {
		default:
			panic("unknown type")

		case *CopyFromGCSRequest:
			err := copyRegion(req.Ctx, p.client, req.Bucket, req.Key, 0, req.Start, req.End-req.Start, req.Filename)
			req.DoneChan <- err

		case *GetChildNodesRequest:
			files, err := getChildNodes(req.Ctx, p.client, req.Bucket, req.Key)
			req.DoneChan <- &GetChildNodesResponse{Err: err, Files: files}
		}

		close(req.DoneChan)
	}
}

// create workers which sit in loop trying to read from pool or done channel
func (p *GCSPool) AddWorker() {

	go p.mainLoop()
}
