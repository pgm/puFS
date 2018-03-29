package core

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

type PullRequest struct {
	Ctx      context.Context
	Bucket   string
	Key      string
	Start    int64
	End      int64
	Filename string
	DoneChan chan error
}

type Pool struct {
	queue chan *PullRequest
}

func (p *Pool) Pull(ctx context.Context, bucket string, key string, start int64, end int64, filename string) chan error {
	done := make(chan error)
	p.queue <- &PullRequest{Ctx: ctx, Bucket: bucket, Key: key, Start: start, End: end, Filename: filename, DoneChan: done}
	return done
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

func (p *Pool) mainLoop(GCSClient *storage.Client) {
	for {
		req, ok := <-p.queue
		if !ok {
			break
		}

		err := copyRegion()
		req.DoneChan <- err
		close(req.DoneChan)
	}
}

// create workers which sit in loop trying to read from pool or done channel
func (p *Pool) AddWorker() {
	go p.mainLoop()
}
