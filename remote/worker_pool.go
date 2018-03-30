package remote

import (
	"context"
	"log"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2/core"
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
	serviceAccountFile string
	queue              chan interface{}
	client             *storage.Client
}

func NewGCSPool(serviceAccountFile string) *GCSPool {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(serviceAccountFile))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	return &GCSPool{serviceAccountFile: serviceAccountFile,
		queue:  make(chan interface{}, 10),
		client: client}
}

func (p *GCSPool) Pull(ctx context.Context, bucket string, key string, start int64, end int64, filename string) chan error {
	done := make(chan error)
	p.queue <- &CopyFromGCSRequest{Ctx: ctx, Bucket: bucket, Key: key, Start: start, End: end, Filename: filename, DoneChan: done}
	return done
}

// func (p *GCSPool) mainLoop() {
// 	for {
// 		req, ok := <-p.queue
// 		if !ok {
// 			break
// 		}

// 		switch req := req.(type) {
// 		default:
// 			panic("unknown type")

// 		case *CopyFromGCSRequest:
// 			err := copyRegion(req.Ctx, p.client, req.Bucket, req.Key, 0, req.Start, req.End-req.Start, req.Filename)
// 			req.DoneChan <- err

// 		case *GetChildNodesRequest:
// 			files, err := getChildNodes(req.Ctx, p.client, req.Bucket, req.Key)
// 			req.DoneChan <- &GetChildNodesResponse{Err: err, Files: files}
// 		}

// 		close(req.DoneChan)
// 	}
// }

// // create workers which sit in loop trying to read from pool or done channel
// func (p *GCSPool) AddWorker() {

// 	go p.mainLoop()
// }
