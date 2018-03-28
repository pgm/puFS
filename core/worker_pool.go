package core

import "context"

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

func (p *Pool) mainLoop() {
	for {
		req, ok := <-p.queue
		if !ok {
			break
		}
		// process req
		close(req.DoneChan)
	}
}

// create workers which sit in loop trying to read from pool or done channel
func (p *Pool) AddWorker() {
	go p.mainLoop()
}
