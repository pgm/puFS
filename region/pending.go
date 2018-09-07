package region

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
)

type PendingReads interface {
	// if there are regions being actively read that overlap, then set their minReadEnd to
	// cover as much of the region as possible and block until the reads compelete. If there
	// no such regions, then return immediately
	WaitForRegion(start int64, end int64)
	StartBackgroundCopy(marker Marker, copier Copier, lastMaxReadMark int64, start int64, end int64, minUncommited int64, maxWindowSize int64, writer io.Writer)
}

type PendingReadsImp struct {
	mutex     sync.Mutex
	flushCond *sync.Cond
	writers   []*MarkingWriter
}

func NewPendingReads() PendingReads {
	p := &PendingReadsImp{}
	p.flushCond = sync.NewCond(&p.mutex)
	return p
}

func (p *PendingReadsImp) WaitForRegion(start int64, end int64) {
	// todo: rewrite so that allReadsComplete checks for any overlapping reads. Return false if no pending reads overlap region.
	needed := make([]*MarkingWriter, 100)
	fmt.Printf("starting WaitForRegion\n")
	for {
		needed := needed[:0]
		p.mutex.Lock()
		for _, w := range p.writers {
			if w.overlaps(start, end) {
				fmt.Printf("Found overlap\n")
				needed = append(needed, w)
			}
		}

		// nothing pending overlaps our current request, so stop waiting
		if len(needed) == 0 {
			p.mutex.Unlock()
			fmt.Printf("Returning from WaitForRegion\n")
			return
		}

		// sleep until we get notification that more data has been flushed
		fmt.Printf("sleeping %d flushCond.Wait()\n", len(needed))
		p.flushCond.Wait()
		fmt.Printf("wake up\n")
	}
}

type Marker interface {
	GetMaxReadMark(n int64) int64
	GetNextRegionStart(n int64) int64
	AddRegion(start int64, end int64)
}

type MarkingWriter struct {
	owner           *PendingReadsImp
	active          bool
	cancelFunc      context.CancelFunc
	marker          Marker
	writer          io.Writer
	offset          int64
	lastMaxReadMark int64

	// how many bytes past lastMaxReadMark do we want to keep reading?
	maxWindowSize int64

	// the start of the current "pending" region
	lastPendingStart int64

	// the largest allowed end of the "pending" region
	maxPendingEnd int64

	// after this many bytes have been copied, update the region map
	minUncommited int64
}

type Copier interface {
	Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error
}

func removeElement(array []*MarkingWriter, element *MarkingWriter) []*MarkingWriter {
	for i := range array {
		if array[i] == element {
			copy(array[i:], array[i+1:])
			array[len(array)-1] = nil
			return array[:len(array)-1]
		}
	}
	return array
}

func (p *PendingReadsImp) StartBackgroundCopy(marker Marker, copier Copier, lastMaxReadMark int64, start int64, end int64, minUncommited int64, maxWindowSize int64, writer io.Writer) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	len := end - start

	if len <= 0 {
		log.Fatalf("len=%d", len)
	}

	markingWriter := &MarkingWriter{p, true, cancelFunc, marker, writer, start, lastMaxReadMark, maxWindowSize, start, end, minUncommited}
	p.mutex.Lock()
	p.writers = append(p.writers, markingWriter)
	p.mutex.Unlock()

	go executeThenCleanup(func() error {
		return copier.Copy(ctx, start, len, markingWriter)
	}, func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		p.writers = removeElement(p.writers, markingWriter)
	})
}

func executeThenCleanup(body func() error, cleanup func()) {
	err := body()
	if err != nil {
		log.Printf("Got error from background copy: %v", err)
	}
	cleanup()
}

func (m *MarkingWriter) overlaps(start int64, end int64) bool {
	if !m.active {
		return false
	}

	if m.lastPendingStart >= end || m.maxPendingEnd <= start {
		return false
	}

	return true
}

func (m *MarkingWriter) flush(nextRegionStart int64) {
	lastMaxReadMark := m.marker.GetMaxReadMark(m.lastMaxReadMark)

	m.owner.mutex.Lock()

	// Check to see if we've hit the next region
	if m.offset >= nextRegionStart {
		// if we have, then there's no point in keep reading. Abort
		m.cancelFunc()
	}

	// update the region map to include what we just finished reading
	newRegionStart := m.lastPendingStart
	newRegionEnd := m.offset
	m.lastPendingStart = m.offset

	// if we've read past the mininum required, check to see if we've read sufficiently past the end of the last read.
	// update lastMaxReadMark and check to see if we've read far enough past the last read
	m.lastMaxReadMark = lastMaxReadMark
	if m.offset >= m.lastMaxReadMark+m.maxWindowSize {
		// if we have more than maxWindowSize bytes read past the maxReadMark, stop copying
		m.cancelFunc()
	}

	m.owner.mutex.Unlock()

	// update the region map after we've exited the block protected by mutex (just in case AddRegion is either slow or does it's own locking)
	m.marker.AddRegion(newRegionStart, newRegionEnd)

	// notify flush after we've offically added the region
	m.owner.mutex.Lock()
	m.owner.flushCond.Broadcast()
	m.owner.mutex.Unlock()
}

func (m *MarkingWriter) Write(buffer []byte) (int, error) {
	n, err := m.writer.Write(buffer)
	if err != nil {
		return n, err
	}

	// lock and get a snapshot of the relevant marker state
	m.owner.mutex.Lock()
	m.offset += int64(n)
	offset := m.offset
	lastPendingStart := m.lastPendingStart
	minUncommited := m.minUncommited
	m.owner.mutex.Unlock()

	fmt.Printf("offset=%d, lastPendingStart=%d, minUncommited=%d\n", offset, lastPendingStart, minUncommited)
	if offset-lastPendingStart >= minUncommited {
		fmt.Printf("flushing..\n")
		nextRegionStart := m.marker.GetNextRegionStart(lastPendingStart)
		fmt.Printf("GetNextRegionStart finished")
		m.flush(nextRegionStart)
	}

	return n, nil
}
