package region

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
)

type CallStatus interface {
	Wait() error
	IsRunning() bool
}

type PendingReads interface {
	// if there are regions being actively read that overlap, then set their minReadEnd to
	// cover as much of the region as possible and block until the reads compelete. If there
	// no such regions, then return immediately
	WaitForRegion(start int64, end int64)
	StartBackgroundCopy(ctx context.Context, marker Marker, copier Copier, start int64, end int64, maxEnd int64, minUncommited int64, maxWindowSize int64, writer io.Writer) CallStatus
	GetStatus(timeUnit time.Duration) []*PendingReadsStatus
}

type callStatus struct {
	waitCount  *int32
	complete   bool
	err        error
	resultChan <-chan error
}

func (c *callStatus) Wait() error {
	atomic.AddInt32(c.waitCount, 1)
	if !c.complete {
		c.err = <-c.resultChan
		c.complete = true
		atomic.AddInt32(c.waitCount, -1)
	}
	return c.err
}

func (c *callStatus) IsRunning() bool {
	if c.complete {
		return false
	}

	select {
	case err := <-c.resultChan:
		c.err = err
		c.complete = true
		return false
	default:
		return true
	}
}

type PendingReadsImp struct {
	mutex     sync.Mutex
	flushCond *sync.Cond
	writers   []*MarkingWriter
	waitCount int32
}

type PendingReadsStatus struct {
	StartTime     time.Time
	Start         int64
	PendingStart  int64
	PendingEnd    int64
	Offset        int64
	MaxPendingEnd int64
	TransferRate  float32
}

func NewPendingReads() PendingReads {
	p := &PendingReadsImp{}
	p.flushCond = sync.NewCond(&p.mutex)
	return p
}

func (p *PendingReadsImp) GetStatus(timeUnit time.Duration) []*PendingReadsStatus {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	log.Printf("GetStatus: waitCount=%d", p.waitCount)

	status := make([]*PendingReadsStatus, len(p.writers))
	for i, w := range p.writers {
		s := &PendingReadsStatus{
			StartTime:     w.startTime,
			Start:         w.originalStart,
			PendingStart:  w.pendingStart,
			PendingEnd:    w.pendingEnd,
			Offset:        w.offset,
			MaxPendingEnd: w.maxPendingEnd,
			TransferRate:  w.offsetHistory.GetRate(timeUnit)}
		status[i] = s
	}

	return status
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
	GetFirstMissingRegion(start int64, end int64) *Region
	AddRegion(start int64, end int64)
}

type waitingCaller struct {
	active     bool
	end        int64
	resultChan chan<- error
}

type MarkingWriter struct {
	owner         *PendingReadsImp
	startTime     time.Time
	originalStart int64
	active        bool
	cancelFunc    context.CancelFunc
	marker        Marker
	writer        io.Writer
	offset        int64

	// how many bytes past lastMaxReadMark do we want to keep reading?
	readheadSize int64

	// the start of the current "pending" region. This is computed as the min(max of all requested regions + readheadSize, maxPendingEnd )
	pendingStart int64
	pendingEnd   int64

	// the largest allowed end of the "pending" region. This is initialized upon construction and what is passed
	// to copy, so we cannot update this.
	maxPendingEnd int64

	// after this many bytes have been copied, update the region map
	minUncommited int64

	callers []*waitingCaller

	offsetHistory OffsetHistory
}

type OffsetTimepoint struct {
	offset    int64
	timestamp time.Time
}

type OffsetHistory struct {
	// circular buffer
	history    [256]OffsetTimepoint
	startIndex int
	endIndex   int
}

func (h *OffsetHistory) next(i int) int {
	i++
	if i >= len(h.history) {
		return 0
	}
	return i
}
func (h *OffsetHistory) prev(i int) int {
	i--
	if i < 0 {
		return len(h.history) - 1
	}
	return i
}

func (h *OffsetHistory) Record(offset int64) {
	nextIndex := h.next(h.endIndex)

	// if we are in danger of wrapping around bump start pos
	if nextIndex == h.startIndex {
		h.startIndex = h.next(h.startIndex)
	}

	h.history[nextIndex] = OffsetTimepoint{offset: offset, timestamp: time.Now()}
	h.endIndex = nextIndex
}

func (h *OffsetHistory) GetRate(divisor time.Duration) float32 {
	if h.startIndex == h.endIndex {
		return float32(math.NaN())
	}

	start := h.history[h.startIndex]
	end := h.history[h.prev(h.endIndex)]

	return float32(end.offset-start.offset) / float32(end.timestamp.Sub(start.timestamp)) / float32(divisor)
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

func (m *MarkingWriter) updatedPendingEnd() {
	if len(m.callers) > 0 {
		maxEnd := int64(0)
		for _, c := range m.callers {
			if maxEnd < c.end {
				maxEnd = c.end
			}
		}
		m.pendingEnd = (maxEnd + m.readheadSize)
		if m.maxPendingEnd < m.pendingEnd {
			m.pendingEnd = m.maxPendingEnd
		}
		m.validate()
	}
}

func (m *MarkingWriter) addCaller(ctx context.Context, caller *waitingCaller) {
	m.callers = append(m.callers, caller)
	go (func() {
		// if we get a cancelation, make sure this gets removed as a blocking caller
		<-ctx.Done()
		m.owner.mutex.Lock()
		defer m.owner.mutex.Unlock()
		caller.active = false
		m.removeInactive()
	})()
}

func (p *PendingReadsImp) StartBackgroundCopy(rootCtx context.Context, marker Marker, copier Copier, start int64, end int64, maxEnd int64, minUncommited int64, readheadSize int64, writer io.Writer) CallStatus {
	length := maxEnd - start

	if length <= 0 {
		log.Fatalf("len=%d", length)
	}

	requiredResultChan := make(chan error, 1)

	caller := &waitingCaller{active: true, end: end, resultChan: requiredResultChan}

	p.mutex.Lock()
	// look to see if there's a copy in progress that we can join
	joined := false
	for _, w := range p.writers {
		// if there's an existing (w) read where pendingStart falls within w.pendingStart and w.pendingEnd
		// then we just want to join this request
		if start >= w.pendingStart && start <= w.pendingEnd {
			if !w.active {
				panic("checked an inactive writer")
			}
			if !joined {
				// log.Printf("Read (%d-%d) joining existing pending read: %p (%d-%d)", start, end, w, w.pendingStart, w.pendingEnd)
				w.addCaller(rootCtx, caller)
				w.updatedPendingEnd()
				joined = true
			} else {
				log.Printf("Warning: Read (%d-%d) could have also joined: %v. Check for race condition?", start, end, w)
			}
		} else {
			log.Printf("Could not join existing start=%d pendingStart=%d pendingEnd=%d", start, w.pendingStart, w.pendingEnd)
		}
	}

	var task *trace.Task
	var taskCtx context.Context
	var markingWriter *MarkingWriter
	if !joined {
		cancelableCtx, cancelFunc := context.WithCancel(context.Background())
		taskCtx, task = trace.NewTask(cancelableCtx, "MarkingCopy")
		markingWriter = &MarkingWriter{owner: p,
			active:        true,
			cancelFunc:    cancelFunc,
			marker:        marker,
			writer:        writer,
			offset:        start,
			originalStart: start,
			startTime:     time.Now(),
			pendingStart:  start,
			pendingEnd:    end,
			readheadSize:  readheadSize,
			maxPendingEnd: maxEnd,
			minUncommited: minUncommited,
			callers:       make([]*waitingCaller, 0, 20)}
		markingWriter.validate()

		markingWriter.addCaller(rootCtx, caller)
		p.writers = append(p.writers, markingWriter)
	}

	p.mutex.Unlock()

	if !joined {
		go executeThenCleanup(func() error {
			err := copier.Copy(taskCtx, start, length, markingWriter)
			log.Printf("copier.Copy returned err: %v", err)
			// if err == context.Canceled {
			// 	missing := markingWriter.marker.GetFirstMissingRegion(start, length)
			// 	if missing == nil {
			// 		log.Printf("copier was aborted, but nothing is missing, so no problem")
			// 		err = nil
			// 	}
			// }
			p.mutex.Lock()
			defer p.mutex.Unlock()
			for _, c := range markingWriter.callers {
				// notify caller of error
				c.resultChan <- err
				close(c.resultChan)
				c.active = false
			}
			markingWriter.removeInactive()
			return err
		}, func() {
			p.mutex.Lock()
			defer p.mutex.Unlock()
			p.writers = removeElement(p.writers, markingWriter)
			task.End()
		})

	}

	return &callStatus{resultChan: requiredResultChan, waitCount: &p.waitCount}
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

	if m.pendingStart >= end || m.maxPendingEnd <= start {
		return false
	}

	return true
}

func removeInactive(callers []*waitingCaller) []*waitingCaller {
	//log.Printf("removingInactives")
	// remove inactives for callers list
	di := 0
	for si, c := range callers {
		if di != si {
			callers[di] = callers[si]
		}
		if c.active {
			di++
		}
	}

	// nil out pointers outside of bounds
	for i := di; i < len(callers); i++ {
		callers[i] = nil
	}

	// shorten slice
	return callers[:di]
}

func (m *MarkingWriter) validate() {
	if m.pendingEnd < m.pendingStart {
		panic(fmt.Sprintf("pendingStart=%d pendingEnd=%d", m.pendingStart, m.pendingEnd))
	}
}

func (m *MarkingWriter) removeInactive() {
	m.callers = removeInactive(m.callers)
}

func (m *MarkingWriter) flush(abortOnceMinReached bool, abortImmediately bool) {
	m.owner.mutex.Lock()

	// update the region map to include what we just finished reading
	newRegionStart := m.pendingStart
	newRegionEnd := m.offset
	m.pendingStart = m.offset

	// if we've read past the mininum required, check to see if we've read sufficiently past the end of the last read.
	// update pendingEnd and check to see if we've read far enough past the last read
	if len(m.callers) == 0 {
		if abortOnceMinReached {
			m.pendingEnd = m.offset
			log.Printf("updating pendingEnd to trigger cancel")
			m.validate()
		}

		if m.offset >= m.pendingEnd {
			// if we have more than maxWindowSize bytes read past the maxReadMark, stop copying
			log.Printf("Canceling we've execeed our readahead (offset=%d, readheadSize=%d)", m.offset, m.readheadSize)
			m.cancelFunc()
		}
	} else {
		inactive := 0
		// for each caller, see if we can wake it up and signal it's done.
		for _, c := range m.callers {
			if m.offset >= c.end || abortImmediately {
				c.resultChan <- nil
				close(c.resultChan)
				c.active = false
				inactive++
			}
		}

		if inactive > 0 {
			m.removeInactive()
		}

		m.updatedPendingEnd()
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
	pendingStart := m.pendingStart
	pendingEnd := m.pendingEnd
	minUncommited := m.minUncommited
	m.offsetHistory.Record(offset)
	m.owner.mutex.Unlock()

	//log.Printf("Wrote: offset=%d, pendingStart=%d, pendingEnd=%d, minUncommited=%d", offset, pendingStart, pendingEnd, minUncommited)
	if offset-pendingStart >= minUncommited || offset >= pendingEnd {
		missing := m.marker.GetFirstMissingRegion(pendingStart, m.maxPendingEnd)

		abortOnceMinReached := false
		abortImmediately := false
		if missing == nil {
			// there's nothing left to do, so abort
			abortImmediately = true
		} else {
			// we've started copying onto a region that another copy task populated.
			abortOnceMinReached = true
		}

		//log.Printf("flushing: maxReadPosition=%d, nextRegionStart=%d, abortOnceMinReached=%d", stats.NextRegionStart, abortOnceMinReached)
		m.flush(abortOnceMinReached, abortImmediately)
	}

	return n, nil
}
