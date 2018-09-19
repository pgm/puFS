package region

import (
	"bytes"
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitWithTimeout(wg *sync.WaitGroup) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return // completed normally
	case <-time.After(time.Second):
		panic("Wait timed out")
	}
}

type History struct {
	mutex        sync.Mutex
	calls        []*Call
	handler      CallHandler
	expectations []*Expectation
}

type Expectation struct {
	expectedCount int
	callCount     int
	name          string
	block         bool
	returnValue   interface{}
	calls         chan *Call
}

type Call struct {
	parameters    interface{}
	waitForReturn sync.WaitGroup
	returnValue   interface{}
}

// type ExpectationCall struct {
// 	calls <-chan *Call
// }

func (h *History) Expect(name string) *Expectation {
	e := &Expectation{name: name, expectedCount: 1000000, block: true, calls: make(chan *Call, 100)}
	h.expectations = append(h.expectations, e)
	return e
}

func (e *Expectation) WaitForCall() *Call {
	select {
	case call := <-e.calls:
		return call
	case <-time.After(1 * time.Second):
		panic("timed out waiting for call")
	}
}

func (e *Expectation) AndThenReturn(returnValue interface{}) *Expectation {
	e.returnValue = returnValue
	e.block = false
	return e
}

func (e *Expectation) NTimes(n int) *Expectation {
	e.expectedCount = n
	return e
}

func (c *Call) Return(returnValue interface{}) {
	c.returnValue = returnValue
	c.waitForReturn.Done()
}

type CallHandler func(name string, parameters interface{}) (bool, interface{})

func (h *History) RegisterHandler(handler CallHandler) {
	h.handler = handler
}

func (h *History) log(name string, parameters interface{}) interface{} {
	for _, expectation := range h.expectations {
		if name == expectation.name && expectation.expectedCount > expectation.callCount {
			expectation.callCount++

			if expectation.block {
				c := &Call{parameters: parameters}
				c.waitForReturn.Add(1)
				expectation.calls <- c
				c.waitForReturn.Wait()
				return c.returnValue
			} else {
				return expectation.returnValue
			}
		}
	}

	panic("Unhandled call")
}

func (h *History) AssertExpectationsCalled() {
	for _, expectation := range h.expectations {
		if expectation.callCount == 0 {
			log.Fatalf("Expected call %v was never called", expectation)
		}
	}
}

type MockMarker struct {
	history *History
}

func (m *MockMarker) GetFirstMissingRegion(start int64, end int64) *Region {
	return (m.history.log("GetFirstMissingRegion", start)).(*Region)
}

type addRegionParams struct {
	start int64
	end   int64
}

func (m *MockMarker) AddRegion(start int64, end int64) {
	m.history.log("AddRegion", &addRegionParams{start, end})
}

type MockCopier struct {
	history *History
}

type copyParams struct {
	ctx    context.Context
	offset int64
	len    int64
	writer io.Writer
}

func (m *MockCopier) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	v := m.history.log("Copy", &copyParams{ctx, offset, len, writer})
	if v == nil {
		return nil
	}
	return v.(error)
}

type joinable struct {
	wait sync.WaitGroup
}

func (j *joinable) Join() {
	waitWithTimeout(&j.wait)
}

func Run(f func()) *joinable {
	j := &joinable{}
	j.wait.Add(1)

	w := func() {
		// note: do we need to do something if f panics? (should join result in panic?)
		f()
		j.wait.Done()
	}

	go w()

	return j
}

func TestBasicPendingSequence(t *testing.T) {
	require := require.New(t)

	ctx := context.Background()

	history := &History{}
	marker := &MockMarker{history}
	copier := &MockCopier{history}

	start := int64(0)
	end := int64(10)
	maxEnd := int64(100)
	minUncommited := int64(5)
	maxWindowSize := int64(10)
	writer := bytes.NewBuffer(make([]byte, 1000))

	p := NewPendingReads()
	var call CallStatus

	// expect Copy is called when we invoke StartBackgroundCopy
	expectCopyCall := history.Expect("Copy")
	j := Run(func() {
		log.Println("Startbackground")
		call = p.StartBackgroundCopy(ctx, marker, copier, start, end, maxEnd, minUncommited, maxWindowSize, writer)
	})
	copyCall := expectCopyCall.WaitForCall()
	copyCallParams := copyCall.parameters.(*copyParams)
	copyCall.Return(nil)
	log.Println("calling join")
	j.Join()

	// expect writing 2 bytes does nothing
	buf := make([]byte, 2)
	j = Run(func() {
		n, err := copyCallParams.writer.Write(buf)
		require.Equal(2, n)
		require.Nil(err)
	})

	// expect AddRegion is called once we've finished writing > 5 bytes
	expectAddRegion := history.Expect("AddRegion")
	// Also expect these calls should be made and will always return a single value
	history.Expect("GetFirstMissingRegion").AndThenReturn(&Region{0, 1000})
	j.Join()

	buf = make([]byte, 10)
	j = Run(func() {
		n, err := copyCallParams.writer.Write(buf)
		require.Equal(10, n)
		require.Nil(err)
	})

	addRegionCall := expectAddRegion.WaitForCall()
	ap := addRegionCall.parameters.(*addRegionParams)
	require.Equal(int64(0), ap.start)
	require.Equal(int64(12), ap.end)
	addRegionCall.Return(nil)
	j.Join()

	// j = Run(func() {
	log.Println("Startbackground wait")
	err := call.Wait()
	log.Println("Startbackground done")
	require.Nil(err)
	// })
	// j.Join()

	history.AssertExpectationsCalled()
}

func TestJoinExistingRequest(t *testing.T) {
	require := require.New(t)

	ctx := context.Background()

	history := &History{}
	marker := &MockMarker{history}
	copier := &MockCopier{history}

	start := int64(0)
	end := int64(10)

	secondStart := int64(2)
	secondEnd := int64(15)

	maxEnd := int64(100)
	minUncommited := int64(5)
	maxWindowSize := int64(10)
	writer := bytes.NewBuffer(make([]byte, 1000))

	p := NewPendingReads()
	var call CallStatus

	// expect Copy is called when we invoke StartBackgroundCopy
	expectCopyCall := history.Expect("Copy").NTimes(1)
	j := Run(func() {
		log.Println("Startbackground")
		call = p.StartBackgroundCopy(ctx, marker, copier, start, end, maxEnd, minUncommited, maxWindowSize, writer)
	})
	copyCall := expectCopyCall.WaitForCall()
	copyCallParams := copyCall.parameters.(*copyParams)
	log.Println("calling join")
	j.Join()

	// expect writing 2 bytes does nothing
	buf := make([]byte, 2)
	j = Run(func() {
		n, err := copyCallParams.writer.Write(buf)
		require.Equal(2, n)
		require.Nil(err)
	})

	// verify first call is still running
	require.True(call.IsRunning())

	// now make a 2nd call to copy
	log.Printf("2nd start backgroud copy")
	secondCall := p.StartBackgroundCopy(ctx, marker, copier, secondStart, secondEnd, maxEnd, minUncommited, maxWindowSize, writer)
	require.True(secondCall.IsRunning())

	// expect AddRegion is called once we've finished writing > 5 bytes
	history.Expect("AddRegion").AndThenReturn(nil)
	// Also expect these calls should be made and will always return a single value
	history.Expect("GetFirstMissingRegion").AndThenReturn(&Region{0, 1000})
	j.Join()

	buf = make([]byte, 10)
	n, err := copyCallParams.writer.Write(buf)
	require.Equal(10, n)
	require.Nil(err)

	// the first call has returned, but the second has not
	err = call.Wait()
	require.Nil(err)
	require.False(call.IsRunning())
	require.True(secondCall.IsRunning())

	// do another write of 10 bytes to complete the second copy
	copyCallParams.writer.Write(buf)
	j = Run(func() {
		secondCall.Wait()
	})
	j.Join()

	copyCall.Return(nil)

	history.AssertExpectationsCalled()

}
