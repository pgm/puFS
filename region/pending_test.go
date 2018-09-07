package region

import (
	"bytes"
	"context"
	"io"
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

type Call struct {
	name        string
	parameters  interface{}
	wait        sync.WaitGroup
	returnValue interface{}
}

func (c *Call) ReturnWith(value interface{}) {
	c.returnValue = value
	c.wait.Done()
}

type History struct {
	mutex   sync.Mutex
	calls   []*Call
	wait    sync.WaitGroup
	handler CallHandler
}

func (h *History) expectCalls(n int) {
	h.wait.Add(n)
}

type CallHandler func(name string, parameters interface{}) (bool, interface{})

func (h *History) RegisterHandler(handler CallHandler) {
	h.handler = handler
}

func (h *History) log(name string, parameters interface{}) interface{} {
	if h.handler != nil {
		handled, returnValue := h.handler(name, parameters)
		if handled {
			return returnValue
		}
	}

	latest := &Call{name: name, parameters: parameters}
	latest.wait.Add(1)
	h.calls = append(h.calls, latest)
	// signal that our call is blocked
	h.wait.Done()
	waitWithTimeout(&latest.wait)
	return latest.returnValue
}

func (h *History) last() *Call {
	return h.calls[len(h.calls)-1]
}

func (h *History) Wait() {
	waitWithTimeout(&h.wait)
}

type MockMarker struct {
	history *History
}

func (m *MockMarker) GetMaxReadMark(n int64) int64 {
	return (m.history.log("GetMaxReadMark", n)).(int64)
}

func (m *MockMarker) GetNextRegionStart(n int64) int64 {
	return (m.history.log("GetNextRegionStart", n)).(int64)
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

func TestBasicPendingSequence(t *testing.T) {
	require := require.New(t)

	history := &History{}
	marker := &MockMarker{history}
	copier := &MockCopier{history}

	start := int64(0)
	end := int64(10)
	minUncommited := int64(5)
	maxWindowSize := int64(10)
	writer := bytes.NewBuffer(make([]byte, 1000))

	p := NewPendingReads()

	history.expectCalls(1) // expect this to result in 1 go routine to make a blocking call logged into the history
	p.StartBackgroundCopy(marker, copier, 0, start, end, minUncommited, maxWindowSize, writer)
	history.Wait()

	copyCall := history.last()
	require.Equal("Copy", copyCall.name)
	copyCallParams := (copyCall.parameters).(*copyParams)

	history.RegisterHandler(func(name string, parameters interface{}) (bool, interface{}) {
		if name == "GetNextRegionStart" {
			return true, int64(1000)
		}

		if name == "GetMaxReadMark" {
			return true, int64(0)
		}

		return false, nil
	})
	history.expectCalls(1)
	buf := make([]byte, 5)
	go (func() {
		n, err := copyCallParams.writer.Write(buf)
		require.Equal(5, n)
		require.Nil(err)
	})()
	history.Wait()

	addRegion := history.last()
	require.Equal("AddRegion", addRegion.name)
	ap := addRegion.parameters.(*addRegionParams)
	require.Equal(int64(0), ap.start)
	require.Equal(int64(5), ap.end)

	addRegion.ReturnWith(nil)
}

func TestWaitForPending(t *testing.T) {
	require := require.New(t)

	history := &History{}
	marker := &MockMarker{history}
	copier := &MockCopier{history}

	start := int64(0)
	end := int64(10)
	minUncommited := int64(5)
	maxWindowSize := int64(10)
	writer := bytes.NewBuffer(make([]byte, 1000))

	p := NewPendingReads()

	history.expectCalls(1) // expect this to result in 1 go routine to make a blocking call logged into the history
	p.StartBackgroundCopy(marker, copier, 0, start, end, minUncommited, maxWindowSize, writer)
	history.Wait()

	copyCall := history.last()
	require.Equal("Copy", copyCall.name)
	copyCallParams := (copyCall.parameters).(*copyParams)

	history.expectCalls(1)
	go (func() {
		p.WaitForRegion(1, 2)
		history.log("WaitForRegionComplete", nil)
	})()

	history.RegisterHandler(func(name string, parameters interface{}) (bool, interface{}) {
		if name == "GetNextRegionStart" {
			return true, int64(1000)
		}

		if name == "GetMaxReadMark" {
			return true, int64(0)
		}

		if name == "AddRegion" {
			return true, nil
		}

		return false, nil
	})
	buf := make([]byte, 5)
	go (func() {
		n, err := copyCallParams.writer.Write(buf)
		require.Equal(5, n)
		require.Nil(err)
	})()
	history.Wait()

	last := history.last()
	require.Equal("WaitForRegionComplete", last.name)

}
