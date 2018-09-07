package fs

import (
	"context"
	"time"

	"github.com/pgm/sply2/core"
)

type AddedLazyDirBlockEvent struct {
	ctx       context.Context
	startTime time.Time
	endTime   time.Time
}

type FetchedRemoteChildrenEvent struct {
	ctx       context.Context
	startTime time.Time
	endTime   time.Time
}

type RegionCopiedEvent struct {
	ctx       context.Context
	startTime time.Time
	endTime   time.Time
	start     int64
	end       int64
}

type MonitorToChan struct {
	events chan interface{}
}

func hasFuseRequest(ctx context.Context) bool {
	return ctx.Value(FUSE_REQUEST) != nil
}

func (m *MonitorToChan) AddedLazyDirBlock(ctx context.Context, startTime time.Time, endTime time.Time) {
	if !hasFuseRequest(ctx) {
		return
	}

	m.events <- &AddedLazyDirBlockEvent{ctx, startTime, endTime}
}

func (m *MonitorToChan) FetchedRemoteChildren(ctx context.Context, startTime time.Time, endTime time.Time) {
	if !hasFuseRequest(ctx) {
		return
	}

	m.events <- &FetchedRemoteChildrenEvent{ctx, startTime, endTime}
}

func (m *MonitorToChan) RegionCopied(ctx context.Context, startTime time.Time, endTime time.Time, start int64, end int64) {
	if !hasFuseRequest(ctx) {
		return
	}

	m.events <- &RegionCopiedEvent{ctx, startTime, endTime, start, end}
}

type ListDirCount struct {
	count     int
	timestamp time.Time
}

// type OpHistory struct {
// 	childListDirCount map[INode]*ListDirCount
// }

func listChildrenInParallel(ds *core.DataStore, id core.INode, concurrentReqs int) error {
	ctx := context.Background()

	entries, err := ds.GetDirContents(ctx, id)
	if err != nil {
		return err
	}

	c := make(chan core.INode, 100)

	if concurrentReqs > len(entries) {
		concurrentReqs = len(entries)
	}

	// worker which does a get dir contents on given INode. Don't worry about errors here.
	// exits once the channel is closed
	requestDir := func() {
		for {
			id, ok := <-c
			if !ok {
				break
			}
			ds.GetDirContents(ctx, id)
		}
	}

	// spawn goroutines
	for i := 0; i < concurrentReqs; i++ {
		go requestDir()
	}

	// queue up all of the child inodes
	for _, entry := range entries {
		c <- entry.ID
	}

	close(c)
	return nil
}

// go through the list of recent reads. attempt read of end-fileLength

func StartMonitor(ds *core.DataStore) chan interface{} {
	// concurrentReqs := 20
	c := make(chan interface{}, 2000)
	// ctx := context.Background()
	// var history OpHistory

	handleFetchedRemoteChildren := func(event *FetchedRemoteChildrenEvent) {
		// parentId := ds.GetParent(listOp.id)
		// count, ok := history.childListDirCount[parentId]
		// if count.expiry < now {
		// 	ok := false
		// }

		// if !ok {
		// 	count = &ListDirCount{count: 0}
		// 	history.childListDirCount[parentId] = count
		// }

		// count.count += 1
		// count.expiry = now + 1*time.Minute
		// if history.childListDirCount[parentId].count > 5 {
		// 	listChildrenInParallel(ds, parentId, concurrentReqs)
		// 	delete(history.childListDirCount[parentId])
		// }
	}

	handleRegionCopied := func(event *RegionCopiedEvent) {
		// // add to queue of pending reads

		// // get first pending read
		// readOp := getFirstOp()
		// id := readOp.ctx.GetValue()
		// ds.EnsureReadable(id, readOp.start, readOp.Length)
		// // reader := ds.GetReadRef(ctx, id)
		// // buffer := new([]byte, 500000, 500000)
		// // reader.Seek(readOp.end, 0)
		// // reader.Read(ctx, buffer)
		// // reader.Release()
	}

	loop := func() {
		for {
			event, ok := <-c
			if !ok {
				break
			}

			if castEvent, ok := event.(*FetchedRemoteChildrenEvent); ok {
				handleFetchedRemoteChildren(castEvent)
			} else if castEvent, ok := event.(*RegionCopiedEvent); ok {
				handleRegionCopied(castEvent)
			} else {
				// ignore
			}

			//		r, err := ds.GetReadRef(ctx, op.inode)
		}
	}

	go loop()

	return c
}

//monitor.FetchedRemoteChildren(id, startTime, endTime)
