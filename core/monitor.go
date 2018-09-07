package core

import (
	"context"
	"time"
)

type Monitor interface {
	AddedLazyDirBlock(ctx context.Context, startTime time.Time, endTime time.Time)
	FetchedRemoteChildren(ctx context.Context, startTime time.Time, endTime time.Time)
	RegionCopied(ctx context.Context, startTime time.Time, endTime time.Time, start int64, end int64)
}

type NullMonitor struct {
}

func (m *NullMonitor) AddedLazyDirBlock(ctx context.Context, startTime time.Time, endTime time.Time) {
}

func (m *NullMonitor) FetchedRemoteChildren(ctx context.Context, startTime time.Time, endTime time.Time) {
}

func (m *NullMonitor) RegionCopied(ctx context.Context, startTime time.Time, endTime time.Time, start int64, end int64) {
}
