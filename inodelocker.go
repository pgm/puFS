package sply2

import (
	"sync"
)

type INodeLocker struct {
	locks sync.Map
}

func (l *INodeLocker) GetMutex(inode INode) *sync.RWMutex {
	ptr, ok := l.locks.Load(inode)
	if !ok {
		ptr, _ = l.locks.LoadOrStore(inode, &sync.RWMutex{})
	}
	return ptr.(*sync.RWMutex)
}

func (l *INodeLocker) Lock(inode INode) {
	l.GetMutex(inode).Lock()
}

func (l *INodeLocker) Unlock(inode INode) {
	l.GetMutex(inode).Unlock()
}

func (l *INodeLocker) RLock(inode INode) {
	l.GetMutex(inode).RLock()
}

func (l *INodeLocker) RUnlock(inode INode) {
	l.GetMutex(inode).RUnlock()
}
