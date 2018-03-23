package core

import (
	"io"
	"strings"
	"time"
)

type INode uint32
type BlockID [32]byte

type Freezer interface {
	GetRef(BID BlockID) (FrozenRef, error)
	AddBlock(BID BlockID, remoteRef RemoteRef) error
	AddFile(path string) (BlockID, error)
	IsPushed(BID BlockID) (bool, error)
}

type FrozenRef interface {
	io.ReadSeeker
	Release()
}

type RemoteRef interface {
	GetSize() int64
	Copy(offset int64, len int64, writer io.Writer) error
	// Release()
}

type WritableRef interface {
	io.ReadSeeker
	io.Writer
	Release()
}

type DirEntry struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time

	BID BlockID // maybe lift this up to header block as previously considered. Would allow GC to trace references without reading/parsing whole block

	URL  string
	ETag string

	Bucket     string
	Key        string
	Generation int64
}

type Dir struct {
	Entries []DirEntry
}

type RTx interface {
	RBucket(name []byte) RBucket
}

type RWTx interface {
	RTx
	WBucket(name []byte) WBucket
}

type KVStore interface {
	Update(func(RWTx) error) error
	View(func(RTx) error) error
	Close() error
}

type RBucket interface {
	Get(key []byte) []byte
	ForEachWithPrefix(prefix []byte, callback func(key []byte, value []byte) error) error
}

type WBucket interface {
	RBucket
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

type MemStore struct {
	perBucket map[string]map[string][]byte
	rollback  []OldValue
}

func NewMemStore(bucketNames [][]byte) *MemStore {
	perBucket := make(map[string]map[string][]byte)
	for _, name := range bucketNames {
		b := make(map[string][]byte)
		perBucket[string(name)] = b
	}
	return &MemStore{perBucket: perBucket}
}

type OldValue struct {
	bucket string
	key    string
	value  []byte
}

type Bucket struct {
	name  string
	store *MemStore
}

func (m *MemStore) RBucket(name []byte) RBucket {
	return &Bucket{string(name), m}
}
func (m *MemStore) WBucket(name []byte) WBucket {
	return &Bucket{string(name), m}
}
func (m *MemStore) Update(callback func(RWTx) error) error {
	m.rollback = nil
	err := callback(m)
	if err != nil {
		for i := len(m.rollback) - 1; i >= 0; i-- {
			old := m.rollback[i]
			m.perBucket[old.bucket][old.key] = old.value
		}
	}
	return err
}
func (m *MemStore) View(callback func(RTx) error) error {
	return callback(m)
}
func (m *MemStore) Close() error {
	return nil
}

func arrayCopy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return a
}

func (m *Bucket) Get(key []byte) []byte {
	value, okay := m.store.perBucket[m.name][string(key)]
	if !okay {
		return nil
	}
	return arrayCopy(value)
}

func (m *Bucket) ForEachWithPrefix(prefix []byte, callback func(key []byte, value []byte) error) error {
	sprefix := string(prefix)
	for k, v := range m.store.perBucket[m.name] {
		if strings.HasPrefix(k, sprefix) && v != nil {
			err := callback([]byte(k), v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Bucket) Put(key []byte, value []byte) error {
	skey := string(key)
	oldValue, okay := m.store.perBucket[m.name][skey]
	if !okay {
		oldValue = nil
	}

	m.store.rollback = append(m.store.rollback, OldValue{m.name, skey, oldValue})
	m.store.perBucket[m.name][skey] = arrayCopy(value)
	return nil
}

func (m *Bucket) Delete(key []byte) error {
	return m.Put(key, nil)
}

func generateUniqueString() string {
	return time.Now().Format(time.RFC3339Nano)
}
