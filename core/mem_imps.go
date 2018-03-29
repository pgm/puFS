package core

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

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

type RemoteRefFactoryMem struct {
	leases  map[string]BlockID
	roots   map[string]BlockID
	objects map[string][]byte
	prefix  string
}

type MemCopy struct {
	buffer []byte
}

func (m *MemCopy) GetSize() int64 {
	return int64(len(m.buffer))
}

func (m *MemCopy) Copy(ctx context.Context, offset int64, len int64, writer io.Writer) error {
	n, err := writer.Write(m.buffer[offset : offset+len])
	if n != int(len) {
		panic(fmt.Sprintf("%d != %d", n, len))
	}
	if err != nil {
		panic(err)
	}
	return nil
}

func (m *MemCopy) GetSource() interface{} {
	return m.buffer
}

func NewRemoteRefFactoryMem() *RemoteRefFactoryMem {
	return &RemoteRefFactoryMem{roots: make(map[string]BlockID),
		objects: make(map[string][]byte),
		prefix:  "blocks/",
		leases:  make(map[string]BlockID)}
}

func (r *RemoteRefFactoryMem) GetRef(ctx context.Context, node *NodeRepr) (RemoteRef, error) {
	key := GetBlockKey(r.prefix, node.BID)
	b, ok := r.objects[key]
	if !ok {
		panic("missing block")
	}
	return &MemCopy{b}, nil
}

type FrozenReader struct {
	ctx context.Context
	fr  Reader
}

func (fr *FrozenReader) Read(buffer []byte) (n int, err error) {
	return fr.fr.Read(fr.ctx, buffer)
}

func (r *RemoteRefFactoryMem) Push(ctx context.Context, BID BlockID, fr FrozenRef) error {
	rfr := &FrozenReader{ctx, fr}
	b, err := ioutil.ReadAll(rfr)
	if err != nil {
		panic(err)
	}
	key := GetBlockKey(r.prefix, BID)
	r.objects[key] = b
	return nil
}

func (r *RemoteRefFactoryMem) SetLease(ctx context.Context, name string, expiry time.Time, BID BlockID) error {
	r.leases[name] = BID
	return nil
}

func (r *RemoteRefFactoryMem) SetRoot(ctx context.Context, name string, BID BlockID) error {
	r.roots[name] = BID
	return nil
}

func (r *RemoteRefFactoryMem) GetRoot(ctx context.Context, name string) (BlockID, error) {
	BID, ok := r.roots[name]
	if !ok {
		return BID, UndefinedRootErr
	}
	return BID, nil
}

func (r *RemoteRefFactoryMem) GetChildNodes(ctx context.Context, node *NodeRepr) ([]*RemoteFile, error) {
	prefix := node.Key + "/"
	dirs := make(map[string]bool)
	result := make([]*RemoteFile, 0, 100)
	now := time.Now()

	for key, value := range r.objects {
		if strings.HasPrefix(key, prefix) {
			name := key[len(prefix):]
			nextSlash := strings.Index(name, "/")
			if nextSlash >= 0 {
				name = name[:nextSlash]
				dirs[name] = true
			} else {
				rec := &RemoteFile{
					Name:       name,
					IsDir:      false,
					Size:       int64(len(value)),
					ModTime:    now,
					Bucket:     node.Bucket,
					Key:        key,
					Generation: 1}
				result = append(result, rec)
			}
		}
	}

	for name, _ := range dirs {
		rec := &RemoteFile{
			Name:       name,
			IsDir:      true,
			Size:       0,
			ModTime:    now,
			Bucket:     node.Bucket,
			Key:        node.Key + "/" + name,
			Generation: 0}
		result = append(result, rec)
	}

	return nil, nil
}
