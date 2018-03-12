package sply2

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

const RootINode = 1
const InvalidINode = 0

type INodeDB struct {
	db        map[INode]*Node
	lastID    INode
	maxINodes uint32
}

func NewINodeDB() *INodeDB {
	db := make(map[INode]*Node)
	db[RootINode] = &Node{IsDir: true, ModTime: time.Now(), names: make(map[string]INode)}

	return &INodeDB{db: db, lastID: RootINode, maxINodes: 10000}
}

func (db *INodeDB) getNextFreeInode() (error, INode) {
	firstID := db.lastID
	id := db.lastID
	for {
		id++
		if uint32(id) > db.maxINodes {
			id = RootINode + 1
		}
		if firstID == id {
			return INodesExhaustedErr, InvalidINode
		}

		if _, ok := db.db[id]; !ok {
			return nil, id
		}
	}
}

func (db *INodeDB) allocateNode(IsDir bool) (error, INode, *Node) {
	now := time.Now()
	node := &Node{IsDir: IsDir, ModTime: now}
	if IsDir {
		node.names = make(map[string]INode)
	}
	err, id := db.getNextFreeInode()
	if err != nil {
		return err, InvalidINode, nil
	}
	db.db[id] = node
	return nil, id, node
}

func (db *INodeDB) releaseNode(id INode) {
	delete(db.db, id)
}

func (db *INodeDB) RemoveNode(parent INode, name string) error {
	parentDir, ok := db.db[parent]
	if !ok {
		return ParentMissingErr
	}

	if !parentDir.IsDir {
		return NotDirErr
	}

	existing, ok := parentDir.names[name]
	if !ok {
		return NoSuchNodeErr
	}

	delete(parentDir.names, name)
	db.releaseNode(existing)
	return nil
}

func (db *INodeDB) prepareForInsert(parent INode, name string) (*Node, error) {
	parentDir, ok := db.db[parent]
	if !ok {
		return nil, ParentMissingErr
	}

	if !parentDir.IsDir {
		return nil, NotDirErr
	}

	_, ok = parentDir.names[name]
	if ok {
		// db.releaseNode(existing)
		return nil, ExistsErr
	}

	return parentDir, nil
}

func (db *INodeDB) AddNode(parent INode, name string, IsDir bool) (INode, *Node, error) {
	parentDir, err := db.prepareForInsert(parent, name)
	if err != nil {
		return InvalidINode, nil, err
	}

	err, id, node := db.allocateNode(IsDir)
	if err != nil {
		return InvalidINode, nil, err
	}

	parentDir.names[name] = id

	return id, node, nil
}

func (db *INodeDB) AddRemoteURL(parent INode, name string, url string) (INode, *Node, error) {
	parentDir, err := db.prepareForInsert(parent, name)
	if err != nil {
		return InvalidINode, nil, err
	}

	err, id, node := db.allocateNode(false)
	if err != nil {
		return InvalidINode, nil, err
	}

	node.URL = url
	// will populate Remote as well as length, etag, etc
	ensureRemotePopulated(node)

	parentDir.names[name] = id

	return id, node, nil
}

func (db *INodeDB) GetDirContents(id INode) ([]string, error) {
	dir, ok := db.db[id]
	if !ok {
		return nil, NoSuchNodeErr
	}

	if !dir.IsDir {
		return nil, NotDirErr
	}

	names := make([]string, 0, 100)
	for name, _ := range dir.names {
		names = append(names, name)
	}

	return names, nil
}

type WriteableStore interface {
	NewWriteRef() (WritableRef, error)
}

type WritableStoreImp struct {
	path string
}

type WritableRefImp struct {
	filename string
}

func NewWritableRefImp(name string) WritableRef {
	return &WritableRefImp{name}
}

func (w *WritableRefImp) Read(offset int64, dest []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return f.Read(dest)
}

func (w *WritableRefImp) Write(offset int64, buffer []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return f.Write(buffer)
}

func (w *WritableRefImp) Release() {

}

func (w *WritableStoreImp) NewWriteRef() (WritableRef, error) {
	f, err := ioutil.TempFile(w.path, "dat")
	if err != nil {
		return nil, err
	}
	name := f.Name()
	f.Close()
	return NewWritableRefImp(name), nil
}

type DataStore struct {
	path          string
	db            *INodeDB
	writableStore WriteableStore
	// # internal
	//    promote_remote_to_frozen(remote_ref) -> FrozenRef
	//    promote_frozen_to_writable(FrozenRef) -> WriteRef

	//    get_remote_ref(inode) -> RemoteRef
	//    get_frozen_ref(inode) -> FrozenRef
	// #public
	//    # write ops
	//    make_dir(parent_inode) -> inode
	//    create_writable(parent_inode) -> inode
	//    push(inode)

	//    # read ops
	//    get_child_inode(parent_inode, name) -> inode
	//    get_stat(inode) -> StatObj
	//    get_dir_contents(inode) -> list of string

	//    get_local_readable(inode) -> ReadableRef
	//    get_local_writable(inode) -> WritableRef
	//    get_frozen_block_id(inode) -> block_id
	// }
}

func NewWritableStore(path string) WriteableStore {
	return &WritableStoreImp{path}
}

func NewDataStore(path string) *DataStore {
	return &DataStore{path: path, db: NewINodeDB(), writableStore: NewWritableStore(path)}
}

func (d *DataStore) GetDirContents(id INode) ([]string, error) {
	return d.db.GetDirContents(id)
}

func (d *DataStore) MakeDir(parent INode, name string) (INode, error) {
	id, _, err := d.db.AddNode(parent, name, true)
	return id, err
}

func (d *DataStore) AddRemoteURL(parent INode, name string, URL string) (INode, error) {
	id, _, err := d.db.AddRemoteURL(parent, name, URL)
	return id, err
}

func (d *DataStore) CreateWritable(parent INode, name string) (INode, WritableRef, error) {
	id, node, err := d.db.AddNode(parent, name, false)
	if err != nil {
		return InvalidINode, nil, err
	}

	writable, err := d.writableStore.NewWriteRef()
	if err != nil {
		return InvalidINode, nil, err
	}

	node.Writable = writable

	return id, writable, err
}

type RemoteURL struct {
	URL           string
	ETag          string
	Length        int64
	AcceptsRanges bool
}

func (r *RemoteURL) Copy(offset int64, len int64, writer io.Writer) error {
	req, err := http.NewRequest("GET", r.URL, nil)
	req.Header.Add("If-Match", `"r.ETag"`)
	if offset != 0 || len != r.Length {
		if r.AcceptsRanges {
			req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+len-1))
		} else {
			return errors.New("server does not accept ranges")
		}
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return errors.New(fmt.Sprintf("Status code: %d", res.StatusCode))
	}

	n, err := io.Copy(writer, res.Body)
	if err != nil {
		return err
	}

	if n != len {
		return errors.New("Did not copy full requested length")
	}

	return nil
}

func NewRemoteURL(url string) (RemoteRef, string, int64, error) {
	res, err := http.Head(url)
	if err != nil {
		return nil, "", 0, err
	}
	contentlength := res.ContentLength
	etag := res.Header.Get("ETag")
	rangeDef := res.Header.Get("Accept-Ranges")
	acceptRanges := rangeDef == "bytes"

	return &RemoteURL{url, etag, contentlength, acceptRanges}, etag, contentlength, nil
}

func ensureRemotePopulated(node *Node) error {
	if node.Remote != nil {
		return nil
	}

	remote, etag, size, err := NewRemoteURL(node.URL)
	if err != nil {
		return err
	}

	node.Remote, node.ETag, node.Size = remote, etag, size

	return nil
}

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{name}
}

func (d *DataStore) freeze(node *Node) (FrozenRef, error) {
	f, err := ioutil.TempFile(d.path, "frozen")
	if err != nil {
		return nil, err
	}

	defer f.Close()

	err = node.Remote.Copy(0, node.Size, f)
	if err != nil {
		return nil, err
	}

	x := NewFrozenRef(f.Name())
	return x, nil
}

type FrozenRefImp struct {
	filename string
}

func (w *FrozenRefImp) Read(offset int64, dest []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return f.Read(dest)
}

func (w *FrozenRefImp) Release() {

}

func (d *DataStore) ensureFrozenPopulated(node *Node) error {
	if node.Frozen != nil {
		return nil
	}

	err := ensureRemotePopulated(node)
	if err != nil {
		return err
	}

	frozen, err := d.freeze(node)
	if err != nil {
		return err
	}

	node.Frozen = frozen

	return nil
}

func (d *DataStore) GetReadRef(childINode INode) (INode, ReadableRef, error) {
	// parentNode, ok := d.db.db[parent]
	// if !ok {
	// 	return InvalidINode, nil, NoSuchNodeErr
	// }

	// childINode, ok := parentNode.names[name]
	// if !ok {
	// 	return InvalidINode, nil, InvalidFilenameErr
	// }

	childNode, ok := d.db.db[childINode]
	if !ok {
		panic("Invalid child inode")
	}

	if childNode.Writable != nil {
		return childINode, childNode.Writable, nil
	}

	err := d.ensureFrozenPopulated(childNode)
	if err != nil {
		return childINode, nil, err
	}

	return childINode, childNode.Frozen, nil

	// if childNode.Writable != nil {

	// }
}
