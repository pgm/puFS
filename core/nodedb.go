package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"time"
)

const RootINode = 1
const InvalidINode = 0

const MaxNodeReprSize = 10000

var ChildNodeBucket []byte = []byte("ChildNode")
var NodeBucket []byte = []byte("Node")

type INodeDB struct {
	db        KVStore
	lastID    INode
	maxINodes uint32
}

type NodeRepr struct {
	ParentINode INode
	IsDir       bool
	Size        int64
	ModTime     time.Time

	BID BlockID

	// Fields for Remote URL
	URL  string
	ETag string

	// Fields for Remote GCS
	Bucket     string
	Key        string
	Generation int64

	IsDeferredChildFetch bool

	// only populated for writable file (implies IsDir is false, and remote fields blank)
	LocalWritablePath string
}

func nodeToBytes(node *NodeRepr) []byte {
	buffer := bytes.NewBuffer(make([]byte, 0, 100))
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(node)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func bytesToNode(b []byte) *NodeRepr {
	if b == nil {
		panic("cannot decode nil")
	}
	var node NodeRepr
	buffer := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&node)
	if err != nil {
		panic(err)
	}
	return &node
}

func putNodeRepr(tx RWTx, id INode, node *NodeRepr) error {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	value := nodeToBytes(node)

	nb := tx.WBucket(NodeBucket)
	return nb.Put(idBytes, value)
}

func getNodeRepr(tx RTx, id INode) (*NodeRepr, error) {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	//	value := make([]byte, MaxNodeReprSize) // how do I know what the max size is?

	nb := tx.RBucket(NodeBucket)
	value := nb.Get(idBytes)

	if value == nil {
		panic(fmt.Sprintf("Could not find node with ID %d", int(id)))
	}

	return bytesToNode(value), nil
}

func (db *INodeDB) Close() {
	db.db.Close()
}

func NewINodeDB(maxINodes uint32, db KVStore) *INodeDB {
	db.Update(func(tx RWTx) error {
		err := addEmptyDir(tx, RootINode, RootINode)

		if err != nil {
			log.Fatal(err)
		}

		return nil
	})

	return &INodeDB{db: db, lastID: RootINode, maxINodes: maxINodes}
}

func (db *INodeDB) update(fn func(tx RWTx) error) error {
	return db.db.Update(fn)
}

func (db *INodeDB) view(fn func(tx RTx) error) error {
	return db.db.View(fn)
}

func (db *INodeDB) getNextFreeInode(tx RTx) (INode, error) {
	firstID := db.lastID
	id := db.lastID
	idBytes := make([]byte, 4)

	b := tx.RBucket(NodeBucket)

	for {
		id++
		if uint32(id) > db.maxINodes {
			id = RootINode + 1
		}
		if firstID == id {
			return InvalidINode, INodesExhaustedErr
		}

		binary.LittleEndian.PutUint32(idBytes, uint32(id))

		if b.Get(idBytes) == nil {
			return id, nil
		}
	}
}

func (db *INodeDB) releaseNode(tx RWTx, id INode) error {
	b := tx.WBucket(NodeBucket)
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	return b.Delete(idBytes)
}

func isDir(tx RTx, id INode) (bool, error) {
	node, error := getNodeRepr(tx, id)
	if error != nil {
		return false, error
	}

	return node.IsDir, nil
}

func (db *INodeDB) isEmptyDir(tx RTx, inode INode) (bool, error) {
	names, err := db.GetDirContents(tx, inode)
	if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

func (db *INodeDB) Rename(tx RWTx, srcParent INode, srcName string, dstParent INode, dstName string) error {
	err := assertValidDirWillMutate(tx, srcParent)
	if err != nil {
		return err
	}

	err = assertValidDirWillMutate(tx, dstParent)
	if err != nil {
		return err
	}

	inode, err := db.GetNodeID(tx, srcParent, srcName)
	if err != nil {
		return err
	}

	err = removeChild(tx, srcParent, srcName)
	if err != nil {
		return err
	}

	err = addChild(tx, dstParent, inode, dstName)
	if err != nil {
		return err
	}

	return nil
}

func removeChild(tx RWTx, parent INode, name string) error {
	key := makeChildKey(parent, name)
	cb := tx.WBucket(ChildNodeBucket)
	err := cb.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func (db *INodeDB) RemoveNode(tx RWTx, parent INode, name string) error {
	err := assertValidDirWillMutate(tx, parent)
	if err != nil {
		return err
	}

	idToDelete, err := db.GetNodeID(tx, parent, name)

	nb := tx.WBucket(NodeBucket)
	if err != nil {
		return err
	}

	isDir, err := isDir(tx, idToDelete)
	if err != nil {
		return err
	}

	if isDir {
		isEmpty, err := db.isEmptyDir(tx, idToDelete)
		if err != nil {
			return err
		}
		if !isEmpty {
			return DirNotEmptyErr
		}
	}

	// delete the entry saying this node is child of the parent node
	err = removeChild(tx, parent, name)
	if err != nil {
		return err
	}

	// delete the actual node
	inodeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(inodeBytes, uint32(idToDelete))
	err = nb.Delete(inodeBytes)
	if err != nil {
		return err
	}

	return nil
}

func assertValidDirWillMutate(tx RWTx, id INode) error {
	node, err := getNodeRepr(tx, id)
	if err != nil {
		return err
	}

	if !node.IsDir {
		return NotDirErr
	}

	// if we need to invalidate the block ID, recurse to the top of the tree
	if node.BID != NABlock {
		node.BID = NABlock
		putNodeRepr(tx, id, node)
		if node.ParentINode != RootINode {
			err := assertValidDirWillMutate(tx, node.ParentINode)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func assertValidDir(tx RTx, id INode) error {
	node, err := getNodeRepr(tx, id)
	if err != nil {
		return err
	}

	if !node.IsDir {
		return NotDirErr
	}

	return nil
}

func (db *INodeDB) addBlockLazyChildren(tx RWTx, parent INode, children []DirEntry) error {
	for _, child := range children {
		newNodeID, err := db.getNextFreeInode(tx)
		if err != nil {
			return err
		}
		err = putNodeRepr(tx, newNodeID, &NodeRepr{ParentINode: parent,
			IsDir:                child.IsDir,
			Size:                 child.Size,
			ModTime:              child.ModTime,
			BID:                  child.BID,
			URL:                  child.URL,
			ETag:                 child.ETag,
			Bucket:               child.Bucket,
			Key:                  child.Key,
			Generation:           child.Generation,
			IsDeferredChildFetch: child.IsDir})
		if err != nil {
			return err
		}
		err = addChild(tx, parent, newNodeID, child.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *INodeDB) addRemoteLazyChildren(tx RWTx, parent INode, children []*RemoteFile) error {
	for _, child := range children {
		newNodeID, err := db.getNextFreeInode(tx)
		if err != nil {
			return err
		}
		err = putNodeRepr(tx, newNodeID, &NodeRepr{ParentINode: parent,
			IsDir:                child.IsDir,
			Size:                 child.Size,
			ModTime:              child.ModTime,
			Bucket:               child.Bucket,
			Key:                  child.Key,
			Generation:           child.Generation,
			IsDeferredChildFetch: child.IsDir})
		if err != nil {
			return err
		}
		err = addChild(tx, parent, newNodeID, child.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *INodeDB) MutateBIDForMount(tx RWTx, id INode, BID BlockID) error {
	err := assertValidDirWillMutate(tx, id)
	if err != nil {
		return err
	}

	children, err := db.GetDirContents(tx, id)
	if err != nil {
		return err
	}

	// only allow empty directories as mount points to avoid worrying about children that would
	// otherwise need to be cleaned out before mounting takes place.
	if len(children) > 0 {
		return DirNotEmptyErr
	}

	node, err := getNodeRepr(tx, id)
	if err != nil {
		return err
	}

	node.BID = BID
	node.IsDeferredChildFetch = true
	err = putNodeRepr(tx, id, node)
	return err
}

func (db *INodeDB) AddDir(tx RWTx, parent INode, name string) (INode, error) {
	err := assertValidDirWillMutate(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	err = addEmptyDir(tx, parent, id)
	if err != nil {
		return InvalidINode, err
	}
	err = addChild(tx, parent, id, name)
	if err != nil {
		return InvalidINode, err
	}

	return id, nil
}

func addEmptyDir(tx RWTx, parentINode INode, inode INode) error {
	return putNodeRepr(tx, inode, &NodeRepr{ParentINode: parentINode, ModTime: time.Now(), IsDir: true})
}

func splitChildKey(key []byte) (INode, string) {
	inode := INode(binary.LittleEndian.Uint32(key[0:4]))
	name := string(key[4:])

	return inode, name
}

func printDbStats(tx RTx) {
	bc := tx.RBucket(NodeBucket)
	bc.ForEachWithPrefix(nil, func(k, v []byte) error {
		inode := INode(binary.LittleEndian.Uint32(k))
		fmt.Printf("node key=%d, value=bytes with len %d\n", inode, len(v))
		return nil
	})
	cn := tx.RBucket(ChildNodeBucket)
	cn.ForEachWithPrefix(nil, func(k, v []byte) error {
		parent, name := splitChildKey(k)
		inode := INode(binary.LittleEndian.Uint32(k))
		fmt.Printf("child key=(%d, %s), value=%s\n", parent, name, inode)
		return nil
	})
}

func makeChildKey(inode INode, name string) []byte {
	nameBytes := []byte(name)
	key := make([]byte, 4+len(nameBytes))
	binary.LittleEndian.PutUint32(key[0:4], uint32(inode))
	copy(key[4:], nameBytes)

	return key
}

func (db *INodeDB) GetNode(tx RTx, parent INode, name string) (*NodeRepr, error) {
	id, err := db.GetNodeID(tx, parent, name)
	if err != nil {
		return nil, err
	}

	return getNodeRepr(tx, id)
}

func (db *INodeDB) GetNodeID(tx RTx, parent INode, name string) (INode, error) {
	err := assertValidDir(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	key := makeChildKey(parent, name)
	cn := tx.RBucket(ChildNodeBucket)

	value := cn.Get(key)
	if value == nil {
		return InvalidINode, NoSuchNodeErr
	}

	return INode(binary.LittleEndian.Uint32(value)), nil
}

func addChild(tx RWTx, parent INode, inode INode, name string) error {
	key := makeChildKey(parent, name)
	inodeBytes := make([]byte, 4)

	binary.LittleEndian.PutUint32(inodeBytes, uint32(inode))

	nb := tx.WBucket(ChildNodeBucket)
	return nb.Put(key, inodeBytes)
}

func (db *INodeDB) AddRemoteGCS(tx RWTx, parent INode, name string, bucket string, key string, generation int64, size int64, ModTime time.Time, isDir bool) (INode, error) {
	err := assertValidDirWillMutate(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	err = addRemoteGCS(tx, parent, id, bucket, key, generation, size, ModTime, isDir)
	if err != nil {
		return InvalidINode, err
	}
	err = addChild(tx, parent, id, name)
	if err != nil {
		return InvalidINode, err
	}

	return id, nil

}

func addRemoteGCS(tx RWTx, parentINode INode, inode INode, bucket string, key string, generation int64, size int64, modTime time.Time, isDir bool) error {
	var BID BlockID
	if isDir {
		BID = NABlock
	} else {
		hashID := Sha256.Sum([]byte(fmt.Sprintf("%s/%s:%d", bucket, key, generation)))
		copy(BID[:], hashID)
	}
	return putNodeRepr(tx, inode, &NodeRepr{ParentINode: parentINode,
		IsDir:                isDir,
		Bucket:               bucket,
		Key:                  key,
		Generation:           generation,
		Size:                 size,
		ModTime:              modTime,
		BID:                  BID,
		IsDeferredChildFetch: isDir})
}

func (db *INodeDB) AddRemoteURL(tx RWTx, parent INode, name string, url string, etag string, size int64, ModTime time.Time) (INode, error) {
	err := assertValidDirWillMutate(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	err = addRemoteURL(tx, parent, id, url, etag, size, ModTime)
	if err != nil {
		return InvalidINode, err
	}
	err = addChild(tx, parent, id, name)
	if err != nil {
		return InvalidINode, err
	}

	return id, nil
}

var Sha256 = sha256.New()

func addRemoteURL(tx RWTx, parentINode INode, inode INode, url string, etag string, size int64, modTime time.Time) error {
	hashID := Sha256.Sum([]byte(url + etag))
	var BID BlockID
	copy(BID[:], hashID)
	return putNodeRepr(tx, inode, &NodeRepr{ParentINode: parentINode, IsDir: false, URL: url, ETag: etag, Size: size, ModTime: modTime, BID: BID})
}

// func (db *INodeDB) AddRemoteObject(tx RTx, parent INode, name string, bucket string, key string, size int64, ModTime time.Time) (INode, error) {
// 	err := assertValidDir(parent)
// 	if err != nil {
// 		return InvalidINode, err
// 	}

// 	id, err := db.getNextFreeInode(tx)
// 	if err != nil {
// 		return InvalidINode, err
// 	}

// 	addRemoteURL(tx, id, bucket, key, size, ModTime)
// 	addChild(tx, parent, id, name)

// 	return id, nil
// }

func addWritable(tx RWTx, parentINode INode, inode INode, filename string) error {
	return putNodeRepr(tx, inode, &NodeRepr{ParentINode: parentINode, IsDir: false, LocalWritablePath: filename})
}

func (db *INodeDB) AddWritableLocalFile(tx RWTx, parent INode, name string, filename string) (INode, error) {
	err := assertValidDirWillMutate(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	err = addWritable(tx, parent, id, filename)
	if err != nil {
		return InvalidINode, err
	}
	err = addChild(tx, parent, id, name)
	if err != nil {
		return InvalidINode, err
	}

	return id, nil
}

type NameINode struct {
	Name string
	ID   INode
}

func (db *INodeDB) GetDirContents(tx RTx, id INode) ([]NameINode, error) {
	err := assertValidDir(tx, id)
	if err != nil {
		return nil, err
	}

	names := make([]NameINode, 0, 100)
	prefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(prefix, uint32(id))

	// start by adding entries for "." and ".."
	node, err := getNodeRepr(tx, id)
	names = append(names, NameINode{Name: ".", ID: id})
	names = append(names, NameINode{Name: "..", ID: node.ParentINode})

	c := tx.RBucket(ChildNodeBucket)

	c.ForEachWithPrefix(prefix, func(k []byte, v []byte) error {
		name := string(k[len(prefix):])
		names = append(names, NameINode{Name: name, ID: INode(binary.LittleEndian.Uint32(v))})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return names, nil
}