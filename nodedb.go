package sply2

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	bolt "github.com/coreos/bbolt"
)

const RootINode = 1
const InvalidINode = 0

const MaxNodeReprSize = 10000

var ChildNodeBucket []byte = []byte("ChildNode")
var NodeBucket []byte = []byte("Node")

type INodeDB struct {
	db        *bolt.DB
	lastID    INode
	maxINodes uint32
}

type NodeRepr struct {
	IsDir   bool
	Size    int64
	ModTime time.Time

	// Fields for Remote URL
	URL  string
	ETag string

	// Fields for Remote GCS
	Bucket     string
	Key        string
	Generation int64

	// only populated for data fetched from remote
	LocalFrozenPath string

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
	var node NodeRepr
	buffer := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&node)
	if err != nil {
		panic(err)
	}
	return &node
}

func putNodeRepr(tx *bolt.Tx, id INode, node *NodeRepr) error {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	value := nodeToBytes(node)

	nb := tx.Bucket(NodeBucket)
	return nb.Put(idBytes, value)
}

func getNodeRepr(tx *bolt.Tx, id INode) (*NodeRepr, error) {
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	//	value := make([]byte, MaxNodeReprSize) // how do I know what the max size is?

	nb := tx.Bucket(NodeBucket)
	value := nb.Get(idBytes)
	// if err != nil {
	// 	return nil, err
	// }

	return bytesToNode(value), nil
}

func (db *INodeDB) Close() {
	db.db.Close()
}

func NewINodeDB(filename string, maxINodes uint32) *INodeDB {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(ChildNodeBucket)
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.CreateBucketIfNotExists(NodeBucket)

		if err != nil {
			log.Fatal(err)
		}

		err = addEmptyDir(tx, RootINode)

		if err != nil {
			log.Fatal(err)
		}

		return nil
	})

	return &INodeDB{db: db, lastID: RootINode, maxINodes: maxINodes}
}

func (db *INodeDB) update(fn func(tx *bolt.Tx) error) error {
	return db.db.Update(fn)
}

func (db *INodeDB) view(fn func(tx *bolt.Tx) error) error {
	return db.db.Update(fn)
}

func (db *INodeDB) getNextFreeInode(tx *bolt.Tx) (INode, error) {
	firstID := db.lastID
	id := db.lastID
	idBytes := make([]byte, 4)

	b := tx.Bucket(NodeBucket)

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

func (db *INodeDB) releaseNode(tx *bolt.Tx, id INode) error {
	b := tx.Bucket(NodeBucket)
	idBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(id))
	return b.Delete(idBytes)
}

func isDir(tx *bolt.Tx, id INode) (bool, error) {
	node, error := getNodeRepr(tx, id)
	if error != nil {
		return false, error
	}

	return node.IsDir, nil
}

func (db *INodeDB) isEmptyDir(tx *bolt.Tx, inode INode) (bool, error) {
	names, err := db.GetDirContents(tx, inode)
	if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

func (db *INodeDB) RemoveNode(tx *bolt.Tx, parent INode, name string) error {
	err := assertValidDir(tx, parent)
	if err != nil {
		return err
	}

	key := makeChildKey(parent, name)

	cb := tx.Bucket(ChildNodeBucket)
	nb := tx.Bucket(NodeBucket)

	value := cb.Get(key)

	idToDelete := INode(binary.LittleEndian.Uint32(value))
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
	err = cb.Delete(key)
	if err != nil {
		return err
	}

	// delete the actual node
	err = nb.Delete(value)
	if err != nil {
		return err
	}

	return nil
}

func assertValidDir(tx *bolt.Tx, parent INode) error {
	isDir, err := isDir(tx, parent)
	if err != nil {
		return err
	}

	if !isDir {
		return NotDirErr
	}

	return nil
}

func (db *INodeDB) AddDir(tx *bolt.Tx, parent INode, name string) (INode, error) {
	err := assertValidDir(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	addEmptyDir(tx, id)
	addChild(tx, parent, id, name)

	return id, nil
}

func addEmptyDir(tx *bolt.Tx, inode INode) error {
	return putNodeRepr(tx, inode, &NodeRepr{ModTime: time.Now(), IsDir: true})
}

func splitChildKey(key []byte) (INode, string) {
	inode := INode(binary.LittleEndian.Uint32(key[0:4]))
	name := string(key[4:])

	return inode, name
}

func printDbStats(tx *bolt.Tx) {
	bc := tx.Bucket(NodeBucket)
	bc.ForEach(func(k, v []byte) error {
		inode := INode(binary.LittleEndian.Uint32(k))
		fmt.Printf("node key=%d, value=bytes with len %d\n", inode, len(v))
		return nil
	})
	cn := tx.Bucket(ChildNodeBucket)
	cn.ForEach(func(k, v []byte) error {
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

func (db *INodeDB) GetNode(tx *bolt.Tx, parent INode, name string) (*NodeRepr, error) {
	err := assertValidDir(tx, parent)
	if err != nil {
		return nil, err
	}

	key := makeChildKey(parent, name)
	cn := tx.Bucket(ChildNodeBucket)

	value := cn.Get(key)

	inode := INode(binary.LittleEndian.Uint32(value))

	return getNodeRepr(tx, inode)
}

func addChild(tx *bolt.Tx, parent INode, inode INode, name string) error {
	key := makeChildKey(parent, name)
	inodeBytes := make([]byte, 4)

	binary.LittleEndian.PutUint32(inodeBytes, uint32(inode))

	nb := tx.Bucket(ChildNodeBucket)
	return nb.Put(key, inodeBytes)
}

// func (db *INodeDB) AddRemoteURL(tx *bolt.Tx, parent INode, name string, url string, etag string, size int64, ModTime time.Time) (INode, error) {
// 	err := assertValidDir(parent)
// 	if err != nil {
// 		return InvalidINode, err
// 	}

// 	id, err := db.getNextFreeInode(tx)
// 	if err != nil {
// 		return InvalidINode, err
// 	}

// 	addRemoteURL(tx, id, url, etag, size, ModTime)
// 	addChild(tx, parent, id, name)

// 	return id, nil
// }

// func (db *INodeDB) AddRemoteObject(tx *bolt.Tx, parent INode, name string, bucket string, key string, size int64, ModTime time.Time) (INode, error) {
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

func addWritable(tx *bolt.Tx, inode INode, filename string) error {
	return putNodeRepr(tx, inode, &NodeRepr{IsDir: false, LocalWritablePath: filename})
}

func (db *INodeDB) AddWritableLocalFile(tx *bolt.Tx, parent INode, name string, filename string) (INode, error) {
	err := assertValidDir(tx, parent)
	if err != nil {
		return InvalidINode, err
	}

	id, err := db.getNextFreeInode(tx)
	if err != nil {
		return InvalidINode, err
	}

	addWritable(tx, id, filename)
	addChild(tx, parent, id, name)

	return id, nil
}

func (db *INodeDB) GetDirContents(tx *bolt.Tx, id INode) ([]string, error) {
	err := assertValidDir(tx, id)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, 100)
	prefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(prefix, uint32(id))

	c := tx.Bucket(ChildNodeBucket).Cursor()

	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		name := string(k[len(prefix):])
		names = append(names, name)
	}

	if err != nil {
		return nil, err
	}

	return names, nil
}
