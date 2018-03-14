package sply2

import (
	"fmt"
	"log"
	"os"
	"time"

	bolt "github.com/coreos/bbolt"
)

type DataStore struct {
	path          string
	db            *INodeDB
	freezer       Freezer
	writableStore WriteableStore
	locker        *INodeLocker
}

func NewDataStore(path string) *DataStore {
	dbFilename := path + "/db"
	freezerPath := path + "/freezer"
	err := os.MkdirAll(freezerPath, 0700)
	if err != nil {
		log.Fatalf("%s: Could not create %s\n", err, freezerPath)
	}
	return &DataStore{path: path, db: NewINodeDB(dbFilename, 1000), writableStore: NewWritableStore(path), freezer: NewFreezer(freezerPath)}
}

func (d *DataStore) Close() {
	d.db.Close()
}

func (d *DataStore) GetDirContents(id INode) ([]string, error) {
	var names []string
	var err error

	err = d.db.view(func(tx *bolt.Tx) error {
		names, err = d.db.GetDirContents(tx, id)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return names, nil
}

func (d *DataStore) MakeDir(parent INode, name string) (INode, error) {
	d.locker.RLock(parent)
	defer d.locker.RUnlock(parent)

	var err error
	var inode INode

	err = d.db.update(func(tx *bolt.Tx) error {
		inode, err = d.db.AddDir(tx, parent, name)
		if err != nil {
			return err
		}
		return nil
	})

	return inode, err
}

func (d *DataStore) Remove(parent INode, name string) error {
	var err error

	err = d.db.update(func(tx *bolt.Tx) error {
		err = d.db.RemoveNode(tx, parent, name)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func (d *DataStore) AddRemoteURL(parent INode, name string, URL string) (INode, error) {
	var inode INode
	var err error

	etag, size, err := getURLAttr(URL)
	if err != nil {
		return InvalidINode, err
	}

	modTime := time.Now()

	err = d.db.update(func(tx *bolt.Tx) error {
		inode, err = d.db.AddRemoteURL(tx, parent, name, URL, etag, size, modTime)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return InvalidINode, err
	}

	return inode, err
}

func (d *DataStore) CreateWritable(parent INode, name string) (INode, WritableRef, error) {
	var inode INode
	var filename string
	var err error

	err = d.db.update(func(tx *bolt.Tx) error {
		filename, err = d.writableStore.NewFile()
		if err != nil {
			return err
		}

		inode, err = d.db.AddWritableLocalFile(tx, parent, name, filename)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return InvalidINode, nil, err
	}

	return inode, &WritableRefImp{filename}, err
}

func (d *DataStore) GetReadRef(inode INode) (ReadableRef, error) {
	var node *NodeRepr
	var err error

	err = d.db.view(func(tx *bolt.Tx) error {
		node, err = getNodeRepr(tx, inode)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if node.IsDir {
		return nil, IsDirErr
	}

	if node.LocalWritablePath != "" {
		return &WritableRefImp{node.LocalWritablePath}, nil
	}

	fmt.Printf("Getting ref\n")
	ref, err := d.freezer.GetRef(node.BID)
	fmt.Printf("Got ref: %s %s\n", ref, err)
	if ref == nil && err == nil {
		// do we have a remote to pull
		err := d.pullAndFreeze(node)
		if err != nil {
			return nil, err
		}

		ref, err = d.freezer.GetRef(node.BID)
	}

	if err != nil {
		return nil, err
	}

	return ref, nil
}

func (d *DataStore) pullAndFreeze(node *NodeRepr) error {
	remote := &RemoteURL{URL: node.URL, ETag: node.ETag, Length: node.Size}
	return d.freezer.AddBlock(node.BID, remote)
}
