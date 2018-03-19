package sply2

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	bolt "github.com/coreos/bbolt"
)

var NABlock BlockID = BlockID{}

type Mount struct {
	mountPoint       INode
	leaseName        string
	lastLeaseRenewal time.Time
	BID              BlockID
}

type DataStore struct {
	path string

	db               *INodeDB
	freezer          Freezer
	writableStore    WriteableStore
	remoteRefFactory RemoteRefFactory
	mounts           []*Mount
	// locker        *INodeLocker
}

// default expiry is 48 hours
const DEFAULT_EXPIRY = 2 * 24 * time.Hour

// Renew leases every hour
const STALE_LEASE_DURATION = 1 * time.Hour

func genRandomString() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}

	return base64.URLEncoding.EncodeToString(b)
}

// need to call this periodically
func (d *DataStore) renewLeases() error {
	now := time.Now()
	staleThreshold := now.Add(-STALE_LEASE_DURATION)
	for _, mount := range d.mounts {
		if mount.lastLeaseRenewal.Before(staleThreshold) {
			err := d.remoteRefFactory.SetLease(mount.leaseName, now.Add(DEFAULT_EXPIRY), mount.BID)
			if err != nil {
				return err
			}
			mount.lastLeaseRenewal = now
		}
	}
	return nil
}

func (d *DataStore) MountByLabel(inode INode, label string) error {
	BID, err := d.remoteRefFactory.GetRoot(label)
	if err != nil {
		return err
	}
	return d.Mount(inode, BID)
}

// TODO: write unmount
func (d *DataStore) Mount(inode INode, BID BlockID) error {
	for _, m := range d.mounts {
		if m.mountPoint == inode {
			return AlreadyMountPointErr
		}
	}

	mount := &Mount{mountPoint: inode, leaseName: genRandomString(), lastLeaseRenewal: time.Now(), BID: BID}
	err := d.remoteRefFactory.SetLease(mount.leaseName, time.Now().Add(DEFAULT_EXPIRY), BID)
	if err != nil {
		return err
	}

	err = d.db.update(func(tx *bolt.Tx) error {
		err = d.db.MutateBIDForMount(tx, inode, BID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	d.mounts = append(d.mounts, mount)

	return nil
}

func NewDataStore(path string, remoteRefFactory RemoteRefFactory) *DataStore {
	dbFilename := path + "/db"
	freezerPath := path + "/freezer"
	err := os.MkdirAll(freezerPath, 0700)
	if err != nil {
		log.Fatalf("%s: Could not create %s\n", err, freezerPath)
	}
	return &DataStore{path: path,
		db:               NewINodeDB(dbFilename, 1000),
		writableStore:    NewWritableStore(path),
		freezer:          NewFreezer(freezerPath),
		remoteRefFactory: remoteRefFactory}
}

func (d *DataStore) Close() {
	d.db.Close()
}

func (d *DataStore) GetNodeID(parent INode, name string) (INode, error) {
	var inode INode
	var err error

	err = d.db.view(func(tx *bolt.Tx) error {
		inode, err = d.db.GetNodeID(tx, parent, name)
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

func (d *DataStore) LoadLazyChildren(tx *bolt.Tx, id INode) error {
	node, err := getNodeRepr(tx, id)
	if node.IsDir && node.IsDeferredChildFetch {
		if node.BID != NABlock {
			remoteRef, err := d.remoteRefFactory.GetRef(node)
			if err != nil {
				return err
			}
			err = d.freezer.AddBlock(node.BID, remoteRef)
			if err != nil {
				return err
			}
			fr, err := d.freezer.GetRef(node.BID)
			if err != nil {
				return err
			}
			buffer := make([]byte, node.Size)
			_, err = fr.Read(0, buffer)
			dec := gob.NewDecoder(bytes.NewReader(buffer))
			var dir Dir
			err = dec.Decode(&dir)
			if err != nil {
				panic(err)
			}
			err = d.db.addBlockLazyChildren(tx, id, dir.Entries)
			if err != nil {
				return err
			}
		} else {
			nodes, err := d.remoteRefFactory.GetChildNodes(node)
			if err != nil {
				return err
			}
			err = d.db.addRemoteLazyChildren(tx, id, nodes)
			if err != nil {
				return err
			}
		}

		node.IsDeferredChildFetch = false
		err = putNodeRepr(tx, id, node)
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func (d *DataStore) GetDirContents(id INode) ([]string, error) {
	var names []string
	var err error

	err = d.db.view(func(tx *bolt.Tx) error {
		err = d.LoadLazyChildren(tx, id)
		if err != nil {
			return err
		}

		names, err = d.db.GetDirNames(tx, id)
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

func (d *DataStore) AddRemoteGCS(parent INode, name string, bucket string, key string) (INode, error) {
	panic("unimp")
	// query GCS to figure out rest of parameters and delegate down to nodedb
}

func (d *DataStore) MakeDir(parent INode, name string) (INode, error) {
	// d.locker.RLock(parent)
	// defer d.locker.RUnlock(parent)

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
	err := d.db.update(func(tx *bolt.Tx) error {
		err := d.db.RemoveNode(tx, parent, name)
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

func freezeDir(tempDir string, freezer Freezer, dir *Dir) (BlockID, error) {
	f, err := ioutil.TempFile(tempDir, "dir")
	if err != nil {
		return NABlock, err
	}

	enc := gob.NewEncoder(f)
	err = enc.Encode(dir)
	if err != nil {
		panic(err)
	}
	f.Close()

	return freezer.AddFile(f.Name())
}

func (ds *DataStore) Push(inode INode, name string) error {
	rootBID, err := ds.Freeze(inode)
	if err != nil {
		return err
	}

	isPushed := func(BID BlockID) (bool, error) {
		return ds.freezer.IsPushed(BID)
	}

	blockList := make([]BlockID, 0, 100)
	err = ds.db.view(func(tx *bolt.Tx) error {
		err = collectUnpushed(ds.db, tx, inode, isPushed, &blockList)
		if err != nil {
			return err
		}
		return nil
	})

	// Could do this in parallel instead of sequentially
	for _, BID := range blockList {
		frozen, err := ds.freezer.GetRef(BID)
		if err != nil {
			return err
		}

		err = ds.remoteRefFactory.Push(BID, frozen)
		if err != nil {
			return err
		}
		frozen.Release()
	}

	err = ds.remoteRefFactory.SetRoot(name, rootBID)
	if err != nil {
		return err
	}

	// now that we've successfully pushed all data, update our lease to reflect the change if this was a mount point
	for _, mount := range ds.mounts {
		if mount.mountPoint == inode {
			mount.BID = rootBID
			err = ds.renewLeases()
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
}

func collectUnpushed(db *INodeDB, tx *bolt.Tx, inode INode, isPushed func(BlockID) (bool, error), blockList *[]BlockID) error {
	node, err := getNodeRepr(tx, inode)
	if err != nil {
		return err
	}

	skip, err := isPushed(node.BID)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}

	if node.IsDir {
		children, err := db.GetDirContents(tx, inode)
		if err != nil {
			return err
		}
		for _, child := range children {
			err = collectUnpushed(db, tx, child.ID, isPushed, blockList)
			if err != nil {
				return err
			}
		}
	}
	*blockList = append(*blockList, node.BID)
	return nil
}

func freeze(tempDir string, freezer Freezer, db *INodeDB, tx *bolt.Tx, inode INode) (BlockID, error) {
	node, err := getNodeRepr(tx, inode)
	if err != nil {
		return NABlock, err
	}

	if node.BID != NABlock {
		return node.BID, nil
	}

	if node.IsDir {
		// if this is a directory, then we need to compute blocks for all child inodes
		children, err := db.GetDirContents(tx, inode)
		dirTable := make([]DirEntry, 0, 100)
		for _, child := range children {
			//child.ID
			childNode, err := getNodeRepr(tx, child.ID)
			if err != nil {
				return NABlock, err
			}

			BID := childNode.BID
			if childNode.BID == NABlock {
				BID, err = freeze(tempDir, freezer, db, tx, child.ID)
				if err != nil {
					return NABlock, err
				}
			}

			dirTable = append(dirTable,
				DirEntry{
					Name:       child.Name,
					IsDir:      childNode.IsDir,
					Size:       childNode.Size,
					ModTime:    childNode.ModTime,
					BID:        BID,
					URL:        childNode.URL,
					ETag:       childNode.ETag,
					Bucket:     childNode.Bucket,
					Key:        childNode.Key,
					Generation: childNode.Generation})
		}

		BID, err := freezeDir(tempDir, freezer, &Dir{dirTable})
		if err != nil {
			return NABlock, err
		}

		node.BID = BID
		err = putNodeRepr(tx, inode, node)
		if err != nil {
			return NABlock, err
		}

		return BID, nil
	}

	if node.LocalWritablePath == "" {
		panic("LocalWritablePath is empty")
	}

	BID, err := freezer.AddFile(node.LocalWritablePath)
	err = putNodeRepr(tx, inode, node)
	if err != nil {
		return NABlock, err
	}

	node.BID = BID
	node.LocalWritablePath = ""
	err = putNodeRepr(tx, inode, node)
	if err != nil {
		return NABlock, err
	}

	return BID, nil
}

func (d *DataStore) Freeze(inode INode) (BlockID, error) {
	var err error
	var BID BlockID

	err = d.db.update(func(tx *bolt.Tx) error {
		BID, err = freeze(d.path, d.freezer, d.db, tx, inode)
		return nil
	})

	if err != nil {
		return NABlock, err
	}

	return BID, err
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
		err := d.pullIntoFreezer(node)
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

func (d *DataStore) pullIntoFreezer(node *NodeRepr) error {
	remote, err := d.remoteRefFactory.GetRef(node)
	if err != nil {
		return err
	}

	return d.freezer.AddBlock(node.BID, remote)
}
