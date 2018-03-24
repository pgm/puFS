package core

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"time"
)

var NABlock BlockID = BlockID{}
var ValidNameRegExp *regexp.Regexp = regexp.MustCompile("[A-Za-z0-9.~#$@ ()+_-]+")

type Mount struct {
	mountPoint       INode
	leaseName        string
	lastLeaseRenewal time.Time
	BID              BlockID
}

type DataStore struct {
	path             string
	mountTablePath   string
	db               *INodeDB
	freezer          Freezer
	writableStore    WriteableStore
	remoteRefFactory RemoteRefFactory
	mounts           []*Mount
	// locker        *INodeLocker

	httpClient HTTPClient
	gcsClient  GCSClient
}

// default expiry is 48 hours
const DEFAULT_EXPIRY = 2 * 24 * time.Hour

// Renew leases every hour
const STALE_LEASE_DURATION = 1 * time.Hour

///////////////////////////

func (d *DataStore) SetClients(httpClient HTTPClient, gcsClient GCSClient) {
	d.httpClient = httpClient
	d.gcsClient = gcsClient
}

func NewDataStore(storagePath string, remoteRefFactory RemoteRefFactory, freezerKV KVStore, nodeKV KVStore) *DataStore {
	freezerPath := path.Join(storagePath, "freezer")
	writablePath := path.Join(storagePath, "writable")
	err := os.MkdirAll(freezerPath, 0700)
	if err != nil {
		log.Fatalf("%s: Could not create %s\n", err, freezerPath)
	}
	err = os.MkdirAll(writablePath, 0700)
	if err != nil {
		log.Fatalf("%s: Could not create %s\n", err, writablePath)
	}

	mountTablePath := path.Join(storagePath, "mounts.gob")
	var mounts []*Mount
	if _, err = os.Stat(mountTablePath); !os.IsNotExist(err) {
		f, err := os.Open(mountTablePath)
		defer f.Close()

		dec := gob.NewDecoder(f)
		err = dec.Decode(&mounts)
		if err != nil {
			log.Fatalf("%s: Could not read %s", err, mountTablePath)
		}
	}

	return &DataStore{path: storagePath,
		mountTablePath:   mountTablePath,
		db:               NewINodeDB(1000, nodeKV),
		writableStore:    NewWritableStore(writablePath),
		freezer:          NewFreezer(freezerPath, freezerKV),
		remoteRefFactory: remoteRefFactory}
}

func (d *DataStore) persistMountTable() {
	f, err := os.OpenFile(d.mountTablePath, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		log.Fatalf("%s: Could not open %s for writing", err, d.mountTablePath)
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(d.mounts)
	if err != nil {
		log.Fatalf("%s: Could not write mount table", err)
	}
}

func (d *DataStore) Close() {
	d.db.Close()
}

///////////////////////////

// Mount

func (d *DataStore) MountByLabel(inode INode, label string) error {
	err := validateName(label)
	if err != nil {
		return err
	}

	BID, err := d.remoteRefFactory.GetRoot(label)
	if err != nil {
		return err
	}

	return d.Mount(inode, BID)
}

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

func (d *DataStore) Unmount(inode INode) error {
	var foundMount *Mount
	newMounts := make([]*Mount, 0, len(d.mounts))

	// copy mounts except for the one unmounted
	for _, m := range d.mounts {
		if m.mountPoint == inode {
			foundMount = m
		} else {
			newMounts = append(newMounts, m)
		}
	}

	if foundMount == nil {
		return NoSuchMountErr
	}

	// record the lease is now expired
	err := d.remoteRefFactory.SetLease(foundMount.leaseName, time.Now(), foundMount.BID)
	if err != nil {
		return err
	}

	d.mounts = newMounts
	d.persistMountTable()

	return nil
}

func (d *DataStore) Mount(inode INode, BID BlockID) error {
	if BID == NABlock {
		panic("Cannot mount invalid block")
	}

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

	err = d.db.update(func(tx RWTx) error {
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
	d.persistMountTable()

	return nil
}

func (d *DataStore) GetParent(inode INode) (INode, error) {
	var err error
	var node *NodeRepr

	err = d.db.view(func(tx RTx) error {
		node, err = getNodeRepr(tx, inode)
		return err
	})

	if err != nil {
		return InvalidINode, err
	}

	return node.ParentINode, nil
}

func (d *DataStore) GetNodeID(parent INode, name string) (INode, error) {
	var inode INode
	var err error

	if name == "." {
		return parent, nil
	}

	if name == ".." {
		return d.GetParent(parent)
	}

	err = validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

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

// func (db *INodeDB) GetDirNames(tx RTx, id INode) ([]string, error) {
// 	nn, err := db.GetDirContents(tx, id)
// 	if err != nil {
// 		return nil, err
// 	}

// 	names := make([]string, 0, 100)
// 	for _, t := range nn {
// 		names = append(names, t.Name)
// 	}

// 	return names, nil
// }

func (d *DataStore) GetDirContents(id INode) ([]*DirEntry, error) {
	var err error
	var entries []*DirEntry

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, id)
		if err != nil {
			return err
		}

		var names []NameINode
		names, err = d.db.GetDirContents(tx, id, true)
		if err != nil {
			return err
		}

		entries = make([]*DirEntry, 0, len(names))
		for _, n := range names {
			var node *NodeRepr
			node, err = getNodeRepr(tx, n.ID)

			size := node.Size
			mtime := node.ModTime

			if node.LocalWritablePath != "" {
				fi, err := os.Stat(node.LocalWritablePath)
				if err != nil {
					return err
				}

				size = fi.Size()
				mtime = fi.ModTime()
			}

			entries = append(entries, &DirEntry{Name: n.Name,
				IsDir:   node.IsDir,
				Size:    size,
				ModTime: mtime,

				BID: node.BID,

				URL:  node.URL,
				ETag: node.ETag,

				Bucket:     node.Bucket,
				Key:        node.Key,
				Generation: node.Generation})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (d *DataStore) loadLazyChildren(tx RWTx, id INode) error {
	node, err := getNodeRepr(tx, id)
	if err != nil {
		return err
	}
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

			buffer, err := ioutil.ReadAll(fr)
			// buffer := make([]byte, node.Size)
			// _, err = fr.Read(buffer)
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

func (d *DataStore) AddRemoteGCS(parent INode, name string, bucket string, key string) (INode, error) {
	var inode INode

	err := validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	attrs, err := d.gcsClient.GetGCSAttr(bucket, key)
	if err != nil {
		return InvalidINode, err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

		inode, err = d.db.AddRemoteGCS(tx, parent, name, bucket, key, attrs.Generation, attrs.Size, attrs.ModTime, attrs.IsDir)
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

func (d *DataStore) MakeDir(parent INode, name string) (INode, error) {
	// d.locker.RLock(parent)
	// defer d.locker.RUnlock(parent)
	err := validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	var inode INode

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

		inode, err = d.db.AddDir(tx, parent, name)
		if err != nil {
			return err
		}
		return nil
	})

	return inode, err
}

func (d *DataStore) PrintDebug() {
	d.db.view(func(tx RTx) error {
		printDbStats(tx)
		return nil
	})
}

func (d *DataStore) Rename(srcParent INode, srcName string, dstParent INode, dstName string) error {
	err := validateName(srcName)
	if err != nil {
		return err
	}

	err = validateName(dstName)
	if err != nil {
		return err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, srcParent)
		if err != nil {
			return err
		}

		err = d.loadLazyChildren(tx, dstParent)
		if err != nil {
			return err
		}

		// check to see if destination exists
		_, err = d.db.GetNodeID(tx, dstParent, dstName)
		if err != NoSuchNodeErr {
			if err == nil {
				// a file already exists with the destination's name
				err = d.db.RemoveNode(tx, dstParent, dstName)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}

		// if we've reached here, we can safely rename
		return d.db.Rename(tx, srcParent, srcName, dstParent, dstName)
	})

	return err
}

func (d *DataStore) Remove(parent INode, name string) error {
	err := validateName(name)
	if err != nil {
		return err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

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

	err = validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	attrs, err := d.httpClient.GetHTTPAttr(URL)
	if err != nil {
		return InvalidINode, err
	}

	modTime := time.Now()

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

		inode, err = d.db.AddRemoteURL(tx, parent, name, URL, attrs.ETag, attrs.Size, modTime)
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

	err = validateName(name)
	if err != nil {
		return InvalidINode, nil, err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(tx, parent)
		if err != nil {
			return err
		}

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

	return inode, &WritableRefImp{filename, 0}, err
}

func freezeDir(tempDir string, freezer Freezer, dir *Dir) (*NewBlock, error) {
	// fmt.Printf("freezeDir\n")
	f, err := ioutil.TempFile(tempDir, "dir")
	if err != nil {
		return nil, err
	}

	enc := gob.NewEncoder(f)
	err = enc.Encode(dir)
	if err != nil {
		panic(err)
	}
	f.Close()

	newBlock, err := freezer.AddFile(f.Name())
	return newBlock, err
}

func (ds *DataStore) Push(inode INode, name string) error {
	err := validateName(name)
	if err != nil {
		return err
	}

	rootBID, err := ds.Freeze(inode)
	if err != nil {
		return err
	}

	isPushed := func(BID BlockID) (bool, error) {
		return ds.freezer.IsPushed(BID)
	}

	blockList := make([]BlockID, 0, 100)
	err = ds.db.view(func(tx RTx) error {
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

	if rootBID == NABlock {
		panic("Cannot set root to invalid block")
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

func collectUnpushed(db *INodeDB, tx RTx, inode INode, isPushed func(BlockID) (bool, error), blockList *[]BlockID) error {
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
		children, err := db.GetDirContents(tx, inode, false)
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

func freeze(tempDir string, freezer Freezer, db *INodeDB, tx RWTx, inode INode) (*NodeRepr, error) {
	// fmt.Printf("freezing %d\n", inode)
	node, err := getNodeRepr(tx, inode)
	if err != nil {
		return nil, err
	}

	if node.BID != NABlock {
		return node, nil
	}

	if node.IsDir {
		// fmt.Printf("inode %d is a dir\n", inode)
		// if this is a directory, then we need to compute blocks for all child inodes
		children, err := db.GetDirContents(tx, inode, false)
		if err != nil {
			return nil, err
		}

		// fmt.Printf("Node %d has %d children\n", inode, len(children))
		dirTable := make([]DirEntry, 0, 100)
		for _, child := range children {
			//child.ID
			childNode, err := getNodeRepr(tx, child.ID)
			if err != nil {
				return nil, err
			}

			if childNode.BID == NABlock {
				childNode, err = freeze(tempDir, freezer, db, tx, child.ID)
				if err != nil {
					return nil, err
				}
			}

			dirTable = append(dirTable,
				DirEntry{
					Name:       child.Name,
					IsDir:      childNode.IsDir,
					Size:       childNode.Size,
					ModTime:    childNode.ModTime,
					BID:        childNode.BID,
					URL:        childNode.URL,
					ETag:       childNode.ETag,
					Bucket:     childNode.Bucket,
					Key:        childNode.Key,
					Generation: childNode.Generation})
		}

		newBlock, err := freezeDir(tempDir, freezer, &Dir{dirTable})
		if err != nil {
			return nil, err
		}

		node.BID = newBlock.BID
		node.Size = newBlock.Size
		node.ModTime = newBlock.ModTime
		err = putNodeRepr(tx, inode, node)
		if err != nil {
			return nil, err
		}

		return node, nil
	}

	if node.LocalWritablePath == "" {
		panic("LocalWritablePath is empty")
	}
	// fmt.Printf("inode %d is a writable file: %s\n", inode, node.LocalWritablePath)

	newBlock, err := freezer.AddFile(node.LocalWritablePath)

	node.BID = newBlock.BID
	node.Size = newBlock.Size
	node.ModTime = newBlock.ModTime
	node.LocalWritablePath = ""
	if node.BID == NABlock {
		panic("Frozen block needs valid BlockID")
	}
	err = putNodeRepr(tx, inode, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *DataStore) Freeze(inode INode) (BlockID, error) {
	var err error
	var BID BlockID

	err = d.db.update(func(tx RWTx) error {
		var newNode *NodeRepr
		newNode, err = freeze(d.path, d.freezer, d.db, tx, inode)
		BID = newNode.BID
		return nil
	})

	if err != nil {
		return NABlock, err
	}

	return BID, err
}

func (d *DataStore) GetReadRef(inode INode) (io.ReadSeeker, error) {
	var node *NodeRepr
	var err error

	err = d.db.view(func(tx RTx) error {
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
		return &WritableRefImp{node.LocalWritablePath, 0}, nil
	}

	// fmt.Printf("Getting ref\n")
	ref, err := d.freezer.GetRef(node.BID)
	// fmt.Printf("Got ref: %s %s\n", ref, err)
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
	// fmt.Printf("Pulling %s/%s\n", node.Bucket, node.Key)
	remote, err := d.remoteRefFactory.GetRef(node)
	if err != nil {
		return err
	}

	return d.freezer.AddBlock(node.BID, remote)
}

func validateName(name string) error {
	if name != "." && name != ".." && ValidNameRegExp.MatchString(name) {
		return nil
	}
	return InvalidCharFilenameErr
}
