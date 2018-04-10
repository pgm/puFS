package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

var NABlock BlockID = BlockID{}
var ValidNameRegExp *regexp.Regexp = regexp.MustCompile("^[A-Za-z0-9.~#$@ ()+_.-]+$")

type Mount struct {
	mountPoint       INode
	leaseName        string
	lastLeaseRenewal time.Time
	BID              BlockID
}

type DataStore struct {
	path              string
	mountTablePath    string
	db                *INodeDB
	freezer           Freezer
	remoteRefFactory2 RemoteRefFactory2
	writableStore     WriteableStore
	remoteRefFactory  RemoteRefFactory
	mounts            []*Mount

	// locker        *INodeLocker

	networkClient NetworkClient

	lazyDirBlockTimes      *Population
	lazyDirFetchChildTimes *Population
}

// default expiry is 48 hours
const DEFAULT_EXPIRY = 2 * 24 * time.Hour

// Renew leases every hour
const STALE_LEASE_DURATION = 1 * time.Hour

///////////////////////////

func (d *DataStore) SetClients(networkClient NetworkClient) {
	d.networkClient = networkClient
}

func NewDataStore(storagePath string, remoteRefFactory RemoteRefFactory, rrf2 RemoteRefFactory2, freezerKV KVStore, nodeKV KVStore) *DataStore {
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

	chunkSize := 200 * 1024
	ds := &DataStore{path: storagePath,
		mountTablePath:         mountTablePath,
		db:                     NewINodeDB(10000000, nodeKV),
		writableStore:          NewWritableStore(writablePath),
		remoteRefFactory2:      rrf2,
		freezer:                NewFreezer(freezerPath, freezerKV, rrf2, chunkSize),
		remoteRefFactory:       remoteRefFactory,
		lazyDirBlockTimes:      NewPopulation(1000),
		lazyDirFetchChildTimes: NewPopulation(1000)}

	return ds
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

func (d *DataStore) MountByLabel(ctx context.Context, inode INode, label string) error {
	err := validateName(label)
	if err != nil {
		return err
	}

	BID, err := d.remoteRefFactory.GetRoot(ctx, label)
	if err != nil {
		return err
	}

	return d.Mount(ctx, inode, BID)
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
func (d *DataStore) renewLeases(ctx context.Context) error {
	now := time.Now()
	staleThreshold := now.Add(-STALE_LEASE_DURATION)
	for _, mount := range d.mounts {
		if mount.lastLeaseRenewal.Before(staleThreshold) {
			err := d.remoteRefFactory.SetLease(ctx, mount.leaseName, now.Add(DEFAULT_EXPIRY), mount.BID)
			if err != nil {
				return err
			}
			mount.lastLeaseRenewal = now
		}
	}
	return nil
}

func (d *DataStore) Unmount(ctx context.Context, inode INode) error {
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
	err := d.remoteRefFactory.SetLease(ctx, foundMount.leaseName, time.Now(), foundMount.BID)
	if err != nil {
		return err
	}

	d.mounts = newMounts
	d.persistMountTable()

	return nil
}

func (d *DataStore) Mount(ctx context.Context, inode INode, BID BlockID) error {
	if BID == NABlock {
		panic("Cannot mount invalid block")
	}

	for _, m := range d.mounts {
		if m.mountPoint == inode {
			return AlreadyMountPointErr
		}
	}

	mount := &Mount{mountPoint: inode, leaseName: genRandomString(), lastLeaseRenewal: time.Now(), BID: BID}
	err := d.remoteRefFactory.SetLease(ctx, mount.leaseName, time.Now().Add(DEFAULT_EXPIRY), BID)
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

func (d *DataStore) GetNodeID(ctx context.Context, parent INode, name string) (INode, error) {
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
		err = d.loadLazyChildren(ctx, tx, parent)
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

func (d *DataStore) GetDirContents(ctx context.Context, id INode) ([]*DirEntryWithID, error) {
	var err error
	var entries []*DirEntryWithID

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, id)
		if err != nil {
			return err
		}

		var names []NameINode
		names, err = d.db.GetDirContents(tx, id, true)
		if err != nil {
			return err
		}

		entries = make([]*DirEntryWithID, 0, len(names))
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

			entry := &DirEntryWithID{ID: n.ID,
				DirEntry: DirEntry{
					Name:    n.Name,
					IsDir:   node.IsDir,
					Size:    size,
					ModTime: mtime,

					BID: node.BID,

					RemoteSource: node.RemoteSource}}
			entries = append(entries, entry)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return entries, nil
}

type refReader struct {
	ctx context.Context
	fr  FrozenRef
}

func (r *refReader) Read(p []byte) (n int, err error) {
	return r.fr.Read(r.ctx, p)
}

func makeReader(ctx context.Context,
	fr FrozenRef) *refReader {
	return &refReader{ctx, fr}
}

func (d *DataStore) loadLazyChildren(ctx context.Context, tx RWTx, id INode) error {
	node, err := getNodeRepr(tx, id)
	if err != nil {
		return err
	}
	if node.IsDir && node.IsDeferredChildFetch {
		if node.BID != NABlock {
			startTime := time.Now()

			remoteSource, err := d.remoteRefFactory.GetBlockSource(ctx, node.BID)
			if err != nil {
				return err
			}
			if remoteSource == nil {
				panic("got a nil source for block")
			}
			fmt.Printf("remoteSource=%v\n", remoteSource)

			remoteRef := d.remoteRefFactory2.GetRef(remoteSource)
			if err != nil {
				return err
			}
			err = d.freezer.AddBlock(ctx, node.BID, remoteRef)
			if err != nil {
				return err
			}
			fr, err := d.freezer.GetRef(node.BID)
			if err != nil {
				return err
			}

			buffer, err := ioutil.ReadAll(makeReader(ctx, fr))
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

			endTime := time.Now()
			d.lazyDirBlockTimes.Add(int(endTime.Sub(startTime) / time.Millisecond))
		} else {
			startTime := time.Now()
			if node.RemoteSource == nil {
				panic("No BID set nor remote source")
			}
			remote := d.remoteRefFactory2.GetRef(node.RemoteSource)
			nodes, err := remote.GetChildNodes(ctx)
			if err != nil {
				return err
			}
			err = d.db.addRemoteLazyChildren(tx, id, nodes)
			if err != nil {
				return err
			}
			endTime := time.Now()
			d.lazyDirFetchChildTimes.Add(int(endTime.Sub(startTime) / time.Millisecond))
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

func (d *DataStore) AddRemoteGCS(ctx context.Context, parent INode, name string, bucket string, key string) (INode, error) {
	var inode INode

	err := validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	attrs, err := d.networkClient.GetGCSAttr(ctx, bucket, key)
	if err != nil {
		return InvalidINode, err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, parent)
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

func (d *DataStore) MakeDir(ctx context.Context, parent INode, name string) (INode, error) {
	// d.locker.RLock(parent)
	// defer d.locker.RUnlock(parent)
	err := validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	var inode INode

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, parent)
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

func (d *DataStore) Rename(ctx context.Context, srcParent INode, srcName string, dstParent INode, dstName string) error {
	err := validateName(srcName)
	if err != nil {
		return err
	}

	err = validateName(dstName)
	if err != nil {
		return err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, srcParent)
		if err != nil {
			return err
		}

		err = d.loadLazyChildren(ctx, tx, dstParent)
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

func (d *DataStore) Remove(ctx context.Context, parent INode, name string) error {
	err := validateName(name)
	if err != nil {
		return err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, parent)
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

func (d *DataStore) AddRemoteURL(ctx context.Context, parent INode, name string, URL string) (INode, error) {
	var inode INode
	var err error

	err = validateName(name)
	if err != nil {
		return InvalidINode, err
	}

	attrs, err := d.networkClient.GetHTTPAttr(ctx, URL)
	if err != nil {
		return InvalidINode, err
	}

	modTime := time.Now()

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, parent)
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

func (d *DataStore) CreateWritable(ctx context.Context, parent INode, name string) (INode, WritableRef, error) {
	var inode INode
	var filename string
	var err error

	err = validateName(name)
	if err != nil {
		return InvalidINode, nil, err
	}

	err = d.db.update(func(tx RWTx) error {
		err = d.loadLazyChildren(ctx, tx, parent)
		if err != nil {
			return err
		}

		if d.db.NodeExists(tx, parent, name) {
			return ExistsErr
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

func (ds *DataStore) Push(ctx context.Context, inode INode, name string) error {
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

		err = ds.remoteRefFactory.Push(ctx, BID, frozen)
		if err != nil {
			return err
		}
		frozen.Release()
	}

	if rootBID == NABlock {
		panic("Cannot set root to invalid block")
	}

	err = ds.remoteRefFactory.SetRoot(ctx, name, rootBID)
	if err != nil {
		return err
	}

	// now that we've successfully pushed all data, update our lease to reflect the change if this was a mount point
	for _, mount := range ds.mounts {
		if mount.mountPoint == inode {
			mount.BID = rootBID
			err = ds.renewLeases(ctx)
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
					Name:         child.Name,
					IsDir:        childNode.IsDir,
					Size:         childNode.Size,
					ModTime:      childNode.ModTime,
					BID:          childNode.BID,
					RemoteSource: childNode.RemoteSource})
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

func (d *DataStore) GetWritableRef(ctx context.Context, inode INode, truncate bool) (WritableRef, error) {
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

	if node.LocalWritablePath == "" {
		return nil, NotWritableErr
	}

	if truncate {
		fp, err := os.OpenFile(node.LocalWritablePath, os.O_TRUNC|os.O_RDWR, 0777)
		if err != nil {
			return nil, err
		}
		fp.Close()
	}

	return &WritableRefImp{node.LocalWritablePath, 0}, nil
}

func (d *DataStore) GetReadRef(ctx context.Context, inode INode) (Reader, error) {
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
	if err == UnknownBlockID {
		// do we have a remote to pull
		err = d.pullIntoFreezer(ctx, node)
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

func (d *DataStore) pullIntoFreezer(ctx context.Context, node *NodeRepr) error {
	var remoteSource interface{}
	var err error
	if node.RemoteSource == nil {
		remoteSource, err = d.remoteRefFactory.GetBlockSource(ctx, node.BID)
	} else {
		remoteSource = node.RemoteSource
	}
	if err != nil {
		return err
	}
	remote := d.remoteRefFactory2.GetRef(remoteSource)
	err = d.freezer.AddBlock(ctx, node.BID, remote)

	return err
}

func validateName(name string) error {
	if name != "." && name != ".." && ValidNameRegExp.MatchString(name) {
		return nil
	}
	return InvalidCharFilenameErr
}

func (d *DataStore) GetAttr(ctx context.Context, inode INode) (*NodeRepr, error) {
	var node *NodeRepr
	var err error

	err = d.db.view(func(tx RTx) error {
		node, err = getNodeRepr(tx, inode)
		return err
	})

	return node, err
}

func (ds *DataStore) SplitPath(ctx context.Context, fullPath string) (INode, string, error) {
	var err error
	if fullPath[0] != '/' {
		return InvalidINode, "", fmt.Errorf("Invalid path: %s", fullPath)
	}

	if fullPath == "/" {
		return RootINode, ".", nil
	}

	components := strings.Split(fullPath[1:], "/")
	parent := INode(RootINode)
	for _, c := range components[0 : len(components)-1] {
		parent, err = ds.GetNodeID(ctx, parent, c)
		if err != nil {
			return InvalidINode, "", err
		}
	}
	return parent, components[len(components)-1], nil
}

func (ds *DataStore) PrintStats() {
	if ps, ok := ds.freezer.(HasPrintStats); ok {
		ps.PrintStats()
	}

	p, ok := ds.lazyDirBlockTimes.Percentiles([]float32{50, 90, 95})
	if ok {
		fmt.Printf("Time taken fetching dirs from block (%d): %d, %d, %d\n", ds.lazyDirBlockTimes.Count(), p[0], p[1], p[2])
	}
	p, ok = ds.lazyDirFetchChildTimes.Percentiles([]float32{50, 90, 95})
	if ok {
		fmt.Printf("Time taken fetching dirs get child calls (%d): %d, %d, %d\n", ds.lazyDirFetchChildTimes.Count(), p[0], p[1], p[2])
	}
}
