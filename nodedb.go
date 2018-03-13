package sply2

import "time"

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
