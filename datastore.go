package sply2

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
