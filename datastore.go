package sply2

import bolt "github.com/coreos/bbolt"

type DataStore struct {
	path          string
	db            *INodeDB
	writableStore WriteableStore
}

func NewDataStore(path string) *DataStore {
	dbFilename := path + "/db"
	return &DataStore{path: path, db: NewINodeDB(dbFilename, 1000), writableStore: NewWritableStore(path)}
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

// func (d *DataStore) AddRemoteURL(parent INode, name string, URL string) (INode, error) {
// 	id, _, err := d.db.AddRemoteURL(parent, name, URL)
// 	return id, err
// }

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
	var filename string

	err := d.db.view(func(tx *bolt.Tx) error {
		node, err := getNodeRepr(tx, inode)
		if err != nil {
			return err
		}
		filename = node.LocalWritablePath
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &WritableRefImp{filename}, nil
}
