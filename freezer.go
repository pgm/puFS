package sply2

import (
	"encoding/hex"
	"fmt"
	"os"
)

// func (d *DataStore) ensureFrozenPopulated(node *Node) error {
// 	if node.Frozen != nil {
// 		return nil
// 	}

// 	err := ensureRemotePopulated(node)
// 	if err != nil {
// 		return err
// 	}

// 	frozen, err := d.freeze(node)
// 	if err != nil {
// 		return err
// 	}

// 	node.Frozen = frozen

// 	return nil
// }

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{name}
}

// func (d *DataStore) freeze(node *Node) (FrozenRef, error) {
// 	f, err := ioutil.TempFile(d.path, "frozen")
// 	if err != nil {
// 		return nil, err
// 	}

// 	defer f.Close()

// 	err = node.Remote.Copy(0, node.Size, f)
// 	if err != nil {
// 		return nil, err
// 	}

// 	x := NewFrozenRef(f.Name())
// 	return x, nil
// }

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

type FreezerImp struct {
	path string
	//	blocks map[BlockID]string
}

func NewFreezer(path string) *FreezerImp {
	return &FreezerImp{path}
}

func (f *FreezerImp) getPath(BID BlockID) string {
	filename := fmt.Sprintf("%s/%s", f.path, hex.EncodeToString(BID[:]))
	return filename
}

func (f *FreezerImp) GetRef(BID BlockID) (FrozenRef, error) {
	filename := f.getPath(BID)
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		fmt.Printf("Path %s does not exists\n", filename)
		return nil, nil
	}
	fmt.Printf("Path %s exists\n", filename)

	return &FrozenRefImp{filename}, nil
}

func (f *FreezerImp) AddBlock(BID BlockID, remoteRef RemoteRef) error {
	filename := f.getPath(BID)

	fi, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		return err
	}

	defer fi.Close()

	err = remoteRef.Copy(0, remoteRef.GetSize(), fi)
	if err != nil {
		return err
	}

	return nil
}
