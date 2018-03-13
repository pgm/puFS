package sply2

import (
	"io/ioutil"
	"os"
)

func (d *DataStore) ensureFrozenPopulated(node *Node) error {
	if node.Frozen != nil {
		return nil
	}

	err := ensureRemotePopulated(node)
	if err != nil {
		return err
	}

	frozen, err := d.freeze(node)
	if err != nil {
		return err
	}

	node.Frozen = frozen

	return nil
}

func NewFrozenRef(name string) FrozenRef {
	return &FrozenRefImp{name}
}

func (d *DataStore) freeze(node *Node) (FrozenRef, error) {
	f, err := ioutil.TempFile(d.path, "frozen")
	if err != nil {
		return nil, err
	}

	defer f.Close()

	err = node.Remote.Copy(0, node.Size, f)
	if err != nil {
		return nil, err
	}

	x := NewFrozenRef(f.Name())
	return x, nil
}

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
