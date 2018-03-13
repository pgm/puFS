package sply2

import (
	"io/ioutil"
	"os"
)

type WriteableStore interface {
	NewWriteRef() (WritableRef, error)
}

type WritableStoreImp struct {
	path string
}

type WritableRefImp struct {
	filename string
}

func NewWritableRefImp(name string) WritableRef {
	return &WritableRefImp{name}
}

func (w *WritableRefImp) Read(offset int64, dest []byte) (int, error) {
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

func (w *WritableRefImp) Write(offset int64, buffer []byte) (int, error) {
	f, err := os.OpenFile(w.filename, os.O_RDWR, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	_, err = f.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	return f.Write(buffer)
}

func (w *WritableRefImp) Release() {

}

func (w *WritableStoreImp) NewWriteRef() (WritableRef, error) {
	f, err := ioutil.TempFile(w.path, "dat")
	if err != nil {
		return nil, err
	}
	name := f.Name()
	f.Close()
	return NewWritableRefImp(name), nil
}
func NewWritableStore(path string) WriteableStore {
	return &WritableStoreImp{path}
}
