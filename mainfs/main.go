package main

import (
	"context"
	"encoding/gob"
	"log"
	"os"
	"path"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2"
	"github.com/pgm/sply2/core"
	"github.com/pgm/sply2/fs"
	"github.com/pgm/sply2/remote"
	"google.golang.org/api/option"
)

func NewDataStore(dir string) *core.DataStore {
	var err error
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0700)
	}
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile("/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := remote.NewRemoteRefFactory(client, bucketName, "blocks/")
	ds := core.NewDataStore(dir, f, f, sply2.NewBoltDB(path.Join(dir, "freezer.db"), [][]byte{core.ChunkStat}),
		sply2.NewBoltDB(path.Join(dir, "nodes.db"), [][]byte{core.ChildNodeBucket, core.NodeBucket}))
	return ds
}

func GobRegisterTypes() {
	var x *core.GCSObjectSource
	gob.Register(core.BlockID{})
	gob.Register(x)
}

func main() {
	GobRegisterTypes()
	args := os.Args
	mountPoint := args[1]
	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		err = os.MkdirAll(mountPoint, 0777)
		if err != nil {
			log.Fatalf("Could not create directory %s", mountPoint)
		}
	}

	repo := path.Join(path.Dir(mountPoint), ".sply2-data-"+path.Base(mountPoint))
	ds := NewDataStore(repo)
	fs.Mount(mountPoint, ds)
}
