package main

import (
	"context"
	"log"
	"os"
	"path"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2"
	"github.com/pgm/sply2/core"
	"github.com/pgm/sply2/fs"
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
	//	projectID := "gcs-test-1136"

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile("/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := sply2.NewRemoteRefFactory(client, bucketName, "blocks/")
	// if err != nil {
	// 	log.Fatalf("Failed to create remote: %v", err)
	// }
	ds := core.NewDataStore(dir, f, sply2.NewBoltDB(path.Join(dir, "freezer.db"), [][]byte{core.ChunkStat}),
		sply2.NewBoltDB(path.Join(dir, "nodes.db"), [][]byte{core.ChildNodeBucket, core.NodeBucket}))
	ds.SetClients(f, f)
	return ds
}

func main() {
	args := os.Args
	mountPoint := args[1]
	repo := args[2]
	ds := NewDataStore(repo)
	fs.Mount(mountPoint, ds)
}
