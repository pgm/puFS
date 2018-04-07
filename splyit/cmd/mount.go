package cmd

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

	"github.com/spf13/cobra"
)

var remoteLabel string

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount the directory",
	Long:  `More desc`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		GobRegisterTypes()
		mountPoint := args[0]
		if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
			err = os.MkdirAll(mountPoint, 0777)
			if err != nil {
				log.Fatalf("Could not create directory %s", mountPoint)
			}
		}

		repo := path.Join(path.Dir(mountPoint), ".sply2-data-"+path.Base(mountPoint))
		ds := NewDataStore(repo)

		if remoteLabel != "" {
			ctx := context.Background()
			err := ds.MountByLabel(ctx, core.RootINode, remoteLabel)
			if err != nil {
				panic(err)
			}
		}

		fs.Mount(mountPoint, ds)
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mountCmd.PersistentFlags().String("foo", "", "A help for foo")
	mountCmd.Flags().StringVarP(&remoteLabel, "remote", "r", "", "Remote directory to mount as the root")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mountCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

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
	ds.SetClients(f)
	return ds
}

func GobRegisterTypes() {
	var x *core.GCSObjectSource
	gob.Register(core.BlockID{})
	gob.Register(x)
}
