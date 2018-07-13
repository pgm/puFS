package cmd

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/spf13/viper"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2"
	"github.com/pgm/sply2/core"
	"github.com/pgm/sply2/fs"
	"github.com/pgm/sply2/remote"
	"google.golang.org/api/option"

	"github.com/spf13/cobra"
)

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount [repoPath] [mountPoint]",
	Short: "Mount the directory",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]
		mountPoint := args[1]

		if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
			err = os.MkdirAll(mountPoint, 0777)
			if err != nil {
				log.Fatalf("Could not create directory %s", mountPoint)
			}
		}

		ds := NewDataStore(repoPath, false)

		ticker := time.NewTicker(5 * time.Second)

		go (func() {
			for {
				_, ok := <-ticker.C
				if ok {
					ds.PrintStats()
				} else {
					return
				}
			}
		})()

		fs.Mount(mountPoint, ds)
		ticker.Stop()
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mountCmd.PersistentFlags().String("foo", "", "A help for foo")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mountCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type NewDataStoreOptions struct {
	mountAsRoot string
}

type NewDataStoreOption func(o *NewDataStoreOptions)

func MountAsRoot(path string) func(o *NewDataStoreOptions) {
	return func(o *NewDataStoreOptions) {
		o.mountAsRoot = path
	}
}

func NewDataStore(dir string, createIfMissing bool, options ...NewDataStoreOption) *core.DataStore {
	bucketName := viper.GetString("bucket")
	keyPrefix := viper.GetString("keyprefix")
	credentialsPath := viper.GetString("credentials")
	var optionStruct NewDataStoreOptions

	for _, option := range options {
		option(&optionStruct)
	}

	fmt.Printf("bucket: %s\nkeyPrefix: %s\nmountAsRoot: %s\n", bucketName, keyPrefix, optionStruct.mountAsRoot)

	var err error
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if createIfMissing {
			err = os.MkdirAll(dir, 0700)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatalf("No repo at %s", dir)
		}
	}

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(credentialsPath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	dsOptions := make([]core.DataStoreOption, 0)
	dsOptions = append(dsOptions, core.OpenExisting())
	if optionStruct.mountAsRoot != "" {
		gcsmatch := GCSUrlExp.FindStringSubmatch(optionStruct.mountAsRoot)
		if gcsmatch != nil {
			bucket := gcsmatch[1]
			key := gcsmatch[2]
			dsOptions = append(dsOptions, core.DataStoreWithGCSRoot(bucket, key))
		} else {
			log.Fatalf("GCS Root was not parsable: %s", optionStruct.mountAsRoot)
		}
	}

	f := remote.NewRemoteRefFactory(client, bucketName, keyPrefix)
	ds, err := core.NewDataStore(dir, f, f,
		sply2.NewBoltDB(path.Join(dir, "freezer.db"),
			[][]byte{core.ChunkStat}),
		sply2.NewBoltDB(path.Join(dir, "nodes.db"),
			[][]byte{core.ChildNodeBucket, core.NodeBucket}),
		dsOptions...,
	)

	if err != nil {
		log.Fatalf("Failed to create DataStore: %v", err)
	}

	ds.SetClients(f)

	return ds
}

func GobRegisterTypes() {
	var x *core.GCSObjectSource
	gob.Register(core.BlockID{})
	gob.Register(x)
}
