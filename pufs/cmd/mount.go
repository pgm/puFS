package cmd

import (
	"context"
	"encoding/gob"
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

var remoteLabel string

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount the directory",
	Long:  `More desc`,
	Args:  cobra.ExactArgs(1),
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

		if remoteLabel != "" {
			ctx := context.Background()
			err := ds.MountByLabel(ctx, core.RootINode, remoteLabel)
			if err != nil {
				panic(err)
			}
		}

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
	mountCmd.Flags().StringVarP(&remoteLabel, "remote", "r", "", "Remote directory to mount as the root")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mountCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func NewDataStore(dir string, createIfMissing bool) *core.DataStore {
	bucketName := viper.GetString("bucket")
	keyPrefix := viper.GetString("keyprefix")
	credentialsPath := viper.GetString("credentials")

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
	f := remote.NewRemoteRefFactory(client, bucketName, keyPrefix)
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
