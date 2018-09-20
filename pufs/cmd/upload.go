// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2/core"
	"google.golang.org/api/option"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type ReadWithContext struct {
	ctx    context.Context
	reader core.Reader
}

func (r *ReadWithContext) Read(buffer []byte) (int, error) {
	return r.reader.Read(r.ctx, buffer)
}

func upload(ctx context.Context, client *storage.Client, reader core.Reader, bucket string, key string, size int64) (int64, error) {
	b := client.Bucket(bucket)
	objHandle := b.Object(key)

	writer := objHandle.NewWriter(ctx)
	n, err := io.Copy(writer, &ReadWithContext{ctx, reader})
	if err != nil {
		return 0, err
	}

	if n != size {
		log.Fatalf("Expected %d written, expected %d bytes", n, size)
	}

	err = writer.Close()
	if err != nil {
		return 0, err
	}

	attrs, err := objHandle.Attrs(ctx)
	if err != nil {
		return 0, err
	}

	return attrs.Generation, nil
}

func uploadTree(ctx context.Context, client *storage.Client, ds *core.DataStore, inode core.INode, dirPath string, bucket string, keyPrefix string) {
	entries, err := ds.GetDirContents(ctx, inode)
	for _, entry := range entries {
		if entry.IsDirty {
			var path string
			if dirPath == "" {
				path = entry.Name
			} else {
				path = dirPath + "/" + entry.Name
			}
			if entry.IsDir {
				if entry.Name != "." && entry.Name != ".." {
					log.Printf("Checking directory %s", path)
					uploadTree(ctx, client, ds, entry.ID, path, bucket, keyPrefix)
				}
			} else {
				key := keyPrefix + "/" + path

				reader, err := ds.GetReadRef(ctx, entry.ID)
				if err != nil {
					log.Fatalf("Could not open %s: %s", path, err)
				}
				generation, err := upload(ctx, client, reader, bucket, key, entry.Size)
				if err != nil {
					log.Fatalf("Could not upload %s: %s", path, err)
				}
				log.Printf("Uploading %s -> gs://%s/%s", path, bucket, key)
				reader.Release()

				ds.UpdateIsRemoteGCS(entry.ID, bucket, key, generation, entry.Size, entry.ModTime)
			}
		}
		if err != nil {
			break
		}
	}

	if err != nil {
		log.Fatal(err)
	}
}

// uploadCmd represents the upload command
var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]
		destination := args[1]
		credentialsPath := viper.GetString("credentials")

		bucket, keyPrefix, ok := parseGCS(destination)
		if !ok {
			log.Fatalf("Could not parse GCS path: %s", destination)
		}

		ds := openExistingDataStore(repoPath)
		ctx := context.Background()

		client, err := storage.NewClient(ctx, option.WithServiceAccountFile(credentialsPath))
		if err != nil {
			log.Fatalf("Failed to create client: %v", err)
		}

		//inode core.INode, dirPath string, destination string
		uploadTree(ctx, client, ds, core.RootINode, "", bucket, keyPrefix)

		ds.Close()
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// uploadCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// uploadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
