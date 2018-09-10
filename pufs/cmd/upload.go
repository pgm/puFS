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
	"log"

	"github.com/pgm/sply2/core"

	"github.com/spf13/cobra"
)

func uploadTree(ctx context.Context, ds *core.DataStore, inode core.INode, destination string) {
	entries, err := ds.GetDirContents(ctx, inode)
	for _, entry := range entries {
		if entry.IsDirty {
			destPath := destination + "/" + entry.Name
			if entry.IsDir {
				uploadTree(ctx, ds, entry.ID, destPath)
			} else {
				panic("unimp")
				// var reader io.Reader
				// reader, err = ds.GetReadRef(ctx, entry.ID)
				// if err != nil {
				// 	break
				// }
				// open object destPath
				// copy between streams
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

		ds := NewDataStore(repoPath, false)
		ctx := context.Background()

		uploadTree(ctx, ds, core.RootINode, destination)

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
