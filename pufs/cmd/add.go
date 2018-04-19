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
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/pgm/sply2/core"
	"github.com/spf13/cobra"
)

var GCSUrlExp *regexp.Regexp = regexp.MustCompile("^gs://([^/]+)/(.*)$")

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]
		url := args[1]
		newFilePath, _ := cmd.Flags().GetString("path")

		ds := NewDataStore(repoPath, false)

		ctx := context.Background()
		parent, name, err := ds.SplitPath(ctx, newFilePath)
		if err != nil {
			log.Fatalf("Error: %s", err)
		}

		gcsmatch := GCSUrlExp.FindStringSubmatch(url)
		if gcsmatch != nil {
			bucket := gcsmatch[1]
			key := gcsmatch[2]
			fmt.Printf("parent: %v, name: %v, bucket: %v, key: %v\n", parent, name, bucket, key)
			_, err = ds.AddRemoteGCS(ctx, parent, name, bucket, key)
		} else if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
			_, err = ds.AddRemoteURL(ctx, parent, name, url)
		} else {
			var inode core.INode
			inode, err = ds.GetNodeID(ctx, parent, name)
			if err == nil {
				err = ds.MountByLabel(ctx, inode, url)
			}
		}
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(addCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	addCmd.Flags().StringP("path", "p", ".", "Path within repo to add")
}
