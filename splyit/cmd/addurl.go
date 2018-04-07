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
	"path"
	"regexp"

	"github.com/spf13/cobra"
)

var GCSUrlExp *regexp.Regexp = regexp.MustCompile("^gs://([^/]+)/(.*)$")

// addurlCmd represents the addurl command
var addurlCmd = &cobra.Command{
	Use:   "addurl",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		GobRegisterTypes()
		mountPoint := args[0]
		newFilePath := args[1]
		url := args[2]

		repo := path.Join(path.Dir(mountPoint), ".sply2-data-"+path.Base(mountPoint))
		ds := NewDataStore(repo)

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
		} else {
			_, err = ds.AddRemoteURL(ctx, parent, name, url)
		}
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(addurlCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addurlCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// addurlCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
