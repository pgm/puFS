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
	"log"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init [new repo path]",
	Short: "Create a local repo which can be mounted",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]
		root, err := cmd.Flags().GetString("root")
		if err != nil {
			log.Fatal(err)
		}
		//		root := viper.GetString("root")
		options := make([]NewDataStoreOption, 0, 10)
		if root != "" {
			options = append(options, MountAsRoot(root))
			log.Printf("Root=%s", root)
		}
		ds := NewDataStore(repoPath, true, options...)
		ds.Close()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().String("root", "", "remote path to use for the root (ie: gs://bucket/path/ or pufs:///label )")
}
