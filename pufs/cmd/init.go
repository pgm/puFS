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
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/pgm/sply2/core"
	"github.com/spf13/cobra"
)

type Link struct {
	Source string `json:"source"`
	Path   string `json:"path"`
}

type MountMap struct {
	Root  string  `json:"root"`
	Links []*Link `json:"links"`
}

func parseMap(filename string) *MountMap {
	fp, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Could not open %s: %s", filename, err)
	}

	defer fp.Close()

	dec := json.NewDecoder(fp)
	var mm MountMap
	err = dec.Decode(&mm)
	if err != nil {
		log.Fatalf("Could not parse %s: %s", filename, err)
	}

	return &mm
}

var initCmd = &cobra.Command{
	Use:   "init [new repo path]",
	Short: "Create a local repo which can be mounted",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]

		log.Printf("a")
		root, err := cmd.Flags().GetString("root")
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("a")
		map_, err := cmd.Flags().GetString("map")
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("a")
		var mapping *MountMap
		options := make([]NewDataStoreOption, 0, 10)
		if root != "" {
			if map_ != "" {
				log.Fatal("Cannot specify both --map and --root parameters")
			}
			options = append(options, MountAsRoot(root))
		} else if map_ != "" {
			mapping = parseMap(map_)
			if mapping.Root != "" {
				options = append(options, MountAsRoot(mapping.Root))
			}
		}
		ds := NewDataStore(repoPath, true, options...)
		if mapping != nil {
			ctx := context.Background()
			inodex := core.INode(core.RootINode)
			for _, link := range mapping.Links {
				pathComponents := strings.Split(link.Path, "/")
				for i := 0; i < len(pathComponents)-1; i++ {
					name := pathComponents[i]
					nextNode, err := ds.GetNodeID(ctx, inodex, name)
					if err == core.NoSuchNodeErr {
						nextNode, err = ds.MakeDir(ctx, inodex, name)
					}
					if err != nil {
						log.Fatalf("Could not map %s -> %s: %v", link.Source, link.Path, err)
					}
					inodex = nextNode
				}
				bucket, key, ok := parseGCS(link.Source)
				if !ok {
					log.Fatalf("The GCS path \"%s\" is not valid", link.Source)
				}
				_, err := ds.AddRemoteGCS(ctx, inodex, pathComponents[len(pathComponents)-1], bucket, key)
				if err != nil {
					log.Fatalf("Problem adding %s -> %s: %v", link.Source, link.Path, err)
				}
			}
		}

		ds.Close()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
	initCmd.Flags().String("root", "", "remote path to use for the root (ie: gs://bucket/path/ or pufs:///label )")
	initCmd.Flags().String("map", "", "json file which describes how to prepopulate the filesystem")
}
