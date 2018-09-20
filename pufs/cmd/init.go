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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
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

		root, err := cmd.Flags().GetString("root")
		if err != nil {
			log.Fatal(err)
		}

		map_, err := cmd.Flags().GetString("map")
		if err != nil {
			log.Fatal(err)
		}

		readahead, err := cmd.Flags().GetInt("readahead")
		if err != nil {
			log.Fatal(err)
		}

		credentialsPath, err := cmd.Flags().GetString("creds")
		if err != nil {
			log.Fatal(err)
		}

		bucketName := ""
		keyPrefix := ""

		var mapping *MountMap
		if root != "" {
			if map_ != "" {
				log.Fatal("Cannot specify both --map and --root parameters")
			}
		} else if map_ != "" {
			mapping = parseMap(map_)
			if mapping.Root != "" {
				root = mapping.Root
			}
		}

		ds := createDataStore(repoPath, root, credentialsPath, bucketName, keyPrefix, readahead)
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
	initCmd.Flags().String("creds", "", "path to json credentials file for service account to use")
	initCmd.Flags().Int("readahead", core.DefaultMaxBackgroundTransfer, "How much streaming in background to perform")
}

func createDataStore(dir string, mountAsRoot string, credentialsPath string, bucketName string, keyPrefix string, maxBackgroundTransfer int) *core.DataStore {
	// log.Printf("mountAsRoot=%s", mountAsRoot)
	socketFile, err := ioutil.TempFile("", "pufs-"+path.Base(dir))
	if err != nil {
		log.Fatalf("Could not create socket in temp dir: %s", err)
	}
	socketAddress := socketFile.Name()
	socketFile.Close()

	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			log.Fatalf("Could not create %s: %s", dir, err)
		}

		pufsDir := path.Join(dir, path.Dir(PufsInfoFilename))
		err = os.MkdirAll(pufsDir, 0700)
		if err != nil {
			log.Fatalf("Could not create %s: %s", pufsDir, err)
		}

		pufsInfoPath := path.Join(dir, PufsInfoFilename)
		f, err := os.Create(pufsInfoPath)
		if err != nil {
			log.Fatalf("Could not create %s: %s", pufsInfoPath, err)
		}

		configStr := fmt.Sprintf("type=repo\n"+
			"maxBackgroundTransfer=%d\n"+
			"credentialsPath=%s\n"+
			"bucketName=%s\n"+
			"keyPrefix=%s\n"+
			"socketAddress=%s\n",
			maxBackgroundTransfer,
			credentialsPath,
			bucketName,
			keyPrefix,
			socketAddress)
		_, err = f.WriteString(configStr)
		if err != nil {
			log.Fatalf("Could not write %s: %s", pufsInfoPath, err)
		}
		defer f.Close()
	}

	dsOptions := make([]core.DataStoreOption, 0)
	//	dsOptions = append(dsOptions, core.OpenExisting())
	if mountAsRoot != "" {
		gcsmatch := GCSUrlExp.FindStringSubmatch(mountAsRoot)

		if gcsmatch != nil {
			bucket := gcsmatch[1]
			key := gcsmatch[2]
			dsOptions = append(dsOptions, core.DataStoreWithGCSRoot(bucket, key))
		} else if pufsmatch := PUFSUrlExp.FindStringSubmatch(mountAsRoot); pufsmatch != nil {
			panic("unimplemented")
			// log.Printf("pufs:%v", pufsmatch)
			// label := pufsmatch[1]

			// BID, err := remoteRefFactory.GetRoot(ctx, label)
			// if err != nil {
			// 	log.Fatalf("Could not get root %s: %v", label, err)
			// }

			// dsOptions = append(dsOptions, core.DataStoreWithBIDRoot(BID))
		} else {
			log.Fatalf("Root was not parsable: %s", mountAsRoot)
		}
	}

	return openDataStore(dir, dsOptions...)
}
