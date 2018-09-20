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
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"text/tabwriter"

	"github.com/pgm/sply2/core"
	"github.com/spf13/cobra"
)

func boolToStr(b bool, t string, f string) string {
	if b {
		return t
	} else {
		return f
	}
}

func base64x(BID core.BlockID) string {
	return base64.RawStdEncoding.EncodeToString(BID[:])
}

func fmtNum(v int64) string {
	if v <= 1024 {
		return fmt.Sprintf("%d", v)
	} else {
		var suffix string
		dv := float64(v)

		if v > 1024*1024*1024*1024 {
			suffix = "T"
			dv = dv / (1024 * 1024 * 1024 * 1024)
		} else if v > 1024*1024*1024 {
			suffix = "G"
			dv = dv / (1024 * 1024 * 1024)
		} else if v > 1024*1024 {
			suffix = "M"
			dv = dv / (1024 * 1024)
		} else {
			suffix = "K"
			dv = dv / 1024
		}
		return fmt.Sprintf("%.2f %s", dv, suffix)
	}
}

var NotValidPufsPathErr = errors.New("Was not a path under a repo nor mountpoint")

func findPufsRoot(longPath string) (repoPath string, remainingPath string, err error) {
	remainingPath = "."
	p := longPath
	for {
		canidatePath := path.Join(p, PufsInfoFilename)
		// log.Printf("Checking %s", canidatePath)
		_, err := os.Stat(canidatePath)
		if err == nil {
			return p, remainingPath, nil
		}

		nextDir := path.Dir(p)
		if nextDir == p {
			return "", "", NotValidPufsPathErr
		}

		// log.Printf("path.Join(path.Base(%s), %s)", p, remainingPath)
		remainingPath = path.Join(path.Base(p), remainingPath)
		p = nextDir
	}
}

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "list files with pufs-specific extended information",
	// 	Long: `A longer description that spans multiple lines and likely contains examples
	// and usage of using your command. For example:

	// Cobra is a CLI library for Go that empowers applications.
	// This application is a tool to generate the needed files
	// to quickly create a Cobra application.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dirPath := args[0]

		repoPath, remainingPath, err := findPufsRoot(dirPath)
		if err != nil {
			log.Fatalf("Could not find pufs repo: %s", err)
		}

		//		fmt.Println("ls called")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0) //tabwriter.AlignRight)

		ds := openExistingDataStore(repoPath)
		ctx := context.Background()
		inode, err := ds.GetINodeForPath(ctx, remainingPath)
		if err != nil {
			log.Fatalf("Could not look up %s: %s", dirPath, err)
		}
		entries, err := ds.GetExtendedDirContents(ctx, inode)
		if err != nil {
			log.Fatalf("Could not list dir %s: %s", dirPath, err)
		}

		columns := []string{"Dirty", "Name", "Size", "PopCnt", "PopSize", "BlockID"}
		//		columns := []string{"Dirty", "Name", "BlockID", "URL", "Size", "PopCnt", "PopSize"}
		fmt.Fprintln(w, strings.Join(columns, "\t"))

		colMapFuns := make(map[string]func(*core.ExtendedDirEntry) string)
		colMapFuns["Dirty"] = func(e *core.ExtendedDirEntry) string {
			return boolToStr(e.IsDirty, "*", "-")
		}
		colMapFuns["Name"] = func(e *core.ExtendedDirEntry) string {
			return e.Name
		}
		colMapFuns["BlockID"] = func(e *core.ExtendedDirEntry) string {
			if e.BID == core.NABlock {
				return "<NA>"
			}
			return base64x(e.BID)
		}
		colMapFuns["URL"] = func(e *core.ExtendedDirEntry) string {
			url := ""
			if e.RemoteSource != nil {
				source, ok := e.RemoteSource.(*core.GCSObjectSource)
				if !ok {
					panic("Could not cast")
				}
				url = fmt.Sprintf("gs://%s/%s", source.Bucket, source.Key)
			}
			return url
		}
		colMapFuns["Size"] = func(e *core.ExtendedDirEntry) string {
			return fmtNum(e.Size)
		}
		colMapFuns["PopCnt"] = func(e *core.ExtendedDirEntry) string {
			return fmtNum(int64(e.PopulatedRegionCount))
		}
		colMapFuns["PopSize"] = func(e *core.ExtendedDirEntry) string {
			return fmtNum(e.PopulatedSize)
		}

		for _, e := range entries {
			row := make([]string, len(columns))

			for i, name := range columns {
				row[i] = colMapFuns[name](e)
			}
			fmt.Fprintln(w, strings.Join(row, "\t"))
		}
		w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
