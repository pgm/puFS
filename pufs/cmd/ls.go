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
	"net"
	"os"
	"path"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pgm/sply2/api"
	"github.com/pgm/sply2/core"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
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

type ClientWrapper struct {
	s api.PufsServer
}

func (c *ClientWrapper) GetDirContents(ctx context.Context, in *api.DirContentsRequest, opts ...grpc.CallOption) (*api.DirContentsResponse, error) {
	return c.s.GetDirContents(ctx, in)
}

func getRepoClient(socketAddress string, repoPath string) api.PufsClient {
	_, err := os.Stat(socketAddress)
	if err == nil {
		// the socket exists, so connect and use that as the client
		log.Printf("socketAddress=%s", socketAddress)
		conn, err := grpc.Dial(socketAddress, grpc.WithInsecure(), grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
		if err != nil {
			log.Fatalf("Could not connect to pufs service: %s", err)
		}
		client := api.NewPufsClient(conn)

		// TODO: try sending a ping to make sure service is up.
		// if not, shut down the client and delete the stale socket
		return client
	} else {
		// okay, open directly
		ds, _ := openExistingDataStore(repoPath)
		localService := &apiService{ds}
		return &ClientWrapper{localService}
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

		repoInfo := loadRepoInfo(repoPath)

		client := getRepoClient(repoInfo.socketAddress, repoPath)

		ctx := context.Background()
		resp, err := client.GetDirContents(ctx, &api.DirContentsRequest{Path: remainingPath})
		if err != nil {
			log.Fatalf("Error calling client: %s", err)
		}
		//		fmt.Println("ls called")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0) //tabwriter.AlignRight)

		// inode, err := ds.GetINodeForPath(ctx, remainingPath)
		// if err != nil {
		// 	log.Fatalf("Could not look up %s: %s", dirPath, err)
		// }
		// entries, err := ds.GetExtendedDirContents(ctx, inode)
		// if err != nil {
		// 	log.Fatalf("Could not list dir %s: %s", dirPath, err)
		// }

		columns := []string{"Dirty", "Name", "Size", "PopCnt", "PopSize", "BlockID"}
		//		columns := []string{"Dirty", "Name", "BlockID", "URL", "Size", "PopCnt", "PopSize"}
		fmt.Fprintln(w, strings.Join(columns, "\t"))

		colMapFuns := make(map[string]func(*api.DirContentsResponse_Entry) string)
		colMapFuns["Dirty"] = func(e *api.DirContentsResponse_Entry) string {
			return boolToStr(e.IsDirty, "*", "-")
		}
		colMapFuns["Name"] = func(e *api.DirContentsResponse_Entry) string {
			return e.Name
		}
		colMapFuns["BlockID"] = func(e *api.DirContentsResponse_Entry) string {
			var BID core.BlockID
			copy(BID[:], e.BlockID)
			if BID == core.NABlock {
				return "<NA>"
			}
			return base64x(BID)
		}
		colMapFuns["URL"] = func(e *api.DirContentsResponse_Entry) string {
			return "unimp"
			// url := ""
			// if e.RemoteSource != nil {
			// 	source, ok := e.RemoteSource.(*api.GCSObjectSource)
			// 	if !ok {
			// 		panic("Could not cast")
			// 	}
			// 	url = fmt.Sprintf("gs://%s/%s", source.Bucket, source.Key)
			// }
			// return url
		}
		colMapFuns["Size"] = func(e *api.DirContentsResponse_Entry) string {
			return fmtNum(e.Size)
		}
		colMapFuns["PopCnt"] = func(e *api.DirContentsResponse_Entry) string {
			return fmtNum(int64(e.PopulatedRegionCount))
		}
		colMapFuns["PopSize"] = func(e *api.DirContentsResponse_Entry) string {
			return fmtNum(e.PopulatedSize)
		}

		if resp.ErrorMsg != "" {
			log.Fatalf("Got error: %s", resp.ErrorMsg)
		}
		for _, e := range resp.Entries {
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
