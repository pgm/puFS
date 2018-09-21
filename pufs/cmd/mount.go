package cmd

import (
	"context"
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"path"
	"regexp"
	"runtime/trace"
	"time"

	"github.com/magiconair/properties"

	"cloud.google.com/go/storage"
	"github.com/pgm/sply2"
	"github.com/pgm/sply2/api"
	"github.com/pgm/sply2/core"
	"github.com/pgm/sply2/fs"
	"github.com/pgm/sply2/remote"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/spf13/cobra"
)

const PufsInfoFilename = ".pufs/info"

var PUFSUrlExp *regexp.Regexp = regexp.MustCompile("^pufs:///(.*)$")

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount [repoPath] [mountPoint]",
	Short: "Mount the directory",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		repoPath := args[0]
		mountPoint := args[1]

		var traceFd io.WriteCloser
		traceFile, err := cmd.Flags().GetString("trace")
		if err != nil {
			panic(err)
		}
		if traceFile != "" {
			traceFd, err = os.Create(traceFile)
			if err != nil {
				log.Fatalf("Could not open trace %s: %s", traceFile, err)
			}
			trace.Start(traceFd)
		}

		if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
			err = os.MkdirAll(mountPoint, 0777)
			if err != nil {
				log.Fatalf("Could not create directory %s", mountPoint)
			}
		}

		ds, repoInfo := openExistingDataStore(repoPath)

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

		lis, err := net.Listen("unix", repoInfo.socketAddress)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()
		api.RegisterPufsServer(grpcServer, &apiService{ds: ds})
		go grpcServer.Serve(lis)

		fs.Mount(mountPoint, ds)
		ticker.Stop()
		trace.Stop()
		if traceFd != nil {
			traceFd.Close()
		}
	},
}

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.Flags().String("trace", "", "Write execution trace to specified file")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mountCmd.PersistentFlags().String("foo", "", "A help for foo")
	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mountCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type apiService struct {
	ds *core.DataStore
}

func (s *apiService) GetDirContents(ctx context.Context, req *api.DirContentsRequest) (*api.DirContentsResponse, error) {
	inode, err := s.ds.GetINodeForPath(ctx, req.Path)
	if err != nil {
		return &api.DirContentsResponse{ErrorMsg: err.Error()}, nil
	}

	srcEntries, err := s.ds.GetExtendedDirContents(ctx, inode)
	if err != nil {
		return &api.DirContentsResponse{ErrorMsg: err.Error()}, nil
	}

	dstEntries := make([]*api.DirContentsResponse_Entry, len(srcEntries))
	for i, src := range srcEntries {
		dstEntries[i] = &api.DirContentsResponse_Entry{
			ID:                   int64(src.ID),
			Name:                 src.Name,
			IsDirty:              src.IsDirty,
			IsDir:                src.IsDir,
			Size:                 src.Size,
			ModTimeSeconds:       src.ModTime.Unix(),
			BlockID:              src.BID[:],
			PopulatedRegionCount: int32(src.PopulatedRegionCount),
			PopulatedSize:        src.PopulatedSize}
	}

	return &api.DirContentsResponse{Entries: dstEntries}, nil
}

// type NewDataStoreOptions struct {
// 	mountAsRoot           string
// 	dsOptions             []core.DataStoreOption
// 	maxBackgroundTransfer int64
// }

// type NewDataStoreOption func(o *NewDataStoreOptions)

// func MountAsRoot(path string) func(o *NewDataStoreOptions) {
// 	return func(o *NewDataStoreOptions) {
// 		o.mountAsRoot = path
// 	}
// }

// func AddDatastoreOption(dso core.DataStoreOption) func(o *NewDataStoreOptions) {
// 	return func(o *NewDataStoreOptions) {
// 		o.dsOptions = append(o.dsOptions, dso)
// 	}
// }

// func MaxBackgroundTransfer(length int64) func(o *NewDataStoreOptions) {
// 	return func(o *NewDataStoreOptions) {
// 		o.maxBackgroundTransfer = length
// 	}
// }

type repoInfo struct {
	credentialsPath       string
	bucketName            string
	keyPrefix             string
	maxBackgroundTransfer int
	socketAddress         string
}

func loadRepoInfo(dir string) *repoInfo {
	pufsInfoPath := path.Join(dir, PufsInfoFilename)
	p := properties.MustLoadFile(pufsInfoPath, properties.UTF8)
	return &repoInfo{credentialsPath: p.MustGetString("credentialsPath"),
		bucketName:            p.MustGetString("bucketName"),
		keyPrefix:             p.MustGetString("keyPrefix"),
		maxBackgroundTransfer: p.MustGetInt("maxBackgroundTransfer"),
		socketAddress:         p.MustGetString("socketAddress")}
	// read config to use from info file
	// f, err := os.Open(pufsInfoPath)
	// if err != nil {
	// 	log.Fatalf("Could not create %s: %s", pufsInfoPath, err)
	// }

	// panic("")
}
func openExistingDataStore(dir string) (*core.DataStore, *repoInfo) {
	return openDataStore(dir, core.OpenExisting())
}

func openDataStore(dir string, dsOptions ...core.DataStoreOption) (*core.DataStore, *repoInfo) {

	repoInfo := loadRepoInfo(dir)

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile(repoInfo.credentialsPath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	remoteRefFactory := remote.NewRemoteRefFactory(client, repoInfo.bucketName, repoInfo.keyPrefix)

	ds, err := core.NewDataStore(dir, remoteRefFactory, remoteRefFactory,
		sply2.NewBoltDB(path.Join(dir, "freezer.db"),
			[][]byte{core.ChunkStat}),
		sply2.NewBoltDB(path.Join(dir, "nodes.db"),
			[][]byte{core.ChildNodeBucket, core.NodeBucket}),
		dsOptions...,
	)

	if err != nil {
		log.Fatalf("Failed to create DataStore: %v", err)
	}

	ds.SetClients(remoteRefFactory)

	return ds, repoInfo

}

func GobRegisterTypes() {
	var x *core.GCSObjectSource
	gob.Register(core.BlockID{})
	gob.Register(x)
}
