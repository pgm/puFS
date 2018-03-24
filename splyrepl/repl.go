package main

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"path"
	"strings"
	"time"

	"errors"
	"fmt"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
	"github.com/chzyer/readline"
	"github.com/pgm/sply2"
	"github.com/pgm/sply2/core"
	"google.golang.org/api/option"
)

var GCSUrlExp *regexp.Regexp = regexp.MustCompile("^gs://([^/]+)/(.*)$")

var ChdirStatement *regexp.Regexp = regexp.MustCompile("^cd ([^ ]+)$")
var AddUrlStatement *regexp.Regexp = regexp.MustCompile("^addurl ([^ ]+) ([^ ]+)$")
var MkdirStatement *regexp.Regexp = regexp.MustCompile("^mkdir ([^ ]+)$")
var UnlinkStatement *regexp.Regexp = regexp.MustCompile("^rm ([^ ]+)$")
var WriteStatement *regexp.Regexp = regexp.MustCompile("^touch ([^ ]+)$")
var ReadStatement *regexp.Regexp = regexp.MustCompile("^cat ([^ ]+)$")
var ListStatement *regexp.Regexp = regexp.MustCompile("^ls\\s*([^ ]*)$")
var FreezeStatement *regexp.Regexp = regexp.MustCompile("^freeze ([^ ]+)$")
var PushStatement *regexp.Regexp = regexp.MustCompile("^push ([^ ]+) ([^ ]+)$")
var MountStatement *regexp.Regexp = regexp.MustCompile("^mount ([^ ]+) ([^ ]+)$")
var UnmountStatement *regexp.Regexp = regexp.MustCompile("^unmount ([^ ]+)$")
var BlankStatement *regexp.Regexp = regexp.MustCompile("^\\s*$")
var RenameStatement *regexp.Regexp = regexp.MustCompile("^mv ([^ ]+) ([^ ]+)$")
var StatementExps []*regexp.Regexp = []*regexp.Regexp{
	ChdirStatement,
	MkdirStatement,
	AddUrlStatement,
	UnlinkStatement,
	WriteStatement,
	ReadStatement,
	ListStatement,
	FreezeStatement, PushStatement, MountStatement, UnmountStatement, BlankStatement,
	RenameStatement}

var ParseError error = errors.New("Parse error")

type Execution struct {
	ds  *core.DataStore
	cwd string
}

func (e *Execution) splitPath(relPath string) (core.INode, string, error) {
	var err error
	fullPath := path.Join(e.cwd, relPath)
	if fullPath[0] != '/' {
		return core.InvalidINode, "", fmt.Errorf("Invalid path: %s", fullPath)
	}

	if fullPath == "/" {
		return core.RootINode, ".", nil
	}

	components := strings.Split(fullPath[1:], "/")
	parent := core.INode(core.RootINode)
	for _, c := range components[0 : len(components)-1] {
		parent, err = e.ds.GetNodeID(parent, c)
		if err != nil {
			return core.InvalidINode, "", err
		}
	}
	return parent, components[len(components)-1], nil
}

func (e *Execution) getINode(relPath string) (core.INode, error) {
	parent, name, err := e.splitPath(relPath)
	if err != nil {
		return core.InvalidINode, err
	}
	inode, err := e.ds.GetNodeID(parent, name)
	if err != nil {
		return core.InvalidINode, err
	}
	return inode, nil
}

func (e *Execution) executeStatement(statementType *regexp.Regexp, match []string) {
	var err error
	var parent core.INode
	var name string
	var inode core.INode

	start := time.Now()

	if statementType == ChdirStatement {
		_, err = e.getINode(match[1])
		if err == nil {
			e.cwd = path.Join(e.cwd, match[1])
		}
	} else if statementType == MkdirStatement {
		parent, name, err = e.splitPath(match[1])
		if err == nil {
			_, err = e.ds.MakeDir(parent, name)
		}
	} else if statementType == UnlinkStatement {
		parent, name, err = e.splitPath(match[1])
		if err == nil {
			err = e.ds.Remove(parent, name)
		}
	} else if statementType == WriteStatement {
		parent, name, err = e.splitPath(match[1])
		if err == nil {
			var w core.WritableRef
			_, w, err = e.ds.CreateWritable(parent, name)
			if err == nil {
				_, err = w.Write([]byte("hello"))
				w.Release()
			}
		}
	} else if statementType == AddUrlStatement {
		parent, name, err = e.splitPath(match[2])

		if err == nil {
			url := match[1]
			gcsmatch := GCSUrlExp.FindStringSubmatch(url)
			if match != nil {
				bucket := gcsmatch[1]
				key := gcsmatch[2]
				_, err = e.ds.AddRemoteGCS(parent, name, bucket, key)
			} else {
				_, err = e.ds.AddRemoteURL(parent, name, url)
			}
		}
	} else if statementType == ReadStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			var r io.Reader
			r, err = e.ds.GetReadRef(inode)
			if err == nil {
				b, err := ioutil.ReadAll(r)
				if err == nil {
					fmt.Println(string(b))
				}
			}
		}
	} else if statementType == ListStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			var files []*core.DirEntry
			files, err = e.ds.GetDirContents(inode)
			for _, e := range files {
				fileType := "f"
				if e.IsDir {
					fileType = "d"
				}
				if e.BID != core.NABlock {
					fileType += "z"
				} else {
					fileType += " "
				}

				fmt.Printf("\t%s %-10s %10d %s\n", fileType, e.Name, e.Size, e.ModTime)
			}
		}
	} else if statementType == FreezeStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			_, err = e.ds.Freeze(inode)
		}
	} else if statementType == PushStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			err = e.ds.Push(inode, match[2])
		}
	} else if statementType == MountStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			err = e.ds.MountByLabel(inode, match[2])
		}
	} else if statementType == UnmountStatement {
		inode, err = e.getINode(match[1])
		if err == nil {
			err = e.ds.Unmount(inode)
		}
	} else if statementType == RenameStatement {
		var srcParent, dstParent core.INode
		var srcName, dstName string
		srcParent, srcName, err = e.splitPath(match[1])
		if err == nil {
			dstParent, dstName, err = e.splitPath(match[2])
			if err == nil {
				err = e.ds.Rename(srcParent, srcName, dstParent, dstName)
			}
		}
	} else if statementType == BlankStatement {
		return
	} else {
		panic("unknown statement type")
	}

	duration := time.Now().Sub(start)

	if err == nil {
		fmt.Printf("Success (%s)\n", duration)
	} else {
		fmt.Printf("Error: %s (%s)\n", err, duration)
	}
}

func NewDataStore(dir string) *Execution {
	var err error
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0700)
	}
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	//	projectID := "gcs-test-1136"

	// Creates a client.
	client, err := storage.NewClient(ctx, option.WithServiceAccountFile("/Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	bucketName := "gcs-test-1136"
	f := sply2.NewRemoteRefFactory(client, bucketName, "blocks/")
	// if err != nil {
	// 	log.Fatalf("Failed to create remote: %v", err)
	// }
	ds := core.NewDataStore(dir, f, sply2.NewBoltDB(path.Join(dir, "freezer.db"), [][]byte{core.ChunkStat}),
		sply2.NewBoltDB(path.Join(dir, "nodes.db"), [][]byte{core.ChildNodeBucket, core.NodeBucket}))
	ds.SetClients(f, f)
	return &Execution{ds: ds, cwd: "/"}
}

func (e *Execution) executeLine(line string) {
	handledLine := false
	for _, exp := range StatementExps {
		match := exp.FindStringSubmatch(line)
		if match != nil {
			e.executeStatement(exp, match)
			handledLine = true
		}
	}
	if !handledLine {
		fmt.Printf("Could not parse \"%s\"\n", line)
	}
}

func main() {
	dir := os.Args[1]
	e := NewDataStore(dir)

	l, err := readline.NewEx(&readline.Config{
		Prompt:      "\033[31m»\033[0m ",
		HistoryFile: "splyrepl.history",
		// AutoComplete:    completer,
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
		// FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		e.executeLine(line)
		l.SetPrompt("(" + e.cwd + ") \033[31m>\033[0m ")
	}
}
