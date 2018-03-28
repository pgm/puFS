package core

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

var MkdirStatement *regexp.Regexp = regexp.MustCompile("^d ([^ ]+)$")
var UnlinkStatement *regexp.Regexp = regexp.MustCompile("^u ([^ ]+)$")
var WriteStatement *regexp.Regexp = regexp.MustCompile("^w ([^ ]+)$")
var ReadStatement *regexp.Regexp = regexp.MustCompile("^r ([^ ]+)$")
var ListStatement *regexp.Regexp = regexp.MustCompile("^l ([^ ]+)$")
var FreezeStatement *regexp.Regexp = regexp.MustCompile("^f ([^ ]+)$")
var PushStatement *regexp.Regexp = regexp.MustCompile("^p ([^ ]+) ([^ ]+)$")
var MountStatement *regexp.Regexp = regexp.MustCompile("^m ([^ ]+) ([^ ]+)$")
var UnmountStatement *regexp.Regexp = regexp.MustCompile("^M ([^ ]+)$")
var SwapStatement *regexp.Regexp = regexp.MustCompile("^s$")
var BlankStatement *regexp.Regexp = regexp.MustCompile("^\\s*$")
var RenameStatement *regexp.Regexp = regexp.MustCompile("^R ([^ ]+) ([^ ]+)$")
var StatementExps []*regexp.Regexp = []*regexp.Regexp{
	MkdirStatement,
	UnlinkStatement,
	WriteStatement,
	ReadStatement,
	ListStatement,
	FreezeStatement, PushStatement, MountStatement, UnmountStatement, SwapStatement, BlankStatement,
	RenameStatement}

var ParseError error = errors.New("Parse error")

type Execution struct {
	ds         *DataStore
	ds2        *DataStore
	errorCount int
}

func (e *Execution) splitPath(ctx context.Context, path string) (INode, string, error) {
	var err error
	components := strings.Split(path, "/")
	parent := INode(RootINode)
	for _, c := range components[0 : len(components)-1] {
		parent, err = e.ds.GetNodeID(ctx, parent, c)
		if err != nil {
			return InvalidINode, "", err
		}
	}
	return parent, components[len(components)-1], nil
}

func (e *Execution) getINode(ctx context.Context, path string) (INode, error) {
	parent, name, err := e.splitPath(ctx, path)
	if err != nil {
		return InvalidINode, err
	}
	inode, err := e.ds.GetNodeID(ctx, parent, name)
	if err != nil {
		return InvalidINode, err
	}
	return inode, nil
}

func (e *Execution) executeStatement(ctx context.Context, statementType *regexp.Regexp, match []string) {
	var err error

	if statementType == MkdirStatement {
		parent, name, err := e.splitPath(ctx, match[1])
		if err == nil {
			_, err = e.ds.MakeDir(ctx, parent, name)
		}
	} else if statementType == UnlinkStatement {
		parent, name, err := e.splitPath(ctx, match[1])
		if err == nil {
			err = e.ds.Remove(ctx, parent, name)
		}
	} else if statementType == WriteStatement {
		parent, name, err := e.splitPath(ctx, match[1])
		if err == nil {
			_, _, err = e.ds.CreateWritable(ctx, parent, name)
		}
	} else if statementType == ReadStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			_, err = e.ds.GetReadRef(ctx, inode)
		}
	} else if statementType == ListStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			_, err = e.ds.GetDirContents(ctx, inode)
		}
	} else if statementType == FreezeStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			_, err = e.ds.Freeze(inode)
		}
	} else if statementType == PushStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			err = e.ds.Push(ctx, inode, match[2])
		}
	} else if statementType == MountStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			err = e.ds.MountByLabel(ctx, inode, match[2])
		}
	} else if statementType == UnmountStatement {
		inode, err := e.getINode(ctx, match[1])
		if err == nil {
			err = e.ds.Unmount(ctx, inode)
		}
	} else if statementType == SwapStatement {
		e.ds, e.ds2 = e.ds2, e.ds
	} else if statementType == RenameStatement {
		srcParent, srcName, err := e.splitPath(ctx, match[1])
		if err == nil {
			dstParent, dstName, err := e.splitPath(ctx, match[2])
			if err == nil {
				err = e.ds.Rename(ctx, srcParent, srcName, dstParent, dstName)
			}
		}
	} else if statementType == BlankStatement {
		// do nothing
	} else {
		panic("unknown statement type")
	}

	if err != nil {
		e.errorCount++
	}
}

func executeScript(script string) (*Execution, error) {
	panic("unimp")
	// dir, err := ioutil.TempDir("", "gofuzz")
	// if err != nil {
	// 	panic(err)
	// }
	// defer os.RemoveAll(dir)
	// e := &Execution{}
	// f := NewRemoteRefFactoryMem()
	// e.ds = NewDataStore(dir, f, NewMemStore([][]byte{ChunkStat}), NewMemStore([][]byte{ChildNodeBucket, NodeBucket}))
	// e.ds2 = NewDataStore(dir, f, NewMemStore([][]byte{ChunkStat}), NewMemStore([][]byte{ChildNodeBucket, NodeBucket}))
	// ctx := context.Background()

	// lines := strings.Split(script, "\n")
	// for _, line := range lines {
	// 	handledLine := false
	// 	for _, exp := range StatementExps {
	// 		match := exp.FindStringSubmatch(line)
	// 		if match != nil {
	// 			e.executeStatement(ctx, exp, match)
	// 			handledLine = true
	// 		}
	// 	}
	// 	if !handledLine {
	// 		return nil, fmt.Errorf("Could not parse \"%s\"", line)
	// 	}
	// }

	// return e, nil
}

func Fuzz(data []byte) int {
	_, err := executeScript(string(data))
	if err == nil {
		return 1
	} else {
		return 0
	}
}
