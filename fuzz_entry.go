package sply2

import (
	"errors"
	"io/ioutil"
	"regexp"
	"strings"
)

func compileRegExp(exp string) *regexp.Regexp {
	re, err := regexp.Compile(exp)
	if err != nil {
		panic(err)
	}
	return re
}

var MkdirStatement *regexp.Regexp = compileRegExp("d ([^ ]+)")
var UnlinkStatement *regexp.Regexp = compileRegExp("u ([^ ]+)")
var WriteStatement *regexp.Regexp = compileRegExp("w ([^ ]+)")
var ReadStatement *regexp.Regexp = compileRegExp("r ([^ ]+)")
var BlankStatement *regexp.Regexp = compileRegExp("^\\s*$")
var StatementExps []*regexp.Regexp = []*regexp.Regexp{MkdirStatement, UnlinkStatement, WriteStatement, ReadStatement}

var ParseError error = errors.New("Parse error")

type Execution struct {
	ds         *DataStore
	errorCount int
}

func (e *Execution) splitPath(path string) (INode, string, error) {
	var err error
	components := strings.Split(path, "/")
	parent := INode(RootINode)
	for _, c := range components[0 : len(components)-1] {
		parent, err = e.ds.GetNodeID(parent, c)
		if err != nil {
			return InvalidINode, "", err
		}
	}
	return parent, components[len(components)-1], nil
}

func (e *Execution) getINode(path string) (INode, error) {
	parent, name, err := e.splitPath(path)
	if err != nil {
		return InvalidINode, err
	}
	inode, err := e.ds.GetNodeID(parent, name)
	if err != nil {
		return InvalidINode, err
	}
	return inode, nil
}

func (e *Execution) executeStatement(statementType *regexp.Regexp, match []string) {
	var err error

	if statementType == MkdirStatement {
		parent, name, err := e.splitPath(match[1])
		if err == nil {
			_, err = e.ds.MakeDir(parent, name)
		}
	} else if statementType == UnlinkStatement {
		parent, name, err := e.splitPath(match[1])
		if err == nil {
			err = e.ds.Remove(parent, name)
		}
	} else if statementType == WriteStatement {
		parent, name, err := e.splitPath(match[1])
		if err == nil {
			_, _, err = e.ds.CreateWritable(parent, name)
		}
	} else if statementType == ReadStatement {
		inode, err := e.getINode(match[1])
		if err == nil {
			_, err = e.ds.GetReadRef(inode)
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
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}
	e := &Execution{}
	e.ds = NewDataStore(dir, nil)

	lines := strings.Split(script, "\n")
	for _, line := range lines {
		handledLine := false
		for _, exp := range StatementExps {
			match := exp.FindStringSubmatch(line)
			if match != nil {
				e.executeStatement(exp, match)
				handledLine = true
			}
		}
		if !handledLine {
			return nil, ParseError
		}
	}

	return e, nil
}

func Fuzz(data []byte) int {
	_, err := executeScript(string(data))
	if err == nil {
		return 1
	} else {
		return 0
	}
}
