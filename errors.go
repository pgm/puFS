package sply2

import "errors"

var INodesExhaustedErr = errors.New("INodes exhausted")
var ParentMissingErr = errors.New("Parent does not exist")
var NotDirErr = errors.New("Not a directory")
var NoSuchNodeErr = errors.New("Does not exist")
var InvalidFilenameErr = errors.New("None existant filename")
var ExistsErr = errors.New("File already exists")
var DirNotEmptyErr = errors.New("Directory is not empty")
