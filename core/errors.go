package core

import "errors"

var UnknownBlockID = errors.New("unknown block id")
var INodesExhaustedErr = errors.New("INodes exhausted")
var ParentMissingErr = errors.New("Parent does not exist")
var NotDirErr = errors.New("Not a directory")
var NoSuchNodeErr = errors.New("Does not exist")
var InvalidFilenameErr = errors.New("None existant filename")
var InvalidCharFilenameErr = errors.New("Filename contains invalid character")
var ExistsErr = errors.New("File already exists")
var DirNotEmptyErr = errors.New("Directory is not empty")
var IsDirErr = errors.New("Is directory, not a normal file")
var AlreadyMountPointErr = errors.New("This path is already mounted")
var NoSuchMountErr = errors.New("Was not a valid mount")
var UndefinedRootErr = errors.New("No such root exists")
var NotWritableErr = errors.New("File is not writable")

var InvalidRepoErr = errors.New("No such repo at that path")
var RepoExistsErr = errors.New("Cannot create repo as directory already exists")
