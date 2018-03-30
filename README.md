# sply2 [![Build Status](https://travis-ci.org/pgm/sply2.svg?branch=master)](https://travis-ci.org/pgm/sply2)

sply2 is an experiment in creating a service to help bridge software which assumes data is accessible on the filesystem and remote object stores such as S3 and GCS.

The design choices are targeting examples with:
  - Write once batch processing (input files consumed and new files writen)
  - Read heavy workloads
  - Large scale, parallel, share-nothing workloads

The core ideas are:
  - The "sply2" service mirrors objects from an object store locally in a managed arena.  All read-only processing can use the version from there.
  - Files are atomically added only after they are fully written.  There is no mechanism for updating.
  - Files are only uploaded upon a "push" operation.  At which time all files not yet stored
  in object storage are uploaded.
  - Files are lazily downloaded on demand and kept in the arena until.
  - Metadata about files is similarly stored in a mutable database which
  points to immutable files or writable files which have not yet been
  pushed.
  - Garbage collection is performed to reclaim space in the object store as well as the local cache arena

The "sply2" command can be used to perform operations such as "push" and
"mount".   There is also a FUSE client which makes the files stored in sply2 visible as a filesystem.
