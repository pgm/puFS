# pufs [![Build Status](https://travis-ci.org/pgm/sply2.svg?branch=master)](https://travis-ci.org/pgm/sply2)

**pufs** is an experiment in creating a service to help bridge software which assumes data is accessible on the filesystem and remote object stores such as S3 and GCS.

The design choices are targeting examples with:
  - Write once batch processing (input files consumed and new files writen)
  - Read heavy workloads
  - Large scale, parallel, share-nothing workloads

The core ideas are:
  - The "pufs" service mirrors objects from an object store locally in a managed arena.  All read-only processing can use the version from there.
  - Files are atomically added only after they are fully written.  There is no mechanism for updating, only replacing.
  - Files are only uploaded upon a "push" or "upload" operation.  At which time all files not yet stored in object storage are uploaded.
  - Files are lazily downloaded on demand and kept locally for future reads.
  - Metadata about files is stored in a mutable database. Each file entry
  points to either an immutable file or a writable files which have not yet been pushed.

The "pufs" command is a command line client used to perform all operations.


Tutorial:

Before we can get started, we'll need a [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) which has [access](https://cloud.google.com/storage/docs/access-control/) to the buckets that we want to use.

First, we need a place to hold cached data. We refer to this as the **pufs repo**. We create the repo with the "init" command. 

Example: *Create an empty repo stored in ~/pufs-data*
```
pufs init ~/pufs-data
```

When creating a repo we can we want the contents of the root directory to include that from a location in google cloud storage. 

Example: *Create an repo which mirrors a GCS path.* 

In this example, we'll map the [Landsat data google hosts in GCS](https://cloud.google.com/storage/docs/public-datasets/landsat) to the root directory

```
pufs init ~/pufs-data --root gs://gcp-public-data-landsat/
```



```
pufs init repo-a
pufs mount repo-a ~/a
umount ~/a
pufs push repo-a a
pufs init repo-b
pufs add repo-b a
pufs mount repo-a ~/a
```
