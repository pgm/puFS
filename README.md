# pufs [![Build Status](https://travis-ci.org/pgm/puFS.svg?branch=master)](https://travis-ci.org/pgm/puFS)

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


## Tutorial:

Before we can get started, we'll need a [service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) which has [access](https://cloud.google.com/storage/docs/access-control/) to the buckets that we want to use.

We'll assume you've downloaded a json credential file named "key.json" which will be used in some of the examples.

First, we need a place to hold cached data. We refer to this as the **pufs repo**. We create the repo with the "init" command. 

Example: *Create an empty repo stored in ~/pufs-data*

When creating the repo, we need to provide the credentials for the service account that pufs will use to access object storage. The credentials are not actually stored in the repo, but it does retain the path to the json file in the repo.

```
$ pufs init ~/pufs-data --creds key.json
2018/09/19 21:04:12 Creating new repo
2018/09/19 21:04:12 Adding empty root dir
$ 
```

We can now proceed to mount this repo as a filesystem.

```
# Create the mount point
$ mkdir ~/pufs-mount 

# Running the mount in the background. Also it writes a bunch of debug
# messages, so send those to mount.log
$ pufs mount ~/pufs-data ~/pufs-mount > mount.log 2>&1 &

# at this point we can cd to ~/pufs-mount, write files, read them, etc. It 
# acts like a normal directory. Once we're done, unmount it via the normal 
# umount command
$ umount ~/pufs-mount

# once the directory is unmounted, we can upload the newly created 
# files to GCS.
$ pufs upload ~/pufs-data gs://dest-bucket/key-prefix
```

Now, a starting with an empty repo isn't particularly interesting, but we can instead create a repo which references some existing GCS objects.

When creating a repo, we can specify the root directory should mirror a location in google cloud storage. 

Example: *Create an repo which mirrors a GCS path.* 

In this example, we'll map the [Landsat data google hosts in GCS](https://cloud.google.com/storage/docs/public-datasets/landsat) to the root directory

```
$ pufs init ~/pufs-data --root gs://gcp-public-data-landsat/
$ pufs mount ~/pufs-data ~/pufs-mount > mount.log 2>&1 &
$ ls -al ~/pufs-mount
total 1134736
drwxrwxr-x    1 pgm  1594166068          0 Sep 19 21:12 .
drwxr-xr-x+ 157 pgm  1594166068       5338 Sep 19 21:12 ..
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LC08
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LE00
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LE07
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LM01
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LM02
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LM03
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LM04
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LM05
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LO08
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LT00
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LT04
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LT05
drwxrwxr-x    1 pgm  1594166068          0 Aug 30  1754 LT08
-r--r--r--    1 pgm  1594166068  580980766 Sep 19 02:15 index.csv.gz
```

The act of listing the root filesystem performed a call to GCS to list objects at `gs://gcp-public-data-landsat/` and create corresponding entries in the repo for these objects.

The first access to this directory may take a second as the call is made, but only directories explored will have their listings fetched. Also, listing directories only causes object metadata to be fetched from GCS. No data is transfered at this stage.

This root directory contains a directory named `LC08`. The contents of that directory won't be fetched unless a process attempts to either list or open a file under that directory.

It's also worth noting the `index.csv.gz` file is readable, but not writable. This reflects pufs knowledge that the data is stored in GCS and pufs operates under the assumption that
the source data is immutable. 

In contrast, all directories are writable. Even directories that mirror the folder structure in GCS are writable. This means that even though `index.csv.gz` is read-only, we can still delete it. However, that change only is reflected in the local mount and will never be applied to GCS.

The filesystem presented by pufs can be used as an overlay filesystem similar in spirit like [OverlayFS](https://en.wikipedia.org/wiki/OverlayFS), however the model is more flexible than simply merging a view of GCS and a view of local files.

The reality is each file is a link to either a writable file or an "immutable" object in GCS, and we can have complete control of the mapping between what pufs presents and the organization in GCS.

For example, imagine we wanted two files from the 1000 genomes project, but only those two files. Also the names of the objects are very long, so it'd be more convenient if those two files had shorter names.

We can create a repo with a custom mapping by creating a file named `files.json` which contains:

```
{
  "links": [
    {"source": "gs://genomics-public-data/1000-genomes/vcf/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf", "path": "chr17.vcf"},
    {"source": "gs://genomics-public-data/1000-genomes/vcf/ALL.chrMT.phase1_samtools_si.20101123.snps.low_coverage.genotypes.vcf", "path": "chrMT.vcf"},
  ]
}
```

And create the repo via:

```
$ pufs init ~/pufs-data --creds key.json --map files.json
```

Now when we mount `~/pufs-data`, we will see two files in the mounted directory: `chr17.vcf` and `chrMT.vcf`. We can completely rearrange the organization of the folder by mapping in objects across GCS regardless of bucket or key.

