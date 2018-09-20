# clean up
umount ~/pufs-mount ; rm -r ~/pufs-data

# create repo
go run ../pufs/main.go init ~/pufs-data --map files.json --creds /Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json

# list file
go run ../pufs/main.go ls ~/pufs-data

# mount
go run ../pufs/main.go mount ~/pufs-data ~/pufs-mount > mount.log 2>&1 &

# umount 
umount ~/pufs-mount

# ls of deep directory
go run ../pufs/main.go ls ~/pufs-data/landsat/LC08/PRE/044/034/LC80440342016259LGN00

# open sample image
open ~/pufs-mount/landsat/LC08/PRE/044/034/LC80440342016259LGN00/LC80440342016259LGN00_B2.TIF