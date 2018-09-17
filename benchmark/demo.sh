# clean up
umount ~/pufs-mount ; rm -r ~/pufs-data

# create repo
go run ../pufs/main.go init ~/pufs-data --map files.json

# list file
go run ../pufs/main.go ls ~/pufs-data

# mount
go run ../pufs/main.go mount ~/pufs-data ~/pufs-mount > mount.log 2>&1 &

# umount 
umount ~/pufs-mount

