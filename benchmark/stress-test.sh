# clean up
umount ~/pufs-mount ; rm -r ~/pufs-data

# create repo
go run ../pufs/main.go init ~/pufs-data --root gs://gcs-test-1136/ --creds /Users/pmontgom/gcs-keys/gcs-test-b3b10d9077bb.json

# list file
go run ../pufs/main.go ls ~/pufs-data

# mount
go run ../pufs/main.go mount --trace t.log ~/pufs-data ~/pufs-mount > mount.log 2>&1 &

sleep 3

# run test
go run stress.go 3 rand-test-data ~/pufs-mount/rand-test-data 50mb

sleep 3
umount ~/pufs-mount
sleep 3
umount ~/pufs-mount

ps -e -o pid,args | grep -v grep | grep pufs | awk '{print $1}' | xargs kill
sleep 1

ps -e -o pid,args | grep -v grep | grep pufs
STATUS=$?
if [ "$STATUS" == "0" ] ; then
    echo "Could not kill processes"
fi


go run ../pufs/main.go ls ~/pufs-data/rand-test-data
