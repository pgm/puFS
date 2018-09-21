rm -r ~/repo-2 
( cd ../pufs && go run main.go init ~/repo-2 --root gs://gcs-test-1136/benchmark-data/ && go run main.go mount ~/repo-2 ~/mount-2 > ../benchmark/mount.log 2>&1 & )
echo sleeping...
sleep 5
time cp ~/mount-2/50mb foo
echo done, sleeping...
sleep 5
umount ~/mount-2
