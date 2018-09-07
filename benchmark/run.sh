rm -r ~/repo-2 
( cd ../pufs && go run main.go init ~/repo-2 --root gs://gcs-test-1136/benchmark-data/ && go run main.go mount ~/repo-2 ~/mount-2 > mount.log 2>&1 & )
sleep 8
echo "starting benchmark"
python walktree.py ~/mount-2/ 4000
echo "running 2nd pass on cached data"
python walktree.py ~/mount-2/ 4000
umount ~/mount-2

