export PATH=$PATH:$GOPATH/bin
protoc -I api/ \
    -I${GOPATH}/src \
    --go_out=plugins=grpc:api \
    api/api.proto
    