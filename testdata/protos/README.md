This directory is copied from https://github.com/googleapis/google-cloud-go/tree/spanner/v1.67.0/spanner/testdata/protos.

#### To generate singer.pb.go and descriptors.pb file from singer.proto using `protoc`
```shell
cd spanner/testdata
protoc --proto_path=protos/ --include_imports --descriptor_set_out=protos/descriptors.pb --go_out=protos/ protos/singer.proto
```
