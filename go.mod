module github.com/creachadair/ffs

go 1.25

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/creachadair/atomicfile v0.4.0
	github.com/creachadair/taskgroup v0.14.2
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/creachadair/mds v0.25.15
	github.com/creachadair/msync v0.8.1
	golang.org/x/crypto v0.46.0
)

require (
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	honnef.co/go/tools v0.6.1 // indirect
)

retract [v0.2.2, v0.2.4]

retract v0.3.0

tool (
	google.golang.org/protobuf/cmd/protoc-gen-go
	honnef.co/go/tools/staticcheck
)
