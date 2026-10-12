module github.com/creachadair/ffs

go 1.26.0

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/creachadair/atomicfile v0.4.2
	github.com/creachadair/mds v0.31.0
	github.com/creachadair/msync v0.10.1
	github.com/creachadair/taskgroup v0.14.4
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	golang.org/x/crypto v0.58.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	honnef.co/go/tools v0.8.2 // indirect
)

retract [v0.2.2, v0.2.4]

retract (
	v0.17.19
	v0.17.18
	v0.3.0
)

tool (
	google.golang.org/protobuf/cmd/protoc-gen-go
	honnef.co/go/tools/staticcheck
)
