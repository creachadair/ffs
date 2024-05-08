module github.com/creachadair/ffs

go 1.21

toolchain go1.21.0

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/creachadair/atomicfile v0.3.4
	github.com/creachadair/taskgroup v0.9.0
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.6.0
	google.golang.org/protobuf v1.34.0
)

require (
	github.com/creachadair/mds v0.14.6
	github.com/creachadair/msync v0.2.1
)

retract [v0.2.2, v0.2.4]

retract v0.3.0
