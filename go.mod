module github.com/creachadair/ffs

go 1.21

toolchain go1.21.0

require (
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/creachadair/atomicfile v0.3.3
	github.com/creachadair/taskgroup v0.7.1
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.6.0
	google.golang.org/protobuf v1.32.0
)

require (
	github.com/creachadair/mds v0.8.2
	github.com/creachadair/msync v0.1.0
)

retract [v0.2.2, v0.2.4]
retract v0.3.0
