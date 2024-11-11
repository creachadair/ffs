module github.com/creachadair/ffs

go 1.23

toolchain go1.23.1

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/creachadair/atomicfile v0.3.5
	github.com/creachadair/taskgroup v0.13.0
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.6.0
	google.golang.org/protobuf v1.35.1
)

require (
	github.com/creachadair/mds v0.21.3
	github.com/creachadair/msync v0.3.1
	honnef.co/go/tools v0.5.1
)

require (
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/tools v0.21.1-0.20240531212143-b6235391adb3 // indirect
)

retract [v0.2.2, v0.2.4]

retract v0.3.0
