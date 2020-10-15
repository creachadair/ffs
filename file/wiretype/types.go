// Package wiretype defines the encoding types for the ffs package.
//
// These types are annotated for use with the github.com/creachadair/binpack package.
package wiretype

// A Node is the top-level encoding of a a file.
type Node struct {
	Index    *Index   `binpack:"tag=1"` // file contents
	Stat     *Stat    `binpack:"tag=2"` // stat metadata (optional)
	XAttrs   []*XAttr `binpack:"tag=3"` // extended attributes
	Children []*Child `binpack:"tag=4"` // child file pointers

	// next id: 5
}

// Stat records POSIX style file metadata. Other than the modification time,
// these metadata are not interpreted by the file plumbing, but are preserved
// for the benefit of external tools.
type Stat struct {
	Mode    uint32 `binpack:"tag=1"`
	ModTime *Time  `binpack:"tag=2"`

	OwnerID   uint64 `binpack:"tag=3"`
	OwnerName string `binpack:"tag=4"`

	GroupID   uint64 `binpack:"tag=5"`
	GroupName string `binpack:"tag=6"`

	// next id: 7
}

// Time is the encoding of a timestamp, in seconds and nanoseconds elapsed
// since the Unix epoch in UTC.
type Time struct {
	Seconds int64 `binpack:"tag=1"`
	Nanos   int64 `binpack:"tag=2"`
}

// An Index records the size and storage locations of file data.
type Index struct {
	TotalBytes uint64    `binpack:"tag=1"`
	Extents    []*Extent `binpack:"tag=2"`

	// reserved: 3
	// next id: 4
}

func (x *Index) GetTotalBytes() uint64 {
	if x == nil {
		return 0
	}
	return x.TotalBytes
}

func (x *Index) GetExtents() []*Extent {
	if x == nil {
		return nil
	}
	return x.Extents
}

// An Extent describes a single contiguous span of stored data.
type Extent struct {
	Base   uint64   `binpack:"tag=1"`
	Bytes  uint64   `binpack:"tag=2"`
	Blocks []*Block `binpack:"tag=3"`

	// next id: 4
}

// A Block describes the size and storage key of a data blob.
type Block struct {
	Bytes uint64 `binpack:"tag=1"`
	Key   []byte `binpack:"tag=2"`

	// next id: 3
}

// An XAttr records the name and value of an extended attribute.
// The contents of the value are not interpreted.
type XAttr struct {
	Name  string `binpack:"tag=1"`
	Value []byte `binpack:"tag=2"`

	// next id: 3
}

// A Child records the name and storage key of a child Node.
type Child struct {
	Name string `binpack:"tag=1"`
	Key  []byte `binpack:"tag=2"`

	// next id: 3
}
