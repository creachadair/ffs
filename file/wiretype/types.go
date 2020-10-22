// Package wiretype defines the encoding types for the ffs package.
//
// These types are annotated for use with the github.com/creachadair/binpack package.
package wiretype

import "sort"

// A Node is the top-level encoding of a a file.
type Node struct {
	Index    *Index   `binpack:"tag=1" json:"index,omitempty"`    // file contents
	Stat     *Stat    `binpack:"tag=2" json:"stat,omitempty"`     // stat metadata (optional)
	XAttrs   []*XAttr `binpack:"tag=3" json:"xattr,omitempty"`    // extended attributes
	Children []*Child `binpack:"tag=4" json:"children,omitempty"` // child file pointers

	// next id: 5
}

// Normalize updates n in-place so that all fields are in canonical order.
func (n *Node) Normalize() {
	n.Index.Normalize()
	sort.Slice(n.XAttrs, func(i, j int) bool {
		return n.XAttrs[i].Name < n.XAttrs[j].Name
	})
	sort.Slice(n.Children, func(i, j int) bool {
		return n.Children[i].Name < n.Children[j].Name
	})
}

// Stat records POSIX style file metadata. Other than the modification time,
// these metadata are not interpreted by the file plumbing, but are preserved
// for the benefit of external tools.
type Stat struct {
	Mode    uint32 `binpack:"tag=1" json:"mode,omitempty"`
	ModTime *Time  `binpack:"tag=2" json:"modTime,omitempty"`

	OwnerID   uint64 `binpack:"tag=3" json:"ownerID,omitempty"`
	OwnerName string `binpack:"tag=4" json:"ownerName,omitempty"`
	GroupID   uint64 `binpack:"tag=5" json:"groupID,omitempty"`
	GroupName string `binpack:"tag=6" json:"groupName,omitempty"`

	// next id: 7
}

// Time is the encoding of a timestamp, in seconds and nanoseconds elapsed
// since the Unix epoch in UTC.
type Time struct {
	Seconds int64 `binpack:"tag=1" json:"seconds,omitempty"`
	Nanos   int64 `binpack:"tag=2" json:"nanos,omitempty"`
}

// An Index records the size and storage locations of file data.
type Index struct {
	TotalBytes uint64    `binpack:"tag=1" json:"totalBytes"`
	Extents    []*Extent `binpack:"tag=2" json:"extents,omitempty"`

	// next id: 3
}

// Normalize updates n in-place so that all fields are in canonical order.
func (x *Index) Normalize() {
	if x == nil {
		return
	}
	sort.Slice(x.Extents, func(i, j int) bool {
		return x.Extents[i].Base < x.Extents[j].Base
	})
}

// An Extent describes a single contiguous span of stored data.
type Extent struct {
	Base   uint64   `binpack:"tag=1" json:"base"`
	Bytes  uint64   `binpack:"tag=2" json:"bytes"`
	Blocks []*Block `binpack:"tag=3" json:"blocks,omitempty"`

	// next id: 4
}

// A Block describes the size and storage key of a data blob.
type Block struct {
	Bytes uint64 `binpack:"tag=1" json:"bytes"`
	Key   []byte `binpack:"tag=2" json:"key"`

	// next id: 3
}

// An XAttr records the name and value of an extended attribute.
// The contents of the value are not interpreted.
type XAttr struct {
	Name  string `binpack:"tag=1" json:"name"`
	Value []byte `binpack:"tag=2" json:"value"`

	// next id: 3
}

// A Child records the name and storage key of a child Node.
type Child struct {
	Name string `binpack:"tag=1" json:"name"`
	Key  []byte `binpack:"tag=2" json:"key"`

	// next id: 3
}
