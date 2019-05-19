// Package file implements a File API over a content-addressable blob.Store.
package file

import (
	"context"
	"io"
	"os"
	"sort"
	"time"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/file/wirepb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
)

type File struct {
	name string // base name, not including directory
	key  string // storage key

	s      blob.CAS
	offset int64
	mode   os.FileMode
	mtime  time.Time
	data   *Data
	kids   []*File
	xattr  map[string]string
}

// Seek sets the starting offset for the next Read or Write, as io.Seeker.
func (f *File) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	target := offset
	switch whence {
	case io.SeekStart:
		// use offset as written
	case io.SeekCurrent:
		target += f.offset
	case io.SeekEnd:
		target += f.data.size()
	default:
		return 0, xerrors.Errorf("seek: invalid offset relation %v", whence)
	}
	if target < 0 {
		return 0, xerrors.Errorf("seek: invalid target offset %d", target)
	}
	f.offset = target
	return f.offset, nil
}

// Read reads up to len(data) bytes into data from the current offset of f, and
// reports the number of bytes successfully read, as io.Reader.
func (f *File) Read(ctx context.Context, data []byte) (int, error) {
	nr, err := f.data.readAt(ctx, f.s, data, f.offset)
	f.offset += int64(nr)
	return nr, err
}

// Write writes len(data) bytes from data to the f at its current offset,
// and reports the number of bytes successfully written, as io.Writer.
func (f *File) Write(ctx context.Context, data []byte) (int, error) {
	nw, err := f.data.writeAt(ctx, f.s, data, f.offset)
	f.offset += int64(nw)
	f.key = ""
	return nw, err
}

// ReadAt reads up to len(data) bytes into data from the given offset, and
// reports the number of bytes successfully read, as io.ReaderAt.
func (f *File) ReadAt(ctx context.Context, data []byte, offset int64) (int, error) {
	return f.data.readAt(ctx, f.s, data, offset)
}

// WriteAt writes len(data) bytes from data at the given offset, and reports
// the number of bytes successfully written, as io.WriterAt.
func (f *File) WriteAt(ctx context.Context, data []byte, offset int64) (int, error) {
	f.key = ""
	return f.data.writeAt(ctx, f.s, data, offset)
}

// Flush flushes the current state of the file to storage if necessary, and
// returns the resulting storage key.
func (f *File) Flush(ctx context.Context) (string, error) {
	if f.key != "" {
		return f.key, nil
	}
	key, err := saveNode(ctx, f.s, f.toProto())
	if err != nil {
		return "", err
	}
	f.key = key
	return key, nil
}

func (f *File) fromProto(pb *wirepb.Node) {
	f.name = pb.GetName()
	f.mode = os.FileMode(pb.GetMode())
	f.kids = nil
	f.xattr = make(map[string]string)

	if ts, err := ptypes.Timestamp(pb.ModTime); err == nil {
		f.mtime = ts
	}
	for _, xa := range pb.Xattrs {
		f.xattr[xa.GetName()] = string(xa.GetValue())
	}
	for _, kid := range pb.Children {
		f.kids = append(f.kids, &File{
			name: kid.GetName(),
			key:  string(kid.GetKey()),
		})
	}
}

func (f *File) toProto() *wirepb.Node {
	n := &wirepb.Node{
		Name: f.name,
		Mode: uint32(f.mode),
		Data: f.data.toProto(),
	}
	if ts, err := ptypes.TimestampProto(f.mtime); err == nil && !f.mtime.IsZero() {
		n.ModTime = ts
	}
	for name, value := range f.xattr {
		n.Xattrs = append(n.Xattrs, &wirepb.XAttr{
			Name:  name,
			Value: []byte(value),
		})
	}
	for _, kid := range f.kids {
		n.Children = append(n.Children, &wirepb.Child{
			Name: kid.name,
			Key:  []byte(kid.key),
		})
	}
	sort.Slice(n.Xattrs, func(i, j int) bool {
		return n.Xattrs[i].Name < n.Xattrs[j].Name
	})
	sort.Slice(n.Children, func(i, j int) bool {
		return n.Children[i].Name < n.Children[j].Name
	})
	return n
}

/*
func loadNode(ctx context.Context, s blob.CAS, key string) (*wirepb.Node, error) {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return nil, xerrors.Errorf("loading node: %w", err)
	}
	pb := new(wirepb.Node)
	if err := proto.Unmarshal(bits, pb); err != nil {
		return nil, xerrors.Errorf("decoding node: %w", err)
	}
	return pb, nil
}
*/

func saveNode(ctx context.Context, s blob.CAS, pb *wirepb.Node) (string, error) {
	bits, err := proto.Marshal(pb)
	if err != nil {
		return "", xerrors.Errorf("encoding node: %w", err)
	}
	return s.PutCAS(ctx, bits)
}
