// Copyright 2019 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package file implements a File API over a content-addressable blob.Store.
//
// Although a *File keeps track of basic stat information, this abstraction
// differs from POSIX files in some important ways.
package file

import (
	"context"
	"io"
	"os"
	"sort"
	"time"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/file/wirepb"
	"bitbucket.org/creachadair/ffs/split"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
)

// New constructs a new, empty File with the given options and backed by s. The
// caller must call the new file's Flush method to ensure it is written to
// storage. If opts == nil, defaults are chosen.
func New(s blob.CAS, opts *NewOptions) *File {
	if opts == nil {
		opts = new(NewOptions)
	}
	return &File{
		s:     s,
		name:  opts.Name,
		mode:  opts.Mode,
		mtime: opts.ModTime,
		data:  fileData{sc: opts.Split},
		xattr: make(map[string]string),
	}
}

// NewOptions control the creation of new files.
type NewOptions struct {
	Name    string
	Mode    os.FileMode
	ModTime time.Time
	Split   split.Config
}

// Open opens an existing file given its storage key in s.
func Open(ctx context.Context, s blob.CAS, key string) (*File, error) {
	var node wirepb.Node
	if err := loadProto(ctx, s, key, &node); err != nil {
		return nil, xerrors.Errorf("loading file %q: %w", key, err)
	}
	f := &File{s: s, key: key}
	f.fromProto(&node)
	return f, nil
}

// A File represents a writable file stored in a content-addressable blobstore.
type File struct {
	s    blob.CAS
	name string // if this file is a child, its attributed name
	key  string // the storage key for the file record (wirepb.Node)

	offset int64       // current seek position (≥ 0)
	mode   os.FileMode // file mode; not used, but persisted
	mtime  time.Time   // timestamp of last content modification

	data  fileData          // binary file data
	kids  []Child           // ordered lexicographically by name
	xattr map[string]string // extended attributes
}

// A Child records the name and storage key of a child file.
type Child struct {
	Name string
	Key  string
}

type byName []Child

func (b byName) Len() int           { return len(b) }
func (b byName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// findChild reports whether f has a child with the specified name and its
// index in the slice if so, or otherwise -1.
func (f *File) findChild(name string) (int, bool) {
	if n := sort.Search(len(f.kids), func(i int) bool {
		return f.kids[i].Name == name
	}); n < len(f.kids) {
		return n, true
	}
	return -1, false
}

func (f *File) modify() { f.key = ""; f.mtime = time.Now() }

// HasChild reports whether f has a child with the given name.
func (f *File) HasChild(name string) bool { _, ok := f.findChild(name); return ok }

// SetChild makes c a child of f under the given name. This operation flushes c
// if necessary, and reports an error if that fails.
func (f *File) SetChild(ctx context.Context, name string, c *File) error {
	ckey, err := c.Flush(ctx)
	if err != nil {
		return err
	}
	if i, ok := f.findChild(name); ok {
		if f.kids[i].Key != ckey {
			f.kids[i].Key = ckey
			f.modify()
		}
		return nil
	}
	f.kids = append(f.kids, Child{Name: name, Key: ckey})
	sort.Sort(byName(f.kids))
	f.modify()
	return nil
}

// RemoveChild removes name as a child of f if present, and reports whether any
// change was made.
func (f *File) RemoveChild(name string) bool {
	if i, ok := f.findChild(name); ok {
		defer f.modify()
		f.kids = append(f.kids[:i], f.kids[i+1:]...)
		return true
	}
	return false
}

// OpenChild opens the specified child file of f.
func (f *File) OpenChild(ctx context.Context, name string) (*File, error) {
	i, ok := f.findChild(name)
	if !ok {
		return nil, xerrors.Errorf("child file %q not found", name)
	}
	f, err := Open(ctx, f.s, f.kids[i].Key)
	if err == nil {
		f.name = name // remember the name the file was opened with
	}
	return f, err
}

// Children returns a slice of all the children of f.
func (f *File) Children() []Child {
	// Make a copy so the caller cannot modify the file via the slice.
	out := make([]Child, len(f.kids))
	copy(out, f.kids)
	return out
}

// Stat returns an os.FileInfo describing f.
func (f *File) Stat() Stat {
	return Stat{
		name:  f.name,
		size:  f.data.size(),
		mode:  f.mode,
		mtime: f.mtime,
	}
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
	defer f.modify()
	nw, err := f.data.writeAt(ctx, f.s, data, f.offset)
	f.offset += int64(nw)
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
	defer f.modify()
	return f.data.writeAt(ctx, f.s, data, offset)
}

// Flush flushes the current state of the file to storage if necessary, and
// returns the resulting storage key.
func (f *File) Flush(ctx context.Context) (string, error) {
	if f.key == "" {
		key, err := saveProto(ctx, f.s, f.toProto())
		if err != nil {
			return "", xerrors.Errorf("flushing file %q: %w", key, err)
		}
		f.key = key
	}
	return f.key, nil
}

// Name reports the attributed name of f, which may be "" if f is not a child
// file and was not assigned a name at creation.
func (f *File) Name() string { return f.name }

// Chmod changes the mode of f to mode. The change will not be persisted until
// the next time f is flushed.
func (f *File) Chmod(mode os.FileMode) { f.mode = mode; f.key = "" }

// IO binds f with a context so that it can be used to satisfy the standard
// interfaces defined by the io package.  The resulting values hould be used
// only during the lifetime of the request whose context it binds.
func (f *File) IO(ctx context.Context) IO { return IO{ctx: ctx, f: f} }

func (f *File) fromProto(pb *wirepb.Node) {
	f.data = fileData{} // reset
	f.data.fromProto(pb.Index)
	f.mode = os.FileMode(pb.GetMode())

	if ts, err := ptypes.Timestamp(pb.ModTime); err == nil {
		f.mtime = ts
	}

	f.xattr = make(map[string]string)
	for _, xa := range pb.XAttrs {
		f.xattr[xa.GetName()] = string(xa.GetValue())
	}

	f.kids = nil
	for _, kid := range pb.Children {
		f.kids = append(f.kids, Child{
			Name: kid.GetName(),
			Key:  string(kid.GetKey()),
		})
	}
	sort.Sort(byName(f.kids))
}

func (f *File) toProto() *wirepb.Node {
	n := &wirepb.Node{
		Mode:  uint32(f.mode),
		Index: f.data.toProto(),
	}
	if ts, err := ptypes.TimestampProto(f.mtime); err == nil && !f.mtime.IsZero() {
		n.ModTime = ts
	}
	for name, value := range f.xattr {
		n.XAttrs = append(n.XAttrs, &wirepb.XAttr{
			Name:  name,
			Value: []byte(value),
		})
	}
	sort.Slice(n.XAttrs, func(i, j int) bool {
		return n.XAttrs[i].Name < n.XAttrs[j].Name
	})
	for _, kid := range f.kids {
		n.Children = append(n.Children, &wirepb.Child{
			Name: kid.Name,
			Key:  []byte(kid.Key),
		})
	}
	return n
}

func saveProto(ctx context.Context, s blob.CAS, msg proto.Message) (string, error) {
	bits, err := proto.Marshal(msg)
	if err != nil {
		return "", xerrors.Errorf("encoding message: %w", err)
	}
	return s.PutCAS(ctx, bits)
}

func loadProto(ctx context.Context, s blob.CAS, key string, msg proto.Message) error {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return xerrors.Errorf("loading message: %w", err)
	}
	return proto.Unmarshal(bits, msg)
}
