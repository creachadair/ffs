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
// A File as defined by this package differs from the POSIX file model in that
// any File may have both binary content and "children". Thus, any File is also
// a directory, which can contain other files in a Merkle tree structure.
//
// A File is addressed by a storage key, corresponding to the current state of
// its content and metadata. File metadata are stored as wire-format protocol
// buffer messages using the wirepb.Node message in file/wirepb/wire.proto.
//
// Basic usage:
//
//   ctx := context.Background()
//
//   f := file.New(cas, nil)   // create a new, empty file
//   f.Write(ctx, data)        // write some data to the file
//   key, err := f.Flush(ctx)  // commit the file to storage
//
// To open an existing file,
//
//   f, err := file.Open(ctx, cas, key)
//
// The I/O methods of a File require a context argument. For compatibility with
// the standard interfaces in the io package, a File provides a wrapper for a
// request scoped context:
//
//    w := file.IO(ctx)
//    _, err := io.Copy(w, src)
//
// A value of the file.IO type should not be retained beyond the dynamic extent
// of the request whose context it captures.
//
package file

import (
	"context"
	"io"
	"sort"
	"time"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/file/wirepb"
	"bitbucket.org/creachadair/ffs/split"
	"github.com/golang/protobuf/proto"
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
		s:        s,
		name:     opts.Name,
		stat:     opts.Stat,
		saveStat: opts.Stat != Stat{},
		data:     fileData{sc: opts.Split},
		xattr:    make(map[string]string),
	}
}

// NewOptions control the creation of new files.
type NewOptions struct {
	// The name to attribute to the new file. The name of a File is not
	// persisted in storage.
	Name string

	// Initial file metadata to associate with the file. If this field is
	// nonzero, the new file will persist stat metadata to storage.  However,
	// the contents are not otherwise interpreted.
	Stat Stat

	// The block splitter configuration to use. If omitted, the default values
	// from the split package are used. The block size limits are persisted in
	// storage.
	Split split.Config
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

	offset   int64 // current seek position (≥ 0)
	stat     Stat  // file metadata
	saveStat bool  // whether to persist file metadata

	data  fileData          // binary file data
	kids  []child           // ordered lexicographically by name
	xattr map[string]string // extended attributes
}

// A child records the name and storage key of a child file.
type child struct {
	Name string
	Key  string // the storage key of the child
	File *File  // the opened file for the child

	// When a file is loaded from storage, the Key of each child is populated
	// but its File is not created until explicitly requested.  After the child
	// is opened, the Key may go out of sync with the file due to modifications
	// by the caller: When the enclosing file is flushed, any child with a File
	// attached is also flushed and the Key is updated.
}

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

func (f *File) inval()  { f.key = "" }
func (f *File) modify() { f.inval(); f.stat.ModTime = time.Now() }

// New constructs a new empty node backed by the same store as f.
// If f persists stat metadata, then the new file does also.
func (f *File) New(opts *NewOptions) *File {
	out := New(f.s, opts)
	if f.saveStat {
		out.saveStat = true
	}
	return out
}

// Stat returns the current stat metadata for f.
func (f *File) Stat() Stat { return f.stat }

// SetStat calls set with the current stat metadata for f, and enables stat
// persistence for the file. Any changes made by set are preserved.
func (f *File) SetStat(set func(*Stat)) {
	defer f.inval()

	cp := f.stat // copy so the pointer does not outlive the call
	set(&cp)
	f.stat = cp
	f.saveStat = true
}

// ClearStat clears the current stat metadata for f, and disables stat
// persistence for the file.
func (f *File) ClearStat() { defer f.inval(); f.stat = Stat{}; f.saveStat = false }

// HasChild reports whether f has a child with the given name.
func (f *File) HasChild(name string) bool { _, ok := f.findChild(name); return ok }

// SetChild makes c a child of f under the given name. This operation flushes c
// if necessary, and reports an error if that fails.
func (f *File) SetChild(ctx context.Context, name string, c *File) error {
	ckey, err := c.Flush(ctx)
	if err != nil {
		return err
	}
	defer f.modify()
	if i, ok := f.findChild(name); ok {
		if f.kids[i].Key != ckey {
			f.kids[i].Key = ckey
			f.kids[i].File = c
		}
		return nil
	}
	f.kids = append(f.kids, child{Name: name, Key: ckey, File: c})

	// Restore lexicographic order.
	for i := len(f.kids) - 1; i > 0; i-- {
		if f.kids[i].Name >= f.kids[i-1].Name {
			break
		}
		f.kids[i], f.kids[i-1] = f.kids[i-1], f.kids[i]
	}
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

// OpenChild opens the specified child file of f, or returns ErrChildNotFound
// if no such child exists.
func (f *File) OpenChild(ctx context.Context, name string) (*File, error) {
	i, ok := f.findChild(name)
	if !ok {
		return nil, xerrors.Errorf("open %q: %w", name, ErrChildNotFound)
	}
	if c := f.kids[i].File; c != nil {
		return c, nil
	}
	c, err := Open(ctx, f.s, f.kids[i].Key)
	if err == nil {
		f.name = name // remember the name the file was opened with
		f.kids[i].File = c
	}
	return c, err
}

// Children returns a slice of the names of the children of f.
func (f *File) Children() []string {
	out := make([]string, len(f.kids))
	for i, kid := range f.kids {
		out[i] = kid.Name
	}
	return out
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
// returns the resulting storage key. This is the canonical way to obtain the
// storage key for a file.
func (f *File) Flush(ctx context.Context) (string, error) {
	needsUpdate := f.key == ""

	// Flush any cached children.
	for i, kid := range f.kids {
		if kid.File != nil {
			fkey, err := kid.File.Flush(ctx)
			if err != nil {
				return "", err
			}
			if fkey != kid.Key {
				needsUpdate = true
			}
			f.kids[i].Key = fkey
		}
	}

	if needsUpdate {
		key, err := saveProto(ctx, f.s, f.toProto())
		if err != nil {
			return "", xerrors.Errorf("flushing file %q: %w", key, err)
		}
		f.key = key
	}
	return f.key, nil
}

// XAttr calls c with a string-to-string map of the extended attributes of f.
// The callback may modify this map directly to add, change, or remove extended
// attributes. The file must be flushed to persist any changes.
func (f *File) XAttr(c func(map[string]string)) { defer f.inval(); c(f.xattr) }

// Name reports the attributed name of f, which may be "" if f is not a child
// file and was not assigned a name at creation.
func (f *File) Name() string { return f.name }

// IO binds f with a context so that it can be used to satisfy the standard
// interfaces defined by the io package.  The resulting values hould be used
// only during the lifetime of the request whose context it binds.
func (f *File) IO(ctx context.Context) IO { return IO{ctx: ctx, f: f} }

func (f *File) fromProto(pb *wirepb.Node) {
	f.data = fileData{} // reset
	f.data.fromProto(pb.Index)
	f.stat.fromProto(pb.Stat)
	f.saveStat = pb.Stat != nil

	f.xattr = make(map[string]string)
	for _, xa := range pb.XAttrs {
		f.xattr[xa.GetName()] = string(xa.GetValue())
	}

	f.kids = nil
	for _, kid := range pb.Children {
		f.kids = append(f.kids, child{
			Name: kid.GetName(),
			Key:  string(kid.GetKey()),
		})
	}
	sort.Slice(f.kids, func(i, j int) bool {
		return f.kids[i].Name < f.kids[j].Name
	})
}

func (f *File) toProto() *wirepb.Node {
	n := &wirepb.Node{Index: f.data.toProto()}
	if f.saveStat {
		n.Stat = f.stat.toProto()
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
