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
// its content, metadata, and children (recursively). File metadata are stored
// as wire-format protocol buffers, as defined in file/wiretype/wiretype.proto.
//
// Basic usage:
//
//	ctx := context.Background()
//
//	f := file.New(cas, nil)   // create a new, empty file
//	f.WriteAt(ctx, data, 0)   // write some data to the file
//	key, err := f.Flush(ctx)  // commit the file to storage
//
// To open an existing file,
//
//	f, err := file.Open(ctx, cas, key)
//
// The I/O methods of a File require a context argument. For compatibility with
// the standard interfaces in the io package, a File provides a wrapper for a
// request-scoped context:
//
//	_, err := io.Copy(dst, f.Cursor(ctx))
//
// A value of the file.Cursor type should not be used outside the dynamic
// extent of the request whose context it captures.
//
// # Metadata
//
// A File supports a subset of POSIX style data metadata, including mode,
// modification time, and owner/group identity. These metadata are not
// interpreted by the API, but will be persisted if they are set.
//
// By default, a File does not persist stat metadata. To enable stat
// persistence, you may either set the Stat field of file.NewOptions when the
// File is created, or use the Persist method of the Stat value to enable or
// disable persistence:
//
//	s := f.Stat()
//	s.ModTime = time.Now()
//	s.Update().Persist(true)
//
// The file.Stat type defines the stat attributes that can be persisted.
//
// # Synchronization
//
// The exported methods of *File and the views of its data (Child, Data, Stat,
// XAttr) are safe for concurrent use by multiple goroutines.
package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/block"
	"github.com/creachadair/ffs/file/wiretype"
)

// New constructs a new, empty File with the given options and backed by s. The
// caller must call the new file's Flush method to ensure it is written to
// storage. If opts == nil, defaults are chosen.
func New(s blob.CAS, opts *NewOptions) *File {
	if opts == nil {
		opts = new(NewOptions)
	}
	f := &File{
		s:        s,
		name:     opts.Name,
		saveStat: opts.PersistStat,
		data:     fileData{sc: opts.Split},
		xattr:    make(map[string]string),
	}
	// If the options contain stat metadata, copy them in.
	if opts.Stat != nil {
		f.setStatLocked(*opts.Stat)
	}
	return f
}

// NewOptions control the creation of new files.
type NewOptions struct {
	// The name to attribute to the new file. The name of a File is not
	// persisted in storage.
	Name string

	// Stat, if non-nil, is the initial stat metadata for the file.  Note that
	// stat metadata will not be persisted to storage when the file is flushed
	// unless PersistStat is also true.
	Stat *Stat

	// PersistStat is whether stat metadata for the new file should be persisted
	// to storage when the file is written out.
	PersistStat bool

	// The block splitter configuration to use. If omitted, the default values
	// from the split package are used. Split configurations are not persisted
	// in storage, but descendants created from a file (via the New method) will
	// inherit the parent file config if they do not specify their own.
	Split *block.SplitConfig
}

// Open opens an existing file given its storage key in s.
func Open(ctx context.Context, s blob.CAS, key string) (*File, error) {
	var obj wiretype.Object
	if err := wiretype.Load(ctx, s, key, &obj); err != nil {
		return nil, fmt.Errorf("load %x: %w", key, err)
	}
	f := &File{s: s, key: key}
	if err := f.fromWireType(&obj); err != nil {
		return nil, fmt.Errorf("decode file %x: %w", key, err)
	}
	return f, nil
}

// A File represents a writable file stored in a content-addressable blobstore.
type File struct {
	s blob.CAS

	mu   sync.RWMutex
	name string // if this file is a child, its attributed name
	key  string // the storage key for the file record (wiretype.Node)

	stat     Stat // file metadata
	saveStat bool // whether to persist file metadata

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

// findChildLocked reports whether f has a child with the specified name and
// its index in the slice if so, or otherwise -1.
func (f *File) findChildLocked(name string) (int, bool) {
	if n := sort.Search(len(f.kids), func(i int) bool {
		return f.kids[i].Name >= name
	}); n < len(f.kids) && f.kids[n].Name == name {
		return n, true
	}
	return -1, false
}

func (f *File) setStatLocked(s Stat) {
	f.stat = s
	if f.saveStat {
		f.invalLocked()
	}
}

func (f *File) invalLocked() { f.key = "" }

func (f *File) modifyLocked() { f.invalLocked(); f.stat.ModTime = time.Now() }

// New constructs a new empty node backed by the same store as f.
// If f persists stat metadata, then the new file does too, even if
// opts.PersistStat is false. The caller can override this default via the Stat
// view after the file is created.
func (f *File) New(opts *NewOptions) *File {
	out := New(f.s, opts)
	if f.saveStat {
		out.saveStat = true
	}

	// Propagate the parent split settings to the child, if the child did not
	// have any specifically defined.
	if opts == nil || opts.Split == nil {
		out.data.sc = f.data.sc
	}
	return out
}

// Stat returns the current stat metadata for f. Calling this method does not
// change stat persistence for f, use the Clear and Update methods of the Stat
// value to do that.
func (f *File) Stat() Stat {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := f.stat
	cp.f = f
	return cp
}

// FileInfo returns a [FileInfo] record for f. The resulting value is a
// snapshot at the moment of construction, and does not track changes to the
// file after the value was constructed.
func (f *File) FileInfo() FileInfo {
	if f == nil {
		return FileInfo{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return FileInfo{
		name:    f.name,
		size:    f.data.totalBytes,
		mode:    f.stat.Mode,
		modTime: f.stat.ModTime,
		file:    f,
	}
}

// Data returns a view of the file content for f.
func (f *File) Data() Data { return Data{f: f} }

var (
	// ErrChildNotFound indicates that a requested child file does not exist.
	ErrChildNotFound = errors.New("child file not found")
)

// Open opens the specified child file of f, or returns ErrChildNotFound if no
// such child exists.
func (f *File) Open(ctx context.Context, name string) (*File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.findChildLocked(name)
	if !ok {
		return nil, fmt.Errorf("open %q: %w", name, ErrChildNotFound)
	}
	if c := f.kids[i].File; c != nil {
		return c, nil
	}
	c, err := Open(ctx, f.s, f.kids[i].Key)
	if err == nil {
		c.name = name // remember the name the file was opened with
		f.kids[i].File = c
	}
	return c, err
}

// Load loads an existing file given its storage key in the store used by f.
// The specified file need not necessarily be a child of f.
func (f *File) Load(ctx context.Context, key string) (*File, error) {
	return Open(ctx, f.s, key)
}

// Child returns a view of the children of f.
func (f *File) Child() Child { return Child{f: f} }

// ReadAt reads up to len(data) bytes into data from the given offset, and
// reports the number of bytes successfully read, as io.ReaderAt.
func (f *File) ReadAt(ctx context.Context, data []byte, offset int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.data.readAt(ctx, f.s, data, offset)
}

// WriteAt writes len(data) bytes from data at the given offset, and reports
// the number of bytes successfully written, as io.WriterAt.
func (f *File) WriteAt(ctx context.Context, data []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.modifyLocked()
	return f.data.writeAt(ctx, f.s, data, offset)
}

// Flush flushes the current state of the file to storage if necessary, and
// returns the resulting storage key. This is the canonical way to obtain the
// storage key for a file.
func (f *File) Flush(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recFlushLocked(ctx, nil)
}

// Key returns the storage key of f if it is known, or "" if the file has not
// been flushed to storage in its current form.
func (f *File) Key() string { f.mu.RLock(); defer f.mu.RUnlock(); return f.key }

// recFlushLocked recursively flushes f and all its child nodes. The path gives
// the path of nodes from the root to the current flush target, and is used to
// verify that there are no cycles in the graph.
func (f *File) recFlushLocked(ctx context.Context, path []*File) (string, error) {
	// Recursive flush is a long operation, check for timeout/cancellation.
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	needsUpdate := f.key == ""

	// Flush any cached children.
	for i, kid := range f.kids {
		if kf := kid.File; kf != nil {
			// Check for direct or indirect cycles. This check is quadratic in the
			// height of the DAG over the whole scan in the worst case. In
			// practice, this doesn't cause any real issues, since it's not common
			// for file structures to be very deep. Compared to the cost of
			// marshaling and writing back invalid entries to storage, the array
			// scan is minor.
			if slices.Contains(path, kf) {
				return "", fmt.Errorf("flush: cycle in path at %p", kf)
			}
			cpath := append(path, f)
			fkey, err := func() (string, error) {
				kf.mu.Lock()
				defer kf.mu.Unlock()
				return kf.recFlushLocked(ctx, cpath)
			}()
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
		key, err := wiretype.Save(ctx, f.s, f.toWireTypeLocked())
		if err != nil {
			return "", fmt.Errorf("flushing file %x: %w", key, err)
		}
		f.key = key
	}
	return f.key, nil
}

// Truncate modifies the length of f to end at offset, extending or contracting
// it as necessary.
func (f *File) Truncate(ctx context.Context, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.modifyLocked()
	return f.data.truncate(ctx, f.s, offset)
}

// SetData fully reads r replaces the binary contents of f with its data.
// On success, any existing data for f are discarded. In case of error, the
// contents of f are not changed.
func (f *File) SetData(ctx context.Context, r io.Reader) error {
	s := block.NewSplitter(r, f.data.sc)
	fd, err := newFileData(s, func(data []byte) (string, error) {
		return f.s.CASPut(ctx, data)
	})
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalLocked()
	f.data = fd
	return nil
}

// Name reports the attributed name of f, which may be "" if f is not a child
// file and was not assigned a name at creation.
func (f *File) Name() string { f.mu.RLock(); defer f.mu.RUnlock(); return f.name }

// A ScanItem is the argument to the Scan callback.
type ScanItem struct {
	*File // the current file being visited

	Name string // the name of File within its parent ("" at the root)
}

// Scan recursively visits f and all its descendants in depth-first
// left-to-right order, calling visit for each file.  If visit returns false,
// no descendants of f are visited.
//
// The visit function may modify the attributes or contents of the files it
// visits, but the caller is responsible for flushing the root of the scan
// afterward to persist changes to storage.
func (f *File) Scan(ctx context.Context, visit func(ScanItem) bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recScanLocked(ctx, "", func(s ScanItem) bool {
		// Yield the lock while the caller visitor runs, then reacquire it.  We
		// do this so that the visitor can use methods that may themselves update
		// the file, without deadlocking on the scan.
		s.File.mu.Unlock() // N.B. unlock → lock
		defer s.File.mu.Lock()
		return visit(s)
	})
}

// recScanLocked recursively scans f and all its child nodes in depth-first
// left-to-right order, calling visit for each file.
func (f *File) recScanLocked(ctx context.Context, name string, visit func(ScanItem) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !visit(ScanItem{File: f, Name: name}) {
		return nil // skip the descendants of f
	}
	for i, kid := range f.kids {
		fp := kid.File
		if fp == nil {
			// If the child was not already open, we need to do so to scan it, but
			// we won't persist it in the parent unless the visitor invalidated it.
			var err error
			fp, err = Open(ctx, f.s, kid.Key)
			if err != nil {
				return err
			}
		}
		err := func() error {
			fp.mu.Lock()
			defer fp.mu.Unlock()
			return fp.recScanLocked(ctx, kid.Name, visit)
		}()
		if err != nil {
			return err
		}

		// If scanning invalidated fp, make sure the parent copy is updated.
		// This ensures the parent will include these changes in a flush.
		if fp.key == "" {
			f.kids[i].File = fp
		}
	}
	return nil
}

// Cursor binds f with a context so that it can be used to satisfy the standard
// interfaces defined by the io package.  The resulting cursor may be used only
// during the lifetime of the request whose context it binds.
func (f *File) Cursor(ctx context.Context) *Cursor { return &Cursor{ctx: ctx, file: f} }

// XAttr returns a view of the extended attributes of f.
func (f *File) XAttr() XAttr { return XAttr{f: f} }

// Precondition: The caller holds f.mu exclusively, or has the only reference to f.
func (f *File) fromWireType(obj *wiretype.Object) error {
	pb, ok := obj.Value.(*wiretype.Object_Node)
	if !ok {
		return errors.New("object does not contain a node")
	}

	pb.Node.Normalize()
	f.data = fileData{} // reset
	if err := f.data.fromWireType(pb.Node.Index); err != nil {
		return fmt.Errorf("index: %w", err)
	}
	f.stat.fromWireType(pb.Node.Stat)
	f.saveStat = pb.Node.Stat != nil

	f.xattr = make(map[string]string)
	for _, xa := range pb.Node.XAttrs {
		f.xattr[xa.Name] = string(xa.Value)
	}

	f.kids = nil
	for _, kid := range pb.Node.Children {
		f.kids = append(f.kids, child{
			Name: kid.Name,
			Key:  string(kid.Key),
		})
	}
	return nil
}

func (f *File) toWireTypeLocked() *wiretype.Object {
	n := &wiretype.Node{Index: f.data.toWireType()}
	if f.saveStat {
		n.Stat = f.stat.toWireType()
	}
	for name, value := range f.xattr {
		n.XAttrs = append(n.XAttrs, &wiretype.XAttr{
			Name:  name,
			Value: []byte(value),
		})
	}
	for _, kid := range f.kids {
		n.Children = append(n.Children, &wiretype.Child{
			Name: kid.Name,
			Key:  []byte(kid.Key),
		})
	}
	n.Normalize()
	return &wiretype.Object{Value: &wiretype.Object_Node{Node: n}}
}

// Encode translates f as a protobuf message for storage.
func Encode(f *File) *wiretype.Object {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.toWireTypeLocked()
}
