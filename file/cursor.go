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

package file

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"time"
)

// A Cursor bundles a *File with a context so that the file can be used with
// the standard interfaces defined by the io package. A Cursor value may be
// used only during the lifetime of the request whose context it binds.
//
// Each Cursor maintains a separate offset position on the underlying file,
// affecting Read, Write, and Seek operations on that cursor. The offset value
// for a newly-created cursor is always 0.
type Cursor struct {
	ctx    context.Context // the captured context
	offset int64           // the current seek position (≥ 0)
	kids   []string        // the directory contents (names only)
	file   *File
}

// Read reads up to len(data) bytes into data from the current offset, and
// reports the number of bytes successfully read, as [io.Reader].
func (c *Cursor) Read(data []byte) (int, error) {
	nr, err := c.file.ReadAt(c.ctx, data, c.offset)
	c.offset += int64(nr)
	return nr, err
}

// Write writes len(data) bytes from data to the current offset, and reports
// the number of bytes successfully written, as [io.Writer].
func (c *Cursor) Write(data []byte) (int, error) {
	nw, err := c.file.WriteAt(c.ctx, data, c.offset)
	c.offset += int64(nw)
	return nw, err
}

// ReadAt implements the [io.ReaderAt] interface.
func (c *Cursor) ReadAt(data []byte, offset int64) (int, error) {
	return c.file.ReadAt(c.ctx, data, offset)
}

// WriteAt implments the [io.WriterAt] interface.
func (c *Cursor) WriteAt(data []byte, offset int64) (int, error) {
	return c.file.WriteAt(c.ctx, data, offset)
}

// ReadDir implements the [fs.ReadDirFile] interface.
func (c *Cursor) ReadDir(n int) ([]fs.DirEntry, error) {
	if !c.file.Stat().Mode.IsDir() {
		return nil, fmt.Errorf("%w: not a directory", fs.ErrInvalid)
	}
	if c.kids == nil {
		c.kids = c.file.Child().Names()
	}
	out := c.kids
	if n > 0 && n < len(out) {
		out = out[:n]
		c.kids = c.kids[n:]
	} else {
		c.kids = nil
	}
	if len(out) == 0 {
		if n > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}

	de := make([]fs.DirEntry, len(out))
	for i, name := range out {
		de[i] = DirEntry{ctx: c.ctx, parent: c.file, name: name}
	}
	return de, nil
}

// Seek sets the starting offset for the next Read or Write, as io.Seeker.
func (c *Cursor) Seek(offset int64, whence int) (int64, error) {
	target := offset
	switch whence {
	case io.SeekStart:
		// use offset as written
	case io.SeekCurrent:
		target += c.offset
	case io.SeekEnd:
		target += c.file.data.size()
	default:
		return 0, fmt.Errorf("seek: invalid offset relation %v", whence)
	}
	if target < 0 {
		return 0, fmt.Errorf("seek: invalid target offset %d", target)
	}
	c.offset = target
	return c.offset, nil
}

// Tell reports the current offset of the cursor.
func (c *Cursor) Tell() int64 { return c.offset }

// Close implements the [io.Closer] interface. A File does not have a system
// descriptor, so "closing" performs a flush but does not invalidate the file.
func (c *Cursor) Close() error { _, err := c.file.Flush(c.ctx); return err }

// Stat implements part of the [fs.File] interface.
func (c *Cursor) Stat() (fs.FileInfo, error) { return c.file.fileInfo(), nil }

// FileInfo implements the fs.FileInfo interface for a [File].
type FileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	file    *File
}

func (n FileInfo) Name() string       { return n.name }
func (n FileInfo) Size() int64        { return n.size }
func (n FileInfo) Mode() fs.FileMode  { return n.mode }
func (n FileInfo) ModTime() time.Time { return n.modTime }
func (n FileInfo) IsDir() bool        { return n.mode.IsDir() }

// Sys returns the the [*file.File] whose stat record n carries.
func (n FileInfo) Sys() any { return n.file }

// DirEntry implements the fs.DirEntry interface.
type DirEntry struct {
	ctx    context.Context
	parent *File
	name   string
	file   *File
}

func (d DirEntry) getFile() (*File, error) {
	if d.file == nil {
		f, err := d.parent.Open(d.ctx, d.name)
		if err != nil {
			return nil, err
		}
		d.file = f
	}
	return d.file, nil
}

// Name implements part of the [fs.DirEntry] interface.
func (d DirEntry) Name() string { return d.name }

func (d DirEntry) IsDir() bool {
	f, err := d.getFile()
	if err == nil {
		return f.Stat().Mode.IsDir()
	}
	return false
}

// Type implements part of the [fs.DirEntry] interface.
func (d DirEntry) Type() fs.FileMode {
	f, err := d.getFile()
	if err == nil {
		return f.Stat().Mode.Type()
	}
	return 0
}

// Info implements part of the [fs.DirEntry] interface.
// The concrete type of a successful result is [FileInfo].
func (d DirEntry) Info() (fs.FileInfo, error) {
	f, err := d.getFile()
	if err != nil {
		return nil, err
	}
	return f.fileInfo(), nil
}
