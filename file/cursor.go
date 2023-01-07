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
	file   *File
}

// Read reads up to len(data) bytes into data from the current offset, and
// reports the number of bytes successfully read, as io.Reader.
func (c *Cursor) Read(data []byte) (int, error) {
	nr, err := c.file.ReadAt(c.ctx, data, c.offset)
	c.offset += int64(nr)
	return nr, err
}

// Write writes len(data) bytes from data to the current offset, and reports
// the number of bytes successfully written, as io.Writer.
func (c *Cursor) Write(data []byte) (int, error) {
	nw, err := c.file.WriteAt(c.ctx, data, c.offset)
	c.offset += int64(nw)
	return nw, err
}

// ReadAt implements the io.ReaderAt interface.
func (c *Cursor) ReadAt(data []byte, offset int64) (int, error) {
	return c.file.ReadAt(c.ctx, data, offset)
}

// WriteAt implments the io.WriterAt interface.
func (c *Cursor) WriteAt(data []byte, offset int64) (int, error) {
	return c.file.WriteAt(c.ctx, data, offset)
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

// Close implements the io.Closer interface. A File does not have a system
// descriptor, so "closing" performs a flush but does not invalidate the file.
func (c *Cursor) Close() error { _, err := c.file.Flush(c.ctx); return err }

// Stat implements part of the fs.File interface.
func (c *Cursor) Stat() (fs.FileInfo, error) { return FileInfo{file: c.file}, nil }

// FileInfo implements the fs.FileInfo interface. The underlying data source
// has concrete type *File.
type FileInfo struct{ file *File }

func (n FileInfo) Name() string       { return n.file.name }
func (n FileInfo) Size() int64        { return n.file.Data().Size() }
func (n FileInfo) Mode() fs.FileMode  { return n.file.stat.Mode }
func (n FileInfo) ModTime() time.Time { return n.file.stat.ModTime }
func (n FileInfo) IsDir() bool        { return n.file.stat.Mode.IsDir() }
func (n FileInfo) Sys() interface{}   { return n.file }
