// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package fpath

import (
	"context"
	"errors"
	"io/fs"

	"github.com/creachadair/ffs/file"
)

func pathErr(op, path string, err error) error {
	return &fs.PathError{Op: op, Path: path, Err: err}
}

// FS implements the standard library fs.FS, fs.SubFS, and fs.ReadDirFS
// interfaces. Path traversal is rooted at a file assigned when the FS is
// created.
type FS struct {
	ctx  context.Context
	root *file.File
}

// NewFS constructs an FS rooted at the given file. The context is held by the
// FS and must be valid for the full extent of its use.
func NewFS(ctx context.Context, root *file.File) FS {
	return FS{ctx: ctx, root: root}
}

// Open implements the fs.FS interface.
func (fp FS) Open(path string) (fs.File, error) {
	target, err := fp.openFile("open", path)
	if err != nil {
		return nil, err
	}
	return target.Cursor(fp.ctx), nil
}

// Sub implements the fs.SubFS interface.
func (fp FS) Sub(dir string) (fs.FS, error) {
	target, err := fp.openFile("sub", dir)
	if err != nil {
		return nil, err
	}
	return NewFS(fp.ctx, target), nil
}

// ReadDir implements the fs.ReadDirFS interface.
func (fp FS) ReadDir(path string) ([]fs.DirEntry, error) {
	target, err := fp.openFile("readdir", path)
	if err != nil {
		return nil, err
	}
	kids := target.Child()
	out := make([]fs.DirEntry, kids.Len())
	for i, name := range kids.Names() {
		kid, err := target.Open(fp.ctx, name)
		if err != nil {
			return nil, pathErr("readdir", path+"/"+name, err)
		}
		out[i] = fs.FileInfoToDirEntry(kid.Stat().FileInfo())
	}
	return out, nil
}

func (fp FS) openFile(op, path string) (*file.File, error) {
	if !fs.ValidPath(path) {
		return nil, pathErr(op, path, fs.ErrInvalid)
	}
	target, err := Open(fp.ctx, fp.root, path)
	if err == nil {
		return target, nil
	} else if errors.Is(err, file.ErrChildNotFound) {
		return nil, pathErr(op, path, fs.ErrNotExist)
	}
	return nil, pathErr(op, path, err)
}
