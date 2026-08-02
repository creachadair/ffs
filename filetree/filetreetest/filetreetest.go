// Copyright 2025 Michael J. Fromberger. All Rights Reserved.
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

// Package filetreetest defines support code for writing tests that use the
// [filetree] package.
package filetreetest

import (
	"context"
	"io/fs"
	"strings"
	"time"

	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/filetree"
)

// TB is the subset of the [testing] interfaces used by this package.
type TB interface {
	Context() context.Context
	Fatalf(string, ...any)
	Helper()
}

// GetRoot returns the named root pointer in s.
func GetRoot(t TB, s filetree.Store, name string) *root.Root {
	t.Helper()
	rp, err := root.Open(t.Context(), s.Roots(), name)
	if err != nil {
		t.Fatalf("Open root %q: %v", name, err)
	}
	return rp
}

// GetFile returns the named file in s.
func GetFile(t TB, s filetree.Store, path string) *filetree.PathInfo {
	pi, err := s.OpenPath(t.Context(), path)
	if err != nil {
		t.Fatalf("OpenPath %q: %v", path, err)
	}
	return pi
}

// SetRoot creates or replaces a root pointer in s with the given name,
// pointing to the given file. If fp == nil, an empty file is created.
// The resulting root is returned.
func SetRoot(t TB, s filetree.Store, name string, fp *file.File) *root.Root {
	t.Helper()
	if name == "" {
		t.Fatalf("Missing root name")
	}
	if fp == nil {
		fp = file.New(s.Files(), nil)
	}
	fk, err := fp.Flush(t.Context())
	if err != nil {
		t.Fatalf("Flush base file: %v", err)
	}
	rp := root.New(s.Roots(), &root.Options{FileKey: fk})
	if err := rp.Save(t.Context(), name); err != nil {
		t.Fatalf("Create root: %v", err)
	}
	return rp
}

// FileInfo describes a file to be added to a store.
type FileInfo struct {
	Path    string            // where the file goes in the tree
	Mode    fs.FileMode       // type and permissions
	ModTime time.Time         // modification time (if non-zero)
	XAttr   map[string]string // extended attributes
	Content string            // content (if non-empty)
}

// SetFile adds or replaces a file in s.
func SetFile(t TB, s filetree.Store, fi FileInfo) *filetree.PathInfo {
	t.Helper()
	if fi.Path == "" {
		t.Fatalf("Missing path")
	}
	fp := file.New(s.Files(), &file.NewOptions{
		Stat:        &file.Stat{Mode: fi.Mode, ModTime: fi.ModTime},
		PersistStat: true,
	})
	for xa, v := range fi.XAttr {
		fp.XAttr().Set(xa, v)
	}
	if fi.Content != "" {
		if err := fp.SetData(t.Context(), strings.NewReader(fi.Content)); err != nil {
			t.Fatalf("Set file data: %v", err)
		}
	}
	if _, err := s.SetPath(t.Context(), fi.Path, fp); err != nil {
		t.Fatalf("SetPath %q: %v", fi.Path, err)
	}
	pi, err := s.OpenPath(t.Context(), fi.Path)
	if err != nil {
		t.Fatalf("OpenPath %q: %v", fi.Path, err)
	}
	return pi
}
