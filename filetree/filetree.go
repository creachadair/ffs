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

// Package filetree defines a composite [blob.Store] implementation that
// handles separate [file.File] and [root.Root] namespaces.
package filetree

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/fpath"
)

// Store is a composite [blob.StoreCloser] that maintains separate buckets for
// root pointers and content-addressed data.
type Store struct {
	roots blob.KV
	files blob.CAS
	fsync blob.KV // as files, but with arbitrary writes for sync operations

	s blob.StoreCloser
}

type nopCloser struct{ blob.Store }

func (nopCloser) Close(context.Context) error { return nil }

// NewStore constructs a Store overlaying the specified base.
func NewStore(ctx context.Context, base blob.Store) (Store, error) {
	var out Store
	if sc, ok := base.(blob.StoreCloser); ok {
		out.s = sc
	} else {
		out.s = nopCloser{base}
	}

	var err error
	out.roots, err = base.KV(ctx, "root")
	if err != nil {
		return Store{}, fmt.Errorf("open root keyspace: %w", err)
	}
	out.fsync, err = base.KV(ctx, "file")
	if err != nil {
		return Store{}, fmt.Errorf("open file keyspace: %w", err)
	}
	out.files, err = base.CAS(ctx, "file")
	if err != nil {
		return Store{}, fmt.Errorf("open file keyspace: %w", err)
	}
	return out, nil
}

// Files returns the files bucket of the underlying storage.
func (s Store) Files() blob.CAS { return s.files }

// Roots returns the roots bucket of the underlying storage.
func (s Store) Roots() blob.KV { return s.roots }

// Sync returns a sync view of the files bucket.
func (s Store) Sync() blob.KV { return s.fsync }

// Base returns the underlying store for c.
func (s Store) Base() blob.Store { return s.s }

// Close closes the store attached to c.
func (s Store) Close(ctx context.Context) error { return s.s.Close(ctx) }

func isAllHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// PathInfo is the result of parsing and opening a path spec.
type PathInfo struct {
	Path    string     // the original input path (unparsed)
	Base    *file.File // the root or starting file of the path
	BaseKey string     // the storage key of the base file
	File    *file.File // the target file of the path
	FileKey string     // the storage key of the target file
	Root    *root.Root // the specified root, or nil if none
	RootKey string     // the key of root, or ""
}

// Flush flushes the base file to reflect any changes and returns its updated
// storage key. If p is based on a root, the root is also updated and saved.
func (p *PathInfo) Flush(ctx context.Context) (string, error) {
	key, err := p.Base.Flush(ctx)
	if err != nil {
		return "", err
	}
	p.BaseKey = key

	// If this path started at a root, write out the updated contents.
	if p.Root != nil {
		// If the file has changed, invalidate the index.
		if p.Root.FileKey != key {
			p.Root.IndexKey = ""
		}
		p.Root.FileKey = key
		if err := p.Root.Save(ctx, p.RootKey, true); err != nil {
			return "", err
		}
	}
	return key, nil
}

// OpenPath parses and opens the specified path in s.
// The path has either the form "<root-key>/some/path" or "@<file-key>/some/path".
func OpenPath(ctx context.Context, s Store, path string) (*PathInfo, error) {
	out := &PathInfo{Path: path}

	first, rest := SplitPath(path)

	// Check for a @file key prefix; otherwise it should be a root.
	if !strings.HasPrefix(first, "@") {
		rp, err := root.Open(ctx, s.Roots(), first)
		if err != nil {
			return nil, err
		}
		rf, err := rp.File(ctx, s.Files())
		if err != nil {
			return nil, err
		}
		out.Root = rp
		out.RootKey = first
		out.Base = rf
		out.File = rf
		out.FileKey = rp.FileKey // provisional

	} else if fk, err := ParseKey(strings.TrimPrefix(first, "@")); err != nil {
		return nil, err

	} else if fp, err := file.Open(ctx, s.Files(), fk); err != nil {
		return nil, err

	} else {
		out.Base = fp
		out.File = fp
		out.FileKey = fk
	}
	out.BaseKey = out.Base.Key() // safe, it was just opened

	// If the rest of the path is empty, the starting point is the target.
	if rest == "" {
		return out, nil
	}

	// Otherwise, open a path relative to the base.
	tf, err := fpath.Open(ctx, out.Base, rest)
	if err != nil {
		return nil, err
	}
	out.File = tf
	out.FileKey = out.File.Key() // safe, it was just opened
	return out, nil
}

// SplitPath parses s as a slash-separated path specification.
// The first segment of s identifies the storage key of a root or file, the
// rest indicates a sequence of child names starting from that file.
// The rest may be empty.
func SplitPath(s string) (first, rest string) {
	if pre, post, ok := strings.Cut(s, "=/"); ok { // <base64>=/more/stuff
		return pre + "=", path.Clean(post)
	}
	if strings.HasSuffix(s, "=") {
		return s, ""
	}
	pre, post, _ := strings.Cut(s, "/")
	return pre, path.Clean(post)
}

// ParseKey parses the string encoding of a key. A key must be a hex string, a
// base64 string, or a literal string prefixed with "@":
//
//	@foo     encodes "foo"
//	@@foo    encodes "@foo"
//	414243   encodes "ABC"
//	eHl6enk= encodes "xyzzy"
func ParseKey(s string) (string, error) {
	if strings.HasPrefix(s, "@") {
		return s[1:], nil
	}
	var key []byte
	var err error
	if isAllHex(s) {
		key, err = hex.DecodeString(s)
	} else if strings.HasSuffix(s, "=") {
		key, err = base64.StdEncoding.DecodeString(s)
	} else {
		key, err = base64.RawStdEncoding.DecodeString(s) // tolerate missing padding
	}
	if err != nil {
		return "", fmt.Errorf("invalid key %q: %w", s, err)
	}
	return string(key), nil
}
