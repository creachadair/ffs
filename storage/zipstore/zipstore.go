// Copyright 2023 Michael J. Fromberger. All Rights Reserved.
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

// Package zipstore implements a read-only view of the blob.Store interface
// using files stored in a ZIP archive.
package zipstore

import (
	"archive/zip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/creachadair/ffs/blob"
)

// A Store wraps a zip.Reader and serves its contents as a blob.Store.  The
// contents of the archive must follow the same layout as a filestore.Store,
// with keys encoded as hexadecimal.
type Store struct {
	prefix string
	zf     *zip.ReadCloser
}

// New constructs a Store from the given open zip.Reader. If opts == nil,
// default options are used as described by the Options type. The Store takes
// ownership of zf, and will close zf when the Store is closed.
func New(zf *zip.ReadCloser, opts *Options) Store {
	// Sort file entries so we get the required lexicographical order.
	sort.Slice(zf.File, func(i, j int) bool {
		return zf.File[i].Name < zf.File[j].Name
	})
	pfx := opts.prefix()
	if pfx == "" {
		pfx = longestPrefix(zf)
	}
	return Store{prefix: pfx, zf: zf}
}

func longestPrefix(zf *zip.ReadCloser) string {
	if len(zf.File) == 0 {
		return ""
	}
	longest := zf.File[0].Name
	for _, fp := range zf.File {
		i, name := 0, fp.Name
		for i < len(name) && i < len(longest) {
			if name[i] != longest[i] {
				break
			}
			i++
		}
		if i == 0 {
			return ""
		}
		longest = longest[:i]
	}
	return strings.TrimSuffix(longest, "/")
}

// Options are optional settings for a Store. A nil *Options is ready for use
// and provides default values as described.
type Options struct {
	// Consider only files whose names have this prefix followed by a "/".
	//
	// As a special case, if Prefix == "" but all the entries in the archive
	// share a non-empty common prefix, that prefix is used.
	Prefix string
}

func (o *Options) prefix() string {
	if o == nil || o.Prefix == "" {
		return ""
	}
	return strings.TrimSuffix(o.Prefix, "/")
}

// Construct the expected storage path for the given key. This is the same
// logic that filestore uses.
func (s Store) keyPath(key string) string {
	base := hex.EncodeToString([]byte(key))
	if n := len(base); n < 4 {
		base += "----"[n:] // pad keys so the parts are not empty
	}
	return filepath.Join(s.prefix, base[:3], base[3:])
}

func decodeKey(enc string) (string, error) {
	dec, err := hex.DecodeString(strings.TrimRight(enc, "-")) // trim length pad
	return string(dec), err
}

func (s Store) findFile(key string) *zip.File {
	path := s.keyPath(key)
	n := sort.Search(len(s.zf.File), func(i int) bool {
		return s.zf.File[i].Name >= path
	})
	if n < len(s.zf.File) && s.zf.File[n].Name == path {
		return s.zf.File[n]
	}
	return nil
}

func (s Store) loadKey(key string) ([]byte, error) {
	fp := s.findFile(key)
	if fp == nil {
		return nil, blob.KeyNotFound(key)
	}
	rc, err := fp.Open()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	return data, err
}

var errReadOnly = errors.New("storage is read-only")

// Get implements a method of the blob.Store interface.
func (s Store) Get(_ context.Context, key string) ([]byte, error) { return s.loadKey(key) }

// Put implements a method of the blob.Store interface.  This implementation
// always reports an error, since the store is read-only.
func (s Store) Put(context.Context, blob.PutOptions) error { return errReadOnly }

// Delete implements a method of the blob.Store interface. This implementation
// always reports an error, since the store is read-only.
func (s Store) Delete(_ context.Context, key string) error { return errReadOnly }

// Size implements a method of the blob.Store interface.
func (s Store) Size(_ context.Context, key string) (int64, error) {
	data, err := s.loadKey(key)
	return int64(len(data)), err
}

// List implements a method of the blob.Store interface.
func (s Store) List(_ context.Context, start string, f func(string) error) error {
	if s.prefix != "" && s.prefix != "/" {
		s.prefix += "/"
	}
	for _, fp := range s.zf.File {
		if !strings.HasPrefix(fp.Name, s.prefix) {
			if fp.Name > s.prefix {
				return nil // no more possible matches
			}
			continue
		}

		// prefix/xxx/yyyyy → [xxx, yyy]
		trimmed := strings.TrimPrefix(fp.Name, s.prefix)
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) != 2 || parts[1] == "" {
			continue // directory only, no key
		}
		dec, err := decodeKey(parts[0] + parts[1])
		if err != nil {
			return fmt.Errorf("decode key %q: %w", parts[0]+parts[1], err)
		}
		if dec < start {
			continue // skip keys prior to start
		}

		if err := f(dec); errors.Is(err, blob.ErrStopListing) {
			return nil
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Len implements a method of the blob.Store interface.
func (s Store) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.List(ctx, "", func(key string) error {
		count++
		return nil
	})
	return count, err
}

// Close implements a method of the blob.Store interface.
func (s Store) Close(context.Context) error { return s.zf.Close() }
