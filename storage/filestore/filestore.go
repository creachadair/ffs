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

// Package filestore implements the blob.Store interface using files.  The
// store comprises a directory with subdirectories keyed by a prefix of the
// encoded blob key.
package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/hexkey"
)

// Store implements the blob.Store interface using a directory structure with
// one file per stored blob. Keys are encoded in hex and used to construct file
// and directory names relative to a root directory, similar to a Git local
// object store.
type Store struct {
	dir string
	key hexkey.Config
}

// Opener constructs a filestore from an address comprising a path, for use
// with the store package.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	return New(strings.TrimPrefix(addr, "//")) // tolerate URL-like paths
}

// New creates a Store associated with the specified root directory, which is
// created if it does not already exist.
func New(dir string) (*Store, error) {
	path := filepath.Clean(dir)
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	return &Store{dir: path, key: hexkey.Config{Shard: 3}}, nil
}

func (s *Store) keyPath(key string) string {
	return filepath.Join(s.dir, filepath.FromSlash(s.key.Encode(key)))
}

// Get implements part of blob.Store. It linearizes to the point at which
// opening the key path for reading returns.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	bits, err := os.ReadFile(s.keyPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			err = blob.KeyNotFound(key)
		}
		return nil, fmt.Errorf("key %q: %w", key, err)
	}
	return bits, nil
}

// Put implements part of blob.Store. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	path := s.keyPath(opts.Key)
	if _, err := os.Stat(path); err == nil && !opts.Replace {
		return blob.KeyExists(opts.Key)
	} else if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return atomicfile.WriteData(path, opts.Data, 0600)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	path := s.keyPath(key)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return blob.KeyNotFound(key)
	}
	return err
}

// List implements part of blob.Store. If any concurrent Put operation on a key
// later than the current scan position succeeds, List linearizes immediately
// prior to the earliest such Put operation. Otherwise, List may be linearized
// to any point during its execution.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	roots, err := listdir(s.dir)
	if err != nil {
		return err
	}
	for _, root := range roots {
		keys, err := listdir(filepath.Join(s.dir, root))
		if err != nil {
			return err
		}
		for _, tail := range keys {
			key, err := s.key.Decode(path.Join(root, tail))
			if err != nil || key < start {
				continue // skip non-key files and keys prior to the start
			} else if err := f(key); errors.Is(err, blob.ErrStopListing) {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

// Len implements part of blob.Store. It is implemented using List, so it
// linearizes in the same manner.
func (s *Store) Len(ctx context.Context) (int64, error) {
	var nb int64
	if err := s.List(ctx, "", func(string) error {
		nb++
		return nil
	}); err != nil {
		return 0, err
	}
	return nb, nil
}

// Close implements part of blob.Store. It is a no-op here.
func (*Store) Close(context.Context) error { return nil }

func listdir(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	sort.Strings(names)
	return names, err
}
