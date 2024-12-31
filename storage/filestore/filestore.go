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

// Package filestore implements the [blob.KV] interface using files.  The store
// comprises a directory with subdirectories keyed by a prefix of the encoded
// blob key.
package filestore

import (
	"context"
	"encoding/hex"
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

// Store implements the [blob.Store] interface using a directory structure with
// one file per stored blob. Keys are encoded in hex and used to construct the
// file and directory names relative to a root directory, similar to a Git
// local object store.
type Store struct {
	key hexkey.Config
}

// New creates a Store associated with the specified root directory, which is
// created if it does not already exist.
func New(dir string) (Store, error) {
	path := filepath.Clean(dir)
	if err := os.MkdirAll(path, 0700); err != nil {
		return Store{}, err
	}
	return Store{key: hexkey.Config{Prefix: path, Shard: 3}}, nil
}

func (s Store) mkPath(name string) (string, error) {
	if name == "" {
		return s.key.Prefix, nil // already known to exist
	}
	// Prefix non-empty name with "_" to avert conflict with hex keys.
	path := filepath.Join(s.key.Prefix, "_"+hex.EncodeToString([]byte(name)))
	return path, os.MkdirAll(path, 0700)
}

// KV implements part of the [blob.Store] interface.
func (s Store) KV(_ context.Context, name string) (blob.KV, error) {
	path, err := s.mkPath(name)
	if err != nil {
		return nil, err
	}
	return KV{key: s.key.WithPrefix(path)}, nil
}

// CAS implements part of the [blob.Store] interface.
func (s Store) CAS(ctx context.Context, name string) (blob.CAS, error) {
	return blob.CASFromKVError(s.KV(ctx, name))
}

// Sub implements part of the [blob.Store] interface.
func (s Store) Sub(_ context.Context, name string) (blob.Store, error) {
	path, err := s.mkPath(name)
	if err != nil {
		return nil, err
	}
	return Store{key: s.key.WithPrefix(path)}, nil
}

// Close implements part of the [blob.StoreCloser] interface.
// This implementation always reports nil.
func (Store) Close(context.Context) error { return nil }

// KV implements the [blob.kV] interface using a directory structure with one
// file per stored blob. Keys are encoded in hex and used to construct file and
// directory names relative to a root directory, similar to a Git local object
// store.
type KV struct {
	key hexkey.Config
}

// Opener constructs a filestore from an address comprising a path, for use
// with the [store] package. The concrete type of the result is [Store].
//
// [store]: https://godoc.org/github.com/creachadair/ffstools/lib/store
func Opener(ctx context.Context, addr string) (blob.StoreCloser, error) {
	return New(strings.TrimPrefix(addr, "//")) // allow URL-like paths
}

func (s KV) keyPath(key string) string { return s.key.Encode(key) }

// Get implements part of [blob.KV]. It linearizes to the point at which
// opening the key path for reading returns.
func (s KV) Get(_ context.Context, key string) ([]byte, error) {
	bits, err := os.ReadFile(s.keyPath(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = blob.KeyNotFound(key)
		}
		return nil, fmt.Errorf("key %q: %w", key, err)
	}
	return bits, nil
}

// Stat implements part of [blob.KV].
func (s KV) Stat(ctx context.Context, keys ...string) (blob.StatMap, error) {
	out := make(blob.StatMap)
	for _, key := range keys {
		fi, err := os.Stat(s.keyPath(key))
		if err == nil {
			out[key] = blob.Stat{Size: fi.Size()}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("key %q: %w", key, err)
		}
	}
	return out, nil
}

// Put implements part of [blob.KV]. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s KV) Put(_ context.Context, opts blob.PutOptions) error {
	path := s.keyPath(opts.Key)
	if _, err := os.Stat(path); err == nil && !opts.Replace {
		return blob.KeyExists(opts.Key)
	} else if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return atomicfile.WriteData(path, opts.Data, 0600)
}

// Delete implements part of [blob.KV].
func (s KV) Delete(_ context.Context, key string) error {
	path := s.keyPath(key)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return blob.KeyNotFound(key)
	}
	return err
}

// List implements part of [blob.KV]. If any concurrent Put operation on a key
// later than the current scan position succeeds, List linearizes immediately
// prior to the earliest such Put operation. Otherwise, List may be linearized
// to any point during its execution.
func (s KV) List(_ context.Context, start string, f func(string) error) error {
	roots, err := listdir(s.Dir())
	if err != nil {
		return err
	}
	for _, root := range roots {
		cur := filepath.Join(s.Dir(), root)
		keys, err := listdir(cur)
		if err != nil {
			return err
		}
		for _, tail := range keys {
			key, err := s.key.Decode(path.Join(cur, tail))
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

// Len implements part of [blob.KV]. It is implemented using List, so it
// linearizes in the same manner.
func (s KV) Len(ctx context.Context) (int64, error) {
	var nb int64
	if err := s.List(ctx, "", func(string) error {
		nb++
		return nil
	}); err != nil {
		return 0, err
	}
	return nb, nil
}

// Dir reports the directory path associated with s.
func (s KV) Dir() string { return s.key.Prefix }

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
