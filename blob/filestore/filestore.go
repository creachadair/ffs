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
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bitbucket.org/creachadair/ffs/blob"
	"golang.org/x/xerrors"
)

// Store implements the blob.Store interface using a directory structure with
// one file per stored blob. Keys are encoded in hex and used to construct file
// and directory names relative to a root directory, similar to a Git local
// object store.
type Store struct {
	dir string
}

// New creates a Store associated with the specified root directory, which is
// created if it does not already exist.
func New(dir string) (*Store, error) {
	path := filepath.Clean(dir)
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	return &Store{dir: path}, nil
}

func (s *Store) keyPath(key string) string {
	base := hex.EncodeToString([]byte(key + "\x00\x00")) // ensure length ≥ 2
	return filepath.Join(s.dir, base[:2], base[2:])
}

func decodeKey(enc string) string {
	dec, _ := hex.DecodeString(enc)
	return strings.TrimSuffix(string(dec), "\x00\x00") // trim length pad
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	bits, err := ioutil.ReadFile(s.keyPath(key))
	if os.IsNotExist(err) {
		return nil, xerrors.Errorf("key %q: %w", key, blob.ErrKeyNotFound)
	}
	return bits, err
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	path := s.keyPath(opts.Key)
	if _, err := os.Stat(path); err == nil && !opts.Replace {
		return xerrors.Errorf("key %q: %w", opts.Key, blob.ErrKeyExists)
	}
	f, err := ioutil.TempFile(s.dir, "put*")
	if err != nil {
		return nil
	}
	_, err = f.Write(opts.Data)
	cerr := f.Close()
	if err != nil {
		return err
	} else if cerr != nil {
		return cerr
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		os.Remove(f.Name()) // best effort
		return err
	}

	// This implementation assumes rename is atomic. It should be when the
	// filesystem is POSIX compliant, since we created the temp file in the same
	// directory as the target file.
	return os.Rename(f.Name(), s.keyPath(opts.Key))
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (int64, error) {
	fi, err := os.Stat(s.keyPath(key))
	if os.IsNotExist(err) {
		return 0, xerrors.Errorf("key %q: %w", key, blob.ErrKeyNotFound)
	} else if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	path := s.keyPath(key)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return xerrors.Errorf("key %q: %w", key, blob.ErrKeyNotFound)
	}
	_ = os.Remove(filepath.Dir(path)) // best effort, if empty
	return err
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	roots, err := listdir(s.dir)
	if err != nil {
		return err
	}
	for _, root := range roots {
		if root < start {
			continue
		}
		keys, err := listdir(filepath.Join(s.dir, root))
		if err != nil {
			return err
		}
		for _, tail := range keys {
			key := decodeKey(root + tail)
			if err := f(key); xerrors.Is(err, blob.ErrStopListing) {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	return nil
}

// Len implements part of blob.Store.
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
