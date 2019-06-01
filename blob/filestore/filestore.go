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
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bitbucket.org/creachadair/ffs/blob"
	"github.com/golang/snappy"
	"golang.org/x/xerrors"
)

// Store implements the blob.Store interface using a directory structure with
// one file per stored blob. Keys are encoded in hex and used to construct file
// and directory names relative to a root directory, similar to a Git local
// object store.
//
// The encoded format of a blob is:
//
//     | length-tag ... | compressed-data ... |
//
// where the length-tag is the varint-encoded length of the original blob and,
// compressed-data are the snappy-compressed content of the original blob.
//
// [1]: https://github.com/google/snappy
type Store struct {
	dir string
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

// blockSize reads enough data from r to recover the length tag.
func blockSize(r io.Reader) (int64, error) {
	var buf [binary.MaxVarintLen64]byte

	// If the entire blob is shorter than a maximum varint we will get a short
	// read and possibly an error. As long as we got some data, defer reporting
	// an error until after decoding the length.
	nr, err := r.Read(buf[:])
	if nr > 0 {
		v, n := binary.Varint(buf[:nr])
		if n > 0 {
			return v, nil
		}
		return 0, xerrors.New("corrupted length tag")
	}
	return 0, err
}

// encodeBlock compresses data with snappy and packs it into a block with a
// varint prefix encoding len(data).
func encodeBlock(data []byte) []byte {
	buf := make([]byte, 4+snappy.MaxEncodedLen(len(data)))
	n := binary.PutVarint(buf, int64(len(data)))
	enc := snappy.Encode(buf[n:], data)
	return buf[:n+len(enc)]
}

// decodeBlock reads a varint prefix from data, decompresses the rest, and
// verifies that the result is of the expected length. If so, the decompressed
// block contents are returned.
func decodeBlock(data []byte) ([]byte, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return nil, xerrors.New("invalid length tag")
	}
	blk, err := snappy.Decode(nil, data[n:])
	if err != nil {
		return nil, err
	}
	if v != int64(len(blk)) {
		return nil, xerrors.Errorf("corrupted block: got %d bytes, want %d", len(blk), v)
	}
	return blk, nil
}

// Get implements part of blob.Store. It linearizes to the point at which
// opening the key path for reading returns.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	bits, err := ioutil.ReadFile(s.keyPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			err = blob.ErrKeyNotFound
		}
		return nil, xerrors.Errorf("key %q: %w", key, err)
	}
	blk, err := decodeBlock(bits)
	if err != nil {
		return nil, xerrors.Errorf("key %q: %w", key, err)
	}
	return blk, nil
}

// Put implements part of blob.Store. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	path := s.keyPath(opts.Key)
	if _, err := os.Stat(path); err == nil && !opts.Replace {
		return xerrors.Errorf("key %q: %w", opts.Key, blob.ErrKeyExists)
	}
	f, err := ioutil.TempFile(s.dir, "put.*")
	if err != nil {
		return err
	}
	_, err = f.Write(encodeBlock(opts.Data))
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
	return os.Rename(f.Name(), path)
}

// Size implements part of blob.Store. It linearizes to the point at which
// opening the key path succeeds.
func (s *Store) Size(_ context.Context, key string) (int64, error) {
	f, err := os.Open(s.keyPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			err = blob.ErrKeyNotFound
		}
		return 0, xerrors.Errorf("key %q: %w", key, err)
	}
	defer f.Close()
	return blockSize(f)
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
		if root < start || strings.HasPrefix(root, "put.") {
			continue // skip keys prior to start and writer temporaries
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
