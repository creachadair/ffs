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

// Package encoded implements a blob.Store that applies a reversible encoding
// such as compression to the data. Storage is delegated to an underlying
// blob.Store implementation to which the encoding is opaque.
package encoded

import (
	"bytes"
	"context"
	"io"

	"github.com/creachadair/ffs/blob"
)

// A Codec defines the capabilities needed to encode and decode.
type Codec interface {
	// Encode writes the encoding of src to w. After encoding, src may be garbage.
	Encode(w io.Writer, src []byte) error

	// Decode writes the decoding of src to w.  After decoding, src may be garbage.
	Decode(w io.Writer, src []byte) error

	// DecodedLen reports the decoded length of src. It reports an error if src
	// is not a valid encoding.  After decoding, src may be garbage.
	DecodedLen(src []byte) (int, error)
}

// A Store wraps an existing blob.Store implementation in which blobs are
// encoded using a Codec.
type Store struct {
	codec Codec      // used to compress and decompress blobs
	real  blob.Store // the underlying storage implementation
}

// New constructs a new encrypted store that delegates to s and uses c to
// encode and decode blob data. New will panic if either s or c is nil.
func New(s blob.Store, c Codec) *Store {
	if s == nil {
		panic("store is nil")
	} else if c == nil {
		panic("codec is nil")
	}
	return &Store{codec: c, real: s}
}

// Get implements part of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	enc, err := s.real.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Ideally we would request the decoded length and use that to allocate a
	// buffer for the output. For some codecs, however, it isn't possible to
	// compute the decoded length without performing the decoding, which loses
	// the benefit.
	var buf bytes.Buffer
	if err := s.codec.Decode(&buf, enc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Put implements part of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	buf := bytes.NewBuffer(make([]byte, 0, len(opts.Data)))
	if err := s.codec.Encode(buf, opts.Data); err != nil {
		return err
	}
	// Leave the original options as given, but replace the data.
	opts.Data = buf.Bytes()
	return s.real.Put(ctx, opts)
}

// Size implements part of the blob.Store interface. This implementation
// requires access to the blob content, since the stored size of an encrypted
// blob is not equivalent to the original.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	enc, err := s.real.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	size, err := s.codec.DecodedLen(enc)
	if err != nil {
		return 0, err
	}
	return int64(size), nil
}

// List implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, f)
}

// Len implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }
