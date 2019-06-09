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

	"bitbucket.org/creachadair/ffs/blob"
)

// A Codec defines the capabilities needed to encode and decode.
type Codec interface {
	// Encode writes the encoding of src to w.
	Encode(w io.Writer, src []byte) error

	// MaxEncodedLen reports an estimate of the length of the encoding of src.
	// The estimate is not required to be accurate, but implementations should
	// prefer an over-estimate to an under-estimate.
	MaxEncodedLen(src []byte) int

	// Decode writes the decoding of src to w.
	Decode(w io.Writer, src []byte) error

	// DecodedLen reports the decoded length of src. It reports an error if src
	// is not a valid encoding.
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
	est := s.codec.MaxEncodedLen(opts.Data)
	if est <= 0 {
		est = len(opts.Data)
	}
	buf := bytes.NewBuffer(make([]byte, 0, est))
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

// Delete implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Delete(ctx context.Context, key string) error { return s.real.Delete(ctx, key) }

// List implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, f)
}

// Len implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }

// Identity implements an identity Codec, that encodes blobs as themselves.
type Identity struct{}

// Encode encodes src to w with no transformation.
func (Identity) Encode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

// MaxEncodedLen reports the exact encoded size of src, which is len(src).
func (Identity) MaxEncodedLen(src []byte) int { return len(src) }

// Decode decodes src to w with no transformation.
func (Identity) Decode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

// DecodedLen reports the decoded length of src, which is len(src).
func (Identity) DecodedLen(src []byte) (int, error) { return len(src), nil }
