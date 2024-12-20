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

// Package encoded implements a [blob.StoreCloser] that applies a reversible
// encoding such as compression or encryption to the data.
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
}

// A Store wraps an existing [blob.Store] implementation so that its key spaces
// are encoded using a [Codec].
type Store struct {
	codec Codec
	real  blob.Store
}

// KV implements a method of [blob.Store]. The concrete type of keyspaces
// returned is [KV].
func (s Store) KV(ctx context.Context, name string) (blob.KV, error) {
	kv, err := s.real.KV(ctx, name)
	if err != nil {
		return nil, err
	}
	return KV{codec: s.codec, real: NewKV(kv, s.codec)}, nil
}

// CAS implements a method of [blob.Store].
func (s Store) CAS(ctx context.Context, name string) (blob.CAS, error) {
	return blob.CASFromKVError(s.KV(ctx, name))
}

// Sub implements a method of [blob.Store]. The concrete type of stores
// returned is [Store].
func (s Store) Sub(ctx context.Context, name string) (blob.Store, error) {
	sub, err := s.real.Sub(ctx, name)
	if err != nil {
		return nil, err
	}
	return Store{codec: s.codec, real: sub}, nil
}

// Close implements a method of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error {
	if c, ok := s.real.(blob.Closer); ok {
		return c.Close(ctx)
	}
	return nil
}

// New constructs a new store that delegates to s and uses c to encode and
// decode blob data. New will panic if either s or c is nil.
func New(s blob.Store, c Codec) Store {
	if s == nil {
		panic("store is nil")
	} else if c == nil {
		panic("codec is nil")
	}
	return Store{codec: c, real: s}
}

// A KV wraps an existing [blob.KV] implementation in which blobs are encoded
// using a [Codec].
type KV struct {
	codec Codec   // used to compress and decompress blobs
	real  blob.KV // the underlying storage implementation
}

// NewKV constructs a new KV that delegates to kv and uses c to encode and
// decode blob data. NewKV will panic if either kv or c is nil.
func NewKV(kv blob.KV, c Codec) KV {
	if kv == nil {
		panic("keyspace is nil")
	} else if c == nil {
		panic("codec is nil")
	}
	return KV{codec: c, real: kv}
}

// Get implements part of the [blob.KV] interface.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
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

// Put implements part of the [blob.KV] interface.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	buf := bytes.NewBuffer(make([]byte, 0, len(opts.Data)))
	if err := s.codec.Encode(buf, opts.Data); err != nil {
		return err
	}
	// Leave the original options as given, but replace the data.
	opts.Data = buf.Bytes()
	return s.real.Put(ctx, opts)
}

// Delete implements part of the [blob.KV] interface.
// It delegates directly to the underlying store.
func (s KV) Delete(ctx context.Context, key string) error {
	return s.real.Delete(ctx, key)
}

// List implements part of the [blob.KV] interface.
// It delegates directly to the underlying store.
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, f)
}

// Len implements part of the [blob.KV] interface.
// It delegates directly to the underlying store.
func (s KV) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }
