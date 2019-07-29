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

// Package encrypted implements an encrypted blob store in which blobs are
// encrypted with a block cipher in CTR mode. Blob storage is delegated to an
// underlying blob.Store implementation, to which the encryption is opaque.
package encrypted

import (
	"context"
	"crypto/cipher"
	"crypto/rand"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/encrypted/wirepb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"golang.org/x/xerrors"
)

// A Store implements the blob.Store interface and encrypts blob data using a
// block cipher in CTR mode. Blob storage is delegated to an underlying store.
//
// Note that keys are not encrypted, only blob contents.
type Store struct {
	blk   cipher.Block       // used to generate the keystream
	newIV func([]byte) error // generate a fresh initialization vector
	real  blob.Store         // the underlying storage implementation
}

// Options control the construction of a *Store.
type Options struct {
	// Replace the contents of iv with fresh initialization vector.
	// If nil, the store uses the crypto/rand package to generate random IVs.
	NewIV func(iv []byte) error
}

func (o *Options) newIV() func([]byte) error {
	if o != nil && o.NewIV != nil {
		return o.NewIV
	}
	return func(iv []byte) error {
		_, err := rand.Read(iv)
		return err
	}
}

// New constructs a new encrypted store that delegates to s.  If opts == nil,
// default options are used.  New will panic if s == nil or blk == nil.
func New(s blob.Store, blk cipher.Block, opts *Options) *Store {
	if s == nil {
		panic("store is nil")
	} else if blk == nil {
		panic("cipher is nil")
	}
	return &Store{
		blk:   blk,
		newIV: opts.newIV(),
		real:  s,
	}
}

// Get implements part of the blob.Store interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	enc, err := s.load(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.decrypt(enc)
}

// Put implements part of the blob.Store interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	enc, err := s.encrypt(opts.Data)
	if err != nil {
		return err
	}
	bits, err := proto.Marshal(enc)
	if err != nil {
		return err
	}

	// Leave the original options as given, but replace the data.
	opts.Data = bits
	return s.real.Put(ctx, opts)
}

// Size implements part of the blob.Store interface. This implementation
// requires access to the blob content, since the stored size of an encrypted
// blob is not equivalent to the original.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	enc, err := s.load(ctx, key)
	if err != nil {
		return 0, err
	}
	return enc.UncompressedSize, nil
}

// List implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, f)
}

// Len implements part of the blob.Store interface.
// It delegates directly to the underlying store.
func (s *Store) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }

// load fetches a stored blob and decodes its storage wrapper.
func (s *Store) load(ctx context.Context, key string) (*wirepb.Encrypted, error) {
	bits, err := s.real.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	pb := new(wirepb.Encrypted)
	if err := proto.Unmarshal(bits, pb); err != nil {
		return nil, err
	}
	return pb, nil
}

// encrypt compresses and encrypts the given data and returns its storage wrapper.
func (s *Store) encrypt(data []byte) (*wirepb.Encrypted, error) {
	compressed := snappy.Encode(nil, data)
	iv := make([]byte, s.blk.BlockSize())
	if err := s.newIV(iv); err != nil {
		return nil, xerrors.Errorf("encrypt: initialization vector: %w", err)
	}
	ctr := cipher.NewCTR(s.blk, iv)
	ctr.XORKeyStream(compressed, compressed)
	return &wirepb.Encrypted{
		Data:             compressed,
		Init:             iv,
		UncompressedSize: int64(len(data)),
	}, nil
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (s *Store) decrypt(enc *wirepb.Encrypted) ([]byte, error) {
	ctr := cipher.NewCTR(s.blk, enc.Init)
	ctr.XORKeyStream(enc.Data, enc.Data)
	decompressed, err := snappy.Decode(make([]byte, enc.UncompressedSize), enc.Data)
	if err != nil {
		return nil, xerrors.Errorf("decrypt: decompress: %w", err)
	} else if int64(len(decompressed)) != enc.UncompressedSize {
		return nil, xerrors.Errorf("decrypt: wrong size (got %d, want %d)",
			len(decompressed), enc.UncompressedSize)
	}
	return decompressed, nil
}

/*
Implementation notes

An encrypted blob is stored as an wirepb.Encrypted protocol buffer, inside which
the payload is compressed with snappy [1] and encrypted with AES in CTR mode.
The wrapper message is not itself encrypted.  The stored format is:

   data []byte  [snappy-compressed, encrypted payload]
   init []byte  [initialization vector for this blob]
   size int64   [size in bytes of the original block]

The uncompressed size is stored to simplify Size queries: The actual stored
size of the blob is not correct, but keeping the original size avoids the need
to decrypt and decompress the blob contents. The blob must still be fetched,
however.

[1]: https://github.com/google/snappy
*/
