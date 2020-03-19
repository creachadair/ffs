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
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/creachadair/ffs/blob/codecs/encrypted/wirepb"
	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"
)

// A Codec implements the encoded.Codec interface and encrypts data using a
// block cipher in CTR mode.
type Codec struct {
	blk   cipher.Block       // used to generate the keystream
	newIV func([]byte) error // generate a fresh initialization vector
}

// Options control the construction of a *Codec.
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
func New(blk cipher.Block, opts *Options) *Codec {
	if blk == nil {
		panic("cipher is nil")
	}
	return &Codec{
		blk:   blk,
		newIV: opts.newIV(),
	}
}

// Encode implements part of the codec interface. It encrypts src with the
// provided cipher in CTR mode and writes it out as a wire-format wrapper
// protobuf message to w.
func (c *Codec) Encode(w io.Writer, src []byte) error {
	wrapper, err := c.encrypt(src)
	if err != nil {
		return fmt.Errorf("encryption failed: %v", err)
	}
	bits, err := proto.MarshalOptions{Deterministic: true}.Marshal(wrapper)
	if err != nil {
		return fmt.Errorf("encoding failed: %v", err)
	}
	_, err = w.Write(bits)
	return err
}

// Decode implements part of the codec interface.  It decodes src as a
// wire-format wrapper prototbuf message, decrypts the message, and writes the
// result to w.  If decryption fails, an error is reported without writing any
// data to w.
func (c *Codec) Decode(w io.Writer, src []byte) error {
	wrapper, err := unmarshal(src)
	if err != nil {
		return err
	}
	return c.decrypt(wrapper, w)
}

// DecodedLen implements part of the codec interface. It decodes src as a
// wire-format wrapper protobuf message, and returns the original message size.
func (c *Codec) DecodedLen(src []byte) (int, error) {
	wrapper, err := unmarshal(src)
	if err != nil {
		return 0, err
	}
	return int(wrapper.UncompressedSize), nil
}

func unmarshal(data []byte) (*wirepb.Encrypted, error) {
	var pb wirepb.Encrypted
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, fmt.Errorf("decoding failed: %v", err)
	}
	return &pb, nil
}

// encrypt compresses and encrypts the given data and returns its storage wrapper.
func (c *Codec) encrypt(data []byte) (*wirepb.Encrypted, error) {
	compressed := snappy.Encode(nil, data)
	iv := make([]byte, c.blk.BlockSize())
	if err := c.newIV(iv); err != nil {
		return nil, fmt.Errorf("encrypt: initialization vector: %w", err)
	}
	ctr := cipher.NewCTR(c.blk, iv)
	ctr.XORKeyStream(compressed, compressed)
	return &wirepb.Encrypted{
		Data:             compressed,
		Init:             iv,
		UncompressedSize: int64(len(data)),
	}, nil
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (c *Codec) decrypt(enc *wirepb.Encrypted, w io.Writer) error {
	ctr := cipher.NewCTR(c.blk, enc.Init)
	ctr.XORKeyStream(enc.Data, enc.Data)
	decompressed, err := snappy.Decode(make([]byte, enc.UncompressedSize), enc.Data)
	if err != nil {
		return fmt.Errorf("decrypt: decompress: %w", err)
	} else if int64(len(decompressed)) != enc.UncompressedSize {
		return fmt.Errorf("decrypt: wrong size (got %d, want %d)",
			len(decompressed), enc.UncompressedSize)
	}
	_, err = w.Write(decompressed)
	return err
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
