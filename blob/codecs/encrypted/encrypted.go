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
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
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
// provided cipher in CTR mode and writes it out as an encoded block to w.
func (c *Codec) Encode(w io.Writer, src []byte) error {
	bits, err := c.encrypt(src)
	if err != nil {
		return fmt.Errorf("encryption failed: %v", err)
	}
	_, err = w.Write(bits)
	return err
}

// Decode implements part of the codec interface.  It decodes src from a
// wrapper block, decrypts the message, and writes the result to w.  If
// decryption fails, an error is reported without writing any data to w.
func (c *Codec) Decode(w io.Writer, src []byte) error {
	blk, err := parseBlock(src)
	if err != nil {
		return err
	}
	return c.decrypt(blk, w)
}

// DecodedLen implements part of the codec interface. It decodes src from a
// wrapper block, decrypts the original size, and returns it.
func (c *Codec) DecodedLen(src []byte) (int, error) {
	enc, err := parseBlock(src)
	if err != nil {
		return 0, err
	}

	ctr := cipher.NewCTR(c.blk, enc.IV)
	ctr.XORKeyStream(enc.Len, enc.Len)
	return int(binary.BigEndian.Uint32(enc.Len)), nil
}

// encrypt compresses and encrypts the given data and returns its encoded block.
func (c *Codec) encrypt(data []byte) ([]byte, error) {
	ivLen := c.blk.BlockSize()
	bufSize := 1 + ivLen + 4 + snappy.MaxEncodedLen(len(data))

	buf := make([]byte, bufSize)
	buf[0] = byte(ivLen)
	if err := c.newIV(buf[1 : 1+ivLen]); err != nil {
		return nil, fmt.Errorf("encrypt: initialization vector: %w", err)
	}

	binary.BigEndian.PutUint32(buf[1+ivLen:], uint32(len(data)))
	compressed := snappy.Encode(buf[1+ivLen+4:], data)
	finalLen := 1 + ivLen + 4 + len(compressed)

	ebuf := buf[1+ivLen : finalLen]
	ctr := cipher.NewCTR(c.blk, buf[1:1+ivLen])
	ctr.XORKeyStream(ebuf, ebuf)

	return buf[:finalLen], nil
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (c *Codec) decrypt(enc block, w io.Writer) error {
	ctr := cipher.NewCTR(c.blk, enc.IV)

	ctr.XORKeyStream(enc.Len, enc.Len)
	ctr.XORKeyStream(enc.Data, enc.Data)
	decLen := int(binary.BigEndian.Uint32(enc.Len))

	decompressed, err := snappy.Decode(make([]byte, decLen), enc.Data)
	if err != nil {
		return fmt.Errorf("decrypt: decompress: %w", err)
	} else if len(decompressed) != decLen {
		return fmt.Errorf("decrypt: wrong size (got %d, want %d)", len(decompressed), decLen)
	}

	_, err = w.Write(decompressed)
	return err
}

type block struct {
	IV   []byte
	Len  []byte
	Data []byte
}

// parseBlock parses the binary encoding of a block, reporting an error if the
// structure of the block is invalid.
func parseBlock(from []byte) (block, error) {
	if len(from) < 5 {
		return block{}, errors.New("parse: invalid block format")
	}
	ivLen := int(from[0])
	if len(from) < 5+ivLen {
		return block{}, errors.New("parse: invalid initialization vector")
	}

	// Copy the input data so that we do not clobber the caller's data.
	return block{
		IV:   from[1 : 1+ivLen],
		Len:  from[1+ivLen : 1+ivLen+4],
		Data: from[1+ivLen+4:],
	}, nil
}

/*
Implementation notes

An encrypted blob is stored as a buffer with the following structure:

   ilen byte    : initialization vector length       | plaintext
   iint []byte  : initialization vector (ilen bytes) | plaintext
   size uint32  : size in bytes of plaintext block   | encrypted
   data []byte  : block data                         | compressed, encrypted

The size of the original block is encrypted but stored without compression at
the head of the encrypted blob, to allow answering size queries without having
to fully decrypt and decompress the blob data.

Block data are compressed with https://github.com/google/snappy.
Encrpytion is done with AES en CTR mode.

A minimal blob is 5 bytes in length, consisting of a 1-byte zero IV tag and
four bytes of encrypted length.
*/
