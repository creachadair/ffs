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

// Package encrypted implements an encryption codec which encodes data by
// encrypting and authenticating with a block cipher in Galois Counter Mode
// (GCM).
package encrypted

import (
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// A Codec implements the encoded.Codec interface and encrypts and
// authenticates data using a block cipher in CTR mode.
type Codec struct {
	aead   cipher.AEAD        // the encryption context
	random func([]byte) error // used to generate nonce values
}

// Options control the construction of a *Codec.
type Options struct {
	// Replace the contents of buf with cryptographically-secure random bytes.
	// If nil, the store uses the crypto/rand package to generate bytes.
	Random func(buf []byte) error
}

func (o *Options) random() func([]byte) error {
	if o != nil && o.Random != nil {
		return o.Random
	}
	return func(buf []byte) error {
		_, err := rand.Read(buf)
		return err
	}
}

// New constructs an encryption codec that uses the given block cipher.
// If opts == nil, default options are used.  New will panic if blk == nil.
func New(blk cipher.Block, opts *Options) (*Codec, error) {
	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, err
	}
	return &Codec{
		aead:   aead,
		random: opts.random(),
	}, nil
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

// lengthWriter is an io.Writer that discards all data written to it, but
// counts the total number of bytes written.
type lengthWriter struct{ length *int }

func (w lengthWriter) Write(data []byte) (int, error) {
	*w.length += len(data)
	return len(data), nil
}

// DecodedLen implements part of the codec interface. It decodes src from a
// wrapper block, decrypts the original size, and returns it.
func (c *Codec) DecodedLen(src []byte) (int, error) {
	blk, err := parseBlock(src)
	if err != nil {
		return 0, err
	}
	var nbytes int
	if err := c.decrypt(blk, lengthWriter{&nbytes}); err != nil {
		return 0, err
	}
	return nbytes, nil
}

// encrypt compresses and encrypts the given data and returns its encoded block.
func (c *Codec) encrypt(data []byte) ([]byte, error) {
	nlen := c.aead.NonceSize()

	// Preallocate a buffer for the result:
	//
	//         [ nlen ][ nonce ... ][ payload ... ]
	//  bytes:  1       nlen         (see below)
	//
	// The payload is compressed, which may expand the plaintext. In addition,
	// the AEAD adds a tag which we need room for. The preallocation takes both
	// overheads into account so we only have to allocate once.
	buf := make([]byte, 1+nlen+snappy.MaxEncodedLen(len(data))+c.aead.Overhead())
	buf[0] = byte(nlen)
	nonce := buf[1 : 1+nlen]
	if err := c.random(nonce); err != nil {
		return nil, fmt.Errorf("encrypt: generating nonce: %w", err)
	}

	// Compress the plaintext into the buffer after the nonce, then encrypt the
	// compressed data in-place. Both of these will change the length of the
	// afflicted buffer segment, so we then have to reslice the buffer to get
	// the final packet.
	compressed := snappy.Encode(buf[1+nlen:], data)
	encrypted := c.aead.Seal(compressed[:0], nonce, compressed, nil)
	return buf[:1+nlen+len(encrypted)], nil
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (c *Codec) decrypt(blk block, w io.Writer) error {
	plain, err := c.aead.Open(blk.Data[:0], blk.Nonce, blk.Data, nil)
	if err != nil {
		return err
	}
	decompressed, err := snappy.Decode(nil, plain)
	if err != nil {
		return fmt.Errorf("decrypt: decompressing: %w", err)
	}
	_, err = w.Write(decompressed)
	return err
}

type block struct {
	Nonce []byte
	Data  []byte
}

// parseBlock parses the binary encoding of a block, reporting an error if the
// structure of the block is invalid.
func parseBlock(from []byte) (block, error) {
	if len(from) == 0 || len(from) < int(from[0])+1 {
		return block{}, errors.New("parse: invalid block format")
	}
	nonceLen := int(from[0])

	// Copy the input data so that we do not clobber the caller's data.
	return block{
		Nonce: from[1 : 1+nonceLen],
		Data:  from[1+nonceLen:],
	}, nil
}

/*
Implementation notes

An encrypted blob is stored as a buffer with the following structure:

   nlen  byte    : nonce length       | plaintext
   nonce []byte  : nonce (nlen bytes) | plaintext
   data  []byte  : block data         | compressed, encrypted

Block data are compressed with https://github.com/google/snappy.
Encryption is done with AES in Galois Counter Mode (GCM).
*/
