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
// encrypting and authenticating with a cipher.AEAD instance.
package encrypted

import (
	"crypto/cipher"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// A Codec implements the encoded.Codec interface and encrypts and
// authenticates data using a cipher.AEAD instance.
type Codec struct {
	aead cipher.AEAD // the encryption context
}

// Options control the construction of a *Codec.
type Options struct {
}

// New constructs an encryption codec that uses the given encryption context.
// If opts == nil, default options are used.  New will panic if aead == nil.
//
// For AES-GCM, you can use the cipher.NewGCM constructor.
// For ChaCha20-Poly1305 (RFC 8439) see golang.org/x/crypto/chacha20poly1305.
func New(aead cipher.AEAD, opts *Options) *Codec {
	if aead == nil {
		panic("aead == nil")
	}
	return &Codec{aead: aead}
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
	crand.Read(nonce) // panics on error

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
	KeyID int
	Nonce []byte
	Data  []byte
}

// parseBlock parses the binary encoding of a block, reporting an error if the
// structure of the block is invalid.
func parseBlock(from []byte) (block, error) {
	if len(from) == 0 {
		return block{}, errors.New("parse: invalid block format")
	}
	hasKeyID := from[0]&0x80 != 0
	nonceLen := int(from[0] & 0x7f)
	if hasKeyID {
		if len(from) < 5+nonceLen {
			return block{}, errors.New("parse: truncated block")
		}
		return block{
			KeyID: int(binary.BigEndian.Uint32(from[1:])),
			Nonce: from[5 : 5+nonceLen],
			Data:  from[5+nonceLen:],
		}, nil
	}
	return block{
		KeyID: 1,
		Nonce: from[1 : 1+nonceLen],
		Data:  from[1+nonceLen:],
	}, nil
}

/*
Implementation notes

The original format of the encrypted block was:

   Pos | Len  | Description
   ----|------|------------------------------
   0   | 1    | nonce length in bytes (= n)
   1   | n    | AEAD nonce
   n+1 | rest | encrypted compressed data

The current format of the encrypted block is:

   Pos | Len  | Description
   ----|------|------------------------------
   0   | 1    | nonce length in byte (= n+128)
   1   | 4    | key ID (BE uint32 = id)
   5   | n    | AEAD nonce
   n+5 | rest | encrypted compressed data

The two can be distinguished by checking the high-order bit of the first byte
of the stored data. This requires that the actual nonce length is < 128, which
it will be in all practical use.

Block data are compressed before encryption (decompressed after decryption)
with https://github.com/google/snappy.
*/
