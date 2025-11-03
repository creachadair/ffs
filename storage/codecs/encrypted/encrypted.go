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
// encrypting and authenticating with a [cipher.AEAD] instance.
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

// A Codec implements the [encoded.Codec] interface and encrypts and
// authenticates data using a [cipher.AEAD] instance.
//
// [encoded.Codec]: https://godoc.org/github.com/creachadair/ffs/storage/encoded#Codec
type Codec struct {
	newCipher func([]byte) (cipher.AEAD, error)
	keys      Keyring
}

// New constructs an encryption codec that uses the given encryption context.
// The newCipher function constructs a [cipher.AEAD] from an encryption key.
// The keys argument is a collection of encryption keys.
// Both newCipher and keys must be non-nil.
func New(newCipher func([]byte) (cipher.AEAD, error), keys Keyring) *Codec {
	switch {
	case newCipher == nil:
		panic("cipher constructor is nil")
	case keys == nil:
		panic("keyring is nil")
	}
	return &Codec{newCipher: newCipher, keys: keys}
}

// Encode encrypts src with the current active key in the provided keyring,
// and writes the result to w.
func (c *Codec) Encode(w io.Writer, src []byte) error { return c.encrypt(w, src) }

// Decode decrypts src with the key ID used to encrypt it, and writes the
// result to w.
func (c *Codec) Decode(w io.Writer, src []byte) error {
	blk, err := parseBlock(src)
	if err != nil {
		return err
	}
	return c.decrypt(w, blk)
}

// encrypt compresses and encrypts the given data and writes it to w.
func (c *Codec) encrypt(w io.Writer, data []byte) error {
	var kbuf [64]byte
	id, key := c.keys.AppendActive(kbuf[:0])

	aead, err := c.newCipher(key)
	if err != nil {
		return err
	}
	nlen := aead.NonceSize()

	// Preallocate a buffer for the result:
	//   size: [   1   |    4    |     nlen     | data ...        ]
	//   desc:    tag     keyID       nonce       payload ...
	//
	// Where tag == 128+nlen.
	//
	// The plaintext is compressed, which may expand it.
	// In addition, the AEAD adds additional data for extra data and message authentication.
	// The buffer must be large enough to hold all of these.
	bufSize := 1 + 4 + nlen + snappy.MaxEncodedLen(len(data)) + aead.Overhead()

	buf := make([]byte, bufSize)
	keyID, nonce, payload := buf[1:5], buf[5:5+nlen], buf[5+nlen:]

	buf[0] = byte(nlen) | 0x80 // tag
	binary.BigEndian.PutUint32(keyID, uint32(id))
	crand.Read(nonce) // panics on error

	// Compress the plaintext into the buffer after the nonce, then encrypt the
	// compressed data in-place. Both of these will change the length of the
	// afflicted buffer segment, so we then have to reslice the buffer to get
	// the final packet.
	compressed := snappy.Encode(payload, data)
	encrypted := aead.Seal(compressed[:0], nonce, compressed, nil)
	outLen := 1 + 4 + nlen + len(encrypted)
	_, err = w.Write(buf[:outLen])
	return err
}

// decrypt decrypts and decompresses the data from a storage wrapper.
func (c *Codec) decrypt(w io.Writer, blk block) error {
	if !c.keys.Has(blk.KeyID) {
		return fmt.Errorf("key id %d not found", blk.KeyID)
	}
	var kbuf [64]byte
	key := c.keys.Append(blk.KeyID, kbuf[:0])
	aead, err := c.newCipher(key)
	if err != nil {
		return err
	}

	plain, err := aead.Open(make([]byte, 0, len(blk.Data)), blk.Nonce, blk.Data, nil)
	if err != nil {
		return err
	}
	dlen, err := snappy.DecodedLen(plain)
	if err != nil {
		return err
	}
	decompressed, err := snappy.Decode(make([]byte, dlen), plain)
	if err != nil {
		return fmt.Errorf("decompress: %w", err)
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

// Keyring is the interface used to fetch encryption keys.
type Keyring interface {
	// Has reports whether the keyring contains a key with the given ID.
	Has(id int) bool

	// Append appends the contents of the specified key to buf, and returns the
	// resulting slice.
	Append(id int, buf []byte) []byte

	// AppendActive appends the contents of the active key to buf, and returns
	// active ID and the updated slice.
	AppendActive(buf []byte) (int, []byte)
}
