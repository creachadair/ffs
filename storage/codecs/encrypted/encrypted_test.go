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

package encrypted_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/mds/mstr"
)

var _ encoded.Codec = (*encrypted.Codec)(nil)

// A fake Keyring implementation with static keys.
type keyring []string

func (k *keyring) Has(id int) bool { return id > 0 && id-1 < len(*k) }

func (k *keyring) Get(id int, buf []byte) []byte { return append(buf, (*k)[id-1]...) }

func (k *keyring) GetActive(buf []byte) (int, []byte) {
	return len(*k), append(buf, (*k)[len(*k)-1]...)
}

func (k *keyring) addKey(s string) { *k = append(*k, s) }

func newCipher(key []byte) (cipher.AEAD, error) {
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(blk)
}

func newTestCodec(t *testing.T) (*encrypted.Codec, *keyring) {
	t.Helper()
	kr := &keyring{"00000000000000000000000000000000"}
	e := encrypted.New(newCipher, kr)
	return e, kr
}

func TestCodec(t *testing.T) {
	e, kr := newTestCodec(t)

	checkEncrypt := func(ptext string) []byte {
		t.Helper()
		var buf bytes.Buffer
		if err := e.Encode(&buf, []byte(ptext)); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		return buf.Bytes()
	}
	checkDecrypt := func(ctext []byte, want string) {
		t.Helper()
		var buf bytes.Buffer
		if err := e.Decode(&buf, ctext); err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if buf.String() != want {
			t.Errorf("Decode:\ngot:  %s\nwant: %s", buf.String(), want)
		}
	}

	const value = "some of what a fool thinks often remains"
	t.Logf("Input (%d bytes): %q", len(value), value)

	// Encode the test value through the encrypted codec.
	// Log the stored block for debugging purposes.
	enc := checkEncrypt(value)
	t.Logf("Stored block 1 (%d bytes):\n%#q", len(enc), enc)
	if id := int(binary.BigEndian.Uint32(enc[1:5])); id != 1 {
		t.Errorf("Key ID is %d, want 1", id)
	}

	checkDecrypt(enc, value)

	// Add a new key and verify that we can still decrypt the old block.
	kr.addKey("11111111111111111111111111111111")
	checkDecrypt(enc, value)

	// Encrypt a new block, which should use the new key.
	enc2 := checkEncrypt(value)
	t.Logf("Stored block 2 (%d bytes):\n%#q", len(enc2), enc2)
	if id := int(binary.BigEndian.Uint32(enc2[1:5])); id != 2 {
		t.Errorf("Key ID is %d, want 2", id)
	}

	checkDecrypt(enc, value)
	checkDecrypt(enc2, value)

	// Verify that we can also handle the legacy block format without a key ID.
	// This format always uses the first key version in the ring.
	legacy := make([]byte, 0, len(enc)-4)
	legacy = append(legacy, enc[0]&0x7f)
	legacy = append(legacy, enc[5:]...)
	t.Logf("Legacy block (%d bytes):\n%#q", len(legacy), legacy)

	checkDecrypt(legacy, value)
}

func TestErrors(t *testing.T) {
	e, kr := newTestCodec(t)
	_ = kr
	t.Run("Empty", func(t *testing.T) {
		if err := e.Decode(io.Discard, nil); err == nil || !strings.Contains(err.Error(), "invalid block") {
			t.Errorf("Decode nil: got %v, want invalid block", err)
		}
		if err := e.Decode(io.Discard, []byte{}); err == nil || !strings.Contains(err.Error(), "invalid block") {
			t.Errorf("Decode empty: got %v, want invalid block", err)
		}
	})
	t.Run("Short/v0", func(t *testing.T) {
		const input = "\x05\x00\x00\x00" // 5-byte nonce length, only 3 bytes provided
		err := e.Decode(io.Discard, []byte(input))
		if err == nil || !strings.Contains(err.Error(), "truncated v0") {
			t.Errorf("Decode short v0: got %v, want truncated v0", err)
		}
	})
	t.Run("Short/v1", func(t *testing.T) {
		for _, input := range []string{
			"\x80\x00\x00\x00",
			//   ^---key ID (short)
			"\x85\x00\x00\x00\x01\x0f\x1e\x2d",
			//   ^----key ID----|^-- nonce (short)
		} {
			err := e.Decode(io.Discard, []byte(input))
			if err == nil || !strings.Contains(err.Error(), "truncated v1") {
				t.Errorf("Decode short v1: got %v, want truncated v1", err)
			}
		}
	})
	t.Run("BadKeyID", func(t *testing.T) {
		for _, input := range []string{
			"\x82\x00\x00\x00\x00\x12\x34\x56\x78",
			//   ^----key ID----| ID 0 is not valid
			"\x82\x00\x00\x00\x02\x12\x34\x56\x78",
			//   ^----key ID----| ID 2 does not exist in the test ring
		} {
			err := e.Decode(io.Discard, []byte(input))
			if err == nil || !mstr.Match(err.Error(), "key id * not found") {
				t.Errorf("Decode missing key: got %v want key not found", err)
			}
		}
	})
}
