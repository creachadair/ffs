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
	"testing"

	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/encoded"
)

var _ encoded.Codec = (*encrypted.Codec)(nil)

// A fake Keyring implementation with static keys.
type keyring []string

func (k *keyring) Has(id int) bool { return id > 0 && id-1 < len(*k) }

func (k *keyring) Append(id int, buf []byte) []byte { return append(buf, (*k)[id-1]...) }

func (k *keyring) AppendActive(buf []byte) (int, []byte) {
	return len(*k), append(buf, (*k)[len(*k)-1]...)
}

func (k *keyring) addKey(s string) { *k = append(*k, s) }

func TestCodec(t *testing.T) {
	newCipher := func(key []byte) (cipher.AEAD, error) {
		blk, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(blk)
	}
	kr := &keyring{"00000000000000000000000000000000"}
	e := encrypted.New(newCipher, kr)

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
