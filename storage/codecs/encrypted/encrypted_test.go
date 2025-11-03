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
	"testing"

	"github.com/creachadair/ffs/storage/codecs/encrypted"
)

func TestRoundTrip(t *testing.T) {
	aes, err := aes.NewCipher([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("Creating AES cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(aes)
	if err != nil {
		t.Fatalf("Creating AES-GCM instance: %v", err)
	}

	e := encrypted.New(gcm, nil)

	const value = "some of what a fool thinks often remains"
	t.Logf("Input (%d bytes): %q", len(value), value)

	// Encode the test value through the encrypted codec.
	var encoded bytes.Buffer
	if err := e.Encode(&encoded, []byte(value)); err != nil {
		t.Fatalf("Encode %q failed: %v", value, err)
	}

	// Log the stored block for debugging purposes.
	t.Logf("Stored block (%d bytes):\n%+v", encoded.Len(), encoded.Bytes())

	// Verify that we can decode the blob to recover the original value.
	var verify bytes.Buffer
	src := encoded.String()
	if err := e.Decode(&verify, []byte(src)); err != nil {
		t.Fatalf("Decode [%d bytes] failed: %v", encoded.Len(), err)
	} else if got := verify.String(); got != value {
		t.Errorf("Decode: got %q, want %q", got, value)
	}
}
