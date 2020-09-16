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
	"testing"

	"github.com/creachadair/ffs/blob/codecs/encrypted"
)

func TestRoundTrip(t *testing.T) {
	aes, err := aes.NewCipher([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("Creating AES cipher: %v", err)
	}
	var ivCalled bool
	e := encrypted.New(aes, &encrypted.Options{
		NewIV: func(iv []byte) error {
			ivCalled = true // verify that our hook is used
			for i := range iv {
				iv[i] = 1 // dummy value for testing
			}
			return nil
		},
	})

	const value = "some of what a fool thinks often remains"

	// Encode the test value through the encrypted codec.
	var encoded bytes.Buffer
	if err := e.Encode(&encoded, []byte(value)); err != nil {
		t.Fatalf("Encode %q failed: %v", value, err)
	}

	if !ivCalled {
		t.Error("Put did not invoke the initialization vector hook")
	}

	// Verify that we can decode the blob to recover the original value.
	var verify bytes.Buffer
	if err := e.Decode(&verify, encoded.Bytes()); err != nil {
		t.Fatalf("Decode [%d bytes] failed: %v", encoded.Len(), err)
	} else if got := verify.String(); got != value {
		t.Errorf("Decode: got %q, want %q", got, value)
	}

	// Verify that DecodedLen reflects the input size, not the encoded size.
	size, err := e.DecodedLen(encoded.Bytes())
	if err != nil {
		t.Errorf("DecodedLen failed: %v", err)
	} else if size != len(value) {
		t.Errorf("DecodedLen: got %d, want %d", size, len(value))
	}

	// Log the stored block for debugging purposes.
	t.Logf("Stored block (%d bytes):\n%+v", encoded.Len(), encoded.Bytes())
}
