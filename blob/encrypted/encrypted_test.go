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
	"context"
	"crypto/aes"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/encrypted"
	"bitbucket.org/creachadair/ffs/blob/encrypted/encpb"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"github.com/golang/protobuf/proto"
)

// testIV generates initialization vectors for testing, which in this case is
// just zeroes. In real usage, it is unsafe to re-use an IV with a given key.
func testIV(iv []byte) error {
	for i := 0; i < len(iv); i++ {
		iv[i] = 0
	}
	return nil
}

func TestRoundTrip(t *testing.T) {
	m := memstore.New()
	aes, err := aes.NewCipher([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("Creating AES cipher: %v", err)
	}
	e := encrypted.New(m, aes, &encrypted.Options{
		NewIV: testIV,
	})

	const key = "molins"
	const value = "some of what a fool thinks often remains"

	// Write the test blob through the encrypted store.
	ctx := context.Background()
	if err := e.Put(ctx, blob.PutOptions{
		Key:  key,
		Data: []byte(value),
	}); err != nil {
		t.Fatalf("Put %q failed: %v", key, err)
	}

	// Verify that we can read the blob back out and get the same result.
	got, err := e.Get(ctx, key)
	if err != nil {
		t.Errorf("Get %q failed: %v", key, err)
	} else if s := string(got); s != value {
		t.Errorf("Get %q: got %q, want %q", key, s, value)
	}

	// Verify that Size reflects the input size, not the encoded size.
	size, err := e.Size(ctx, key)
	if err != nil {
		t.Errorf("Size %q failed: %v", key, err)
	} else if size != int64(len(value)) {
		t.Errorf("Size %q: got %d, want %d", key, size, len(value))
	}

	// Verify that Len works.
	if v, err := e.Len(ctx); err != nil {
		t.Errorf("Len failed: %v", err)
	} else if v != 1 {
		t.Errorf("Len: got %d, want 1", v)
	}

	// Log the stored block for debugging purposes.
	raw, err := m.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get raw %q failed: %v", key, err)
	}
	pb := new(encpb.Encrypted)
	if err := proto.Unmarshal(raw, pb); err != nil {
		t.Fatalf("Decoding storage wrapper: %v", err)
	}
	t.Logf("Stored block (%d bytes):\n%s", len(raw), proto.MarshalTextString(pb))
	t.Logf("Encoded data size: %d bytes", len(pb.Data))
}
