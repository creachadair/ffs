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

package encoded_test

import (
	"context"
	"io"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/encoded"
)

func TestStore(t *testing.T) {
	base := memstore.New(nil)
	enc := encoded.New(base, identity{})
	storetest.Run(t, storetest.NopCloser(enc))
}

// identity implements an identity Codec, that encodes blobs as themselves.
type identity struct{}

// Encode encodes src to w with no transformation.
func (identity) Encode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

// Decode decodes src to w with no transformation.
func (identity) Decode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

func TestRegression(t *testing.T) {
	ctx := context.Background()

	t.Run("DoubleEncode", func(t *testing.T) {
		// Verify that a given Put or Get only encodes/decodes once.
		base := memstore.New(nil)
		enc := encoded.New(base, tagger("@"))
		kv := storetest.SubKV(t, ctx, enc, "test")

		const testValue = "bar"
		if err := kv.Put(ctx, blob.PutOptions{
			Key:  "foo",
			Data: []byte(testValue),
		}); err != nil {
			t.Fatalf("Put foo: %v", err)
		}

		real := storetest.SubKV(t, ctx, base, "test")
		if val, err := real.Get(ctx, "foo"); err != nil {
			t.Fatalf("Get foo: %v", err)
		} else if got, want := string(val), testValue+"@"; got != want {
			t.Errorf("Base foo: got %q, want %q", got, want)
		}

		if val, err := kv.Get(ctx, "foo"); err != nil {
			t.Fatalf("Get foo: %v", err)
		} else if got, want := string(val), testValue; got != want {
			t.Errorf("Get foo: got %q, want %q", got, want)
		}
	})
}

type tagger string

func (t tagger) Encode(w io.Writer, src []byte) error {
	_, err := w.Write(append(src, t...))
	return err
}

func (t tagger) Decode(w io.Writer, src []byte) error {
	_, err := w.Write(src[:len(src)-1])
	return err
}
