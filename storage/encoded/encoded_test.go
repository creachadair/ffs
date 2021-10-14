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
	"io"
	"testing"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/encoded"
)

func TestStore(t *testing.T) {
	base := memstore.New()
	enc := encoded.New(base, identity{})
	storetest.Run(t, enc)
}

// identity implements an identity Codec, that encodes blobs as themselves.
type identity struct{}

// Encode encodes src to w with no transformation.
func (identity) Encode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

// Decode decodes src to w with no transformation.
func (identity) Decode(w io.Writer, src []byte) error { _, err := w.Write(src); return err }

// DecodedLen reports the decoded length of src, which is len(src).
func (identity) DecodedLen(src []byte) (int, error) { return len(src), nil }
