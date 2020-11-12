// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
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

package wiretype_test

import (
	"crypto/sha1"
	"strings"
	"testing"

	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/split"
	"github.com/google/go-cmp/cmp"
)

type lineHash struct{}

func newLineHash() split.RollingHash { return lineHash{} }

func (lineHash) Update(b byte) uint {
	if b == '\x00' {
		return 1
	}
	return 2
}

func (lineHash) Size() int { return 1 }

func TestNewIndex(t *testing.T) {
	const input = "This is the first line" + // ext 1, block 1
		"\x00This is the second" + // ext 1, block 2
		"\x00This is the third" + // ext 1, block 3
		"\x00\x00\x00\x00\x00" + // zeroes (not stored)
		"\x00And the fourth line then beckoned" // ext 2, block 1

	s := split.New(strings.NewReader(input), &split.Config{
		Hash: newLineHash,
		Min:  5,
		Max:  100,
		Size: 16,
	})
	idx, err := wiretype.NewIndex(s, func(data []byte) (string, error) {
		t.Logf("Block: %q", string(data))
		h := sha1.New()
		h.Write(data)
		return string(h.Sum(nil)), nil
	})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	t.Logf("Input size: %d, total bytes: %d", len(input), idx.TotalBytes)
	if idx.TotalBytes != uint64(len(input)) {
		t.Errorf("TotalBytes: got %d, want %d", idx.TotalBytes, len(input))
	}

	type extinfo struct {
		Base, Bytes uint64
		Blocks      int
	}
	want := []extinfo{
		{0, 59, 3},
		{64, 34, 1},
	}
	var got []extinfo
	for i, ext := range idx.Extents {
		got = append(got, extinfo{
			Base:   ext.Base,
			Bytes:  ext.Bytes,
			Blocks: len(ext.Blocks),
		})
		for j, b := range ext.Blocks {
			t.Logf("Extent %d block %d: %d bytes, key=%x", i+1, j+1, b.Bytes, b.Key)
		}
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Wrong extents: (-want, +got):\n%s", diff)
	}
}
