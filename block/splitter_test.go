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

package block_test

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/creachadair/ffs/block"
)

// burstyReader implements io.Reader, returning chunks from r whose size is
// bounded above by the specified byte lengths, to simulate a reader that does
// not always deliver all that was requested.
type burstyReader struct {
	r   io.Reader
	len []int
	pos int
}

func (b *burstyReader) Read(buf []byte) (int, error) {
	cap := len(buf)
	if len(b.len) > b.pos {
		if n := b.len[b.pos]; n < cap {
			cap = b.len[b.pos]
		}
		b.pos = (b.pos + 1) % len(b.len)
	}
	return b.r.Read(buf[:cap])
}

func newBurstyReader(s string, sizes ...int) io.Reader {
	return &burstyReader{strings.NewReader(s), sizes, 0}
}

// dummyHash is a mock Hash implementation used for testing a block.Splitter.
// It returns a fixed value for all updates except a designated value.
type dummyHash struct {
	magic byte
	hash  uint64
	size  int
}

func (d dummyHash) Hash() block.Hash { return d }

func (d dummyHash) Update(in byte) uint64 {
	if in == d.magic {
		return 1
	}
	return d.hash
}

func TestSplitterMin(t *testing.T) {
	const minBytes = 10
	d := dummyHash{
		magic: '|',
		hash:  12345,
		size:  1,
	}
	r := strings.NewReader("abc|def|ghi|jkl|mno")
	s := block.NewSplitter(r, &block.SplitConfig{
		Hasher: d,
		Min:    minBytes,
	})
	b, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < minBytes {
		t.Errorf("len(b): got %d, want at least %d", len(b), minBytes)
	}
	t.Logf("b=%q", string(b))
}

func TestSplitterMax(t *testing.T) {
	const maxBytes = 10
	d := dummyHash{
		hash: 12345,
		size: 1,
	}
	r := strings.NewReader("abc|def|ghi|jkl|mno")
	s := block.NewSplitter(r, &block.SplitConfig{
		Hasher: d,
		Max:    maxBytes,
	})
	b, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > maxBytes {
		t.Errorf("len(b): got %d, want at most %d", len(b), maxBytes)
	}
	t.Logf("b=%q", string(b))
}

func TestSplitterBlocks(t *testing.T) {
	tests := []struct {
		input    string
		min, max int
		blocks   []string
	}{
		// In these test cases, any "|" in the input triggers a hash cut.  This
		// permits us to verify the various corner cases of when a cut occurs
		// vs. the length constraints.
		{"", 5, 15, nil},
		{"abc", 5, 15, []string{"abc"}},
		{"|", 0, 15, []string{"|"}},
		{"x||y", 1, 15, []string{"x", "|", "|y"}},
		{"|||x", 1, 5, []string{"|", "|", "|x"}},
		{"a|bc|defg|hijklmno|pqrst", 2, 8, []string{"a|bc", "|defg", "|hijklmn", "o|pqrst"}},
		{"abcdefgh|ijklmnop|||q", 5, 100, []string{"abcdefgh", "|ijklmnop", "|||q"}},
		{"a|b|c|d|e|", 1, 2, []string{"a", "|b", "|c", "|d", "|e", "|"}},
		{"abcdefghijk", 4, 4, []string{"abcd", "efgh", "ijk"}},
	}
	d := dummyHash{
		magic: '|',
		hash:  12345,
		size:  5,
	}
	for _, test := range tests {
		r := newBurstyReader(test.input, 3, 5, 1, 4, 17, 20)
		s := block.NewSplitter(r, &block.SplitConfig{
			Hasher: d,
			Min:    test.min,
			Max:    test.max,
		})
		var bs []string
		if err := s.Split(func(b []byte) error {
			bs = append(bs, string(b))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(bs, test.blocks) {
			t.Errorf("split %q: got %+q, want %+q", test.input, bs, test.blocks)
		}
	}
}

func TestLongValue(t *testing.T) {
	rng := rand.New(rand.NewSource(1)) // change to update test data

	const alphabet = "abcdefghijklmnopqrstuvwxyz 0123456789"
	const inputLen = 32000
	var buf bytes.Buffer
	for buf.Len() < inputLen {
		buf.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}
	cfg := &block.SplitConfig{
		Min:  200,
		Size: 800,
		Max:  20000,
	}
	s := block.NewSplitter(&buf, cfg)
	var total int
	var sizes []int
	if err := s.Split(func(blk []byte) error {
		total += len(blk)
		sizes = append(sizes, len(blk))
		if len(blk) < cfg.Min {
			t.Errorf("Block too short: %d bytes < %d", len(blk), cfg.Min)

			// N.B. This could legitimately happen at end of input.
		} else if len(blk) > cfg.Max {
			t.Errorf("Block too long: %d bytes > %d", len(blk), cfg.Max)
		}
		return nil
	}); err != nil {
		t.Errorf("Split failed: %v", err)
	}
	t.Logf("Split: %d blocks, %d bytes total :: %+v", len(sizes), total, sizes)
	if total != inputLen {
		t.Errorf("Total size of blocks: got %d, want %d", total, inputLen)
	}
}
