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
	"testing"

	"github.com/creachadair/ffs/block"
)

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func TestModHashSimple(t *testing.T) {
	// A trivial validation, make sure we get the expected results when the
	// base and modulus are round powers of two, so that the hash values will
	// match an exact suffix of the input bytes.
	h := block.RabinKarpHasher(256, 1<<32, 8).Hash()
	tests := []struct {
		in   byte
		want uint64
	}{
		{1, 0x00000001},
		{2, 0x00000102},
		{3, 0x00010203},
		{4, 0x01020304},
		{160, 0x020304A0},
		{163, 0x0304a0a3},
		{170, 0x04a0a3aa},
		{15, 0xa0a3aa0f},
		{16, 0xa3aa0f10},
		{17, 0xaa0f1011},
		{18, 0x0f101112}, // match 1
		{15, 0x1011120f},
		{16, 0x11120f10},
		{17, 0x120f1011},
		{18, 0x0f101112}, // match 2
	}

	for _, test := range tests {
		got := h.Update(test.in)
		if got != test.want {
			t.Errorf("Update(%x): got %x, want %x", test.in, got, test.want)
		}
	}
}

func TestModHashComplex(t *testing.T) {
	const (
		base = 7
		mod  = 257
		size = 5
	)
	input := []byte{
		1, 3, 2, 8, 9, 4, 7, 11, 75,
		1, 0, 1, 3, 2, 8, 9, 15, 7,
		13, 15, 24, 100, 125, 180, 1, 0,
		0, 1, 0, 9, 80, 3, 2, 1,
	}

	// Walk through each viable slice of input comparing the rolling hash value
	// to the expected value computed by brute force without rolling.
	h := block.RabinKarpHasher(base, mod, size).Hash()
	for i := range input {
		data := input[max(0, i-size):i]
		if len(data) == 0 {
			continue
		}

		b := data[len(data)-1]
		want := wantHash(base, mod, data)
		got := h.Update(b)
		if got != want {
			t.Errorf("At offset %d: Update(%x): got %x, want %x", i, b, got, want)
		}
	}
}

func TestModHash(t *testing.T) {
	const (
		base      = 2147483659
		mod       = 1031
		maxWindow = 8
	)
	for i := 1; i <= maxWindow; i++ {
		windowTest(t, block.RabinKarpHasher(base, mod, i), i)
	}
}

func windowTest(t *testing.T, h block.Hasher, size int) {
	// Make sure that we get the same hash value when the window has the same
	// contents.
	const keyValue = 22
	testData := make([]byte, size)
	testData = append(testData, []byte{
		1, 2, 3, 4, 5, 6, 7, 8, 11, keyValue, 2, 3, 4, 5, 6, 7, 8, 11, 15, 17,
		33, 44, 55, 66, 77, 88, 3, 5, 7, 11, 13, 17, 19, 23, 3, 4, 5, 6, 7, 8,
		11, keyValue, 2, 3, 4, 5, 6, 7, 8, 11, keyValue, 24, 26, 28, 30,
	}...)

	var keyHash uint64
	rh := h.Hash()
	for i, in := range testData[size:] {
		v := rh.Update(in)
		if in != keyValue {
			continue
		}
		if keyHash == 0 {
			keyHash = v
			t.Logf("At #%d, set hash for key value %d to %08x", i, keyValue, keyHash)
		} else if v != keyHash {
			t.Errorf("#%d: Update(%02x): got %d, want %d", i, in, v, keyHash)
		}
	}
}

// wantHash computes a raw mod-hash over the given slice without using sliding.
// This is used to check the outcome of a modHash that does slide.
func wantHash(base, mod int, data []byte) uint64 {
	var want int
	for _, v := range data {
		want = ((want * base) + int(v)) % mod
	}
	return uint64(want)
}
