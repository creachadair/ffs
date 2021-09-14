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

package block

// A Hasher constructs rolling hash instances. Use the Hash method to obtain a
// fresh instance.
type Hasher interface {
	// Hash returns a fresh Hash instance using the settings from the Hasher.
	// Instances are independent and can be safely used concurrently.
	Hash() Hash
}

// A Hash implements a rolling hash.
type Hash interface {
	// Add a byte to the rolling hash, and return the updated value.
	Update(byte) uint
}

// rkHasher implements the Hasher interface using the Rabin-Karp construction.
type rkHasher struct {
	// hashing rounds compute base^x % mod
	// mod should be prime, and must be coprime to base.
	base, mod int64 //

	// precomputed modular inverse of base^(size-1) for quick subtraction
	inv int64

	// buffer window size
	size int
}

// Hash implements the required method of Hasher.
func (h rkHasher) Hash() Hash {
	return &rkHash{rkHasher: h, buf: make([]byte, h.size)}
}

// RabinKarpHasher returns a Rabin-Karp rolling hasher using the given base,
// modulus, and window size. The base and modulus must be coprime and the
// modulus should be prime (but note that the constructor does not check this).
func RabinKarpHasher(base, modulus int64, windowSize int) Hasher {
	return rkHasher{
		base: base,
		mod:  modulus,
		inv:  exptmod(base, int64(windowSize-1), modulus),
		size: windowSize,
	}
}

// rkHash implements a rolling hash using the settings from an rkHasher.
type rkHash struct {
	rkHasher // base settings shared by all instances

	hash uint   // last hash value
	next int    // next offset in the window buffer
	buf  []byte // window buffer (per instance)
}

// Update adds b to the rolling hash and returns the updated hash value.
func (h *rkHash) Update(b byte) uint {
	old := int64(h.buf[h.next]) // the displaced oldest byte
	h.buf[h.next] = b
	h.next = (h.next + 1) % h.size

	// Subtract away the old byte being displaced. Multiplying by h.inv shifts
	// the value the correct number of digits forward (mod m).
	newHash := (h.base*(int64(h.hash)-h.inv*old) + int64(b)) % h.mod
	if newHash < 0 {
		newHash += h.mod // pin a non-negative representative
	}
	h.hash = uint(newHash)
	return h.hash
}

// exptmod(b, e, m) computes b**e modulo m. This is an expensive way to compute
// a modular inverse, but it only needs to be done once per rkHasher.
func exptmod(b, e, m int64) int64 {
	s := int64(1)
	for e != 0 {
		if e&1 == 1 {
			s = (s * b) % m
		}
		b = (b * b) % m
		e >>= 1
	}
	return s
}
