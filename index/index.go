// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

// Package index constructs a Bloom filter index for a set of string keys.
package index

import (
	"math"
	"math/rand"

	"github.com/cespare/xxhash/v2"
)

// An Index holds a Bloom filter index for a set of keys.
type Index struct {
	numKeys int       // number of keys stored
	bits    bitVector // a multiple of 64 bits
	nbits   uint64    // the number of bits in the vector (≥ m)
	seeds   []uint64  // hash seeds (length = k)
	hash    func(s string) uint64
}

// New constructs an empty index with capacity for the specified number of
// keys. A nil opts value is ready for use and provides default values as
// described on Options. New will panic if numKeys ≤ 0.
func New(numKeys int, opts *Options) *Index {
	idx := &Index{hash: opts.hashFunc()}
	idx.init(numKeys, opts.falsePositiveRate())
	return idx
}

// Add adds the specified key to the index.
func (idx *Index) Add(key string) {
	hash := idx.hash(key)
	for _, seed := range idx.seeds {
		pos := int((hash ^ seed) % idx.nbits)
		idx.bits.Set(pos)
	}
	idx.numKeys++
}

// Has reports whether key is one of the indexed keys. False positives are
// possible for keys that were not added to the index, but no false negatives.
func (idx *Index) Has(key string) bool {
	hash := idx.hash(key)
	for _, seed := range idx.seeds {
		pos := int((hash ^ seed) % idx.nbits)
		if !idx.bits.IsSet(pos) {
			return false
		}
	}
	return true
}

// Stats returns size and capacity statistics for the index.
func (idx *Index) Stats() Stats {
	return Stats{
		NumKeys:    idx.numKeys,
		FilterBits: int(idx.nbits),
		NumHashes:  len(idx.seeds),
	}
}

// init initializes the internal data structures for the index Bloom filter,
// where n is the expected capacity in number of keys and p is the desired
// false positive rate.
func (idx *Index) init(n int, p float64) {
	// The optimal width for a Bloom filter with n elements and false-positive
	// rate p:
	//
	//             -n * ln(p)
	//  m = ceil( ------------ )
	//              ln(2)**2
	//
	m := math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))

	// The optimal number of hashes for an m-bit filter holding n elements:
	//
	//             m * ln(2)
	//  k = ceil( ----------- )
	//                 n
	//
	k := math.Ceil((m * math.Ln2) / float64(n))

	idx.bits = newBitVector(int(m))
	idx.nbits = 64 * uint64(len(idx.bits))
	idx.seeds = make([]uint64, int(k))

	for i := range idx.seeds {
		idx.seeds[i] = rand.Uint64()
	}
}

// Options provide optional settings for an index. A nil *Options is ready for
// use and provides default values as described.
type Options struct {
	// Compute a 64-bit hash of s. If nil, uses xxhash.Sum64String.
	Hash func(s string) uint64

	// The maximum false positive rate to permit. A value ≤ 0 defaults to 0.03.
	// Decreasing this value increases the memory required for the index.
	FalsePositiveRate float64
}

func (o *Options) hashFunc() func(string) uint64 {
	if o == nil || o.Hash == nil {
		return xxhash.Sum64String
	}
	return o.Hash
}

func (o *Options) falsePositiveRate() float64 {
	if o == nil || o.FalsePositiveRate <= 0 {
		return 0.03
	}
	return o.FalsePositiveRate
}

// Stats record size and capacity statistics for an Index.
type Stats struct {
	NumKeys    int // the number of keys added to the index
	FilterBits int // the number of bits allocated to the Bloom filter (m)
	NumHashes  int // the number of hash seeds allocated (k)
}

type bitVector []uint64

func newBitVector(size int) bitVector  { return make(bitVector, (size+63)/64) }
func (b bitVector) IsSet(pos int) bool { return b[(pos>>6)%len(b)]&(uint64(1)<<(pos&0x3f)) != 0 }
func (b bitVector) Set(pos int)        { b[(pos>>6)%len(b)] |= uint64(1) << (pos & 0x3f) }
