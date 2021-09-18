// Package index constructs a minimal perfect hash index of a collection of
// string keys using the algorithm from:
//
//   A. Limasset, G. Rizk, Rr. Chikhi, and P. Peterlongo:
//   Fast and scalable minimal perfect hashing for massive key sets
//   https://arxiv.org/pdf/1702.03154.pdf.
//
package index

import (
	"math"
	"math/rand"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// A Builder holds the context needed to build an index for a set of keys.
// When the index is complete, call the Index method to extract the index in a
// format suitable for queries or encoding to storage.
type Builder struct {
	keys       []string
	bitsPerKey float64
	hash       func(s string, seed uint64) uint64
}

// NewBuilder constructs an empty index builder. A nil opts value is ready for
// use and provides default values as described on BuilderOpts.
func NewBuilder(opts *BuilderOpts) *Builder {
	return &Builder{
		hash:       opts.hashFunc(),
		bitsPerKey: opts.bitsPerKey(),
	}
}

func (b *Builder) hashPos(s string, seed uint64, size int) int {
	return int(b.hash(s, seed) % uint64(size))
}

func (b *Builder) capacity() int {
	return int(math.Ceil(float64(len(b.keys)) * b.bitsPerKey))
}

// AddKey adds the specified key to the builder. All the keys to be indexed
// must be added before an index can be constructed.
func (b *Builder) AddKey(key string) { b.keys = append(b.keys, key) }

// Build constructs an index from the keys added to the builder.
func (b *Builder) Build() *Index {
	nkeys := len(b.keys)  // save
	var table []bitVector // perfect hash vectors
	var seeds []uint64    // hash seeds, parallel to table
	var tail []string     // unpartitioned keys at end

	conf := newBitVector(b.capacity())
	for len(b.keys) != 0 {
		rank := newBitVector(b.capacity())
		seed := rand.Uint64()
		nbits := rank.Len()
		conf.Reset()

		// Round 1: Check for conflicts.
		for _, key := range b.keys {
			pos := b.hashPos(key, seed, nbits)
			if rank.IsSet(pos) {
				rank.Clear(pos)
				conf.Set(pos)
			} else if conf.IsSet(pos) {
				continue // skip key on this round
			} else {
				rank.Set(pos)
			}
		}

		// Round 2: Save non-conflicting keys.
		var i, hits int
		for i < len(b.keys) {
			key := b.keys[i]

			// This key has no conflicts; swap it to the end of the array and
			// slice it off so that we will not consider it in future ranks.
			pos := b.hashPos(key, seed, nbits)
			if rank.IsSet(pos) {
				j := len(b.keys) - 1
				b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
				b.keys = b.keys[:j]
				hits++
				continue
				// leave i where it is so we will check the moved key
			}
			i++
		}

		// If we were not able to partition the remaining keys, sort them and put
		// them into the trailing search collection.
		if hits == 0 {
			tail = b.keys
			sort.Strings(tail)
			break
		}

		// Save the constructed rank and do the next round.
		seeds = append(seeds, seed)
		table = append(table, rank)
	}
	return &Index{
		nkeys: nkeys,
		table: table,
		seeds: seeds,
		hash:  b.hash,
		tail:  tail,
	}
}

type bitVector []uint64

func newBitVector(size int) bitVector  { return make(bitVector, (size+63)/64) }
func (b bitVector) Len() int           { return 64 * len(b) }
func (b bitVector) IsSet(pos int) bool { return b[(pos>>6)%len(b)]&(uint64(1)<<(pos&0x3f)) != 0 }
func (b bitVector) Set(pos int)        { b[(pos>>6)%len(b)] |= uint64(1) << (pos & 0x3f) }
func (b bitVector) Clear(pos int)      { b[(pos>>6)%len(b)] &^= uint64(1) << (pos & 0x3f) }

func (b bitVector) Reset() bitVector {
	for i := range b {
		b[i] = 0
	}
	return b
}

// BuilderOpts provide optional settings for a Builder. A nil *BuilderOpts is
// ready for use and provides default values as described.
type BuilderOpts struct {
	// Compute a 64-bit hash of s incorporating seed. If nil, uses xxhash.Sum64.
	Hash func(s string, seed uint64) uint64

	// The number of bits to allocate per key. A value < 1 defaults to 8.
	BitsPerKey float64
}

func (o *BuilderOpts) hashFunc() func(string, uint64) uint64 {
	if o == nil || o.Hash == nil {
		return func(s string, seed uint64) uint64 {
			return xxhash.Sum64String(s) ^ seed
		}
	}
	return o.Hash
}

func (o *BuilderOpts) bitsPerKey() float64 {
	if o == nil || o.BitsPerKey < 1 {
		return 8
	}
	return o.BitsPerKey
}

// An Index is a minimal perfect hash index of the exact set of keys given to
// the builder that constructed the index.
type Index struct {
	nkeys int         // total number of keys indexed
	table []bitVector // bit vectors by rank
	seeds []uint64    // hash seeds by rank
	tail  []string    // sorted tail keys

	hash func(s string, seed uint64) uint64
}

func (idx Index) hashPos(s string, seed uint64, size int) int {
	return int(idx.hash(s, seed) % uint64(size))
}

// NumKeys returns the number of keys indexed.
func (idx *Index) NumKeys() int { return idx.nkeys }

// Has reports whether key is one of the indexed keys. False positives are
// possible for keys that were not part of the original index, but there are no
// false negatives.
func (idx *Index) Has(key string) bool {
	for i, v := range idx.table {
		pos := idx.hashPos(key, idx.seeds[i], v.Len())
		if v.IsSet(pos) {
			return true
		}
	}
	n := len(idx.tail)
	pos := sort.Search(n, func(i int) bool {
		return key <= idx.tail[i]
	})
	return pos < n && idx.tail[pos] == key
}

func (idx *Index) Size() int {
	var size int
	for _, v := range idx.table {
		size += len(v)*8 + 8 // +8 for the corresponding seed
	}
	for _, key := range idx.tail {
		size += len(key)
	}
	return size
}
