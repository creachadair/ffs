// Package index constructs an index of a collection of hash keys.
package index

import (
	"hash"
	"hash/maphash"
	"log"
)

// A Builder holds the context needed to build an index for a collection of
// keys.  When the index is complete, call the Index method to extract the
// index in a format suitable for queries or encoding to storage.
type Builder struct {
	keys   []string
	hasher hash.Hash64
}

// NewBuilder constructs an empty index builder.
func NewBuilder(opts *BuilderOpts) *Builder {
	return &Builder{hasher: opts.hasher()}
}

func (b *Builder) hash(s string, size int) int {
	h := b.hasher
	h.Reset()
	h.Write([]byte(s))
	return int(h.Sum64() % uint64(size))
}

// AddKey adds the specified key to the builder.
func (b *Builder) AddKey(key string) { b.keys = append(b.keys, key) }

// Build constructs an index from the keys added so far.
func (b *Builder) Build() *Index {
	nkeys := len(b.keys) // save
	var table []bitVector
	rank := newBitVector(nkeys)
	conf := newBitVector(nkeys)

	for len(b.keys) != 0 {
		rank = rank.Resize(len(b.keys)).Reset()
		conf = conf.Resize(len(b.keys)).Reset()
		nbits := rank.Len()

		// Round 1: Check for conflicts.
		for _, key := range b.keys {
			pos := b.hash(key, nbits)
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
		i := 0
		for i < len(b.keys) {
			key := b.keys[i]

			// This key has no conflicts; swap it to the end of the array and
			// slice it off so that we will not consider it in future ranks.
			pos := b.hash(key, nbits)
			if rank.IsSet(pos) {
				j := len(b.keys) - 1
				b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
				b.keys = b.keys[:j]
				continue
				// leave i where it is so we will check the moved key
			}
			i++
		}

		// Save the constructed rank and do the next round.
		table = append(table, rank)
	}
	return &Index{table: table, hasher: b.hasher}
}

type bitVector []uint64

func newBitVector(size int) bitVector         { return make(bitVector, (size+63)/64) }
func (b bitVector) Resize(size int) bitVector { return b[:(size+63)/64] }

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
	// Construct the hash function to use to generate the index vector. If no
	// constructor is provided, a maphash with a random seed is used.
	Hasher hash.Hash64
}

func (o *BuilderOpts) hasher() hash.Hash64 {
	if o == nil || o.Hasher == nil {
		seed := maphash.MakeSeed()
		h := new(maphash.Hash)
		h.SetSeed(seed)
		return h
	}
	return o.Hasher
}

type Index struct {
	table  []bitVector
	hasher hash.Hash64
}

func (idx Index) hash(s string) uint64 {
	h := idx.hasher
	h.Reset()
	h.Write([]byte(s))
	return h.Sum64()
}

func (idx *Index) Has(key string) bool {
	for i, v := range idx.table {
		pos := int(idx.hash(key) % uint64(v.Len()))
		log.Printf("MJF :: key=%q i=%d pos=%d", key, i, pos)
		if v.IsSet(pos) {
			return true
		}
	}
	return false
}
