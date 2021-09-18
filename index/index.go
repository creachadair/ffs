// Package index constructs an index of a collection of hash keys.
package index

import (
	"hash"
	"hash/maphash"
	"log"
	"sort"
)

// A Builder holds the context needed to build an index for a collection of
// keys.  When the index is complete, call the Index method to extract the
// index in a format suitable for queries or encoding to storage.
type Builder struct {
	keys   []string
	hasher func() hash.Hash64
}

// NewBuilder constructs an empty index builder.
func NewBuilder(opts *BuilderOpts) *Builder {
	return &Builder{hasher: opts.hasher()}
}

func (b *Builder) hash(s string, size int) int {
	h := b.hasher()
	h.Write([]byte(s))
	return int(h.Sum64() % uint64(size))
}

// AddKey adds the specified key to the builder.
func (b *Builder) AddKey(key string) { b.keys = append(b.keys, key) }

// Build constructs an index from the keys added so far.
func (b *Builder) Build() *Index {
	nkeys := len(b.keys)  // save
	var table []bitVector // perfect hash lookup
	var tail []string     // unpartitioned keys at end

	conf := newBitVector(nkeys)
	for len(b.keys) != 0 {
		log.Printf("MJF :: -- begin rank %d", len(table)+1)
		rank := newBitVector(len(b.keys))
		nbits := rank.Len()
		conf.Reset()

		// Round 1: Check for conflicts.
		for _, key := range b.keys {
			pos := b.hash(key, nbits)
			if rank.IsSet(pos) {
				rank.Clear(pos)
				conf.Set(pos)
				log.Printf("MJF :: key %q pos=%d conflict", key, pos)
			} else if conf.IsSet(pos) {
				continue // skip key on this round
			} else {
				log.Printf("MJF :: key %q pos=%d set", key, pos)
				rank.Set(pos)
			}
		}

		// Round 2: Save non-conflicting keys.
		var i, hits int
		for i < len(b.keys) {
			key := b.keys[i]

			// This key has no conflicts; swap it to the end of the array and
			// slice it off so that we will not consider it in future ranks.
			pos := b.hash(key, nbits)
			if rank.IsSet(pos) {
				j := len(b.keys) - 1
				log.Printf("MJF :: rank %d accept %q swap in %q", len(table)+1, b.keys[i], b.keys[j])
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
		log.Printf("MJF :: -- end rank %d hits=%d", len(table)+1, hits)

		// Save the constructed rank and do the next round.
		table = append(table, rank)
	}
	return &Index{table: table, hasher: b.hasher, tail: tail}
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
	// Construct a hash function to use to generate the index vector. If no
	// constructor is provided, a maphash with a random seed is used.
	Hasher func() hash.Hash64
}

func (o *BuilderOpts) hasher() func() hash.Hash64 {
	if o == nil || o.Hasher == nil {
		seed := maphash.MakeSeed()
		return func() hash.Hash64 {
			h := new(maphash.Hash)
			h.SetSeed(seed)
			return h
		}
	}
	return o.Hasher
}

type Index struct {
	table  []bitVector
	hasher func() hash.Hash64
	tail   []string // sorted
}

func (idx Index) hash(s string) uint64 {
	h := idx.hasher()
	h.Write([]byte(s))
	return h.Sum64()
}

func (idx *Index) Has(key string) bool {
	for i, v := range idx.table {
		pos := int(idx.hash(key) % uint64(v.Len()))
		log.Printf("Has(%q) rank=%d pos=%d len=%d isSet=%v", key, i+1, pos, v.Len(), v.IsSet(pos))
		if v.IsSet(pos) {
			return true
		}
	}
	n := len(idx.tail)
	pos := sort.Search(n, func(i int) bool {
		return idx.tail[i] <= key
	})
	return pos < n && idx.tail[pos] == key
}
