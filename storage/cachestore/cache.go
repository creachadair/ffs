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

package cachestore

// newCache constructs a new empty cache with the given capacity in bytes.  The
// cache is not safe for concurrent use without external synchronization.
//
// The cache implements a "less recently used" eviction policy; new blocks are
// inserted at the front of the cache, and cache hits move blocks toward the
// front. Evictions occur from the rear.
func newCache(maxBytes int) *cache {
	sentinel := &entry{key: "sentinel"}
	return &cache{
		pos:     make(map[string]*entry),
		entries: sentinel,
		tail:    sentinel,
		cap:     maxBytes,
	}
}

type cache struct {
	pos     map[string]*entry
	entries *entry
	tail    *entry
	size    int // resident size in bytes
	cap     int // capacity in bytes
}

func (c *cache) clear() {
	c.entries.link = nil
	c.tail = c.entries
	c.size = 0
	c.pos = make(map[string]*entry)
}

// drop discards the entry for key if it exists.
func (c *cache) drop(key string) {
	if elt := c.pos[key]; elt != nil {
		c.discard(elt)
	}
}

// empty reports whether the entry list is empty.
func (c *cache) empty() bool { return c.entries.link == nil }

// get reports whether any data are cached for key. In addition, if wantData is
// true, get returns a copy of the data stored for key.
func (c *cache) get(key string, wantData bool) ([]byte, bool) {
	elt := c.pos[key]
	if elt == nil {
		return nil, false
	} else if elt.link == nil || elt.link.key != key {
		panic("cache integrity failure: " + key)
	}
	defer c.update(elt)
	if wantData {
		return copyOf(elt.link.val), true
	}
	return nil, true
}

// update moves elt forward in the cache to reduce the likelihood that it will
// be evicted. This does nothing if elt is already at the head.
func (c *cache) update(elt *entry) {
	cur := elt.link
	next := elt.link.link
	if next != nil {
		c.pos[cur.key] = cur
		c.pos[next.key] = elt
		cur.key, next.key = next.key, cur.key
		cur.val, next.val = next.val, cur.val
	}
}

func (c *cache) discard(elt *entry) int {
	cur := elt.link

	// If we're discarding the last element, update the tail.
	// Otherwise, update the owner of the subsequent entry.
	if c.tail == cur {
		c.tail = elt
	} else {
		c.pos[cur.link.key] = elt
	}

	// Splice out the discarded entry and update sizes.
	elt.link = cur.link
	cur.link = nil
	c.size -= len(cur.val)
	delete(c.pos, cur.key)
	return len(cur.val)
}

// evict discards eldest entries until there is room enough for increase
// additional bytes. Precondition: increase <= c.cap.
func (c *cache) evict(increase int) {
	newSize := c.size + increase
	for newSize > c.cap && !c.empty() {
		newSize -= c.discard(c.entries)
	}
}

// precondition: len(val) < c.cap; key is not present.
func (c *cache) putNew(key string, val []byte) {
	elt := &entry{key: key, val: copyOf(val)}
	c.tail.link = elt
	c.pos[key] = c.tail
	c.tail = elt
	c.size += len(val)
}

// put adds or updates key in the cache if it fits.
func (c *cache) put(key string, val []byte) {
	elt := c.pos[key]
	if elt == nil {
		// Case 1: The key is not already cached.
		//
		// 1a: The value is too big to ever be cached.
		if len(val) > c.cap {
			return
		}

		// 1b: Evict blocks as necessary for adding the new value to fit.
		c.evict(len(val))
		c.putNew(key, val)
		return
	}

	// Case 2: The key is already cached, and the new value is the same or
	// lesser size than the existing one. No eviction is required.
	if len(val) <= len(elt.link.val) {
		c.size = (c.size - len(elt.link.val)) + len(val)
		elt.link.val = append(elt.link.val[:0], val...) // re-use the existing array
		c.update(elt)
		return
	}

	// Case 3a: The key is already cached, but the new value is too big.
	if len(val) > c.cap {
		c.discard(elt)
		return
	}

	// Case 3b: Evict blocks as necessary for the new value to fit.
	c.evict(len(val) - len(elt.link.val))

	// Eviction may have moved or discarded the original location.
	if elt := c.pos[key]; elt == nil {
		c.putNew(key, val)
	} else {
		// The old block is still around; update value and sizes.
		c.size = (c.size - len(elt.link.val)) + len(val)
		elt.link.val = append(elt.link.val[:0], val...) // re-use the existing array
		c.update(elt)
	}
}

type entry struct {
	key  string
	val  []byte
	link *entry
}

func copyOf(buf []byte) []byte {
	if len(buf) == 0 {
		return buf
	}
	cp := make([]byte, len(buf))
	copy(cp, buf)
	return cp
}
