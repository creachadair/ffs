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

package file

import "sort"

// Child provides access to the children of a file.
type Child struct{ f *File }

// Has reports whether the file has a child with the given name.
func (c Child) Has(name string) bool {
	c.f.mu.RLock()
	defer c.f.mu.RUnlock()
	_, ok := c.f.findChildLocked(name)
	return ok
}

// Set makes kid a child of f under the given name. Set will panic if kid == nil.
func (c Child) Set(name string, kid *File) {
	if kid == nil {
		panic("set: nil file")
	}
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	defer c.f.modifyLocked()
	kid.name = name
	if i, ok := c.f.findChildLocked(name); ok {
		c.f.kids[i].File = kid // replace an existing child
		return
	}
	c.f.kids = append(c.f.kids, child{Name: name, File: kid})

	// Restore lexicographic order.
	for i := len(c.f.kids) - 1; i > 0; i-- {
		if c.f.kids[i].Name >= c.f.kids[i-1].Name {
			break
		}
		c.f.kids[i], c.f.kids[i-1] = c.f.kids[i-1], c.f.kids[i]
	}
}

// Len returns the number of children of the file.
func (c Child) Len() int { c.f.mu.RLock(); defer c.f.mu.RUnlock(); return len(c.f.kids) }

// Remove removes name as a child of f, and reports whether a change was made.
func (c Child) Remove(name string) bool {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	if i, ok := c.f.findChildLocked(name); ok {
		defer c.f.modifyLocked()
		c.f.kids = append(c.f.kids[:i], c.f.kids[i+1:]...)
		return true
	}
	return false
}

// Names returns a lexicographically ordered slice of the names of all the
// children of the file.
func (c Child) Names() []string {
	c.f.mu.RLock()
	defer c.f.mu.RUnlock()
	out := make([]string, len(c.f.kids))
	for i, kid := range c.f.kids {
		out[i] = kid.Name
	}
	return out
}

// Release discards all up-to-date cached children of the file. It returns the
// number of records that were released.
func (c Child) Release() int {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	var n int
	for i, kid := range c.f.kids {
		if kid.File != nil && kid.Key != "" && kid.Key == kid.File.key {
			c.f.kids[i].File = nil
			n++
		}
	}
	return n
}

// Data is a view of the data associated with a file.
type Data struct{ f *File }

// Size returns the effective size of the file content in bytes.
func (d Data) Size() int64 { d.f.mu.RLock(); defer d.f.mu.RUnlock(); return d.f.data.totalBytes }

// Len returns the number of data blocks for the file.
func (d Data) Len() int { d.f.mu.RLock(); defer d.f.mu.RUnlock(); return d.lenLocked() }

func (d Data) lenLocked() int {
	var nb int
	for _, e := range d.f.data.extents {
		nb += len(e.blocks)
	}
	return nb
}

// Keys returns the storage keys of the data blocks for the file.  If the file
// has no binary data, the slice is empty.
func (d Data) Keys() []string {
	d.f.mu.RLock()
	defer d.f.mu.RUnlock()
	nb := d.lenLocked()
	if nb == 0 {
		return nil
	}
	keys := make([]string, 0, nb)
	for _, e := range d.f.data.extents {
		for _, blk := range e.blocks {
			keys = append(keys, blk.key)
		}
	}
	return keys
}

// XAttr provides access to the extended attributes of a file.
type XAttr struct{ f *File }

// Has reports whether the specified attribute is defined.
func (x XAttr) Has(key string) bool {
	x.f.mu.RLock()
	defer x.f.mu.RUnlock()
	_, ok := x.f.xattr[key]
	return ok
}

// Get returns the value corresponding to the given key, or "" if the key is
// not defined.
func (x XAttr) Get(key string) string { x.f.mu.RLock(); defer x.f.mu.RUnlock(); return x.f.xattr[key] }

// Set sets the specified xattr.
func (x XAttr) Set(key, value string) {
	x.f.mu.Lock()
	defer x.f.mu.Unlock()
	defer x.f.invalLocked()
	x.f.xattr[key] = value
}

// Len reports the number of extended attributes defined on f.
func (x XAttr) Len() int { x.f.mu.RLock(); defer x.f.mu.RUnlock(); return len(x.f.xattr) }

// Remove removes the specified xattr.
func (x XAttr) Remove(key string) {
	x.f.mu.Lock()
	defer x.f.mu.Unlock()
	if _, ok := x.f.xattr[key]; ok {
		delete(x.f.xattr, key)
		x.f.invalLocked()
	}
}

// Names returns a slice of the names of all the extended attributes defined.
func (x XAttr) Names() []string {
	x.f.mu.RLock()
	defer x.f.mu.RUnlock()
	names := make([]string, 0, len(x.f.xattr))
	for key := range x.f.xattr {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

// Clear removes all the extended attributes set on the file.
func (x XAttr) Clear() {
	x.f.mu.Lock()
	defer x.f.mu.Unlock()
	if len(x.f.xattr) != 0 {
		defer x.f.invalLocked()
		clear(x.f.xattr)
	}
}
