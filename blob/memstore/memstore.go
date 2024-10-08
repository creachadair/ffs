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

// Package memstore implements the blob.Store interface using a map.
package memstore

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/stree"
)

// Store implements the blob.Store interface using an in-memory dictionary. The
// contents of a Store are not persisted. All operations on a memstore are safe
// for concurrent use by multiple goroutines.
type Store struct {
	μ sync.Mutex
	m *stree.Tree[entry]
}

// An entry is a pair of a string key and value.  The value is not part of the
// comparison key.
type entry struct {
	key, val string
}

func compareEntries(a, b entry) int { return strings.Compare(a.key, b.key) }

// Opener constructs a memstore, for use with the store package.  The address
// is ignored, and an error will never be returned.
func Opener(_ context.Context, _ string) (blob.Store, error) { return New(), nil }

// New constructs a new, empty store.
func New() *Store { return &Store{m: stree.New(300, compareEntries)} }

// Clear removes all keys and values from s.
func (s *Store) Clear() {
	s.μ.Lock()
	defer s.μ.Unlock()
	s.m.Clear()
}

// Snapshot copies a snapshot of the keys and values of s into m.
// If m == nil, a new empty map is allocated and returned.
// It returns m to allow chaining with construction.
func (s *Store) Snapshot(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	s.μ.Lock()
	defer s.μ.Unlock()
	for e := range s.m.Inorder {
		m[e.key] = e.val
	}
	return m
}

// Init replaces the contents of s with the keys and values in m.
// It returns s to permit chaining with construction.
func (s *Store) Init(m map[string]string) *Store {
	s.μ.Lock()
	defer s.μ.Unlock()
	s.m.Clear()
	for key, val := range m {
		s.m.Add(entry{key, val})
	}
	return s
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	if e, ok := s.m.Get(entry{key: key}); ok {
		return []byte(e.val), nil
	}
	return nil, blob.KeyNotFound(key)
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	ent := entry{opts.Key, string(opts.Data)}
	if opts.Replace {
		s.m.Replace(ent)
	} else if !s.m.Add(ent) {
		return blob.KeyExists(opts.Key)
	}
	return nil
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	if e, ok := s.m.Get(entry{key: key}); ok {
		return int64(len(e.val)), nil
	}
	return 0, blob.KeyNotFound(key)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	if !s.m.Remove(entry{key: key}) {
		return blob.KeyNotFound(key)
	}
	return nil
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	for e := range s.m.InorderAfter(entry{key: start}) {
		if err := f(e.key); errors.Is(err, blob.ErrStopListing) {
			return nil
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Len implements part of blob.Store.
func (s *Store) Len(context.Context) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	return int64(s.m.Len()), nil
}

// Close implements part of blob.Store. It is a no-op here.
func (*Store) Close(context.Context) error { return nil }
