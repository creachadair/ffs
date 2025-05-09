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

// Package memstore implements the [blob.Store] and [blob.KV] interfaces using
// in-memory dictionaries. This is primarily useful for testing, as the
// contents are not persisted.
package memstore

import (
	"cmp"
	"context"
	"iter"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/stree"
)

// A Store implements the [blob.Store] interface using an in-memory dictionary
// for each keyspace. A zero value is ready for use, but must not be copied
// after its first use.
type Store struct {
	newKV func() blob.KV // Set on construction, read-only thereafter

	μ    sync.Mutex
	kvs  map[string]blob.KV
	subs map[string]*Store
}

func (s *Store) kv() blob.KV {
	if s.newKV == nil {
		return NewKV()
	}
	return s.newKV()
}

// KV implements part of [blob.Store].
// This implementation never reports an error.
func (s *Store) KV(_ context.Context, name string) (blob.KV, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	kv, ok := s.kvs[name]
	if !ok {
		kv = s.kv()
		if s.kvs == nil {
			s.kvs = make(map[string]blob.KV)
		}
		s.kvs[name] = kv
	}
	return kv, nil
}

// CAS implements part of [blob.Store].
// This implementation never reports an error.
func (s *Store) CAS(ctx context.Context, name string) (blob.CAS, error) {
	return blob.CASFromKVError(s.KV(ctx, name))
}

// Sub implements part of [blob.Store].
// This implementation never reports an error.
func (s *Store) Sub(_ context.Context, name string) (blob.Store, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	sub, ok := s.subs[name]
	if !ok {
		sub = &Store{newKV: s.newKV}
		if s.subs == nil {
			s.subs = make(map[string]*Store)
		}
		s.subs[name] = sub
	}
	return sub, nil
}

// Close implements part of [blob.StoreCloser]. This implementation is a no-op.
func (*Store) Close(context.Context) error { return nil }

// New constructs a new empty Store that uses newKV to construct keyspaces.
// If newKV == nil, [NewKV] is used.
func New(newKV func() blob.KV) *Store {
	return &Store{kvs: make(map[string]blob.KV), newKV: newKV}
}

// KV implements the [blob.KV] interface using an in-memory dictionary. The
// contents of a Store are not persisted. All operations on a memstore are safe
// for concurrent use by multiple goroutines.
type KV struct {
	μ sync.RWMutex
	m *stree.Tree[entry]
}

// An entry is a pair of a string key and value.  The value is not part of the
// comparison key.
type entry = stree.KV[string, string]

// Opener constructs a [blob.StoreCloser] for use with the [store] package.
// The concrete type of the result is [memstore.Store]. The address is ignored,
// and an error is never returned.
//
// [store]: https://godoc.org/github.com/creachadair/ffstools/lib/store
func Opener(_ context.Context, _ string) (blob.StoreCloser, error) { return New(nil), nil }

// NewKV constructs a new, empty key-value namespace.
func NewKV() *KV { return &KV{m: stree.New(300, entry{}.Compare(cmp.Compare))} }

// Clear removes all keys and values from s.
func (s *KV) Clear() {
	s.μ.Lock()
	defer s.μ.Unlock()
	s.m.Clear()
}

// Snapshot copies a snapshot of the keys and values of s into m.
// If m == nil, a new empty map is allocated and returned.
// It returns m to allow chaining with construction.
func (s *KV) Snapshot(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	s.μ.RLock()
	defer s.μ.RUnlock()
	for e := range s.m.Inorder {
		m[e.Key] = e.Value
	}
	return m
}

// Init replaces the contents of s with the keys and values in m.
// It returns s to permit chaining with construction.
func (s *KV) Init(m map[string]string) *KV {
	s.μ.Lock()
	defer s.μ.Unlock()
	s.m.Clear()
	for key, val := range m {
		s.m.Add(entry{Key: key, Value: val})
	}
	return s
}

// Get implements part of [blob.KV].
func (s *KV) Get(_ context.Context, key string) ([]byte, error) {
	s.μ.RLock()
	defer s.μ.RUnlock()

	if e, ok := s.m.Get(entry{Key: key}); ok {
		return []byte(e.Value), nil
	}
	return nil, blob.KeyNotFound(key)
}

// Has implements part of [blob.KV].
func (s *KV) Has(_ context.Context, keys ...string) (blob.KeySet, error) {
	s.μ.RLock()
	defer s.μ.RUnlock()
	out := make(blob.KeySet)
	for _, key := range keys {
		if _, ok := s.m.Get(entry{Key: key}); ok {
			out.Add(key)
		}
	}
	return out, nil
}

// Put implements part of [blob.KV].
func (s *KV) Put(_ context.Context, opts blob.PutOptions) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	ent := entry{Key: opts.Key, Value: string(opts.Data)}
	if opts.Replace {
		s.m.Replace(ent)
	} else if !s.m.Add(ent) {
		return blob.KeyExists(opts.Key)
	}
	return nil
}

// Delete implements part of [blob.KV].
func (s *KV) Delete(_ context.Context, key string) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	if !s.m.Remove(entry{Key: key}) {
		return blob.KeyNotFound(key)
	}
	return nil
}

// List implements part of [blob.KV].
func (s *KV) List(_ context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		s.μ.RLock()
		defer s.μ.RUnlock()

		for e := range s.m.InorderAfter(entry{Key: start}) {
			if !yield(e.Key, nil) {
				return
			}
		}
	}
}

// Len implements part of [blob.KV].
func (s *KV) Len(context.Context) (int64, error) {
	s.μ.RLock()
	defer s.μ.RUnlock()
	return int64(s.m.Len()), nil
}
