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
	"sort"
	"sync"

	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface using a string-to-string map. The
// contents of a Store are not persisted. All operations on a memstore are
// protected by a mutex, so they are effectively atomic and may be linearized
// to any point during their critical section.
type Store struct {
	μ sync.Mutex
	m map[string]string
}

// Opener constructs a memstore, for use with the store package.  The address
// is ignored, and an error will never be returned.
func Opener(_ context.Context, _ string) (blob.Store, error) { return New(), nil }

// New constructs a new, empty store.
func New() *Store { return &Store{m: make(map[string]string)} }

// Clear removes all keys and values from s.
func (s *Store) Clear() {
	s.μ.Lock()
	defer s.μ.Unlock()
	for key := range s.m {
		delete(s.m, key)
	}
}

// Snapshot copies a snapshot of the keys and values of s into m.
// It returns m to allow chaining with construction.
func (s *Store) Snapshot(m map[string]string) map[string]string {
	s.μ.Lock()
	defer s.μ.Unlock()
	for key, val := range s.m {
		m[key] = val
	}
	return m
}

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	if v, ok := s.m[key]; ok {
		return []byte(v), nil
	}
	return nil, blob.KeyNotFound(key)
}

// Put implements part of blob.Store.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	if _, ok := s.m[opts.Key]; ok && !opts.Replace {
		return blob.KeyExists(opts.Key)
	}
	s.m[opts.Key] = string(opts.Data)
	return nil
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()

	if v, ok := s.m[key]; ok {
		return int64(len(v)), nil
	}
	return 0, blob.KeyNotFound(key)
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	if _, ok := s.m[key]; !ok {
		return blob.KeyNotFound(key)
	}
	delete(s.m, key)
	return nil
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	s.μ.Lock()
	var keys []string
	for key := range s.m {
		if key >= start {
			keys = append(keys, key)
		}
	}
	s.μ.Unlock()
	sort.Strings(keys)

	for _, key := range keys {
		if err := f(key); err != nil {
			if errors.Is(err, blob.ErrStopListing) {
				break
			}
			return err
		}
	}
	return nil
}

// Len implements part of blob.Store.
func (s *Store) Len(context.Context) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	return int64(len(s.m)), nil
}
