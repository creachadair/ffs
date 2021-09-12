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

// Package cachestore implements the blob.Store that delegates to an underlying
// store through an in-memory cache.
package cachestore

import (
	"context"
	"sync"

	"github.com/creachadair/ffs/blob"
)

// Store implements a blob.Store that delegates to an underlying store through
// an in-memory cache. This is appropriate for a high-latency or quota-limited
// remote store (such as a GCS or S3 bucket) that will not be concurrently
// written by other processes; concurrent readers are fine.
//
// Both reads and writes are cached, and the store writes through to the
// underlying store.  Negative hits from Get and Size are also cached.
type Store struct {
	base blob.Store

	μ      sync.Mutex
	nexist map[string]bool // non-existence cache
	cache  *cache          // blob cache
}

// New constructs a new cache with the specified capacity in bytes.
// It will panic if maxBytes < 0.
func New(s blob.Store, maxBytes int) *Store {
	return &Store{
		base:   s,
		nexist: make(map[string]bool),
		cache:  newCache(maxBytes),
	}
}

// Get implements a method of blob.Store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if s.nexist[key] {
		return nil, blob.KeyNotFound(key)
	} else if data, ok := s.cache.get(key); ok {
		return data, nil
	}
	data, err := s.base.Get(ctx, key)
	if err != nil {
		if blob.IsKeyNotFound(err) {
			s.nexist[key] = true
		}
		return nil, err
	}
	s.cache.put(key, data)
	return data, nil
}

// Put implements a method of blob.Store.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.base.Put(ctx, opts); err != nil {
		return err
	}
	s.cache.put(opts.Key, opts.Data)
	delete(s.nexist, opts.Key)
	return nil
}

// Delete implements a method of blob.Store.  Although a successful Delete
// certifies the key does not exist, deletes are not cached as negative hits.
// This avoids cluttering the cache with keys for blobs whose content are not
// interesting enough to fetch.
func (s *Store) Delete(ctx context.Context, key string) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.base.Delete(ctx, key); err != nil {
		return err
	}
	s.cache.drop(key)
	return nil
}

// Size implements a method of blob.Store.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if s.nexist[key] {
		return 0, blob.KeyNotFound(key)
	}
	size, err := s.base.Size(ctx, key)
	if blob.IsKeyNotFound(err) {
		s.nexist[key] = true
	}
	return size, err
}

// List implements a method of blob.Store. The results are not cached.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.base.List(ctx, start, f)
}

// Len implements a method of blob.Store. This result is not cached.
func (s *Store) Len(ctx context.Context) (int64, error) {
	return s.base.Len(ctx)
}

// Close implements blob.Closer by closing the underlying store.
func (s *Store) Close(ctx context.Context) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	// Release the memory held by the caches.
	s.cache.clear()
	s.nexist = nil
	return blob.CloseStore(ctx, s.base)
}
