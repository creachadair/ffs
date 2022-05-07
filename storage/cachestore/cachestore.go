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
	"github.com/creachadair/scapegoat"
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
	listed bool                          // keymap has a complete list
	keymap *scapegoat.Tree[string, bool] // known keys
	cache  *cache                        // blob cache
}

// New constructs a new cached store with the specified capacity in bytes,
// delegating storage operations to s.  It will panic if maxBytes < 0.
func New(s blob.Store, maxBytes int) *Store {
	return &Store{
		base:   s,
		keymap: scapegoat.New[string, bool](300, scapegoat.LessThan[string]),
		cache:  newCache(maxBytes),
	}
}

// Get implements a method of blob.Store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if in, ok := s.keymap.Lookup(key); ok && !in {
		return nil, blob.KeyNotFound(key)
	} else if data, ok := s.cache.get(key, true); ok {
		return data, nil
	}
	data, err := s.base.Get(ctx, key)
	if err != nil {
		if blob.IsKeyNotFound(err) {
			s.keymap.Replace(key, false)
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
	if !opts.Replace {
		if _, ok := s.cache.get(opts.Key, false); ok {
			return blob.KeyExists(opts.Key)
		}
	}
	if err := s.base.Put(ctx, opts); err != nil {
		return err
	}
	s.cache.put(opts.Key, opts.Data)
	s.keymap.Replace(opts.Key, true)
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
	s.keymap.Remove(key)
	return nil
}

// Size implements a method of blob.Store.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if in, ok := s.keymap.Lookup(key); ok && !in {
		return 0, blob.KeyNotFound(key)
	}
	size, err := s.base.Size(ctx, key)
	if blob.IsKeyNotFound(err) {
		s.keymap.Replace(key, false)
	}
	return size, err
}

// initKeyMap fills the key map from the base store.  The caller must hold s.μ.
func (s *Store) initKeyMap(ctx context.Context) error {
	if s.listed {
		return nil
	}
	if err := s.base.List(ctx, "", func(key string) error {
		s.keymap.Replace(key, true)
		return nil
	}); err != nil {
		return err
	}
	s.listed = true
	return nil
}

// List implements a method of blob.Store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMap(ctx); err != nil {
		return err
	}

	var ferr error
	s.keymap.InorderAfter(start, func(key string, ok bool) bool {
		if ok { // skip keys flagged non-existent
			ferr = f(key)
		}
		return ferr == nil
	})
	return ferr
}

// Len implements a method of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMap(ctx); err != nil {
		return 0, err
	}
	var n int64
	s.keymap.Inorder(func(key string, ok bool) bool {
		if ok {
			n++
		}
		return true
	})
	return n, nil
}

// Close implements blob.Closer by closing the underlying store.
func (s *Store) Close(ctx context.Context) error {
	s.μ.Lock()
	defer s.μ.Unlock()

	// Release the memory held by the caches.
	s.cache.clear()
	s.keymap = nil
	return blob.CloseStore(ctx, s.base)
}

// CAS implements a cached wrapper around a blob.CAS instance.
type CAS struct {
	*Store
	cas blob.CAS
}

// NewCAS constructs a new cached store with the specified capacity in bytes,
// delegating storage operations to cas.  It will panic if maxBytes < 0.
func NewCAS(cas blob.CAS, maxBytes int) CAS {
	return CAS{
		Store: New(cas, maxBytes),
		cas:   cas,
	}
}

// CASPut implements part of blob.CAS using the underlying store.
func (c CAS) CASPut(ctx context.Context, data []byte) (string, error) {
	c.μ.Lock()
	defer c.μ.Unlock()
	key, err := c.cas.CASPut(ctx, data)
	if err != nil {
		return "", err
	}
	c.cache.put(key, data)
	c.keymap.Replace(key, true)
	return key, nil
}

// CASKey implements part of blob.CAS using the underlying store.
func (c CAS) CASKey(ctx context.Context, data []byte) (string, error) {
	return c.cas.CASKey(ctx, data)
}
