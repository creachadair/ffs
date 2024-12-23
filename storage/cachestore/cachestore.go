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

// Package cachestore implements a [blob.Store] that wraps the keyspaces of an
// underlying store in memory caches.
package cachestore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/creachadair/mds/cache"
	"github.com/creachadair/mds/stree"
	"github.com/creachadair/taskgroup"
)

// Store implements the [blob.StoreCloser] interface.
type Store struct {
	*monitor.M[state, *KV]
}

type state struct {
	base     blob.Store
	maxBytes int
}

// New constructs a new root Store delegated to base.
// It will panic if maxBytes < 0.
func New(base blob.Store, maxBytes int) Store {
	if maxBytes < 0 {
		panic("cache size is negative")
	}
	return Store{M: monitor.New(monitor.Config[state, *KV]{
		DB: state{base: base, maxBytes: maxBytes},
		NewKV: func(ctx context.Context, db state, _ dbkey.Prefix, name string) (*KV, error) {
			kv, err := db.base.KV(ctx, name)
			if err != nil {
				return nil, err
			}
			return &KV{
				base:   kv,
				keymap: stree.New[string](300, strings.Compare),
				cache: cache.New(int64(db.maxBytes), cache.LRU[string, []byte]().
					WithSize(cache.Length),
				),
			}, nil
		},
	})}
}

// Close implements a method of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error {
	if c, ok := s.M.DB.base.(blob.Closer); ok {
		return c.Close(ctx)
	}
	return nil
}

// KV implements a [blob.KV] that delegates to an underlying store through an
// in-memory cache. This is appropriate for a high-latency or quota-limited
// remote store (such as a GCS or S3 bucket) that will not be concurrently
// written by other processes; concurrent readers are fine.
//
// Both reads and writes are cached, and the store writes through to the
// underlying store.  Negative hits from Get and Size are also cached.
type KV struct {
	base blob.KV

	μ      sync.Mutex
	listed bool                         // keymap has been fully populated
	keymap *stree.Tree[string]          // known keys
	cache  *cache.Cache[string, []byte] // blob cache

	// The keymap is initialized to the keyspace of the underlying store.
	// Additional keys are added by store queries.
}

// NewKV constructs a new cached [KV] with the specified capacity in bytes,
// delegating storage operations to s.  It will panic if maxBytes < 0.
func NewKV(s blob.KV, maxBytes int) *KV {
	return &KV{
		base:   s,
		keymap: stree.New[string](300, strings.Compare),
		cache: cache.New(int64(maxBytes), cache.LRU[string, []byte]().
			WithSize(cache.Length),
		),
	}
}

// Get implements a method of [blob.KV].
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMapLocked(ctx); err != nil {
		return nil, err
	} else if data, ok := s.cache.Get(key); ok {
		return bytes.Clone(data), nil
	} else if _, ok := s.keymap.Get(key); !ok {
		return nil, blob.KeyNotFound(key)
	}
	data, err := s.base.Get(ctx, key)
	if err != nil {
		if blob.IsKeyNotFound(err) {
			s.keymap.Remove(key)
		}
		return nil, err
	}
	s.cache.Put(key, data)
	return data, nil
}

// Put implements a method of [blob.KV].
func (s *KV) Put(ctx context.Context, opts blob.PutOptions) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMapLocked(ctx); err != nil {
		return err
	}
	if !opts.Replace {
		if s.cache.Has(opts.Key) {
			return blob.KeyExists(opts.Key)
		}
	}
	if err := s.base.Put(ctx, opts); err != nil {
		return err
	}
	s.cache.Put(opts.Key, opts.Data)
	s.keymap.Replace(opts.Key)
	return nil
}

// Delete implements a method of [blob.KV].
func (s *KV) Delete(ctx context.Context, key string) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMapLocked(ctx); err != nil {
		return err
	}

	// Even if we fail to delete the key from the underlying store, take this as
	// a signal that we should forget about its data.
	s.cache.Remove(key)
	s.keymap.Remove(key)
	return s.base.Delete(ctx, key)
}

// initKeyMapLocked fills the key map from the base store.
// The caller must hold s.μ.
func (s *KV) initKeyMapLocked(ctx context.Context) error {
	if s.listed {
		return nil
	}
	ictx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)

	// The keymap is not safe for concurrent use by multiple goroutines, so
	// serialize insertions through a collector.
	coll := taskgroup.Gather(g.Go, func(key string) {
		s.keymap.Add(key)
	})

	for i := 0; i < 256; i++ {
		pfx := string([]byte{byte(i)})
		coll.Report(func(report func(string)) error {
			return s.base.List(ictx, pfx, func(key string) error {
				if !strings.HasPrefix(key, pfx) {
					return blob.ErrStopListing
				}
				report(key)
				return nil
			})
		})
	}
	err := g.Wait()
	s.listed = err == nil
	return err
}

// List implements a method of [blob.KV].
func (s *KV) List(ctx context.Context, start string, f func(string) error) error {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMapLocked(ctx); err != nil {
		return err
	}

	for key := range s.keymap.InorderAfter(start) {
		if err := f(key); errors.Is(err, blob.ErrStopListing) {
			return nil
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Len implements a method of [blob.KV].
func (s *KV) Len(ctx context.Context) (int64, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if err := s.initKeyMapLocked(ctx); err != nil {
		return 0, err
	}
	return int64(s.keymap.Len()), nil
}
