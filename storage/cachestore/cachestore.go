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
	"iter"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/creachadair/mds/cache"
	"github.com/creachadair/mds/stree"
	"github.com/creachadair/msync/throttle"
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
			return NewKV(kv, db.maxBytes), nil
		},
		NewSub: func(ctx context.Context, db state, _ dbkey.Prefix, name string) (state, error) {
			sub, err := db.base.Sub(ctx, name)
			if err != nil {
				return state{}, err
			}
			return state{base: sub, maxBytes: db.maxBytes}, nil
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

	listed atomic.Bool // keymap has been fully populated

	cache *cache.Cache[string, []byte] // blob cache
	init  *throttle.Throttle[any]      // populate key map
	get   throttle.Set[string, []byte] // get key (data)
	put   throttle.Set[string, any]    // put key (error only)
	del   throttle.Set[string, any]    // delete key (error only)

	μ      sync.RWMutex        // protects the keymap
	keymap *stree.Tree[string] // known keys

	// The keymap is initialized to the keyspace of the underlying store.
	// Additional keys are added by store queries.
}

// NewKV constructs a new cached [KV] with the specified capacity in bytes,
// delegating storage operations to s.  It will panic if maxBytes < 0.
func NewKV(s blob.KV, maxBytes int) *KV {
	kv := &KV{
		base: s,
		cache: cache.New(cache.LRU[string, []byte](int64(maxBytes)).
			WithSize(cache.Length),
		),
	}
	kv.init = throttle.New(throttle.Adapt[any](kv.loadKeyMap))
	return kv
}

// Get implements a method of [blob.KV].
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.initKeyMap(ctx); err != nil {
		return nil, err
	}

	data, cached, err := s.getLocal(ctx, key)
	if err != nil {
		return nil, err
	} else if cached {
		return bytes.Clone(data), nil
	}

	// Reaching here, the key is in the keymap but not in the cache, so we have
	// to fetch it from the underlying store.
	return s.get.Call(ctx, key, func(ctx context.Context) ([]byte, error) {
		data, err := s.base.Get(ctx, key)
		if err != nil {
			return nil, err

			// This shouldn't be able to fail, but it is possible the store was
			// modified out of band, so in that case just don't cache the key.
		}
		s.cache.Put(key, data)
		return data, nil
	})
}

// getLocal reports whether key is present in the store, and if so whether
// its contents are cached locally.
//
// If key is not present, it returns nil, false, ErrKeyNotFound.
// IF key is present but not cached, it returns nil, false, nil.
// If key is present and cached, it returns data, true, nil.
//
// Precondition: initKeyMap must have previously succeeded.
func (s *KV) getLocal(ctx context.Context, key string) ([]byte, bool, error) {
	s.μ.Lock()
	defer s.μ.Unlock()
	if _, ok := s.keymap.Get(key); !ok {
		return nil, false, blob.KeyNotFound(key)
	}
	if data, ok := s.cache.Get(key); ok {
		return data, true, nil
	}
	return nil, false, nil
}

// Has implements a method of [blob.KV].
func (s *KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	if err := s.initKeyMap(ctx); err != nil {
		return nil, err
	}
	s.μ.RLock()
	defer s.μ.RUnlock()
	var out blob.KeySet
	for _, key := range keys {
		if _, ok := s.keymap.Get(key); ok {
			out.Add(key)
		}
	}
	return out, nil
}

// Put implements a method of [blob.KV].
func (s *KV) Put(ctx context.Context, opts blob.PutOptions) error {
	if err := s.initKeyMap(ctx); err != nil {
		return err
	}
	if !opts.Replace {
		s.μ.RLock()
		_, ok := s.keymap.Get(opts.Key)
		s.μ.RUnlock()
		if ok {
			return blob.KeyExists(opts.Key)
		}
	}
	_, err := s.put.Call(ctx, opts.Key, func(ctx context.Context) (any, error) {
		if err := s.base.Put(ctx, opts); err != nil {
			return nil, err
		}
		s.μ.Lock()
		s.keymap.Replace(opts.Key)
		s.μ.Unlock()
		s.cache.Put(opts.Key, opts.Data)
		return nil, nil
	})
	return err
}

// Delete implements a method of [blob.KV].
func (s *KV) Delete(ctx context.Context, key string) error {
	if err := s.initKeyMap(ctx); err != nil {
		return err
	}
	_, err := s.del.Call(ctx, key, func(ctx context.Context) (any, error) {
		// Even if we fail to delete the key from the underlying store, take this as
		// a signal that we should forget about its data. Don't remove it from the
		// keymap, however, unless the deletion actually succeeds.
		s.cache.Remove(key)
		err := s.base.Delete(ctx, key)

		if err == nil || errors.Is(err, blob.ErrKeyNotFound) {
			s.μ.Lock()
			defer s.μ.Unlock()
			s.keymap.Remove(key)
		}
		return nil, err
	})
	return err
}

// initKeyMap initializes the key map from the base store.
func (s *KV) initKeyMap(ctx context.Context) error {
	if s.listed.Load() {
		return nil // affirmatively already done
	}
	_, err := s.init.Call(ctx)
	return err
}

func (s *KV) loadKeyMap(ctx context.Context) error {
	ictx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)

	// The keymap is not safe for concurrent use by multiple goroutines, so
	// serialize insertions through a collector.
	keymap := stree.New[string](300, strings.Compare)
	coll := taskgroup.Gather(g.Go, func(key string) {
		keymap.Add(key)
	})

	for i := range 256 {
		pfx := string([]byte{byte(i)})
		coll.Report(func(report func(string)) error {
			for key, err := range s.base.List(ictx, pfx) {
				if err != nil {
					return err
				} else if !strings.HasPrefix(key, pfx) {
					break
				}
				report(key)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	s.μ.Lock()
	s.keymap = keymap
	s.μ.Unlock()
	s.listed.Store(true)
	return nil
}

func (s *KV) firstKey(start string) (string, bool) {
	s.μ.RLock()
	defer s.μ.RUnlock()
	cur := s.keymap.Find(start)
	return cur.Key(), cur.Valid()
}

func (s *KV) nextKey(prev string) (string, bool) {
	s.μ.RLock()
	defer s.μ.RUnlock()
	cur := s.keymap.Find(prev)
	if cur.Key() > prev {
		return cur.Key(), true
	} else if next := cur.Next(); next.Valid() {
		return next.Key(), true
	}
	return "", false
}

// List implements a method of [blob.KV].
func (s *KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if err := s.initKeyMap(ctx); err != nil {
			yield("", err)
			return
		}
		cur, ok := s.firstKey(start)
		for ok {
			if !yield(cur, nil) {
				return
			}
			cur, ok = s.nextKey(cur)
		}
	}
}

// Len implements a method of [blob.KV].
func (s *KV) Len(ctx context.Context) (int64, error) {
	if err := s.initKeyMap(ctx); err != nil {
		return 0, err
	}
	s.μ.RLock()
	defer s.μ.RUnlock()
	return int64(s.keymap.Len()), nil
}
