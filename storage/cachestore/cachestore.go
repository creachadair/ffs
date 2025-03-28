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

	μ      sync.RWMutex        // protects the keymap
	keymap *stree.Tree[string] // known keys

	// The keymap is initialized to the keyspace of the underlying store.
	// Additional keys are added by store queries.
}

// NewKV constructs a new cached [KV] with the specified capacity in bytes,
// delegating storage operations to s.  It will panic if maxBytes < 0.
func NewKV(s blob.KV, maxBytes int) *KV {
	return &KV{
		base:   s,
		keymap: stree.New[string](300, strings.Compare),
		cache: cache.New(cache.LRU[string, []byte](int64(maxBytes)).
			WithSize(cache.Length),
		),
	}
}

// Get implements a method of [blob.KV].
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.initKeyMap(ctx); err != nil {
		return nil, err
	}
	s.μ.RLock()
	defer s.μ.RUnlock()
	data, cached, err := s.getLocked(ctx, key)
	if err != nil {
		return nil, err
	} else if cached {
		return bytes.Clone(data), nil
	}
	return data, nil
}

// getLocked implements the lookup of a key in the store.  On success, it also
// reports whether the result is shared with the cache.  If so, the caller must
// copy the bytes before returning them, though it is safe to read the contents
// without a copy or a lock.
//
// Precondition: initKeyMap must have previously succeeded. The caller may hold
// s.μ either exclusively or shared.
func (s *KV) getLocked(ctx context.Context, key string) ([]byte, bool, error) {
	if _, ok := s.keymap.Get(key); !ok {
		return nil, false, blob.KeyNotFound(key)
	}
	if data, ok := s.cache.Get(key); ok {
		return data, true, nil
	}

	// Reaching here, the key is in the key map but not in the cache.
	data, err := s.base.Get(ctx, key)
	if err != nil {
		// This shouldn't happen, it means the underlying store was probably
		// modified out of band. Treat it as a missing key.
		return nil, false, err
	}

	// Update the cache before returning the value.
	cached := s.cache.Put(key, data)
	return data, cached, nil
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
	s.μ.Lock()
	defer s.μ.Unlock()

	if !opts.Replace {
		if _, ok := s.keymap.Get(opts.Key); ok {
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
	if err := s.initKeyMap(ctx); err != nil {
		return err
	}
	s.μ.Lock()
	defer s.μ.Unlock()

	// Even if we fail to delete the key from the underlying store, take this as
	// a signal that we should forget about its data. Don't remove it from the
	// keymap, however, unless the deletion actually succeeds.
	s.cache.Remove(key)
	err := s.base.Delete(ctx, key)
	if err == nil || errors.Is(err, blob.ErrKeyNotFound) {
		s.keymap.Remove(key)
	}
	return err
}

// initKeyMap initializes the key map from the base store.
func (s *KV) initKeyMap(ctx context.Context) error {
	if s.listed.Load() {
		return nil // affirmatively already done
	}
	s.μ.Lock()
	defer s.μ.Unlock()
	if s.listed.Load() {
		return nil // someone else did it, OK
	}

	ictx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := taskgroup.New(cancel)

	// The keymap is not safe for concurrent use by multiple goroutines, so
	// serialize insertions through a collector.
	coll := taskgroup.Gather(g.Go, func(key string) {
		s.keymap.Add(key)
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
	err := g.Wait()
	s.listed.Store(err == nil)
	return err
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
