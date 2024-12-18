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

// Package wbstore implements a wrapper for a [blob.Store] that caches
// non-replacement writes of in a buffer and pushes them to the base store
// concurrently in the background.
package wbstore

import (
	"context"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/msync"
	"github.com/creachadair/msync/trigger"
	"github.com/creachadair/taskgroup"
)

// Store implements the [blob.Store] interface by delegating to a base store.
// Non-replacement writes to [blob.KV] instances derived from the base store
// are buffered and written back to the underlying store by a background worker
// that runs concurrently with the store.
type Store struct {
	wb   *writer    // writeback worker, shared among all derived stores
	base blob.Store // the underlying delegated store
}

// Keyspace implements part of [blob.Store]. It wraps the [blob.KV] produced by
// the base store to direct writes through the buffer.
//
// If the [blob.KV] returned by the base store implements [blob.CAS], then the
// returned wrapper does also. Otherwise it does not.
func (s Store) Keyspace(ctx context.Context, name string) (blob.KV, error) {
	kv, err := s.base.Keyspace(ctx, name)
	if err != nil {
		return nil, err
	}
	id := s.wb.addKV(kv)
	if cas, ok := kv.(blob.CAS); ok {
		// Their KV is also a CAS, so ours will be too.
		return casWrapper{
			kvWrapper: kvWrapper{wb: s.wb, id: id, kv: kv},
			cas:       cas,
		}, nil
	}
	// Their KV is not a CAS so ours will not be either.
	return kvWrapper{wb: s.wb, id: id, kv: kv}, nil
}

// Sub implements part of [blob.Store]. It wraps the substore produced by the
// base store to direct writes through the buffer.
func (s Store) Sub(ctx context.Context, name string) (blob.Store, error) {
	sub, err := s.base.Sub(ctx, name)
	if err != nil {
		return nil, err
	}
	return Store{wb: s.wb, base: sub}, nil
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error { return s.wb.Close(ctx) }

// New constructs a [blob.Store] wrapper that delegates to base and uses buf as
// a local buffer store. New will panic if base == nil or buf == nil. The ctx
// value governs the operation of the background writer, which will run until
// the store is closed or ctx terminates.
//
// If the buffer store is not empty, existing blobs there will be discarded.
func New(ctx context.Context, base blob.Store, buf blob.KV) Store {
	if base == nil {
		panic("base is nil")
	} else if buf == nil {
		panic("buffer is nil")
	}

	ctx, cancel := context.WithCancel(ctx)

	// Discard existing contents of the buffer.
	buf.List(ctx, "", func(key string) error {
		buf.Delete(ctx, key) // best-effort
		return nil
	})
	w := &writer{
		buf:      buf,
		exited:   make(chan struct{}),
		stop:     cancel,
		nempty:   msync.NewFlag[any](),
		bufClean: trigger.New(),
		kvs:      make(map[uint16]blob.KV),
	}

	w.nempty.Set(nil) // prime
	g := taskgroup.Go(func() error { return w.run(ctx) })

	// When the background writer exits, record the error it reported.
	// A goroutine observing s.exited as closed may safely read s.err.
	go func() {
		w.err = g.Wait()
		close(w.exited)
	}()
	return Store{wb: w, base: base}
}

// Buffer returns the buffer store used by s.
func (s Store) Buffer() blob.KV { return s.wb.buffer() }

// Sync blocks until the buffer is empty or ctx ends.
func (s Store) Sync(ctx context.Context) error { return s.wb.Sync(ctx) }
