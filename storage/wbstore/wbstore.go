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
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/creachadair/msync"
	"github.com/creachadair/msync/trigger"
	"github.com/creachadair/taskgroup"
)

// Store implements the [blob.Store] interface by delegating to a base store.
// Non-replacement writes to [blob.KV] instances derived from the base store
// are buffered and written back to the underlying store by a background worker
// that runs concurrently with the store.
type Store struct {
	*monitor.M[wbState, kvWrapper]
}

type wbState struct {
	wb   *writer
	base blob.Store
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(ctx context.Context) error {
	var berr error
	if c, ok := s.M.DB.base.(blob.Closer); ok {
		berr = c.Close(ctx)
	}
	return errors.Join(berr, s.M.DB.wb.Close(ctx))
}

// New constructs a [blob.Store] wrapper that delegates to base and uses buf as
// a local buffer store. New will panic if base == nil or buf == nil. The ctx
// value governs the operation of the background writer, which will run until
// the store is closed or ctx terminates.
func New(ctx context.Context, base blob.Store, buf blob.KV) Store {
	if base == nil {
		panic("base is nil")
	} else if buf == nil {
		panic("buffer is nil")
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &writer{
		buf:      buf,
		exited:   make(chan struct{}),
		stop:     cancel,
		nempty:   msync.NewFlag[any](),
		bufClean: trigger.New(),
		kvs:      make(map[dbkey.Prefix]blob.KV),
	}
	w.nempty.Set(nil) // prime
	g := taskgroup.Go(func() error { return w.run(ctx) })

	// When the background writer exits, record the error it reported.
	// A goroutine observing s.exited as closed may safely read s.err.
	go func() {
		w.err = g.Wait()
		close(w.exited)
	}()
	return Store{M: monitor.New(monitor.Config[wbState, kvWrapper]{
		DB: wbState{wb: w, base: base},
		NewKV: func(ctx context.Context, db wbState, pfx dbkey.Prefix, name string) (kvWrapper, error) {
			kv, err := db.base.KV(ctx, name)
			if err != nil {
				return kvWrapper{}, err
			}
			db.wb.addKV(pfx, kv)
			return kvWrapper{wb: db.wb, pfx: pfx, kv: kv}, nil
		},
	})}
}

// Buffer returns the buffer store used by s.
func (s Store) Buffer() blob.KV { return s.M.DB.wb.buffer() }

// Sync blocks until the buffer is empty or ctx ends.
func (s Store) Sync(ctx context.Context) error { return s.M.DB.wb.Sync(ctx) }
