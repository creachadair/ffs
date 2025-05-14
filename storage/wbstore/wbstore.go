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
	"github.com/creachadair/taskgroup"
)

// Store implements the [blob.Store] interface by delegating to a base store.
// Non-replacement writes to [blob.KV] instances derived from the base store
// are buffered and written back to the underlying store by a background worker
// that runs concurrently with the store.
type Store struct {
	*monitor.M[wbState, *kvWrapper]

	writers *taskgroup.Group
	stop    context.CancelFunc
}

type wbState struct {
	base blob.Store
	buf  blob.Store
}

// New constructs a [blob.Store] wrapper that delegates to base and uses buf as
// a local buffer store. New will panic if base == nil or buf == nil. The ctx
// value governs the operation of the background writer, which will run until
// the store is closed or ctx terminates.
func New(ctx context.Context, base, buf blob.Store) Store {
	if base == nil {
		panic("base is nil")
	} else if buf == nil {
		panic("buffer is nil")
	}

	wctx, cancel := context.WithCancel(ctx)
	g := taskgroup.New(nil)
	return Store{
		writers: g,
		stop:    cancel,
		M: monitor.New(monitor.Config[wbState, *kvWrapper]{
			DB: wbState{base: base, buf: buf},
			NewKV: func(ctx context.Context, db wbState, pfx dbkey.Prefix, name string) (*kvWrapper, error) {
				baseKV, err := db.base.KV(ctx, name)
				if err != nil {
					return nil, err
				}
				bufKV, err := db.buf.KV(ctx, name)
				if err != nil {
					return nil, err
				}

				// Each KV gets its own writeback worker.
				w := &kvWrapper{base: baseKV, buf: bufKV}
				w.nempty.Set() // prime
				g.Run(func() { w.run(wctx) })
				return w, nil
			},
			NewSub: func(ctx context.Context, db wbState, pfx dbkey.Prefix, name string) (wbState, error) {
				baseSub, err := db.base.Sub(ctx, name)
				if err != nil {
					return wbState{}, err
				}
				bufSub, err := db.buf.Sub(ctx, name)
				if err != nil {
					return wbState{}, err
				}
				return wbState{base: baseSub, buf: bufSub}, nil
			},
		})}
}

// Close implements the [blob.Closer] interface for s.
func (s Store) Close(ctx context.Context) error {
	s.stop()
	s.writers.Wait()

	// N.B. Close the buffer first, since writes back depend on the base.
	var bufErr, baseErr error
	if c, ok := s.M.DB.buf.(blob.Closer); ok {
		bufErr = c.Close(ctx)
	}
	if c, ok := s.M.DB.base.(blob.Closer); ok {
		baseErr = c.Close(ctx)
	}
	return errors.Join(baseErr, bufErr)
}

// BufferLen reports the total number of keys buffered for writeback in the
// buffer storage of s.
func (s Store) BufferLen(ctx context.Context) (int64, error) {
	var count int64
	for kv := range s.M.AllKV() {
		n, err := kv.buf.Len(ctx)
		if err != nil {
			return 0, err
		}
		count += n
	}
	return count, nil
}
