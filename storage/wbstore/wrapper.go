// Copyright 2024 Michael J. Fromberger. All Rights Reserved.
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

package wbstore

import (
	"context"
	"fmt"
	"iter"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/stree"
)

// Get implements part of [blob.KV]. If key is in the write-behind store, its
// value there is returned; otherwise it is fetched from the base store.
func (w *kvWrapper) Get(ctx context.Context, key string) ([]byte, error) {
	// Fetch from the buffer and the base store concurrently.
	// A hit in the buffer takes precedence, but if that fails we want the base
	// result to be available quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bufc := fetch(ctx, w.buf, key)
	base := fetch(ctx, w.base, key)
	r := <-bufc
	if r.err == nil {
		return r.bits, nil
	} else if !blob.IsKeyNotFound(r.err) {
		return nil, r.err
	}
	r = <-base
	return r.bits, r.err
}

// Has implements part of [blob.KV].
func (w *kvWrapper) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	// Look up keys in the buffer first. It is possible we may have some there
	// that are not yet written back. Do this first so that if a writeback
	// completes while we're checking the base store, we will still have a
	// coherent value.
	have, err := w.buf.Has(ctx, keys...)
	if err != nil {
		return nil, fmt.Errorf("buffer stat: %w", err)
	}
	if len(have) == len(keys) {
		return have, nil // we found everything
	}

	// Check for any keys we did not find in the buffer, in the base.
	missing := have.Clone().Remove(keys...)
	base, err := w.base.Has(ctx, missing.Slice()...)
	if err != nil {
		return nil, fmt.Errorf("base stat: %w", err)
	}
	have.AddAll(base)
	return have, nil
}

// Delete implements part of [blob.KV]. The key is deleted from both the buffer
// and the base store, and succeeds as long as either of those operations
// succeeds.
func (w *kvWrapper) Delete(ctx context.Context, key string) error {
	cerr := w.buf.Delete(ctx, key)
	berr := w.base.Delete(ctx, key)
	if cerr != nil && berr != nil {
		return berr
	}
	return nil
}

// Put implements part of [blob.KV]. It delegates to the base store directly
// for writes that request replacement; otherwise it stores the blob into the
// buffer for writeback.
func (w *kvWrapper) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Replace {
		// Don't buffer writes that request replacement.
		return w.base.Put(ctx, opts)
	}

	// Preflight check: If the underlying store already has the key, we do not
	// need to put it in the buffer. Treat an error in this check as the key not
	// being present (the write-behind will handle that case).
	if got, _ := w.base.Has(ctx, opts.Key); got.Has(opts.Key) {
		return blob.KeyExists(opts.Key)
	}
	if err := w.buf.Put(ctx, opts); err != nil {
		return err
	}
	w.signal()
	return nil
}

// bufferKeys returns a tree of the keys currently stored in the buffer that
// are greater than or equal to start.
func (w *kvWrapper) bufferKeys(ctx context.Context, start string) (*stree.Tree[string], error) {
	buf := stree.New(300, strings.Compare)
	for key, err := range w.buf.List(ctx, start) {
		if err != nil {
			return nil, err
		}
		buf.Add(key)
	}
	return buf, nil
}

// Len implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the reported length reflects
// the total number of unique keys across both the buffer and the base store.
func (w *kvWrapper) Len(ctx context.Context) (int64, error) {
	buf, err := w.bufferKeys(ctx, "")
	if err != nil {
		return 0, err
	}

	numKeys := int64(buf.Len())
	for key, err := range w.base.List(ctx, "") {
		if err != nil {
			return 0, err
		}
		_, ok := buf.Get(key)
		if !ok {
			numKeys++
		}
	}
	return numKeys, nil
}

// List implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the keys reported include
// those that have not yet been written back.
func (w *kvWrapper) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		buf, err := w.bufferKeys(ctx, start)
		if err != nil {
			yield("", err)
			return
		}

		prev := start
		for key, err := range w.base.List(ctx, start) {
			if err != nil {
				yield("", err)
				return
			}
			// Pull out keys from the buffer that are between prev and key, and
			// report them to the caller before sending key itself.
			for p := range keysBetween(buf, prev, key) {
				if !yield(p, nil) {
					return
				}
			}
			prev = key // save
			if !yield(key, nil) {
				return // deliver key itself
			}
		}

		// Now ship any keys left in the buffer after the last key we sent.
		for p := range keysBetween(buf, prev, buf.Max()+"x") {
			if !yield(p, nil) {
				return
			}
		}
	}
}

// keysBetween returns the keys in t strictly between lo and hi.
func keysBetween(t *stree.Tree[string], lo, hi string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for key := range t.InorderAfter(lo) {
			if key >= hi {
				return
			}
			if !yield(key) {
				return
			}
		}
	}
}
