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
	"errors"
	"fmt"
	"iter"
	"log"
	"net"
	"syscall"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/msync"
	"github.com/creachadair/taskgroup"
)

var errSlowWriteRetry = errors.New("slow write retry")

// A kvWrapper implements the [blob.KV] interface, and manages the forwarding
// of cached Put requests to an underlying KV.
type kvWrapper struct {
	base blob.KV
	buf  blob.KV

	// The background writer waits on nempty when it finds no blobs to push.
	nempty *msync.Flag[any]
}

func (w *kvWrapper) signal() { w.nempty.Set(nil) }

// run implements the backround writer. It runs until ctx terminates or until
// it receives an unrecoverable error.
func (w *kvWrapper) run(ctx context.Context) {
	g, run := taskgroup.New(nil).Limit(64)
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			return // normal shutdown
		case <-w.nempty.Ready():
		}

		for key, err := range w.buf.List(ctx, "") {
			if err != nil {
				log.Printf("DEBUG :: error scanning buffer: %v", err)
				break
			}
			if ctx.Err() != nil {
				break
			}
			run(func() error {
				// Read the blob and forward it to the base store, then delete it.
				// Because the buffer contains only non-replacement blobs, it is
				// safe to delete the blob even if another copy was written while
				// we worked, since the content will be the same.  If Get or Delete
				// fails, it means someone deleted the key before us. That's fine.

				data, err := w.buf.Get(ctx, key)
				if blob.IsKeyNotFound(err) {
					return nil
				} else if err != nil {
					return err
				}

				const maxTries = 3
				const tryTimeout = 10 * time.Second
				for try := 1; ; try++ {
					// An individual write should not be allowed to stall for too long.
					rtctx, cancel := context.WithTimeoutCause(ctx, tryTimeout, errSlowWriteRetry)
					err := w.base.Put(rtctx, blob.PutOptions{
						Key:     key,
						Data:    data,
						Replace: false,
					})
					cause := context.Cause(rtctx)
					cancel()
					if err == nil || blob.IsKeyExists(err) {
						break // OK, drop this blob from the buffer and keep going
					} else if try <= maxTries && (isRetryableError(err) || errors.Is(cause, errSlowWriteRetry)) {
						// failed in a retryable way, and we have attempts left
					} else if ctx.Err() != nil {
						return ctx.Err() // give up, the writeback thread is closing
					} else {
						return fmt.Errorf("put %x failed after %d tries: %w", key, try, err)
					}
					time.Sleep(100 * time.Millisecond)
				}
				if err := w.buf.Delete(ctx, key); err != nil && !blob.IsKeyNotFound(err) {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			log.Printf("DEBUG :: error in writeback: %v", err)
			w.signal()
		}
	}
}

func isRetryableError(err error) bool {
	var derr *net.DNSError
	if errors.As(err, &derr) {
		return derr.Temporary() || derr.IsNotFound
	}
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED)
}

// Get implements part of [blob.KV]. If key is in the write-behind store, its
// value there is returned; otherwise it is fetched from the base store.
func (w *kvWrapper) Get(ctx context.Context, key string) ([]byte, error) {
	// Fetch from the buffer and the base store concurrently.
	// A hit in the buffer takes precedence, but if that fails we want the base
	// result to be available quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	buf := taskgroup.Call(func() ([]byte, error) {
		return w.buf.Get(ctx, key)
	})
	base := taskgroup.Call(func() ([]byte, error) {
		return w.base.Get(ctx, key)
	})
	if data, err := buf.Wait().Get(); err == nil {
		return data, nil
	}
	return base.Wait().Get()
}

// Has implements part of [blob.KV].
func (w *kvWrapper) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	// Look up keys in the buffer first. It is possible we may have some there
	// that are not yet written back. Do this first so that if a writeback
	// completes while we're checking the base store, we will still have a
	// coherent value.
	want := mapset.New(keys...)
	have, err := w.buf.Has(ctx, want.Slice()...)
	if err != nil {
		return nil, fmt.Errorf("buffer stat: %w", err)
	}
	if have.Equals(want) {
		return have, nil // we found everything
	}

	// Check for any keys we did not find in the buffer, in the base.
	want.RemoveAll(have)
	base, err := w.base.Has(ctx, want.Slice()...)
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
	return errors.Join(berr, cerr)
}

// Put implements part of [blob.KV]. It delegates to the base store directly
// for writes that request replacement; otherwise it stores the blob into the
// buffer for writeback.
func (w *kvWrapper) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Replace {
		// Don't buffer writes that request replacement.
		return w.base.Put(ctx, opts)
	}
	if err := w.buf.Put(ctx, opts); err != nil {
		return err
	}
	w.signal()
	return nil
}

// bufferKeysBetween iterates the keys in the buffer that are greater than or
// equal to notBefore and less than notAfter.
func (w *kvWrapper) bufferKeysBetween(ctx context.Context, notBefore, notAfter string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range w.buf.List(ctx, notBefore) {
			if err != nil {
				yield("", err)
				return
			}
			if key >= notAfter {
				return
			}
			if !yield(key, nil) {
				return
			}
		}
	}
}

// Len implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the reported length reflects
// the total number of unique keys across both the buffer and the base store.
func (w *kvWrapper) Len(ctx context.Context) (int64, error) {
	var bufKeys mapset.Set[string]
	for key, err := range w.buf.List(ctx, "") {
		if err != nil {
			return 0, err
		}
		bufKeys.Add(key)
	}

	numKeys := int64(bufKeys.Len())
	for key, err := range w.base.List(ctx, "") {
		if err != nil {
			return 0, err
		}
		if !bufKeys.Has(key) {
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
		prev := start
		for key, err := range w.base.List(ctx, start) {
			if err != nil {
				yield("", err)
				return
			}
			for fill, err := range w.bufferKeysBetween(ctx, prev, key) {
				if err != nil {
					yield("", err)
					return
				}
				if !yield(fill, nil) {
					return
				}
			}
			if !yield(key, nil) {
				return
			}
			prev = key
		}

		// Now ship any keys left in the buffer after the last key we sent.
		for key, err := range w.buf.List(ctx, prev) {
			if err != nil {
				yield("", err)
				return
			}
			if prev == key {
				continue // we already shipped this key
			}
			if !yield(key, nil) {
				return
			}
		}
	}
}
