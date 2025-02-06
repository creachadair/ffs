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

package wbstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"syscall"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/msync"
	"github.com/creachadair/taskgroup"
)

var errWriterStopped = errors.New("background writer stopped")

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
func (w *kvWrapper) run(ctx context.Context) error {
	errSlowWriteRetry := errors.New("slow write retry")

	g, run := taskgroup.New(nil).Limit(64)
	var work []string // reusable buffer
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			return errWriterStopped // normal shutdown
		case <-w.nempty.Ready():
		}

		// List all the buffered keys and shuffle them so that we don't hammer
		// the same shards of the underlying store in cases where that matters.
		work = work[:0]
		for key, err := range w.buf.List(ctx, "") {
			if err != nil {
				log.Printf("DEBUG :: error scanning buffer: %v", err)
				continue
			}
			work = append(work, key)
		}
		rand.Shuffle(len(work), func(i, j int) { work[i], work[j] = work[j], work[i] })

		for _, key := range work {
			if ctx.Err() != nil {
				return errWriterStopped
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
				const tryTimeout = 30 * time.Second
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
						break // OK, keep going
					} else if (isRetryableError(err) || cause == errSlowWriteRetry) && try <= maxTries {
						// try again
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

type getResult struct {
	bits []byte
	err  error
}

func fetch(ctx context.Context, s blob.KV, key string) <-chan getResult {
	ch := make(chan getResult, 1)
	go func() {
		defer close(ch)
		bits, err := s.Get(ctx, key)
		ch <- getResult{bits: bits, err: err}
	}()
	return ch
}
