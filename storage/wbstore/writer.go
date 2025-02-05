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
	"sync"
	"syscall"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/msync"
	"github.com/creachadair/msync/trigger"
	"github.com/creachadair/taskgroup"
)

var errWriterStopped = errors.New("background writer stopped")

// A writer manages the forwarding of cached Put requests to underlying KVs.
type writer struct {
	buf blob.KV

	exited chan struct{}      // closed when background writer is done
	stop   context.CancelFunc // signals the background writer to exit
	err    error              // error that caused shutdown

	// The background writer waits on nempty when it finds no blobs to push.
	nempty *msync.Flag[any]

	// Callers of Sync wait on this condition.
	bufClean *trigger.Cond

	μ   sync.Mutex // protects the fields below
	kvs map[dbkey.Prefix]blob.KV
}

func (w *writer) buffer() blob.KV { return w.buf }

func (w *writer) signal() { w.nempty.Set(nil) }

func (w *writer) addKV(pfx dbkey.Prefix, kv blob.KV) {
	w.μ.Lock()
	defer w.μ.Unlock()
	w.kvs[pfx] = kv
}

func (w *writer) findKV(taggedKey string) (string, blob.KV) {
	w.μ.Lock()
	defer w.μ.Unlock()
	id := dbkey.Prefix(taggedKey[:dbkey.PrefixLen])
	key := taggedKey[dbkey.PrefixLen:]
	return key, w.kvs[id]
}

func (w *writer) checkExited() (bool, error) {
	select {
	case <-w.exited:
		return true, w.err
	default:
		return false, nil
	}
}

func (w *writer) Close(ctx context.Context) error {
	w.stop()
	var wberr error
	select {
	case <-ctx.Done():
		wberr = ctx.Err()
	case <-w.exited:
		if w.err != errWriterStopped && w.err != context.Canceled {
			wberr = w.err
		}
	}
	var buferr error
	if c, ok := w.buf.(blob.Closer); ok {
		buferr = c.Close(ctx)
	}
	return errors.Join(wberr, buferr)
}

// Sync blocks until the buffer is empty or ctx ends.
func (w *writer) Sync(ctx context.Context) error {
	for {
		// Check whether the buffer is empty. If not, wait for the writeback
		// thread to signal that it is done with another pass, then try again.
		ready := w.bufClean.Ready()
		n, err := w.buf.Len(ctx)
		if err != nil {
			return err
		} else if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
			// try again
		}
	}
}

// run implements the backround writer. It runs until ctx terminates or until
// it receives an unrecoverable error.
func (w *writer) run(ctx context.Context) error {
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

		for _, tagged := range work {
			if ctx.Err() != nil {
				return errWriterStopped
			}

			key, kv := w.findKV(tagged)
			if kv == nil {
				// This key does not belong to a currently known keyspace.  That
				// may be because the keyspace hasn't been reloaded since the store
				// was started up. Keep calm and carry on, and try it again on a
				// subsequent pass.
				continue
			}

			run(func() error {
				// Read the blob and forward it to the base store, then delete it.
				// Because the buffer contains only non-replacement blobs, it is
				// safe to delete the blob even if another copy was written while
				// we worked, since the content will be the same.  If Get or Delete
				// fails, it means someone deleted the key before us. That's fine.

				data, err := w.buf.Get(ctx, tagged) // N.B. tagged in the buffer
				if blob.IsKeyNotFound(err) {
					return nil
				} else if err != nil {
					return err
				}

				const maxTries = 3
				for try := 1; ; try++ {
					// An individual write should not be allowed to stall for too long.
					rtctx, cancel := context.WithTimeoutCause(ctx, 10*time.Second, errSlowWriteRetry)
					err := kv.Put(rtctx, blob.PutOptions{
						Key:     key,
						Data:    data,
						Replace: false,
					})
					cancel()
					if err == nil || blob.IsKeyExists(err) {
						break // OK, keep going
					} else if (isRetryableError(err) || context.Cause(rtctx) == errSlowWriteRetry) && try <= maxTries {
						if try > 1 {
							log.Printf("DEBUG :: error in writeback %x (try %d): %v (retrying)", key, try, err)
						}
					} else if ctx.Err() != nil {
						return ctx.Err() // give up, the writeback thread is closing
					} else {
						return fmt.Errorf("put %x failed after %d tries: %w", key, try, err)
					}
					time.Sleep(50 * time.Millisecond)
				}
				if err := w.buf.Delete(ctx, tagged); err != nil && !blob.IsKeyNotFound(err) {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			log.Printf("DEBUG :: error in writeback: %v", err)
		}

		// Signal any pending sync that the buffer may be clean.
		// Sync must check whether it really is empty.
		w.bufClean.Signal()
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
