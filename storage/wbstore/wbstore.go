// Package wbstore implements a wrapper for a blob.CAS that caches writes
// of content-addressed data in a buffer and pushes them into a base store in
// the background.
package wbstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/stree"
	"github.com/creachadair/msync"
	"github.com/creachadair/msync/trigger"
	"github.com/creachadair/taskgroup"
)

var errWriterStopped = errors.New("background writer stopped")

// A CAS implements the [blob.KV] and [blob.CAS] interfaces delegated to an
// underlying (base) store. Non-replacement writes are cached locally in a
// buffer store, and written behind to the base store in the background. Get
// and Size queries respect the buffer cache, so that the local application
// sees a consistent view. Other store operations delegate directly to the base
// store.
//
// This wrapper is intended for use with base store implementations that are
// remote and expensive to write to, such as cloud storage. This approach is
// suitable for applications that do not require immediate consistency of the
// base store.
type CAS struct {
	blob.CAS
	buf blob.KV

	exited chan struct{}      // closed when background writer is done
	stop   context.CancelFunc // signals the background writer to exit
	err    error              // error that caused shutdown

	// The background writer waits on nempty when it finds no blobs to push.
	nempty *msync.Flag[any]

	// Callers of Sync wait on this condition.
	bufClean *trigger.Cond
}

// New constructs a store wrapper that delegates to base and uses buf as the
// local buffer store. New will panic if base == nil or buf == nil. The ctx
// value governs the operation of the background writer, which will run until
// the store is closed or ctx terminates.
//
// If the buffer store is not empty, any existing blobs will be mirrored to the
// base store. This allows the caller to gracefully resume after failures.
func New(ctx context.Context, base blob.CAS, buf blob.KV) *CAS {
	if base == nil {
		panic("base is nil")
	} else if buf == nil {
		panic("buffer is nil")
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &CAS{
		CAS:      base,
		buf:      buf,
		exited:   make(chan struct{}),
		stop:     cancel,
		nempty:   msync.NewFlag[any](),
		bufClean: trigger.New(),
	}

	s.nempty.Set(nil) // prime
	g := taskgroup.Go(func() error {
		return s.run(ctx)
	})

	// When the background writer exits, record the error it reported.
	// A goroutine observing s.exited as closed may safely read s.err.
	go func() {
		s.err = g.Wait()
		close(s.exited)
	}()
	return s
}

// Buffer returns the buffer store used by s.
func (s *CAS) Buffer() blob.KV { return s.buf }

// Close implements the optional [blob.Closer] interface. It signals the
// background writer to terminate, and blocks until it has done so or until ctx
// ends.
func (s *CAS) Close(ctx context.Context) error {
	s.stop()
	var wberr error
	select {
	case <-ctx.Done():
		wberr = ctx.Err()
	case <-s.exited:
		if s.err != errWriterStopped && s.err != context.Canceled {
			wberr = s.err
		}
	}
	caserr := blob.Close(ctx, s.CAS)
	buferr := blob.Close(ctx, s.buf)
	if wberr != nil {
		return wberr
	} else if caserr != nil {
		return caserr
	}
	return buferr
}

// Sync blocks until the buffer is empty or ctx ends.
func (s *CAS) Sync(ctx context.Context) error {
	for {
		// Check whether the buffer is empty. If not, wait for the writeback
		// thread to signal that it is done with another pass, then try again.
		ready := s.bufClean.Ready()
		n, err := s.buf.Len(ctx)
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
func (s *CAS) run(ctx context.Context) error {
	errSlowWriteRetry := errors.New("slow write retry")

	g, run := taskgroup.New(nil).Limit(64)
	var work []string // reusable buffer
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			return errWriterStopped // normal shutdown
		case <-s.nempty.Ready():
		}

		// List all the buffered keys and shuffle them so that we don't hammer
		// the same shards of the underlying store in cases where that matters.
		work = work[:0]
		if err := s.buf.List(ctx, "", func(key string) error {
			work = append(work, key)
			return nil
		}); err != nil {
			log.Printf("DEBUG :: error scanning buffer: %v", err)
			continue
		}
		rand.Shuffle(len(work), func(i, j int) { work[i], work[j] = work[j], work[i] })

		for _, key := range work {
			if ctx.Err() != nil {
				return errWriterStopped
			}
			key := key
			run(func() error {
				// Read the blob and forward it to the base store, then delete it.
				// Because the buffer contains only non-replacement blobs, it is
				// safe to delete the blob even if another copy was written while
				// we worked, since the content will be the same.  If Get or Delete
				// fails, it means someone deleted the key before us. That's fine.
				data, err := s.buf.Get(ctx, key)
				if blob.IsKeyNotFound(err) {
					return nil
				} else if err != nil {
					return err
				}
				for try := 1; ; try++ {
					// An individual write should not be allowed to stall for too long.
					rtctx, cancel := context.WithTimeoutCause(ctx, 10*time.Second, errSlowWriteRetry)
					err := s.CAS.Put(rtctx, blob.PutOptions{
						Key:     key,
						Data:    data,
						Replace: false,
					})
					cancel()
					if err == nil || blob.IsKeyExists(err) {
						break // OK, keep going
					} else if (isRetryableError(err) || context.Cause(rtctx) == errSlowWriteRetry) && try <= 3 {
						log.Printf("DEBUG :: error in writeback %x (try %d): %v (retrying)", key, try, err)
					} else if ctx.Err() != nil {
						return ctx.Err() // give up, the writeback thread is closing
					} else {
						return fmt.Errorf("put %x failed after %d tries: %w", key, try, err)
					}
					time.Sleep(50 * time.Millisecond)
				}
				if err := s.buf.Delete(ctx, key); err != nil && !blob.IsKeyNotFound(err) {
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
		s.bufClean.Signal()
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

// Get implements part of [blob.KV]. If key is in the write-behind store, its
// value there is returned; otherwise it is fetched from the base store.
func (s *CAS) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case <-s.exited:
		return nil, s.err
	default:
	}

	// Fetch from the buffer and the base store concurrently.
	// A hit in the buffer takes precedence, but if that fails we want the base
	// result to be available quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bufc := fetch(ctx, s.buf, key)
	base := fetch(ctx, s.CAS, key)
	r := <-bufc
	if r.err == nil {
		return r.bits, nil
	} else if !blob.IsKeyNotFound(r.err) {
		return nil, r.err
	}
	r = <-base
	return r.bits, r.err
}

// Delete implements part of [blob.KV]. The key is deleted from both the buffer
// and the base store, and succeeds as long as either of those operations
// succeeds.
func (s *CAS) Delete(ctx context.Context, key string) error {
	cerr := s.buf.Delete(ctx, key)
	berr := s.CAS.Delete(ctx, key)
	if cerr != nil && berr != nil {
		return berr
	}
	return nil
}

// CASPut implements part of blob.CAS. It queries the base store for the
// content key, but stores the blob only in the buffer.
func (s *CAS) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	select {
	case <-s.exited:
		return "", s.err
	default:
	}
	key, err := s.CAS.CASKey(ctx, opts)
	if err != nil {
		return "", err
	}
	err = s.buf.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    opts.Data,
		Replace: false, // no need to replace content-addressed data
	})
	if blob.IsKeyExists(err) {
		err = nil // ignore, this is fine for a CAS write
	}
	if err == nil {
		s.nempty.Set(nil)
	}
	return key, err
}

// Put implements part of [blob.KV]. It delegates to the base store directly
// for writes that request replacement; otherwise it stores the blob into the
// buffer for writeback.
func (s *CAS) Put(ctx context.Context, opts blob.PutOptions) error {
	select {
	case <-s.exited:
		return s.err
	default:
	}
	if opts.Replace {
		// Don't buffer writes that request replacement.
		return s.CAS.Put(ctx, opts)
	}
	if err := s.buf.Put(ctx, opts); err != nil {
		return err
	}
	s.nempty.Set(nil)
	return nil
}

// bufferKeys returns a tree of the keys currently stored in the buffer that
// are greater than or equal to start.
func (s *CAS) bufferKeys(ctx context.Context, start string) (*stree.Tree[string], error) {
	buf := stree.New(300, strings.Compare)
	if err := s.buf.List(ctx, "", func(key string) error {
		buf.Add(key)
		return nil
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

// Len implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the reported length reflects
// the total number of unique keys across both the buffer and the base store.
func (s *CAS) Len(ctx context.Context) (int64, error) {
	buf, err := s.bufferKeys(ctx, "")
	if err != nil {
		return 0, err
	}
	var baseKeys int64
	if err := s.CAS.List(ctx, "", func(key string) error {
		baseKeys++
		buf.Remove(key)
		return nil
	}); err != nil {
		return 0, err
	}

	// Now any keys remaining in buf are ONLY in buf, so we can add their number
	// to the total to get the effective length.
	return baseKeys + int64(buf.Len()), nil
}

// List implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the keys reported include
// those that have not yet been written back.
func (s *CAS) List(ctx context.Context, start string, f func(string) error) error {
	buf, err := s.bufferKeys(ctx, start)
	if err != nil {
		return err
	}

	prev := start
	if err := s.CAS.List(ctx, start, func(key string) error {
		// Pull out keys from the buffer that are between prev and key, and
		// report them to the caller before sending key itself.
		for _, p := range keysBetween(buf, prev, key) {
			if err := f(p); err != nil {
				return err
			}
		}
		prev = key // save
		return f(key)
	}); err != nil {
		return err
	}

	// Now ship any keys left in the buffer after the last key we sent.
	for _, p := range keysBetween(buf, prev, buf.Max()+"x") {
		if err := f(p); errors.Is(err, blob.ErrStopListing) {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

// keysBetween returns the keys in t strictly between lo and hi.
func keysBetween(t *stree.Tree[string], lo, hi string) (between []string) {
	for key := range t.InorderAfter(lo) {
		if key >= hi {
			break
		}
		between = append(between, key)
	}
	return
}
