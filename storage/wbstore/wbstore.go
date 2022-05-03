// Package wbstore implements a wrapper for a blob.CAS that caches writes
// of content-addressed data in a buffer and pushes them into a base store in
// the background.
package wbstore

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/msync"
	"github.com/creachadair/taskgroup"
)

var errWriterStopped = errors.New("background writer stopped")

// A Store implements the blob.CAS interface delegated to an underlying (base)
// store. Content-addressed writes are cached locally in a buffer store, and
// written behind to the base store in the background. Get and Size queries
// respect the buffer cache, so that the local application sees a consistent
// view. Other store operations delegate directly to the base store.
//
// This wrapper is intended for use with base store implementations that are
// remote and expensive to write to, such as cloud storage. This approach is
// suitable for applications that do not require immediate consistency of the
// base store.
type Store struct {
	blob.CAS
	buf blob.Store

	exited chan struct{} // closed when background writer is done
	stop   func()        // signals the background writer to exit
	err    error         // error that caused shutdown

	// The background writer waits on nempty when it finds no blobs to push.
	nempty msync.Handoff

	// Callers of Sync wait on this condition.
	bufClean *msync.Trigger
}

// New constructs a store wrapper that delegates to base and uses buf as the
// local buffer store. New will panic if base == nil or buf == nil. The ctx
// value governs the operation of the background writer, which will run until
// the store is closed or ctx terminates.
//
// If the buffer store is not empty, any existing blobs will be mirrored to the
// base store. This allows the caller to gracefully resume after failures.
func New(ctx context.Context, base blob.CAS, buf blob.Store) *Store {
	if base == nil {
		panic("base is nil")
	} else if buf == nil {
		panic("buffer is nil")
	}

	ctx, cancel := context.WithCancel(ctx)
	s := &Store{
		CAS:      base,
		buf:      buf,
		exited:   make(chan struct{}),
		stop:     cancel,
		nempty:   msync.NewHandoff(),
		bufClean: msync.NewTrigger(),
	}

	s.nempty.Send(nil) // prime
	g := taskgroup.New(nil).Go(func() error {
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
func (s *Store) Buffer() blob.Store { return s.buf }

// Close implements the optional blob.Closer interface. It signals the
// background writer to terminate, and blocks until it has done so or until ctx
// ends.
func (s *Store) Close(ctx context.Context) error {
	s.stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.exited:
		if s.err == errWriterStopped || s.err == context.Canceled {
			return nil
		}
		return s.err
	}
}

// Sync blocks until the buffer is empty or ctx ends.
func (s *Store) Sync(ctx context.Context) error {
	for {
		n, err := s.buf.Len(ctx)
		if err != nil {
			return err
		} else if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.bufClean.Ready():
			// try again
		}
	}
}

// run implements the backround writer. It runs until ctx terminates or until
// it receives an unrecoverable error.
func (s *Store) run(ctx context.Context) error {
	g := taskgroup.New(nil)
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			return errWriterStopped // normal shutdown
		case <-s.nempty.Ready():
		}

		for i := 0; i < 256; i++ {
			pfx := string([]byte{byte(i)})
			g.Go(func() error {
				// Scan the buffer.
				return s.buf.List(ctx, pfx, func(key string) error {
					if !strings.HasPrefix(key, pfx) {
						return blob.ErrStopListing
					}
					// Read the blob and forward it to the base store, then delete it.
					// Because the buffer contains only content-addressed blobs, it is
					// safe to delete the blob even if another copy was written while we
					// worked, since the content will be the same.
					data, err := s.buf.Get(ctx, key)
					if err != nil {
						return err
					}
					if err := s.CAS.Put(ctx, blob.PutOptions{
						Key:     key,
						Data:    data,
						Replace: false,
					}); err != nil && !blob.IsKeyExists(err) {
						return err
					}
					return s.buf.Delete(ctx, key)
				})
			})
		}
		if err := g.Wait(); err != nil && !isRetryableError(err) {
			log.Printf("DEBUG :: error in writeback, exiting: %v", err)
			return err
		}
		s.bufClean.Signal()
	}
}

func isRetryableError(err error) bool {
	var derr *net.DNSError
	if errors.As(err, &derr) {
		return derr.Temporary() || derr.IsNotFound
	}
	return false
}

type getResult struct {
	bits []byte
	err  error
}

func fetch(ctx context.Context, s blob.Store, key string) <-chan getResult {
	ch := make(chan getResult, 1)
	go func() {
		defer close(ch)
		bits, err := s.Get(ctx, key)
		ch <- getResult{bits: bits, err: err}
	}()
	return ch
}

// Get implements part of blob.Store. If key is in the write-behind store, its
// value there is returned; otherwise it is fetched from the base store.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
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

// Size implements part of blob.Store. If an unflushed write for the key exists
// in the buffer, its size is reported; otherwise the size of the base key.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	select {
	case <-s.exited:
		return 0, s.err
	default:
	}
	z, err := s.buf.Size(ctx, key)
	if err == nil {
		return z, nil
	} else if blob.IsKeyNotFound(err) {
		return s.CAS.Size(ctx, key)
	}
	return 0, err
}

// CASPut implements part of blob.CAS. It queries the base store for the
// content key, but stores the blob only in the buffer.
func (s *Store) CASPut(ctx context.Context, data []byte) (string, error) {
	select {
	case <-s.exited:
		return "", s.err
	default:
	}
	key, err := s.CAS.CASKey(ctx, data)
	if err != nil {
		return "", err
	}
	err = s.buf.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    data,
		Replace: false, // no need to replace content-addressed data
	})
	if blob.IsKeyExists(err) {
		err = nil // ignore, this is fine for a CAS write
	}
	if err == nil {
		s.nempty.Send(nil)
	}
	return key, err
}

// Put implements part of blob.Store. It delegates to the base store directly
// for writes that request replacement; otherwise it stores the blob into the
// buffer for writeback.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
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
	s.nempty.Send(nil)
	return nil
}
