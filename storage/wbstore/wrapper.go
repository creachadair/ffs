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
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/mds/stree"
)

// kvWrapper implements [blob.KV] but not [blob.CAS].
type kvWrapper struct {
	wb *writer
	id uint16  // the key prefix for this KV instance (used by the writer)
	kv blob.KV // the underlying KV to which writes are forwarded
}

// casWrapper implements [blob.KV] and [blob.CAS].
type casWrapper struct {
	kvWrapper
	cas blob.CAS
}

// Get implements part of [blob.KV]. If key is in the write-behind store, its
// value there is returned; otherwise it is fetched from the base store.
func (s kvWrapper) Get(ctx context.Context, key string) ([]byte, error) {
	if ok, err := s.wb.checkExited(); ok {
		return nil, err
	}

	// Fetch from the buffer and the base store concurrently.
	// A hit in the buffer takes precedence, but if that fails we want the base
	// result to be available quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	bufc := fetch(ctx, s.wb.buffer(), joinKey(s.id, key))
	base := fetch(ctx, s.kv, key)
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
func (s kvWrapper) Delete(ctx context.Context, key string) error {
	cerr := s.wb.buffer().Delete(ctx, joinKey(s.id, key))
	berr := s.kv.Delete(ctx, key)
	if cerr != nil && berr != nil {
		return berr
	}
	return nil
}

// Put implements part of [blob.KV]. It delegates to the base store directly
// for writes that request replacement; otherwise it stores the blob into the
// buffer for writeback.
func (s kvWrapper) Put(ctx context.Context, opts blob.PutOptions) error {
	if ok, err := s.wb.checkExited(); ok {
		return err
	}
	if opts.Replace {
		// Don't buffer writes that request replacement.
		return s.kv.Put(ctx, opts)
	}
	opts.Key = joinKey(s.id, opts.Key)
	if err := s.wb.buffer().Put(ctx, opts); err != nil {
		return err
	}
	s.wb.signal()
	return nil
}

// bufferKeys returns a tree of the keys currently stored in the buffer that
// are greater than or equal to start.
func (s kvWrapper) bufferKeys(ctx context.Context, start string) (*stree.Tree[string], error) {
	buf := stree.New(300, strings.Compare)
	pfx := joinKey(s.id, "")
	if err := s.wb.buffer().List(ctx, pfx, func(key string) error {
		if !strings.HasPrefix(key, pfx) {
			return blob.ErrStopListing // not ours
		}
		_, rkey := splitKey(key) // discard the tag
		buf.Add(rkey)
		return nil
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

// Len implements part of [blob.KV]. It merges contents from the buffer that
// are not listed in the underlying store, so that the reported length reflects
// the total number of unique keys across both the buffer and the base store.
func (s kvWrapper) Len(ctx context.Context) (int64, error) {
	buf, err := s.bufferKeys(ctx, "")
	if err != nil {
		return 0, err
	}

	var baseKeys int64
	if err := s.kv.List(ctx, "", func(key string) error {
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
func (s kvWrapper) List(ctx context.Context, start string, f func(string) error) error {
	buf, err := s.bufferKeys(ctx, start)
	if err != nil {
		return err
	}

	prev := start
	if err := s.kv.List(ctx, start, func(key string) error {
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

// CASPut implements part of blob.CAS. It queries the base store for the
// content key, but stores the blob only in the buffer.
func (s casWrapper) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	if ok, err := s.wb.checkExited(); ok {
		return "", err
	}
	key, err := s.cas.CASKey(ctx, opts)
	if err != nil {
		return "", err
	}
	err = s.wb.buffer().Put(ctx, blob.PutOptions{
		Key:     joinKey(s.id, key),
		Data:    opts.Data,
		Replace: false, // no need to replace content-addressed data
	})
	if blob.IsKeyExists(err) {
		err = nil // ignore, this is fine for a CAS write
	}
	if err == nil {
		s.wb.signal()
	}
	return key, err
}

// CASKey implements part of [blob.CAS].
func (s casWrapper) CASKey(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	return s.cas.CASKey(ctx, opts)
}