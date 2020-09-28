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

// Package blob implements an interface and support code for persistent storage
// of untyped binary blobs.
package blob

import (
	"context"
	"errors"
	"hash"
	"io"
)

// A Store represents a mutable blob store in which each blob is identified by
// a unique, opaque string key.  An implementation of Store is permitted (but
// not required) to report an error from Put when given an empty key.  Even if
// the implementation cannot store empty keys, it must report ErrKeyNotFound as
// described below when given an empty key.
//
// A Store implementation may optionally implement the blob.Closer interface.
// Clients of a Store should call blob.CloseStore on the store value when it is
// no longer in use.
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines.  Moreover, any sequence of operations on a Store that
// does not overlap with any Delete executions must be linearizable.[1]
//
// [1]: https://en.wikipedia.org/wiki/Linearizability
type Store interface {
	// Get fetches the contents of a blob from the store. If the key is not
	// found in the store, Get must report an ErrKeyNotFound error.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put writes a blob to the store. If the store already contains the
	// specified key and opts.Replace is true, the existing value is replaced
	// without error; otherwise Put must report an ErrKeyExists error.
	Put(ctx context.Context, opts PutOptions) error

	// Delete atomically removes a blob from the store. If the key is not found
	// in the store, Delete must report an ErrKeyNotFound error.
	Delete(ctx context.Context, key string) error

	// Size reports the size in bytes of the value stored for key. If the key is
	// not found in the store, Size must report an ErrKeyNotFound error.
	Size(ctx context.Context, key string) (int64, error)

	// List calls f with each key in the store in lexicographic order, beginning
	// with the first key greater than or equal to start.  If f reports an error
	// listing stops and List returns.  If f reported an ErrStopListing error,
	// List returns nil; otherwise List returns the error reported by f.
	List(ctx context.Context, start string, f func(string) error) error

	// Len reports the number of keys currently in the store.
	Len(ctx context.Context) (int64, error)
}

// Closer is an optional interface that a store may implement if it needs an
// opportunity to clean up or flush buffers before going out of service.
type Closer interface {
	Close(context.Context) error
}

// CloseStore closes s and reports any error that results. If s implements
// blob.Closer or io.Closer, its Close method is invoked; otherwise this is a
// no-op without error.
func CloseStore(ctx context.Context, s Store) error {
	switch t := s.(type) {
	case io.Closer:
		return t.Close()
	case Closer:
		return t.Close(ctx)
	default:
		return nil
	}
}

// PutOptions regulate the behaviour of the Put method of a Store
// implementation.
type PutOptions struct {
	Key     string // the key to associate with the data
	Data    []byte // the data to write
	Replace bool   // whether to replace an existing value for this key
}

var (
	// ErrKeyExists is reported by Put when writing a key that already exists in
	// the store.
	ErrKeyExists = errors.New("key already exists")

	// ErrKeyNotFound is reported by Get or Size when given a key that does not
	// exist in the store.
	ErrKeyNotFound = errors.New("key not found")

	// ErrStopListing is used by a List callback to terminate the listing.
	ErrStopListing = errors.New("stop listing keys")
)

// A CAS is a content-addressable wrapper that delegates to a blob.Store.  It
// adds a PutCAS method that writes blobs keyed by their content.
type CAS struct {
	Store

	newHash func() hash.Hash
}

// NewCAS constructs a CAS that delegates to s and uses h to assign keys.
func NewCAS(s Store, h func() hash.Hash) CAS { return CAS{Store: s, newHash: h} }

// PutCAS writes data to a content-addressed blob in the underlying store, and
// returns the assigned key. The target key is returned even in case of error.
func (c CAS) PutCAS(ctx context.Context, data []byte) (string, error) {
	key := c.Key(data)

	// Write the block to storage. Because we are using a content address we
	// do not request replacement, but we also don't consider it an error if
	// the address already exists.
	err := c.Put(ctx, PutOptions{
		Key:  key,
		Data: data,
	})
	if errors.Is(err, ErrKeyExists) {
		err = nil
	}
	return key, err
}

// Key constructs the content address for the specified data.
func (c CAS) Key(data []byte) string {
	h := c.newHash()
	h.Write(data)
	return string(h.Sum(nil))
}
