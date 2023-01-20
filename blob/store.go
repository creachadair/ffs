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
)

// A Store represents a mutable blob store in which each blob is identified by
// a unique, opaque string key.  An implementation of Store is permitted (but
// not required) to report an error from Put when given an empty key.  If the
// implementation cannot store empty keys, it must report ErrKeyNotFound when
// operating on an empty key.
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

	// Close allows the store to release any resources held open while in use.
	// If an implementation has nothing to release, it must return nil.
	Close(context.Context) error
}

// CloseStore closes s and reports any error that results. If s implements
// blob.Closer or io.Closer, its Close method is invoked; otherwise this is a
// no-op without error.
//
// Deprecated: Use the Close method of the Store interface directly.
func CloseStore(ctx context.Context, s Store) error { return s.Close(ctx) }

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

// IsKeyNotFound reports whether err or is or wraps ErrKeyNotFound.
// It is false if err == nil.
func IsKeyNotFound(err error) bool {
	return err != nil && errors.Is(err, ErrKeyNotFound)
}

// IsKeyExists reports whether err is or wraps ErrKeyExists.
func IsKeyExists(err error) bool {
	return err != nil && errors.Is(err, ErrKeyExists)
}

// KeyError is the concrete type of errors involving a blob key.
// The caller may type-assert to *blob.KeyError to recover the key.
type KeyError struct {
	Err error  // the underlying error
	Key string // the key implicated by the error
}

// Error implements the error interface for KeyError.
// The default error string does not include the key, since error values are
// often logged by default and keys may be sensitive.
func (k *KeyError) Error() string { return k.Err.Error() }

// Unwrap returns the underlying error from k, to support error wrapping.
func (k *KeyError) Unwrap() error { return k.Err }

// KeyNotFound returns an ErrKeyNotFound error reporting that key was not found.
// The concrete type is *blob.KeyError.
func KeyNotFound(key string) error { return &KeyError{Key: key, Err: ErrKeyNotFound} }

// KeyExists returns an ErrKeyExists error reporting that key exists in the store.
// The concrete type is *blob.KeyError.
func KeyExists(key string) error { return &KeyError{Key: key, Err: ErrKeyExists} }

// CAS is an optional interface that a store may implement to support content
// addressing using a one-way hash.
type CAS interface {
	Store

	// CASPut writes data to a content-addreessed blob in the underlying store,
	// and returns the assigned key. The target key is returned even in case of
	// error.
	CASPut(ctx context.Context, data []byte) (string, error)

	// CASKey returns the content address of data without modifying the store.
	// This must be the same value that would be returned by a successful call
	// to CASPut on data.
	CASKey(ctx context.Context, data []byte) (string, error)
}

// A HashCAS is a content-addressable wrapper that adds the CAS methods to a
// delegated blob.Store.
type HashCAS struct {
	Store

	newHash func() hash.Hash
}

// NewCAS constructs a HashCAS that delegates to s and uses h to assign keys.
func NewCAS(s Store, h func() hash.Hash) HashCAS { return HashCAS{Store: s, newHash: h} }

// key computes the content key for data using the provided hash.
func (c HashCAS) key(data []byte) string {
	h := c.newHash()
	h.Write(data)
	return string(h.Sum(nil))
}

// CASPut writes data to a content-addressed blob in the underlying store, and
// returns the assigned key. The target key is returned even in case of error.
func (c HashCAS) CASPut(ctx context.Context, data []byte) (string, error) {
	key := c.key(data)

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

// CASKey constructs the content address for the specified data.
// This implementation never reports an error.
func (c HashCAS) CASKey(_ context.Context, data []byte) (string, error) {
	return c.key(data), nil
}
