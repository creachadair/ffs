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
	"slices"
	"sort"
)

// A Store represents a collection of key-value namespaces ("keyspaces")
// identified by string labels. Each keyspace in a store is logically distinct;
// the keys from one space are independent of the keys in another.
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines. When constructing a new store, the caller should
// arrange to call [blob.Close] on it when it is no longer in use, to give the
// implementation an opportunity to clean up any internal state.
type Store interface {
	// Keyspace returns a key space on the store.
	Keyspace(name string) (KV, error)

	// Sub returns a new Store subordinate to the receiver.  A substore shares
	// logical storage with its parent store, but keyspaces derived from the
	// substore are distinct from keyspaces of the parent store or any other
	// substores derived from it.
	Sub(name string) Store
}

// A KV represents a mutable set of key-value pairs in which each value is
// identified by a unique, opaque string key.  An implementation of KV is
// permitted (but not required) to report an error from Put when given an empty
// key.  If the implementation cannot store empty keys, it must report
// ErrKeyNotFound when operating on an empty key.
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines.  Moreover, any sequence of operations on a KV that does
// not overlap with any Delete executions must be linearizable.[1]
//
// When constructing a new KV, the caller should arrange to call [blob.Close]
// on it when it is no longer in use, to give the implementation an opportunity
// to clean up any internal state.
//
// [1]: https://en.wikipedia.org/wiki/Linearizability
type KV interface {
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

	// List calls f with each key in the store in lexicographic order, beginning
	// with the first key greater than or equal to start.  If f reports an error
	// listing stops and List returns.  If f reported an ErrStopListing error,
	// List returns nil; otherwise List returns the error reported by f.
	List(ctx context.Context, start string, f func(string) error) error

	// Len reports the number of keys currently in the store.
	Len(ctx context.Context) (int64, error)
}

// Close closes the specified value. If the concrete type of the implementation
// includes [io.Closer] or exports a Close method with the signature
//
//	Close(context.Context) error
//
// then Close invokes it and returns the error it reports. Otherwise, Close
// returns nil.
func Close(ctx context.Context, s any) error {
	switch t := s.(type) {
	case interface{ Close(context.Context) error }:
		return t.Close(ctx)
	case interface{ Close() error }:
		return t.Close()
	default:
		return nil
	}
}

// PutOptions regulate the behaviour of the Put method of a [KV]
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
	KV

	// CASPut writes data to a content-addressed blob in the underlying store,
	// and returns the assigned key. The target key is returned even in case of
	// error.
	CASPut(ctx context.Context, data CASPutOptions) (string, error)

	// CASKey returns the content address of data without modifying the store.
	// This must be the same value that would be returned by a successful call
	// to CASPut on data.
	CASKey(ctx context.Context, opts CASPutOptions) (string, error)
}

// CASPutOptions are the arguments to the CASPut and CASKey methods of a CAS
// implementation.
type CASPutOptions struct {
	Data           []byte // the data to be stored
	Prefix, Suffix string // a prefix and suffix to add to the computed key
}

// A HashCAS is a content-addressable wrapper that adds the CAS methods to a
// delegated [KV].
type HashCAS struct {
	KV

	newHash func() hash.Hash
}

// NewCAS constructs a HashCAS that delegates to s and uses h to assign keys.
func NewCAS(kv KV, h func() hash.Hash) HashCAS { return HashCAS{KV: kv, newHash: h} }

// key computes the content key for data using the provided hash.
func (c HashCAS) key(opts CASPutOptions) string {
	h := c.newHash()
	h.Write(opts.Data)
	hash := string(h.Sum(nil))
	return opts.Prefix + hash + opts.Suffix
}

// CASPut writes data to a content-addressed blob in the underlying store, and
// returns the assigned key. The target key is returned even in case of error.
func (c HashCAS) CASPut(ctx context.Context, opts CASPutOptions) (string, error) {
	key := c.key(opts)

	// Write the block to storage. Because we are using a content address we
	// do not request replacement, but we also don't consider it an error if
	// the address already exists.
	err := c.Put(ctx, PutOptions{
		Key:  key,
		Data: opts.Data,
	})
	if IsKeyExists(err) {
		err = nil
	}
	return key, err
}

// CASKey constructs the content address for the specified data.
// This implementation never reports an error.
func (c HashCAS) CASKey(_ context.Context, opts CASPutOptions) (string, error) {
	return c.key(opts), nil
}

// SyncKeyer is an optional interface that a store may implement to support
// checking for the presence of keys in the store without fetching them.
type SyncKeyer interface {
	KV

	// SyncKeys reports which of the given keys are not present in the store.
	// If all the keys are present, SyncKeys returns an empty slice or nil.
	// The order of returned keys is unspecified.
	SyncKeys(ctx context.Context, keys []string) ([]string, error)
}

// ListSyncKeyer is a wrapper that adds the SyncKeys method to a delegated
// [KV], using its List method.
type ListSyncKeyer struct {
	KV
}

// SyncKeys implements the SyncKeyer interface using the List method of the
// underlying store. Keys are returned in lexicographic order.
func (s ListSyncKeyer) SyncKeys(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	cp := slices.Clone(keys)
	sort.Strings(cp)

	var missing []string
	i := 0
	if err := s.List(ctx, cp[0], func(got string) error {
		// The order of these checks matters. If got is bigger than the current
		// key, it is possible it may be equal a later one.
		for i < len(cp) && got > cp[i] {
			missing = append(missing, cp[i])
			i++
		}

		// Reaching here, either there are no more keys left or got <= cp[i].
		if i < len(cp) && got == cp[i] {
			i++
		}

		if i >= len(cp) {
			return ErrStopListing
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return append(missing, cp[i:]...), nil
}
