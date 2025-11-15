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
// of opaque (untyped) binary blobs.
//
// # Summary
//
// A [Store] represents a collection of disjoint named key-value namespaces
// backed by a shared pool of storage.  A store may further be partititioned
// into named "substores", each of which manages its own collection of
// keyspaces within its enclosing store. While stores and their keyspaces are
// logically distinct, they are intended to represent partitions of a single
// underlying storage layer.
//
// Keyspaces are either arbitrary ([KV]) or content-addressed ([CAS]).
// Both types implement the common [KVCore] interface.
// Arbitrary keyspaces allow writing of values under user-chosen keys ("Put"),
// while content-addressed keyspaces write values under their content address
// only ("CASPut"). An arbitrary keyspace can be converted into a content
// addressed keyspace using [CASFromKV].
//
// # Implementation Notes
//
// The [Store] and [KV] interfaces defined here are intended to be
// implementable on a variety of concrete substrates (files, databases,
// key-value stores) in a straightforward manner.  The API of these types is
// intended to support blobs of a "reasonable" size, where any individual blob
// can be efficiently processed in memory without streaming or chunking.
//
// While in principle blobs of arbitrary size may be stored, an implementation
// may reject "very large" blobs. Practically an implementation should try to
// accept blobs on the order of (up to) ~100MIB, but may reject blobs much
// larger than that.  This interface is intended to store data that is
// partitioned at a higher level in the protocol, and may not be a good fit for
// use cases that require large individual blobs.
//
// The [memstore] package provides an implementation suitable for use in
// testing. The [filestore] package provides an implementation that uses files
// and directories on a local filesystem. More interesting implementations
// using other storage libraries can be found in other repositories.
//
// [memstore]: https://godoc.org/github.com/creachadair/ffs/blob/memstore
// [filestore]: https://godoc.org/github.com/creachadair/ffs/storage/filestore
package blob

import (
	"context"
	"errors"
	"iter"

	"github.com/creachadair/mds/mapset"
	"golang.org/x/crypto/blake2b"
)

// A Store represents a collection of key-value namespaces ("keyspaces")
// identified by string labels. Each keyspace in a store is logically distinct;
// the keys from one space are independent of the keys in another.
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines.
//
// The KV and CAS methods share a namespace, meaning that a KV and a CAS
// derived from the same Store and using the same name must share the same
// underlying key-value space.  In particular a Put to a KV or a CASPut to a
// CAS must be visible to a Get or List from either.
type Store interface {
	// KV returns a key space on the store.
	//
	// Multiple calls to KV with the same name are not required to return
	// exactly the same KV value, but should return values that will converge
	// (eventually) to the same view of the storage.
	KV(ctx context.Context, name string) (KV, error)

	// CAS returns a content-addressed key space on the store.
	//
	// Multiple calls to CAS with the same name are not required to return
	// exactly the same CAS value, but should return values that will converge
	// (eventually) to the same view of the storage.
	//
	// Implementations of this method that do not require special handling are
	// encouraged to use CASFromKV to derive a CAS from a KV.
	CAS(ctx context.Context, name string) (CAS, error)

	// Sub returns a new Store subordinate to the receiver (a "substore").
	// A substore shares logical storage with its parent store, but keyspaces
	// derived from the substore are distinct from keyspaces of the parent store
	// or any other substores derived from it.
	//
	// Multiple calls to Sub with the same name are not required to return
	// exactly the same [Store] value, but should return values that will
	// converge (eventually) to the same view of the storage.
	Sub(ctx context.Context, name string) (Store, error)
}

// Closer is an extension interface representing the ability to close and
// release resources claimed by a storage component.
type Closer interface {
	Close(context.Context) error
}

// StoreCloser combines a [Store] with a Close method that settles state and
// releases any resources from the store when it is no longer in use.
type StoreCloser interface {
	Store
	Closer
}

// KVCore is the common interface shared by implementations of a key-value
// namespace. Users will generally not use this interface directly; it is
// included by reference in [KV] and [CAS].
type KVCore interface {
	// Get fetches the contents of a blob from the store. If the key is not
	// found in the store, Get must report an ErrKeyNotFound error.
	Get(ctx context.Context, key string) ([]byte, error)

	// Has reports which of the specified keys are present in the store.
	// The result set contains one entry for each requested key that is present
	// in the store. If none of the requested keys is present, the resulting set
	// may be either empty or nil.
	Has(ctx context.Context, keys ...string) (KeySet, error)

	// Delete atomically removes a blob from the store. If the key is not found
	// in the store, Delete must report an ErrKeyNotFound error.
	Delete(ctx context.Context, key string) error

	// List returns an iterator over each key in the store greater than or equal
	// to start, in lexicographic order.
	//
	// Requirements:
	//
	// Each pair reported by the iterator MUST be either a valid key and a nil
	// error, or an empty key and a non-nil error.
	//
	// After the iterator reports an error, it MUST immediately return, even if
	// the yield function reports true.
	//
	// The caller should check the error as part of iteration:
	//
	//  for key, err := range kv.List(ctx, start) {
	//     if err != nil {
	//        return fmt.Errorf("list: %w", err)
	//     }
	//     // ... process key
	//  }
	//
	// It must be safe to call Get, Has, List, and Len during iteration.
	// A caller should not attempt to modify the store while listing, unless the
	// storage implementation documents that it is safe to do so.
	List(ctx context.Context, start string) iter.Seq2[string, error]

	// Len reports the number of keys currently in the store.
	Len(ctx context.Context) (int64, error)
}

// A KV represents a mutable set of key-value pairs in which each value is
// identified by a unique, opaque string key.  An implementation of KV is
// permitted (but not required) to report an error from Put when given an empty
// key.  If the implementation cannot store empty keys, it must report
// [ErrKeyNotFound] when operating on an empty key (see [KeyNotFound]).
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines.  Moreover, any sequence of operations on a KV that does
// not overlap with any Delete executions must be [linearizable].
//
// [linearizable]: https://en.wikipedia.org/wiki/Linearizability
type KV interface {
	KVCore

	// Put writes a blob to the store. If the store already contains the
	// specified key and opts.Replace is true, the existing value is replaced
	// without error; otherwise Put must report an ErrKeyExists error without
	// modifying the previous value..
	Put(ctx context.Context, opts PutOptions) error
}

// CAS represents a mutable set of content-addressed key-value pairs in which
// each value is identified by a unique, opaque string key.
type CAS interface {
	KVCore

	// CASPut writes data to a content-addressed blob in the underlying store,
	// and returns the assigned key. The target key is returned even in case of
	// error.
	CASPut(ctx context.Context, data []byte) (string, error)

	// CASKey returns the content address of data without modifying the store.
	// This must be the same value that would be returned by a successful call
	// to CASPut on data.
	CASKey(ctx context.Context, data []byte) string
}

// PutOptions regulate the behaviour of the Put method of a [KV]
// implementation.
type PutOptions struct {
	Key     string // the key to associate with the data
	Data    []byte // the data to write
	Replace bool   // whether to replace an existing value for this key
}

// CASFromKV converts a [KV] into a [CAS]. This is intended for use by storage
// implementations to support the CAS method of the [Store] interface.
//
// If the concrete type of kv already implements [CAS], it is returned as-is;
// otherwise it is wrapped in an implementation that computes content addresses
// using a [blake2b] digest of the content.
//
// [blake2b]: https://datatracker.ietf.org/doc/html/rfc7693
func CASFromKV(kv KV) CAS {
	if cas, ok := kv.(CAS); ok {
		return cas
	}
	return hashCAS{kv}
}

// CASFromKVError converts a [KV] into a [CAS]. This is a convenience wrapper
// to combine an error check with a call to [CASFromKV] for use in storage
// implementations.
func CASFromKVError(kv KV, err error) (CAS, error) {
	if err != nil {
		return nil, err
	}
	return CASFromKV(kv), nil
}

var (
	// ErrKeyExists is reported by Put when writing a key that already exists in
	// the store.
	ErrKeyExists = errors.New("key already exists")

	// ErrKeyNotFound is reported by Get or Size when given a key that does not
	// exist in the store.
	ErrKeyNotFound = errors.New("key not found")
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
// The caller may type-assert to [*KeyError] to recover the key.
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
// The concrete type is [*KeyError].
func KeyNotFound(key string) error { return &KeyError{Key: key, Err: ErrKeyNotFound} }

// KeyExists returns an ErrKeyExists error reporting that key exists in the store.
// The concrete type is [*KeyError].
func KeyExists(key string) error { return &KeyError{Key: key, Err: ErrKeyExists} }

// KeySet represents a set of keys. It is aliased here so the caller does not
// need to explicitly import [mapset].
type KeySet = mapset.Set[string]

// A HashCAS is a content-addressable wrapper that adds the CAS methods to a
// delegated [KV].
type hashCAS struct{ KV }

// hash is the digest function used to compute content addresses for hashCAS.
var hash = blake2b.Sum256

// key computes the content key for data using the provided hash.
func (c hashCAS) key(data []byte) string {
	h := hash(data)
	return string(h[:])
}

// CASPut writes data to a content-addressed blob in the underlying store, and
// returns the assigned key. The target key is returned even in case of error.
func (c hashCAS) CASPut(ctx context.Context, data []byte) (string, error) {
	key := c.key(data)

	// Skip writing if the content address is already present.
	if st, err := c.Has(ctx, key); err == nil && st.Has(key) {
		return key, nil
	}

	// Write the block to storage. Because we are using a content address we
	// do not request replacement, but we also don't consider it an error if
	// the address already exists.
	err := c.Put(ctx, PutOptions{
		Key:     key,
		Data:    data,
		Replace: false,
	})
	if IsKeyExists(err) {
		err = nil
	}
	return key, err
}

// CASKey constructs the content address for the specified data.
func (c hashCAS) CASKey(_ context.Context, data []byte) string { return c.key(data) }

// SyncKeys reports which of the given keys are not present in the key space.
// If all the keys are present, SyncKeys returns an empty [KeySet].
func SyncKeys(ctx context.Context, ks KVCore, keys []string) (KeySet, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	have, err := ks.Has(ctx, keys...)
	if err != nil {
		return nil, err
	}
	var missing KeySet
	for _, key := range keys {
		if !have.Has(key) {
			missing.Add(key)
		}
	}
	return missing, nil
}
