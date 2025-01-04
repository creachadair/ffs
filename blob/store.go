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

	"github.com/creachadair/mds/mapset"
	"golang.org/x/crypto/sha3"
)

// A Store represents a collection of key-value namespaces ("keyspaces")
// identified by string labels. Each keyspace in a store is logically distinct;
// the keys from one space are independent of the keys in another.
//
// Implementations of this interface must be safe for concurrent use by
// multiple goroutines.
//
// The [Store.KV] and [Store.CAS] methods share a namespace, meaning that a KV
// and a CAS on the same name must share the same underlying key-value space.
// In particular a Put to a KV or a CASPut (from a CAS) must be visible to a
// Get or List from either, if both were made from the same Store with the same
// name.
type Store interface {
	// KV returns a key space on the store.
	//
	// Multiple calls to KV with the same name are not required to return
	// exactly the same [KV] value, but should return values that will converge
	// (eventually) to the same view of the storage.
	KV(ctx context.Context, name string) (KV, error)

	// CAS returns a content-addressed key space on the store.
	//
	// Multiple calls to CAS with the same name are not required to return
	// exactly the same [KV] value, but should return values that will converge
	// (eventually) to the same view of the storage.
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

	// List calls f with each key in the store in lexicographic order, beginning
	// with the first key greater than or equal to start.  If f reports an error
	// listing stops and List returns.  If f reported an ErrStopListing error,
	// List returns nil; otherwise List returns the error reported by f.
	List(ctx context.Context, start string, f func(string) error) error

	// Len reports the number of keys currently in the store.
	Len(ctx context.Context) (int64, error)
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
// [1]: https://en.wikipedia.org/wiki/Linearizability
type KV interface {
	KVCore

	// Put writes a blob to the store. If the store already contains the
	// specified key and opts.Replace is true, the existing value is replaced
	// without error; otherwise Put must report an ErrKeyExists error.
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

// KeySet represents a set of keys. It is aliased here so the caller does not
// need to explicitly import [mapset].
type KeySet = mapset.Set[string]

// A HashCAS is a content-addressable wrapper that adds the CAS methods to a
// delegated [KV].
type hashCAS struct{ KV }

// hash is the digest function used to compute content addresses for hashCAS.
var hash = sha3.Sum256

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
// If all the keys are present, SyncKeys returns an empty slice or nil.  The
// order of returned keys is unspecified.
func SyncKeys(ctx context.Context, ks KVCore, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	have, err := ks.Has(ctx, keys...)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, key := range keys {
		if !have.Has(key) {
			missing = append(missing, key)
			have.Add(key) // filter duplicates
		}
	}
	return missing, nil
}
