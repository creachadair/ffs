// Package blob implements an interface and support code for persistent storage
// of untyped binary blobs.
package blob

import (
	"context"

	"golang.org/x/xerrors"
)

// A Store represents a mutable blob store in which each blob is identified by
// a unique, opaque string key. Implementations of this interface must be safe
// for concurrent use by multiple goroutines, and all operations must be atomic
// with respect to concurrent writers.
type Store interface {
	// Get fetches the contents of a blob from the store. If the key is not
	// found in the store, Get must report an ErrKeyNotFound error.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put writes a blob to the store. If the store already contains the
	// specified key and opts.Replace is true, the existing value is replaced
	// without error; otherwise Put must report an ErrKeyExists error.
	Put(ctx context.Context, opts PutOptions) error

	// Size reports the size in bytes of the value stored for key. If the key is
	// not found in the store, Size must report an ErrKeyNotFound error.
	Size(ctx context.Context, key string) (int64, error)

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
	ErrKeyExists = xerrors.New("key already exists")

	// ErrKeyNotFound is reported by Get or Size when given a key that does not
	// exist in the store.
	ErrKeyNotFound = xerrors.New("key not found")

	// ErrStopListing is used by a List callback to terminate the listing.
	ErrStopListing = xerrors.New("stop listing keys")
)
