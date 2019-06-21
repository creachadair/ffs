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

// TODO: Move this to another package, its dependencies are gross.

// Package badgerstore implements the blob.Store interface using Badger.
package badgerstore

import (
	"context"

	"github.com/creachadair/ffs/blob"
	"github.com/dgraph-io/badger/v2"
)

// Store implements the blob.Store interface using a Badger key-value store.
type Store struct {
	db *badger.DB
}

// Opener constructs a filestore from an address comprising a path, for use
// with the store package.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	// TODO: Parse other options out of the address string somehow.
	opts := badger.DefaultOptions
	opts.Dir = addr
	opts.ValueDir = addr
	opts.Logger = nil
	return New(opts)
}

// New creates a Store by opening the Badger database specified by opts.
func New(opts badger.Options) (*Store, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close implements the io.Closer interface. It closes the underlying database
// instance and reports its result.
func (s *Store) Close() error { return s.db.Close() }

// Get implements part of blob.Store.
func (s *Store) Get(_ context.Context, key string) (data []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err == nil {
			data, err = itm.ValueCopy(data)
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = blob.ErrKeyNotFound
	}
	return
}

// Put implements part of blob.Store. A successful Put linearizes to the point
// at which the rename of the write temporary succeeds; a Put that fails due to
// an existing key linearizes to the point when the key path stat succeeds.
func (s *Store) Put(_ context.Context, opts blob.PutOptions) error {
	key := []byte(opts.Key)
	return s.db.Update(func(txn *badger.Txn) error {
		if !opts.Replace {
			_, err := txn.Get(key)
			if err == nil {
				return blob.ErrKeyExists
			} else if err != badger.ErrKeyNotFound {
				return err
			}
		}
		return txn.Set(key, opts.Data)
	})
}

// Size implements part of blob.Store.
func (s *Store) Size(_ context.Context, key string) (size int64, err error) {
	if key == "" {
		return 0, blob.ErrKeyNotFound // badger cannot store empty keys
	}
	err = s.db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte(key))
		if err == nil {
			size = itm.ValueSize()
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = blob.ErrKeyNotFound
	}
	return
}

// Delete implements part of blob.Store.
func (s *Store) Delete(_ context.Context, key string) error {
	if key == "" {
		return blob.ErrKeyNotFound // badger cannot store empty keys
	}
	return s.db.Update(func(txn *badger.Txn) error {
		key := []byte(key)
		_, err := txn.Get(key)
		if err == nil {
			return txn.Delete(key)
		} else if err == badger.ErrKeyNotFound {
			return blob.ErrKeyNotFound
		}
		return err
	})
}

// List implements part of blob.Store.
func (s *Store) List(_ context.Context, start string, f func(string) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		// N.B. We don't use the default here, which prefetches the values.
		it := txn.NewIterator(badger.IteratorOptions{})
		defer it.Close()

		for it.Seek([]byte(start)); it.Valid(); it.Next() {
			key := it.Item().Key()
			err := f(string(key))
			if err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
		return nil
	})
}

// Len implements part of blob.Store.
func (s *Store) Len(ctx context.Context) (int64, error) {
	var numKeys int64

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			numKeys++
		}
		return nil
	})
	return numKeys, err
}
