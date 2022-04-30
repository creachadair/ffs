// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

// Package prefixed implements a blob.Store interface that delegates to another
// Store, with keys namespaced by a fixed prefix concatenated with each key.
package prefixed

import (
	"context"
	"strings"

	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface by delegating to an underlying
// store, but with each key prefixed by a fixed non-empty string. This allows
// multiple consumers to share non-overlapping namespaces within a single
// storage backend.
type Store struct {
	real   blob.Store
	prefix string
}

// New creates a Store associated with the specified store. The initial store
// is exactly equivalent to the underlying store; use Derive to create clones
// that use a different prefix.
func New(store blob.Store) *Store { return &Store{real: store} }

// Derive creates a clone of s that delegates to the same underlying store, but
// using a different prefix. If prefix == "", Derive returns s unchanged.
func (s *Store) Derive(prefix string) *Store {
	if prefix == "" {
		return s
	}
	return &Store{real: s.real, prefix: prefix}
}

// Prefix returns the key prefix associated with s.
func (s *Store) Prefix() string { return s.prefix }

func (s *Store) wrapKey(key string) string { return s.prefix + key }

func (s *Store) unwrapKey(key string) string { return strings.TrimPrefix(key, s.prefix) }

// Get implements part of blob.Store by delegation.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.real.Get(ctx, s.wrapKey(key))
}

// Put implements part of blob.Store by delegation.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	// Leave the options as-given, except the key must be wrapped.
	opts.Key = s.wrapKey(opts.Key)
	return s.real.Put(ctx, opts)
}

// Size implements part of blob.Store by delegation.
func (s *Store) Size(ctx context.Context, key string) (int64, error) {
	return s.real.Size(ctx, s.wrapKey(key))
}

// Delete implements part of blob.Store by delegation.
func (s *Store) Delete(ctx context.Context, key string) error {
	return s.real.Delete(ctx, s.wrapKey(key))
}

// List implements part of blob.Store by delegation. It filters the underlying
// list results to include only keys prefixed for this store.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, s.wrapKey(start), func(wrappedKey string) error {
		// Since keys are listed lexicographically, all the keys starting with
		// our prefix should be grouped together. Thus, once we find any key that
		// does NOT have our prefix, we can stop iterating.
		if !strings.HasPrefix(wrappedKey, s.prefix) {
			return blob.ErrStopListing
		}
		return f(s.unwrapKey(wrappedKey))
	})
}

// Len implements part of blob.Store by delegation. It reports the total number
// of keys in the underlying store, not only those with the chosen prefix.
func (s *Store) Len(ctx context.Context) (int64, error) { return s.real.Len(ctx) }
