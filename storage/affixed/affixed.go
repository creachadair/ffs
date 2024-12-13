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

// Package affixed implements a blob.Store interface that delegates to another
// Store, with keys namespaced by a fixed prefix and/or suffix concatenated
// with each key.
package affixed

import (
	"context"
	"strings"

	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface by delegating to an underlying
// store, but with each key prefixed and/or suffixed by fixed non-empty
// strings.  This allows multiple consumers to share non-overlapping namespaces
// within a single storage backend.
type Store struct {
	real   blob.Store
	prefix string
	suffix string
}

// New creates a Store associated with the specified s. The initial store is
// exactly equivalent to the underlying store; use Derive to create clones that
// use a different prefix/suffix.
//
// Affixes do not nest: If s is already an affixed.Store, it is returned as-is.
func New(s blob.Store) Store {
	if p, ok := s.(Store); ok {
		return p
	}
	return Store{real: s}
}

// Base returns the underlying store associated with s.
func (s Store) Base() blob.Store { return s.real }

// WithPrefix creates a clone of s that delegates to the same underlying store,
// but using a different prefix. The suffix, if any, is unchanged.
func (s Store) WithPrefix(prefix string) Store {
	return Store{real: s.real, prefix: prefix, suffix: s.suffix}
}

// WithSuffix creates a clone of s that delegates to the same underlying store,
// but using a different suffix. The prefix, if any, is unchanged.
func (s Store) WithSuffix(suffix string) Store {
	return Store{real: s.real, prefix: s.prefix, suffix: suffix}
}

// Derive creates a clone of s that delegates to the same underlying store, but
// using a different prefix and suffix. If prefix == suffix == "", Derive
// returns a store that is equivalent to the original base store.
func (s Store) Derive(prefix, suffix string) Store {
	return Store{real: s.real, prefix: prefix, suffix: suffix}
}

// Prefix returns the key prefix associated with s.
func (s Store) Prefix() string { return s.prefix }

// Suffix returns the key suffix associated with s.
func (s Store) Suffix() string { return s.suffix }

// WrapKey returns the wrapped version of key as it would be stored into the
// base store with the current prefix and suffix attached.
func (s Store) WrapKey(key string) string { return s.prefix + key + s.suffix }

// UnwrapKey returns the unwrapped version of key with the current prefix and
// suffix removed (if present).
func (s Store) UnwrapKey(key string) string {
	p := strings.TrimPrefix(key, s.prefix)
	return strings.TrimSuffix(p, s.suffix)
}

// Close implements the optional blob.Closer interface. It delegates to the
// underlying store if possible.
func (s Store) Close(ctx context.Context) error { return s.real.Close(ctx) }

// Get implements part of blob.Store by delegation.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.real.Get(ctx, s.WrapKey(key))
}

// Put implements part of blob.Store by delegation.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	// Leave the options as-given, except the key must be wrapped.
	opts.Key = s.WrapKey(opts.Key)
	return s.real.Put(ctx, opts)
}

// Delete implements part of blob.Store by delegation.
func (s Store) Delete(ctx context.Context, key string) error {
	return s.real.Delete(ctx, s.WrapKey(key))
}

// List implements part of blob.Store by delegation. It filters the underlying
// list results to include only keys prefixed/suffixed for this store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	// If we have no affixes, we do not need to filter.
	if s.prefix == "" && s.suffix == "" {
		return s.real.List(ctx, start, f)
	}

	// For the starting key, don't include the suffix, since the first match
	// might not actually be there.
	return s.real.List(ctx, s.prefix+start, func(wrappedKey string) error {
		// Since keys are listed lexicographically, all the keys starting with
		// our prefix should be grouped together. Thus, once we find any key that
		// does NOT have our prefix, we can stop iterating.
		if !strings.HasPrefix(wrappedKey, s.prefix) {
			return blob.ErrStopListing
		} else if !strings.HasSuffix(wrappedKey, s.suffix) {
			return nil // not my key
		}
		return f(s.UnwrapKey(wrappedKey))
	})
}

// Len implements part of blob.Store by delegation. It reports only the number
// of keys matching the current prefix.
func (s Store) Len(ctx context.Context) (int64, error) {
	// If the prefix and suffix are empty, we can delegate directly to the base.
	if s.prefix == "" && s.suffix == "" {
		return s.real.Len(ctx)
	}

	// Otherwise, we have to iterate.
	var nk int64
	err := s.real.List(ctx, s.prefix, func(cur string) error {
		if !strings.HasPrefix(cur, s.prefix) {
			return blob.ErrStopListing
		} else if !strings.HasSuffix(cur, s.suffix) {
			return nil // not my key
		}
		nk++
		return nil
	})
	return nk, err
}

// CAS implements an affixed wrapper around a blob.CAS instance.
// The resulting store adds designated prefix/suffix strings to the keys
// delegated to its care.
type CAS struct {
	Store
	cas blob.CAS
}

// NewCAS creates a new affixed Store associated with the specified cas.
// Affixes do not nest: If cas is already an affixed.CAS, it is returned as.is.
func NewCAS(cas blob.CAS) CAS {
	if p, ok := cas.(CAS); ok {
		return p
	}
	return CAS{Store: New(cas), cas: cas}
}

// Base returns the underlying store associated with c.
func (c CAS) Base() blob.CAS { return c.cas }

// WithPrefix creates a clone of c that delegates to the same underlying store,
// but using a different prefix. The suffix, if any, is unchanged.
func (c CAS) WithPrefix(prefix string) CAS {
	return CAS{Store: c.Store.WithPrefix(prefix), cas: c.cas}
}

// WithSuffix creates a clone of c that delegates to the same underlying store,
// but using a different suffix. The prefix, if any, is unchanged.
func (c CAS) WithSuffix(suffix string) CAS {
	return CAS{Store: c.Store.WithSuffix(suffix), cas: c.cas}
}

// Derive creates a clone of c that delegates to the same underlying store, but
// using a different prefix and suffix. If prefix == suffix == "", Derive
// returns a store that is equivalent to the original base store.
func (c CAS) Derive(prefix, suffix string) CAS {
	return CAS{Store: c.Store.Derive(prefix, suffix), cas: c.cas}
}

func (c CAS) setOptions(opts blob.CASPutOptions) blob.CASPutOptions {
	return blob.CASPutOptions{
		Data:   opts.Data,
		Prefix: opts.Prefix + c.Store.prefix,
		Suffix: c.Store.suffix + opts.Suffix,
	}
}

// CASPut implements part of the blob.CAS interface.
func (c CAS) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	key, err := c.cas.CASPut(ctx, c.setOptions(opts))
	return c.UnwrapKey(key), err
}

// CASKey implements part of the blob.CAS interface.
func (c CAS) CASKey(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	key, err := c.cas.CASKey(ctx, c.setOptions(opts))
	return c.UnwrapKey(key), err
}
