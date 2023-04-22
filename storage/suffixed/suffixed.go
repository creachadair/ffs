// Copyright 2023 Michael J. Fromberger. All Rights Reserved.
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

// Package suffixed implements a blob.Store interface that delegates to another
// Store, with keys namespaced by a fixed suffix concatenated with each key.
package suffixed

import (
	"context"
	"strings"

	"github.com/creachadair/ffs/blob"
)

// Store implements the blob.Store interface by delegating to an underlying
// store, but with each key suffixed by a fixed non-empty string. This allows
// multiple consumers to share non-overlapping namespaces within a single
// storage backend.
type Store struct {
	real   blob.Store
	suffix string
}

// New creates a Store associated with the specified s. The initial store is
// exactly equivalent to the underlying store; use Derive to create clones that
// use a different suffix.
//
// Suffixes do not nest: If s is already a suffixed.Store, it is returned as-is.
func New(s blob.Store) Store {
	if p, ok := s.(Store); ok {
		return p
	}
	return Store{real: s}
}

// Base returns the underlying store associated with s.
func (s Store) Base() blob.Store { return s.real }

// Derive creates a clone of s that delegates to the same underlying store, but
// using a different suffix. If suffix == "", Derive returns a store that is
// equivalent to the underlying base store.
func (s Store) Derive(suffix string) Store {
	return Store{real: s.real, suffix: suffix}
}

// Suffix returns the key suffix associated with s.
func (s Store) Suffix() string { return s.suffix }

func (s Store) wrapKey(key string) string { return key + s.suffix }

func (s Store) unwrapKey(key string) string { return strings.TrimSuffix(key, s.suffix) }

// Close implements the optional blob.Closer interface. It delegates to the
// underlying store if possible.
func (s Store) Close(ctx context.Context) error {
	return s.real.Close(ctx)
}

// Get implements part of blob.Store by delegation.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.real.Get(ctx, s.wrapKey(key))
}

// Put implements part of blob.Store by delegation.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	// Leave the options as-given, except the key must be wrapped.
	opts.Key = s.wrapKey(opts.Key)
	return s.real.Put(ctx, opts)
}

// Delete implements part of blob.Store by delegation.
func (s Store) Delete(ctx context.Context, key string) error {
	return s.real.Delete(ctx, s.wrapKey(key))
}

// List implements part of blob.Store by delegation. It filters the underlying
// list results to include only keys suffixed for this store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	return s.real.List(ctx, start, func(wrappedKey string) error {
		if strings.HasSuffix(wrappedKey, s.suffix) {
			return f(s.unwrapKey(wrappedKey))
		}
		return nil
	})
}

// Len implements part of blob.Store by delegation. It reports only the number
// of keys matching the current suffix.
func (s Store) Len(ctx context.Context) (int64, error) {
	// If the suffix is empty, we can delegate directly to the base.
	if s.suffix == "" {
		return s.real.Len(ctx)
	}

	// Otherwise, we have to iterate.
	var nk int64
	err := s.real.List(ctx, "", func(cur string) error {
		if strings.HasSuffix(cur, s.suffix) {
			nk++
		}
		return nil
	})
	return nk, err
}

// CAS implements a suffixed wrapper around a blob.CAS instance.
type CAS struct {
	Store
	cas blob.CAS
}

// NewCAS creates a new suffixed Store associated with the specified cas.
// Suffixes do not nest: If cas is already a suffixed.CAS, it is returned
// as-is.
func NewCAS(cas blob.CAS) CAS {
	if p, ok := cas.(CAS); ok {
		return p
	}
	return CAS{Store: New(cas), cas: cas}
}

// Base returns the underlying store associated with c.
func (c CAS) Base() blob.CAS { return c.cas }

// Derive creates a clone of c that delegates to the same underlying store, but
// using a different suffix. If suffix == "", Derive returns a CAS that is
// equivalent to the underlying base store.
func (c CAS) Derive(suffix string) CAS {
	return CAS{Store: c.Store.Derive(suffix), cas: c.cas}
}

func (c CAS) setOptions(opts blob.CASPutOptions) blob.CASPutOptions {
	return blob.CASPutOptions{
		Data:   opts.Data,
		Prefix: opts.Prefix,
		Suffix: c.Store.suffix + opts.Suffix,
	}
}

// CASPut implements part of the blob.CAS interface.
func (c CAS) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	key, err := c.cas.CASPut(ctx, c.setOptions(opts))
	return strings.TrimSuffix(key, c.Store.suffix), err
}

// CASKey implements part of the blob.CAS interface.
func (c CAS) CASKey(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	key, err := c.cas.CASKey(ctx, c.setOptions(opts))
	return strings.TrimSuffix(key, c.Store.suffix), err
}
