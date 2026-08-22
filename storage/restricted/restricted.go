// Copyright 2026 Michael J. Fromberger. All Rights Reserved.
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

// Package restricted provides a [blob.StoreCloser] that delegates to an
// underlying storage implementation with a [Policy] check on access to
// substores and keyspaces defined in that store.
package restricted

import (
	"context"
	"errors"
	"slices"

	"github.com/creachadair/ffs/blob"
)

// ErrNoAccess is a sentinel error reported when access to a substore or
// keyspace is rejected by policy.
var ErrNoAccess = errors.New("access denied")

// AllowAll is a [Policy] that allows access to all substores and all keyspaces.
var AllowAll allowAllPolicy

type allowAllPolicy struct{}

func (allowAllPolicy) CheckSub(ctx context.Context, path []string, name string) error { return nil }
func (allowAllPolicy) CheckKV(ctx context.Context, path []string, name string) error  { return nil }

// A Policy defines access rules for substores and keyspaces.
type Policy interface {
	// CheckSub checks whether a substore with the given name is permitted
	// below the given path.
	//
	// An empty path denotes the root store; otherwise it contains the sequence
	// of names leading to the store where the name is being accessed.
	// If CheckSub reports an error, the substore will not be accessed.
	CheckSub(ctx context.Context, path []string, name string) error

	// CheckKV checks whether a keyspace (KV or CAS) with the given name is
	// permitted below the given path.
	//
	// An empty path denotes the root store; otherwise it contains the sequence
	// of names leading to the store where the name is being accessed.
	// If CheckKV reports an error, the keyspace will not be accessed.
	CheckKV(ctx context.Context, path []string, name string) error
}

// NewStore constructs a new restricted store using the given base store and
// policy implementation.
//
// Both base and policy must be non-nil, or NewStore will panic.
func NewStore(base blob.StoreCloser, policy Policy) Store {
	if base == nil || policy == nil {
		panic("base store and policy must be non-nil")
	}
	return Store{base: base, closer: base, policy: policy}
}

// Store implements [blob.StoreCloser].
type Store struct {
	policy Policy
	base   blob.Store
	path   []string
	closer blob.Closer // non-nil at the top level only
}

// Sub implements part of [blob.Store]. It reports [ErrNoAccess] if the
// specified substore name is disallowed by policy.
func (s Store) Sub(ctx context.Context, name string) (blob.Store, error) {
	if err := s.policy.CheckSub(ctx, s.path, name); err != nil {
		return nil, wrapPolicyError(err)
	}
	sub, err := s.base.Sub(ctx, name)
	if err != nil {
		return nil, err
	}
	return Store{
		policy: s.policy,
		path:   append(slices.Clip(s.path), name),
		base:   sub,
		// no closer
	}, nil
}

// KV implements part of [blob.Store]. It reports [ErrNoAccess] if the
// specified keyspace name is disallowed by policy.
func (s Store) KV(ctx context.Context, name string) (blob.KV, error) {
	if err := s.policy.CheckKV(ctx, s.path, name); err != nil {
		return nil, wrapPolicyError(err)
	}
	return s.base.KV(ctx, name)
}

// CAS implements part of [blob.Store]. It reports [ErrNoAccess] if the
// specified keyspace name is disallowed by policy.
func (s Store) CAS(ctx context.Context, name string) (blob.CAS, error) {
	if err := s.policy.CheckKV(ctx, s.path, name); err != nil {
		return nil, wrapPolicyError(err)
	}
	return s.base.CAS(ctx, name)
}

// Close implements part of [blob.StoreCloser].
func (s Store) Close(ctx context.Context) error {
	if s.closer != nil {
		return s.closer.Close(ctx)
	}
	return nil
}

func wrapPolicyError(err error) error {
	if errors.Is(err, ErrNoAccess) {
		return err
	}
	return errors.Join(err, ErrNoAccess)
}
