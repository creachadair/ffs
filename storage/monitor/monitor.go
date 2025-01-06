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

// Package monitor implements common plumbing for implementations of the
// [blob.Store] interface based on storage with a flat key space.
//
// # Overview
//
// The [M] type implements shared plumbing for the methods of a [blob.Store].
// It is intended to be embedded in another type to provide the required
// methods of the interface. The monitor is parameterized by a storage handle
// (DB) and an implementation of the [blob.KV] interface using that storage.
//
// Keyspaces managed by the monitor are partitioned by adding a key prefix.
// The prefix is derived by hashing the path of (sub)space and keyspace names
// from the root of the store (see [dbkey]). This ensures the prefix for a
// given path is stable without explicitly persisting the mapping of names.
//
// To construct an [M], the caller must provide, at minimum:
//
//   - A storage handle (typically a database or storage client).
//   - An implementation of the [blob.KV] interface based on that storage.
//   - A constructor to create new instances of that KV implementation.
//
// The caller may also optionally provide a constructor to derive new substore
// instances. This is not necessary unless the storage handle requires state to
// track its own subspace organization, and is typically omitted.
package monitor

import (
	"context"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
)

// Config carries settings for construction of an [M].
// At minimum, the DB and NewKV fields must be populated.
type Config[DB any, KV blob.KV] struct {
	// DB represents the initial state of the monitor.
	// It must be safe to copy DB, so if the state contains locks or other
	// values that cannot be safely copied, use a pointer type.
	DB DB

	// Prefix gives the initial storage prefix of the root.  Empty is a valid,
	// safe default.
	Prefix dbkey.Prefix

	// NewKV construts a KV instance from the current state, where db is the
	// store state, pfx the derived prefix for the new KV, and name is the name
	// passed to the KV call.
	NewKV func(ctx context.Context, db DB, pfx dbkey.Prefix, name string) (KV, error)

	// NewSub constructs a sub-DB state from the current state, where db is the
	// store state, pfx the derived prefix for the new subspace, and name is the
	// name passed to the Sub call.  If NewSub is nil, the existing state is
	// copied without change.
	NewSub func(ctx context.Context, db DB, pfx dbkey.Prefix, name string) (DB, error)
}

// A M value manages keyspace and substore allocations for the specified
// database and KV implementations. The resulting value implements [blob.Store].
type M[DB any, KV blob.KV] struct {
	DB     DB
	prefix dbkey.Prefix
	newKV  func(context.Context, DB, dbkey.Prefix, string) (KV, error)
	newSub func(context.Context, DB, dbkey.Prefix, string) (DB, error)

	μ    sync.Mutex
	subs map[string]*M[DB, KV]
	kvs  map[string]KV
}

// New constructs a new empty store using the specified database, prefix, and
// KV constructor function. New will panic if cfg.NewKV is nil.
func New[DB any, KV blob.KV](cfg Config[DB, KV]) *M[DB, KV] {
	if cfg.NewKV == nil {
		panic("KV constructor is nil")
	}
	if cfg.NewSub == nil {
		cfg.NewSub = func(_ context.Context, old DB, _ dbkey.Prefix, _ string) (DB, error) {
			return old, nil
		}
	}
	return &M[DB, KV]{
		DB:     cfg.DB,
		prefix: cfg.Prefix,
		newKV:  cfg.NewKV,
		newSub: cfg.NewSub,
		subs:   make(map[string]*M[DB, KV]),
		kvs:    make(map[string]KV),
	}
}

// KV implements a method of [blob.Store].  A successful result has concrete
// type [KV]. Any error reported by this method is from the NewKV callback
// provided at construction.
func (d *M[DB, KV]) KV(ctx context.Context, name string) (blob.KV, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	kv, ok := d.kvs[name]
	if !ok {
		var err error
		kv, err = d.newKV(ctx, d.DB, d.prefix.Keyspace(name), name)
		if err != nil {
			return nil, err
		}
		d.kvs[name] = kv
	}
	return kv, nil
}

// CAS implements a method of [blob.Store]. This implementation uses the
// default [blob.CASFromKV] construction.
func (d *M[DB, KV]) CAS(ctx context.Context, name string) (blob.CAS, error) {
	return blob.CASFromKVError(d.KV(ctx, name))
}

// Sub implements a method of [blob.Store].  Any error reported by this method
// is from the NewSub callback provided at construction. If no such callback
// was provided, it will always succeed.
func (d *M[DB, KV]) Sub(ctx context.Context, name string) (blob.Store, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	sub, ok := d.subs[name]
	if !ok {
		npfx := d.prefix.Sub(name)
		ndb, err := d.newSub(ctx, d.DB, npfx, name)
		if err != nil {
			return nil, err
		}
		sub = New(Config[DB, KV]{
			DB:     ndb,
			Prefix: npfx,
			NewKV:  d.newKV,
			NewSub: d.newSub,
		})
		d.subs[name] = sub
	}
	return sub, nil
}
