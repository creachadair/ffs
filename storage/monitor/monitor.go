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
package monitor

import (
	"context"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
)

// Config carries settings for construction of an [M].
type Config[DB any, KV blob.KV] struct {
	// DB represents the initial state of the monitor.
	DB DB

	// Prefix gives the initial storage prefix of the root.
	Prefix dbkey.Prefix

	// NewKV construts a KV instance from the current state.
	NewKV func(DB, string) (KV, error)

	// NewSub constructs a sub-DB state from the current state.
	// If nil, the existing state is copied.
	NewSub func(DB, string) (DB, error)
}

// A M value manages keyspace and substore allocations for the specified
// database and KV implementations. The resulting value implements [blob.Store].
type M[DB any, KV blob.KV] struct {
	DB     DB
	prefix dbkey.Prefix
	newKV  func(DB, string) (KV, error)
	newSub func(DB, string) (DB, error)

	μ    sync.Mutex
	subs map[string]*M[DB, KV]
	kvs  map[string]KV
}

// New constructs a new empty store using the specified database, prefix, and
// KV constructor function. New will panic if newKV == nil.
func New[DB any, KV blob.KV](cfg Config[DB, KV]) *M[DB, KV] {
	if cfg.NewKV == nil {
		panic("KV constructor is nil")
	}
	if cfg.NewSub == nil {
		cfg.NewSub = func(old DB, _ string) (DB, error) { return old, nil }
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

// Keyspace implements a method of [blob.Store].  A successful result has
// concrete type [KV].  This method never reports an error.
func (d *M[DB, KV]) Keyspace(_ context.Context, name string) (blob.KV, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	kv, ok := d.kvs[name]
	if !ok {
		var err error
		kv, err = d.newKV(d.DB, name)
		if err != nil {
			return nil, err
		}
		d.kvs[name] = kv
	}
	return kv, nil
}

// Sub implements a method of [blob.Store].  This method never reports an
// error.
func (d *M[DB, KV]) Sub(_ context.Context, name string) (blob.Store, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	sub, ok := d.subs[name]
	if !ok {
		ndb, err := d.newSub(d.DB, name)
		if err != nil {
			return nil, err
		}
		sub = New(Config[DB, KV]{
			DB:     ndb,
			Prefix: d.prefix.Sub(name),
			NewKV:  d.newKV,
			NewSub: d.newSub,
		})
		d.subs[name] = sub
	}
	return sub, nil
}
