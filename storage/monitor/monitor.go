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

// A M value manages keyspace and substore allocations for the specified
// database and KV implementations. The resulting value implements [blob.Store].
type M[DB any, KV blob.KV] struct {
	db     DB
	prefix dbkey.Prefix
	newKV  func(Config[DB]) KV

	μ    sync.Mutex
	subs map[string]*M[DB, KV]
	kvs  map[string]KV
}

// Config is the set of arguments passed to the newKV constructor when the
// monitor needs a new KV value.
type Config[DB any] struct {
	DB     DB
	Prefix dbkey.Prefix
}

// New constructs a new empty store using the specified database, prefix, and
// KV constructor function. New will panic if newKV == nil.
func New[DB any, KV blob.KV](db DB, prefix dbkey.Prefix, newKV func(Config[DB]) KV) *M[DB, KV] {
	if newKV == nil {
		panic("KV constructor is nil")
	}
	return &M[DB, KV]{
		db:     db,
		prefix: prefix,
		newKV:  newKV,
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
		kv = d.newKV(Config[DB]{DB: d.db, Prefix: d.prefix})
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
		sub = New(d.db, d.prefix, d.newKV)
		d.subs[name] = sub
	}
	return sub, nil
}
