// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
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

package cachestore_test

import (
	"context"
	"crypto/sha1"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/cachestore"
)

var (
	_ blob.KV    = (*cachestore.KV)(nil)
	_ blob.CAS   = cachestore.CAS{}
	_ blob.Store = cachestore.Store{}
)

func TestStore(t *testing.T) {
	t.Run("KV", func(t *testing.T) {
		s := cachestore.New(memstore.New(nil), 100)
		storetest.Run(t, storetest.NopCloser(s))
	})
	t.Run("CAS", func(t *testing.T) {
		s := cachestore.New(memstore.New(func() blob.KV {
			return blob.NewCAS(memstore.NewKV(), sha1.New)
		}), 100)
		storetest.Run(t, storetest.NopCloser(s))
	})
}

func TestRegression_keyMap(t *testing.T) {
	const data = "stuff"
	m := memstore.NewKV()
	m.Put(context.Background(), blob.PutOptions{
		Key:  "init",
		Data: []byte(data),
	})
	c := cachestore.NewKV(m, 100)
	got, err := c.Get(context.Background(), "init")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	} else if s := string(got); s != data {
		t.Fatalf("Wrong data: got %#q, want %#q", s, data)
	}
}
