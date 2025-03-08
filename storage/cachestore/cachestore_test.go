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
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/cachestore"
)

var (
	_ blob.KV          = (*cachestore.KV)(nil)
	_ blob.StoreCloser = cachestore.Store{}
)

func TestStore(t *testing.T) {
	s := cachestore.New(memstore.New(nil), 100)
	storetest.Run(t, storetest.NopCloser(s))
}

func TestRegression_keyMap(t *testing.T) {
	const data = "stuff"
	m := memstore.NewKV()
	m.Put(t.Context(), blob.PutOptions{
		Key:  "init",
		Data: []byte(data),
	})
	c := cachestore.NewKV(m, 100)
	got, err := c.Get(t.Context(), "init")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	} else if s := string(got); s != data {
		t.Fatalf("Wrong data: got %#q, want %#q", s, data)
	}
}

func TestRecurrentList(t *testing.T) {
	ctx := t.Context()

	want := map[string]string{
		"1": "one",
		"2": "two",
		"3": "three",
		"4": "four",
	}
	base := memstore.New(func() blob.KV {
		return memstore.NewKV().Init(want)
	})
	cs := cachestore.New(base, 100)
	kv := storetest.SubKV(t, ctx, cs, "test")

	for key, err := range kv.List(ctx, "") {
		if err != nil {
			t.Fatalf("List: unexpected error: %v", err)
		}
		if got, err := kv.Get(ctx, key); err != nil {
			t.Errorf("Get %q: unexpected error: %v", key, err)
		} else if string(got) != want[key] {
			t.Errorf("Get %q: got %q, want %q", key, got, want[key])
		}

		for k2, err := range kv.List(ctx, key) {
			if err != nil {
				t.Fatalf("Inner List: unexpected error: %v", err)
			}
			t.Logf("List %q OK", k2)
		}
	}
}
