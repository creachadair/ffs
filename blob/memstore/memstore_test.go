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

package memstore_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/google/go-cmp/cmp"
)

func TestStore(t *testing.T) {
	var s memstore.Store
	storetest.Run(t, &s)
}

func TestSnapshot(t *testing.T) {
	kv := memstore.NewKV()
	kv.Put(context.Background(), blob.PutOptions{
		Key:  "foo",
		Data: []byte("bar"),
	})
	kv.Put(context.Background(), blob.PutOptions{
		Key:  "baz",
		Data: []byte("quux"),
	})
	kv.Delete(context.Background(), "baz")

	if diff := cmp.Diff(kv.Snapshot(nil), map[string]string{
		"foo": "bar",
	}); diff != "" {
		t.Errorf("Wrong snapshot: (-want, +got):\n%s", diff)
	}
}

func TestConsistency(t *testing.T) {
	ctx := context.Background()
	data := map[string]string{
		"natha":  "striped",
		"zuulie": "roumnd",
		"thena":  "scurred",
		"asha":   "wild",
	}
	s := memstore.New(func() blob.KV {
		return memstore.NewKV().Init(data)
	})

	k1 := storetest.SubKV(t, ctx, s, "foo", "bar")
	k2 := storetest.SubKV(t, ctx, s, "foo", "bar")

	for key, want := range data {
		got1, err := k1.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get 1 key %q: %v", key, err)
		}
		got2, err := k2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get 2 key %q: %v", key, err)
		}
		if string(got1) != want || string(got2) != want {
			t.Errorf("Check key %q: got (%q, %q), want %q", key, got1, got2, want)
		}
	}
}

func TestReadWhileListing(t *testing.T) {
	ctx := context.Background()

	want := map[string]string{
		"cheddar": "ham",
		"babou":   "dozing",
		"olive":   "slumpt",
		"monty":   "grumpus",
		"luna":    "buckwild",
	}
	kv := memstore.NewKV().Init(want)
	for key, err := range kv.List(ctx, "") {
		if err != nil {
			t.Fatalf("Unexpected error from list: %v", err)
		}
		got, err := kv.Get(ctx, key)
		if err != nil {
			t.Errorf("Get %q: unexpected error: %v", key, err)
		} else if string(got) != want[key] {
			t.Errorf("Get %q: got %q, want %q", key, got, want[key])
		}
	}
}
