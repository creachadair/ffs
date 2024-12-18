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

package wbstore_test

import (
	"context"
	"crypto/sha1"
	"sort"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/google/go-cmp/cmp"
)

type slowKV struct {
	blob.CAS
	next <-chan chan struct{}
}

func (s slowKV) Put(ctx context.Context, opts blob.PutOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p := <-s.next:
		defer close(p)
		return s.CAS.Put(ctx, opts)
	}
}

func (s slowKV) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case p := <-s.next:
		defer close(p)
		return s.CAS.CASPut(ctx, opts)
	}
}

func TestWrapperTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("YesCAS", func(t *testing.T) {
		base := memstore.New(func() blob.KV {
			return blob.NewCAS(memstore.NewKV(), sha1.New)
		})
		st := wbstore.New(ctx, base, memstore.NewKV())
		kv, err := st.Keyspace(ctx, "test")
		if err != nil {
			t.Fatalf("Create test keyspace: %v", err)
		}
		if _, ok := kv.(blob.CAS); !ok {
			t.Errorf("KV wrapper does not implement CAS: %T", kv)
		}
	})

	t.Run("NoCAS", func(t *testing.T) {
		base := memstore.New(nil)
		st := wbstore.New(ctx, base, memstore.NewKV())
		kv, err := st.Keyspace(ctx, "test")
		if err != nil {
			t.Fatalf("Create test keyspace: %v", err)
		}
		if cas, ok := kv.(blob.CAS); ok {
			t.Errorf("KV wrapper unexpectedly implements CAS: %T", cas)
		}
	})
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	phys := memstore.NewKV() // represents the "physical" storage at the far end

	next := make(chan chan struct{}, 1)
	base := memstore.New(func() blob.KV {
		return slowKV{
			CAS:  blob.NewCAS(phys, sha1.New),
			next: next,
		}
	})

	buf := memstore.NewKV()
	st := wbstore.New(ctx, base, buf)
	kv, err := st.Keyspace(ctx, "test")
	if err != nil {
		t.Fatalf("Create test keyspace: %v", err)
	}
	s, ok := kv.(blob.CAS)
	if !ok {
		t.Fatalf("Keyspace is not a CAS: %[1]T %[1]#v", kv)
	}

	bufKey := func(key string) string { return "\x00\x01" + key }

	mustWrite := func(val string) string {
		t.Helper()
		key, err := s.CASPut(ctx, blob.CASPutOptions{Data: []byte(val)})
		if err != nil {
			t.Fatalf("CASPut %q failed: %v", val, err)
		}
		return key
	}
	mustPut := func(key, val string, replace bool) {
		t.Helper()
		if err := s.Put(ctx, blob.PutOptions{
			Key:     key,
			Data:    []byte(val),
			Replace: replace,
		}); err != nil {
			t.Fatalf("Put %q failed: %v", val, err)
		}
	}
	checkVal := func(m blob.KV, key, want string) {
		t.Helper()
		bits, err := m.Get(ctx, key)
		if blob.IsKeyNotFound(err) && want == "" {
			return
		} else if err != nil {
			t.Errorf("Get %x: unexpected error: %v", key, err)
		} else if got := string(bits); got != want {
			t.Errorf("Get %x: got %q, want %q", key, got, want)
		}
	}
	checkLen := func(m blob.KV, want int) {
		t.Helper()
		got, err := m.Len(ctx)
		if err != nil {
			t.Errorf("Len: unexpected error: %v", err)
		} else if got != int64(want) {
			t.Errorf("Len: got %d, want %d", got, want)
		}
	}
	checkList := func(m blob.KV, want ...string) {
		t.Helper()
		sort.Strings(want)
		var got []string
		if err := m.List(ctx, "", func(key string) error {
			got = append(got, key)
			return nil
		}); err != nil {
			t.Errorf("List: unexpected error: %v", err)
		} else if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("List (-got, +want):\n%s", diff)
		}
	}
	push := func() <-chan struct{} {
		p := make(chan struct{})
		next <- p
		return p
	}

	checkLen(buf, 0)
	checkLen(phys, 0)

	// The base writer stalls until push is called, so we can simulate a slow
	// write and check the contents of the buffer.
	//
	// The test cases write a value, verify it lands in the cache, then unblock
	// the writer and verify it lands in the base store.
	k1 := mustWrite("foo")
	checkVal(buf, bufKey(k1), "foo") // the write should have hit the buffer
	checkVal(phys, k1, "")           // it should not have hit the base
	<-push()
	checkVal(phys, k1, "foo")

	k2 := mustWrite("bar")
	checkVal(buf, bufKey(k2), "bar")
	checkVal(phys, k2, "")
	<-push()
	checkVal(phys, k2, "bar")

	// A replacement Put should go directly to base, and not hit the buffer.
	p := push()
	mustPut("baz", "quux", true)
	checkVal(buf, bufKey("baz"), "")
	<-p
	checkVal(phys, "baz", "quux")

	// A non-replacement Put should hit the buffer, and not go to base.
	mustPut("frob", "argh", false)
	checkVal(buf, bufKey("frob"), "argh")
	checkVal(phys, "frob", "")

	// The top-level store should see all the keys, even though they are not all
	// settled yet.
	checkList(buf, bufKey("frob"))
	checkList(phys, k1, k2, "baz")
	checkList(s, k1, k2, "baz", "frob")
	checkLen(s, 4)
	<-push()

	if err := st.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	// After synchronization, everything should be in the base.
	checkList(phys, k1, k2, "baz", "frob")
	checkList(s, k1, k2, "baz", "frob")
	checkLen(s, 4)

	checkVal(phys, k1, "foo")
	checkVal(s, k1, "foo")
	checkVal(phys, k2, "bar")
	checkVal(s, k2, "bar")
	checkVal(phys, "baz", "quux")
	checkVal(s, "baz", "quux")
	checkVal(phys, "frob", "argh")
	checkVal(s, "frob", "argh")

	// Sync should still succeed after no further changes.
	if err := st.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	if err := st.Close(ctx); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
