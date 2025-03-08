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
	"sort"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/google/go-cmp/cmp"
)

type slowKV struct {
	blob.KV
	next <-chan chan struct{}
}

func (s slowKV) Put(ctx context.Context, opts blob.PutOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p := <-s.next:
		defer close(p)
		return s.KV.Put(ctx, opts)
	}
}

func (s slowKV) CASPut(ctx context.Context, data []byte) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case p := <-s.next:
		defer close(p)
		return blob.CASFromKV(s.KV).CASPut(ctx, data)
	}
}

func TestStore(t *testing.T) {
	ctx := t.Context()

	phys := memstore.NewKV() // represents the "physical" storage at the far end

	next := make(chan chan struct{}, 1)
	base := memstore.New(func() blob.KV {
		return slowKV{KV: phys, next: next}
	})

	bufStore := memstore.New(nil)
	buf := storetest.SubKV(t, ctx, bufStore, "test")
	st := wbstore.New(ctx, base, bufStore)
	kv, err := st.KV(ctx, "test")
	if err != nil {
		t.Fatalf("Create test KV: %v", err)
	}
	cas, err := st.CAS(ctx, "test")
	if err != nil {
		t.Fatalf("Create test CAS: %v", err)
	}

	mustWrite := func(val string) string {
		t.Helper()
		key, err := cas.CASPut(ctx, []byte(val))
		if err != nil {
			t.Fatalf("CASPut %q failed: %v", val, err)
		}
		return key
	}
	mustPut := func(key, val string, replace bool) {
		t.Helper()
		if err := kv.Put(ctx, blob.PutOptions{
			Key:     key,
			Data:    []byte(val),
			Replace: replace,
		}); err != nil {
			t.Fatalf("Put %q failed: %v", val, err)
		}
	}
	checkVal := func(m blob.KVCore, key, want string) {
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
	checkLen := func(m blob.KVCore, want int) {
		t.Helper()
		got, err := m.Len(ctx)
		if err != nil {
			t.Errorf("Len: unexpected error: %v", err)
		} else if got != int64(want) {
			t.Errorf("Len: got %d, want %d", got, want)
		}
	}
	checkList := func(m blob.KVCore, want ...string) {
		t.Helper()
		sort.Strings(want)
		var got []string
		for key, err := range m.List(ctx, "") {
			if err != nil {
				t.Errorf("List: unexpected error: %v", err)
				break
			}
			got = append(got, key)
		}
		if diff := cmp.Diff(got, want); diff != "" {
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
	checkVal(buf, k1, "foo") // the write should have hit the buffer
	checkVal(phys, k1, "")   // it should not have hit the base
	<-push()
	checkVal(phys, k1, "foo")

	k2 := mustWrite("bar")
	checkVal(buf, k2, "bar")
	checkVal(phys, k2, "")
	<-push()
	checkVal(phys, k2, "bar")

	// A replacement Put should go directly to base, and not hit the buffer.
	p := push()
	mustPut("baz", "quux", true)
	checkVal(buf, "baz", "")
	<-p
	checkVal(phys, "baz", "quux")

	// A non-replacement Put should hit the buffer, and not go to base.
	mustPut("frob", "argh", false)
	checkVal(buf, "frob", "argh")
	checkVal(phys, "frob", "")

	// Verify that the buffer size reflects what we've stored.
	if n, err := st.BufferLen(ctx); err != nil {
		t.Errorf("BufferLen: unexpected error: %v", err)
	} else if n != 1 {
		t.Errorf("BufferLen = %d, want 1", n)
	}

	// The top-level store should see all the keys, even though they are not all
	// settled yet.
	checkList(buf, "frob")
	checkList(phys, k1, k2, "baz")
	checkList(kv, k1, k2, "baz", "frob")
	checkLen(kv, 4)
	<-push()

	// After settlement, everything should be in the base.
	checkList(phys, k1, k2, "baz", "frob")
	checkList(cas, k1, k2, "baz", "frob")
	checkLen(cas, 4)

	checkVal(phys, k1, "foo")
	checkVal(cas, k1, "foo")
	checkVal(phys, k2, "bar")
	checkVal(cas, k2, "bar")
	checkVal(phys, "baz", "quux")
	checkVal(cas, "baz", "quux")
	checkVal(phys, "frob", "argh")
	checkVal(cas, "frob", "argh")

	if n, err := st.BufferLen(ctx); err != nil {
		t.Errorf("BufferLen: unexpected error: %v", err)
	} else if n != 0 {
		t.Errorf("BufferLen = %d, want 0", n)
	}

	if err := st.Close(ctx); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
