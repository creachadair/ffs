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

var _ blob.CAS = (*wbstore.Store)(nil)

type slowCAS struct {
	blob.CAS
	next <-chan chan struct{}
}

func (s slowCAS) Put(ctx context.Context, opts blob.PutOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p := <-s.next:
		defer close(p)
		return s.CAS.Put(ctx, opts)
	}
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	buf := memstore.New()
	base := memstore.New()
	next := make(chan chan struct{}, 1)

	s := wbstore.New(ctx, slowCAS{
		CAS:  blob.NewCAS(base, sha1.New),
		next: next,
	}, buf)

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
	checkVal := func(m blob.Store, key, want string) {
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
	checkLen := func(m blob.Store, want int) {
		t.Helper()
		got, err := m.Len(ctx)
		if err != nil {
			t.Errorf("Len: unexpected error: %v", err)
		} else if got != int64(want) {
			t.Errorf("Len: got %d, want %d", got, want)
		}
	}
	checkList := func(m blob.Store, want ...string) {
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
	checkLen(base, 0)

	// The base writer stalls until push is called, so we can simulate a slow
	// write and check the contents of the buffer.
	//
	// The test cases write a value, verify it lands in the cache, then unblock
	// the writer and verify it lands in the base store.
	k1 := mustWrite("foo")
	checkVal(buf, k1, "foo") // the write should have hit the buffer
	checkVal(base, k1, "")   // it should not have hit the base
	<-push()
	checkVal(base, k1, "foo")

	k2 := mustWrite("bar")
	checkVal(buf, k2, "bar")
	checkVal(base, k2, "")
	<-push()
	checkVal(base, k2, "bar")

	// A replacement Put should go directly to base, and not hit the buffer.
	p := push()
	mustPut("baz", "quux", true)
	checkVal(buf, "baz", "")
	<-p
	checkVal(base, "baz", "quux")

	// A non-replacement Put should hit the buffer, and not go to base.
	mustPut("frob", "argh", false)
	checkVal(buf, "frob", "argh")
	checkVal(base, "frob", "")

	// The top-level store should see all the keys, even though they are not all
	// settled yet.
	checkList(buf, "frob")
	checkList(base, k1, k2, "baz")
	checkList(s, k1, k2, "baz", "frob")
	checkLen(s, 4)

	<-push()

	if err := s.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	// After synchronization, everything should be in the base.
	checkList(base, k1, k2, "baz", "frob")
	checkList(s, k1, k2, "baz", "frob")
	checkLen(s, 4)

	checkVal(base, k1, "foo")
	checkVal(s, k1, "foo")
	checkVal(base, k2, "bar")
	checkVal(s, k2, "bar")
	checkVal(base, "baz", "quux")
	checkVal(s, "baz", "quux")
	checkVal(base, "frob", "argh")
	checkVal(s, "frob", "argh")

	// Sync should still succeed after no further changes.
	if err := s.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	if err := s.Close(ctx); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
