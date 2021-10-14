package wbstore_test

import (
	"context"
	"crypto/sha1"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/wbstore"
)

var _ blob.CAS = (*wbstore.Store)(nil)

type slowCAS struct {
	blob.CAS
	next <-chan bool
}

func (s slowCAS) Put(ctx context.Context, opts blob.PutOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.next:
		return s.CAS.Put(ctx, opts)
	}
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	buf := memstore.New()
	base := memstore.New()
	next := make(chan bool, 1)

	s := wbstore.New(ctx, slowCAS{
		CAS:  blob.NewCAS(base, sha1.New),
		next: next,
	}, buf)

	mustWrite := func(val string) string {
		t.Helper()
		key, err := s.CASPut(ctx, []byte(val))
		if err != nil {
			t.Fatalf("Write %q failed: %v", val, err)
		}
		return key
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
	push := func() { next <- true }

	// The base writer stalls until push is called, so we can simulate a slow
	// write and check the contents of the buffer.
	//
	// The test cases write a value, verify it lands in the cache, then unblock
	// the writer and verify it lands in the base store.
	k1 := mustWrite("foo")
	checkVal(s, k1, "foo") // fetch against the buffer
	checkVal(buf, k1, "foo")
	checkVal(base, k1, "")
	push()

	k2 := mustWrite("bar")
	checkVal(s, k2, "bar")
	checkVal(buf, k2, "bar")
	checkVal(base, k2, "")
	push()

	if err := s.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	checkVal(base, k1, "foo")
	checkVal(s, k1, "foo")
	checkVal(base, k2, "bar")
	checkVal(s, k2, "bar")

	// Ordinary writes go straight to the base.
	push()
	if err := s.Put(ctx, blob.PutOptions{
		Key:  "normal",
		Data: []byte("xyzzy"),
	}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := s.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}

	checkVal(buf, "normal", "")
	checkVal(base, "normal", "xyzzy")

	if err := s.Sync(ctx); err != nil {
		t.Errorf("Sync: unexpected error: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
