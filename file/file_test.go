package file

import (
	"context"
	"crypto/sha1"
	"testing"

	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/splitter"
)

func TestThings(t *testing.T) {
	mem := memstore.New()
	f := &Data{
		s: mem,
		sc: &splitter.Config{
			Min:  16,
			Size: 1024,
			Max:  65535,
		},
		newHash: sha1.New,
	}

	t.Logf("f=%+v", f)

	const message = "all your base are belong to us"

	ctx := context.Background()
	{
		nw, err := f.writeAt(ctx, []byte(message), 107)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("W1 nw=%d, f=%+v", nw, f)
	}
	{
		nw, err := f.writeAt(ctx, []byte("shovel your crappy business elsewhere"), 1024)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("W2 nw=%d, f=%+v", nw, f)
	}
	{
		nw, err := f.writeAt(ctx, []byte(message), 107+int64(len(message)))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("W3 nw=%d, f=%+v", nw, f)
	}

	{
		buf := make([]byte, 2048)
		nr, err := f.readAt(ctx, buf, 0)
		if err != nil {
			t.Logf("readAt: %v", err)
		}
		t.Logf("nr=%d, f=%+v, buf=%q", nr, f, string(buf[:nr]))
	}

	if err := f.truncate(ctx, 500); err != nil {
		t.Logf("truncate: %v", err)
	}
	t.Logf("f=%+v", f)

	{
		buf := make([]byte, 2048)
		nr, err := f.readAt(ctx, buf, 0)
		if err != nil {
			t.Logf("readAt: %v", err)
		}
		t.Logf("nr=%d, f=%+v, buf=%q", nr, f, string(buf[:nr]))
	}

	m := make(map[string]string)
	mem.Snapshot(m)
	t.Logf("storage=%+v", m)
}
