package file_test

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/file"
	"bitbucket.org/creachadair/ffs/file/wirepb"
	"bitbucket.org/creachadair/ffs/splitter"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
)

func TestNewStat(t *testing.T) {
	cas := newCAS()
	f := file.New(cas, &file.NewOptions{
		Name: "testfile",
		Mode: 0644,
	})
	stat := f.Stat()
	// Verify that changing the file stat does not affect the instance we
	// already obtained.
	f.Chmod(0700)

	if got := stat.Name(); got != "testfile" {
		t.Errorf("New file name: got %q, want testfile", got)
	}
	if got := stat.Size(); got != 0 {
		t.Errorf("New file size: got %d, want 0", got)
	}
	if got, want := stat.Mode(), os.FileMode(0644); got != want {
		t.Errorf("New file mode: got %v, want %v", got, want)
	}
	if got := stat.ModTime(); !got.IsZero() {
		t.Errorf("New file mtime: got %v, want zero", got)
	}
	if stat.IsDir() {
		t.Error("New file isDir: got true, want false")
	}
	if got := stat.Sys(); got != nil {
		t.Errorf("New file sys: got %v, want nil", got)
	}
}

func TestRoundTrip(t *testing.T) {
	cas := newCAS()
	f := file.New(cas, &file.NewOptions{
		Mode:  0640,
		Split: splitter.Config{Min: 17, Size: 84, Max: 500},
	})
	ctx := context.Background()

	const testMessage = "Four fat fennel farmers fell feverishly for Felicia Frances"
	fkey := mustWrite(t, f, testMessage)

	g, err := file.Open(ctx, cas, fkey)
	if err != nil {
		t.Fatalf("Open %s: %v", fmtKey(fkey), err)
	}
	bits, err := ioutil.ReadAll(g.IO(ctx))
	if err != nil {
		t.Errorf("Reading %s: %v", fmtKey(fkey), err)
	}
	if got := string(bits); got != testMessage {
		t.Errorf("Reading %s: got %q, want %q", fmtKey(fkey), got, testMessage)
	}

	logIndex(t, cas, fkey)
}

func TestChildren(t *testing.T) {
	cas := newCAS()
	ctx := context.Background()
	root := file.New(cas, nil)

	f := file.New(cas, nil)
	fkey := mustWrite(t, f, "higgledy piggledy")
	if err := root.SetChild(ctx, "foo", f); err != nil {
		t.Fatalf("SetChild failed: %v", err)
	}

	rkey, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush root failed: %v", err)
	}

	t.Logf("Root key %s, child key %s", fmtKey(rkey), fmtKey(fkey))
	got := root.Children()
	if diff := cmp.Diff([]file.Child{{
		Name: "foo",
		Key:  fkey,
	}}, got); diff != "" {
		t.Errorf("Children of root (-want, +got):\n%s", diff)
	}

	logIndex(t, cas, rkey)
}

func fmtKey(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func newCAS() blob.CAS { return blob.NewCAS(memstore.New(), sha1.New) }

func mustWrite(t *testing.T, f *file.File, s string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := io.WriteString(f.IO(ctx), s); err != nil {
		t.Fatalf("Writing %q failed: %v", s, err)
	}
	key, err := f.Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing %q failed: %v", fmtKey(key), err)
	}
	return key
}

func logIndex(t *testing.T, cas blob.CAS, fkey string) {
	t.Helper()
	ctx := context.Background()
	bits, err := cas.Get(ctx, fkey)
	if err != nil {
		t.Fatalf("Reading %s from storage: %v", fmtKey(fkey), err)
	}
	node := new(wirepb.Node)
	if err := proto.Unmarshal(bits, node); err != nil {
		t.Errorf("Decoding wire node: %v", err)
	}
	t.Log("Index:\n", proto.CompactTextString(node))
}
