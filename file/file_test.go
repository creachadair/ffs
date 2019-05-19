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
)

func TestNewStat(t *testing.T) {
	mem := memstore.New()
	cas := blob.NewCAS(mem, sha1.New)

	f := file.New(cas, &file.NewOptions{
		Name: "testfile",
		Mode: 0644,
	})
	stat := f.Stat()
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
	mem := memstore.New()
	cas := blob.NewCAS(mem, sha1.New)
	f := file.New(cas, &file.NewOptions{
		Mode:  0640,
		Split: splitter.Config{Min: 17, Size: 84, Max: 500},
	})
	ctx := context.Background()

	const testMessage = "Four fat fennel farmers fell feverishly for Felicia Frances"
	io.WriteString(f.IO(ctx), testMessage)
	fkey, err := f.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Flushed output file to %s", fmtKey(fkey))

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

	bits, err = mem.Get(ctx, fkey)
	if err != nil {
		t.Fatalf("Reading %s from storage: %v", fmtKey(fkey), err)
	}
	node := new(wirepb.Node)
	if err := proto.Unmarshal(bits, node); err != nil {
		t.Errorf("Decoding wire node: %v", err)
	}
	t.Log("Index:\n", proto.MarshalTextString(node))
}

func fmtKey(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}
