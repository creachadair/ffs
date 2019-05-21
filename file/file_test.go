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

package file_test

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/file"
	"bitbucket.org/creachadair/ffs/split"
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
		Split: split.Config{Min: 17, Size: 84, Max: 500},
	})
	ctx := context.Background()

	wantx := map[string]string{
		"fruit": "apple",
		"nut":   "hazelnut",
	}
	f.XAttr(func(m map[string]string) {
		for k, v := range wantx {
			m[k] = v
		}
	})

	const testMessage = "Four fat fennel farmers fell feverishly for Felicia Frances"
	mustWrite(t, f, testMessage)
	fkey, err := f.Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing failed: %v", err)
	}

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
	g.XAttr(func(got map[string]string) {
		if diff := cmp.Diff(wantx, got); diff != "" {
			t.Errorf("XAttr (-want, +got)\n%s", diff)
		}
	})
}

func TestChildren(t *testing.T) {
	cas := newCAS()
	ctx := context.Background()
	root := file.New(cas, nil)

	names := []string{"all.txt", "your.go", "base.exe"}
	for _, name := range names {
		if err := root.SetChild(ctx, name, root.New(nil)); err != nil {
			t.Fatalf("SetChild %q failed: %v", name, err)
		}
	}

	// Names should come out in lexicographic order.
	sort.Strings(names)

	// Child names should be correct even without a flush.
	if diff := cmp.Diff(names, root.Children()); diff != "" {
		t.Errorf("Wrong children (-want, +got):\n%s", diff)
	}

	// Flushing shouldn't disturb the names.
	rkey, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("root.Flush failed: %v", err)
	}
	t.Logf("Flushed root to %s", fmtKey(rkey))

	if diff := cmp.Diff(names, root.Children()); diff != "" {
		t.Errorf("Wrong children (-want, +got):\n%s", diff)
	}
}

func fmtKey(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func newCAS() blob.CAS { return blob.NewCAS(memstore.New(), sha1.New) }

func mustWrite(t *testing.T, f *file.File, s string) {
	t.Helper()
	ctx := context.Background()
	if _, err := io.WriteString(f.IO(ctx), s); err != nil {
		t.Fatalf("Writing %q failed: %v", s, err)
	}
}
