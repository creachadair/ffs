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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/split"
	"github.com/google/go-cmp/cmp"
)

func TestRoundTrip(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)

	// Construct a new file and write it to storage, then read it back and
	// verify that the original state was correctly restored.
	f := file.New(cas, &file.NewOptions{
		Stat:  file.Stat{Mode: 0640},
		Split: split.Config{Min: 17, Size: 84, Max: 500},
	})
	if n := f.Size(); n != 0 {
		t.Errorf("Size: got %d, want 0", n)
	}
	ctx := context.Background()

	wantx := map[string]string{
		"fruit": "apple",
		"nut":   "hazelnut",
	}
	for k, v := range wantx {
		f.XAttr().Set(k, v)
	}

	const testMessage = "Four fat fennel farmers fell feverishly for Felicia Frances"
	fmt.Fprint(f.IO(ctx), testMessage)
	if n := f.Size(); n != int64(len(testMessage)) {
		t.Errorf("Size: got %d, want %d", n, len(testMessage))
	}
	fkey, err := f.Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing failed: %v", err)
	}

	g, err := file.Open(ctx, cas, fkey)
	if err != nil {
		t.Fatalf("Open %x: %v", fkey, err)
	}

	// Verify that file contents were preserved.
	bits, err := ioutil.ReadAll(g.IO(ctx))
	if err != nil {
		t.Errorf("Reading %x: %v", fkey, err)
	}
	if got := string(bits); got != testMessage {
		t.Errorf("Reading %x: got %q, want %q", fkey, got, testMessage)
	}

	// Verify that extended attributes were preserved.
	gotx := make(map[string]string)
	g.XAttr().List(func(key, val string) {
		if v, ok := g.XAttr().Get(key); !ok || v != val {
			t.Errorf("GetXAttr(%q): got (%q, %v), want (%q, true)", key, v, ok, val)
		}
		gotx[key] = val
	})
	if diff := cmp.Diff(wantx, gotx); diff != "" {
		t.Errorf("XAttr (-want, +got)\n%s", diff)
	}

	// Verify that file stat was preserved.
	if diff := cmp.Diff(f.Stat(), g.Stat()); diff != "" {
		t.Errorf("Stat (-want, +got)\n%s", diff)
	}

	// Verify that seek and truncation work.
	if err := g.Truncate(ctx, 15); err != nil {
		t.Errorf("Truncate(15): unexpected error: %v", err)
	} else if pos, err := g.Seek(ctx, 0, io.SeekStart); err != nil {
		t.Errorf("Seek(0): unexpected error: %v", err)
	} else if pos != 0 {
		t.Errorf("Pos after Seek(0): got %d, want 0", pos)
	} else if bits, err := ioutil.ReadAll(g.IO(ctx)); err != nil {
		t.Errorf("Read failed: %v", err)
	} else if got, want := string(bits), testMessage[:15]; got != want {
		t.Errorf("Truncated message: got %q, want %q", got, want)
	}

	// Exercise the scanner.
	if err := g.Scan(ctx, func(key string, isFile bool) bool {
		if isFile && key != fkey {
			t.Errorf("File key: got %x, want %x", key, fkey)
		} else {
			t.Logf("Data key %x OK", key)
		}
		return true
	}); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
}

func TestChildren(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	root := file.New(cas, nil)

	names := []string{"all.txt", "your.go", "base.exe"}
	for _, name := range names {
		root.Set(name, root.New(nil))
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
	t.Logf("Flushed root to %x", rkey)

	if diff := cmp.Diff(names, root.Children()); diff != "" {
		t.Errorf("Wrong children (-want, +got):\n%s", diff)
	}
}

func TestCycleCheck(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	root := file.New(cas, nil)

	kid := file.New(cas, nil)
	root.Set("harmless", kid)
	kid.Set("harmful", root)

	key, err := root.Flush(ctx)
	if err == nil {
		t.Errorf("Cyclic flush: got %q, nil, want error", key)
	} else {
		t.Logf("Cyclic flush correctly failed: %v", err)
	}
}

func TestRootRoundTrip(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()

	// Set up an empty root, flush it out, read it back in, and check that the
	// results look the same.

	r := file.NewRoot(cas, &file.NewOptions{
		Name: "carrot",
		Stat: file.Stat{Mode: 0135},
	})
	fk, err := r.File().Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing root file failed: %v", err)
	}
	t.Logf("Root file key: %x", fk)

	sk, err := r.SetSnapshot(ctx, "sniplet")
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	} else if sk != fk {
		t.Errorf("Snapshot key: got %x, want %x", sk, fk)
	}
	t.Logf("Snapshot key: %x", sk)

	rk, err := r.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Root key: %x", rk)

	c, err := file.OpenRoot(ctx, cas, rk)
	if err != nil {
		t.Fatalf("Open root: %v", err)
	}
	cf := c.File()

	// Verify that file stat was preserved.
	if diff := cmp.Diff(r.File().Stat(), cf.Stat()); diff != "" {
		t.Errorf("Stat (-want, +got)\n%s", diff)
	}

	// Verify that the snapshot was preserved.
	snap, ok := c.Snapshot("sniplet")
	if !ok {
		t.Error("Snapshot not found on opened root")
	} else if snap.Key != fk {
		t.Errorf("Restored snapshot key: got %x, want %x", snap.Key, fk)
	}
}

func TestView(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	mustFlush := func(tag string, f *file.File) string {
		key, err := f.Flush(ctx)
		if err != nil {
			t.Fatalf("Flush %s: %v", tag, err)
		}
		return key
	}

	// Create and store a file to manipulate in the tests below.
	const fileText = "Hello, is there anybody in there?"

	f := file.New(cas, nil)
	if _, err := fmt.Fprintln(f.IO(ctx), fileText); err != nil {
		t.Fatalf("Writing test file: %v", err)
	}
	fkey := mustFlush("test file", f)
	t.Logf("Created test file %x", fkey)

	// Open a view on the test file, make some changes, and verify that the
	// changes are not persisted.
	view, err := file.View(ctx, cas, fkey)
	if err != nil {
		t.Fatalf("Viewing: %v", err)
	}
	if _, err := fmt.Fprintln(view.IO(ctx), "Somebody that I used to know"); err != nil {
		t.Fatalf("Writing view: %v", err)
	}
	viewKey := mustFlush("view", view)
	if viewKey != fkey {
		t.Errorf("Flush view: got key %x, want %x", viewKey, fkey)
	}

	// Create a view-only (virtual) file to use below.
	// Flushing the file must not fail, but should not result in a key that can
	// be fetched from our CAS.
	virt := file.New(cas, &file.NewOptions{Virtual: true})
	virt.Set("A", f)
	virtKey, err := virt.Flush(ctx)
	if err != nil {
		t.Errorf("Flush virtual: unexpected error: %v", err)
	}
	if c, err := file.Open(ctx, cas, virtKey); err == nil {
		t.Errorf("Open virtual: got %v, %v; want error", c, err)
	}

	// Create a file with v as its child.
	g := file.New(cas, nil)
	g.Set("view kid", view)
	g.Set("virtual kid", virt)
	g.Set("other", file.New(cas, nil)) // a non-view child
	gkey := mustFlush("parent", g)

	// Verify that the view chilren were not persisted, the other was.
	h, err := file.Open(ctx, cas, gkey)
	if err != nil {
		t.Fatalf("Opening parent: %v", err)
	}

	if c, err := h.Open(ctx, "view kid"); !errors.Is(err, file.ErrChildNotFound) {
		t.Errorf("Open view kid: got %v, %v; want error %v", c, err, file.ErrChildNotFound)
	}
	if c, err := h.Open(ctx, "virtual kid"); !errors.Is(err, file.ErrChildNotFound) {
		t.Errorf("Open virtual kid: got %v, %v; want error %v", c, err, file.ErrChildNotFound)
	}
	if c, err := h.Open(ctx, "other"); err != nil {
		t.Errorf("Open other: unexpected error: %v", err)
	} else {
		t.Logf("Open other succeeded for %x", mustFlush("other", c))
	}

}
