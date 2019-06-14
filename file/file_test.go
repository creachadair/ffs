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
