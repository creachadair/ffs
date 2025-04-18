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

package fpath_test

import (
	"crypto/sha1"
	"errors"
	"flag"
	"hash"
	"io/fs"
	"strconv"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/google/go-cmp/cmp"
)

var (
	saveStore = flag.String("save", "", "Save blobs to a filestore at this path")

	// Interface satisfaction checks.
	_ fs.FS        = fpath.FS{}
	_ fs.StatFS    = fpath.FS{}
	_ fs.SubFS     = fpath.FS{}
	_ fs.ReadDirFS = fpath.FS{}
)

func mustNewCAS(t *testing.T, h func() hash.Hash) blob.CAS {
	t.Helper()
	if *saveStore == "" {
		return blob.CASFromKV(memstore.NewKV())
	}
	fs, err := filestore.New(*saveStore)
	if err != nil {
		t.Fatalf("Opening filestore %q: %v", *saveStore, err)
	}
	ks, err := fs.KV(t.Context(), "")
	if err != nil {
		t.Fatalf("Opening keyspace: %v", err)
	}
	t.Logf("Saving test output to filestore %q", *saveStore)
	return blob.CASFromKV(ks)
}

func TestPaths(t *testing.T) {
	cas := mustNewCAS(t, sha1.New)

	ctx := t.Context()
	root := file.New(cas, nil)
	setDir := func(s *file.Stat) { s.Mode = fs.ModeDir | 0755 }
	openPath := func(path string, werr error) *file.File {
		got, err := fpath.Open(ctx, root, path)
		if !errorOK(err, werr) {
			t.Errorf("OpenPath %q: got error %v, want %v", path, err, werr)
		}
		return got
	}
	createPath := func(path string, werr error) *file.File {
		newf, err := fpath.Set(ctx, root, path, &fpath.SetOptions{
			Create:  true,
			SetStat: setDir,
		})
		if !errorOK(err, werr) {
			t.Errorf("CreatePath %q: got error %v, want %v", path, err, werr)
		}
		return newf
	}
	removePath := func(path string, werr error) {
		err := fpath.Remove(ctx, root, path)
		if !errorOK(err, werr) {
			t.Errorf("RemovePath %q: got error %v, want %v", path, err, werr)
		}
	}
	setPath := func(path string, f *file.File, werr error) {
		_, err := fpath.Set(ctx, root, path, &fpath.SetOptions{File: f})
		if !errorOK(err, werr) {
			t.Errorf("SetPath %q: got error %v, want %v", path, err, werr)
		}
	}

	// Opening the empty path should return the root.
	if got := openPath("", nil); got != root {
		t.Errorf("Open empty path: got %p, want %p", got, root)
	}

	// Removing an empty path should quietly do nothing.
	removePath("", nil)
	removePath("/", nil)

	// Setting a nil file without creation enabled should fail.
	setPath("", nil, fpath.ErrNilFile)

	// Setting on a non-existent path should fail, but the last element of the
	// path may be missing.
	setPath("/no/such/path", root.New(nil), file.ErrChildNotFound)
	setPath("/okay", root.New(nil), nil)

	// Removing non-existing non-empty paths should report an error,
	removePath("nonesuch", file.ErrChildNotFound)
	removePath("/no/such/path", file.ErrChildNotFound)

	// Opening a non-existing path should report an error.
	openPath("/a/lasting/peace", file.ErrChildNotFound)

	// After creating a path, we should be able to open it and get back the same
	// file value we created.
	{
		want := createPath("/a/lasting/peace", nil)
		got := openPath("/a/lasting/peace", nil)
		if got != want {
			t.Errorf("Open returned the wrong file: got %+v, want %+v", got, want)
		}
	}

	// Verify that the stat callback was properly invoked for path components
	// that we created.
	for _, path := range []string{"/a", "/a/lasting", "/a/lasting/peace"} {
		got := openPath(path, nil).Stat().Mode
		if want := fs.ModeDir | 0755; got != want {
			t.Errorf("Wrong path mode for %q: got %v, want %v", path, got, want)
		}
	}

	// Verify that the stat callback is not called for the final path element if
	// we provided the file that is to be inserted.
	{
		const path = "/a/lasting/itch"
		if newf, err := fpath.Set(ctx, root, path, &fpath.SetOptions{
			Create:  true,
			SetStat: setDir,
			File:    root.New(nil),
		}); err != nil {
			t.Errorf("Create %q: got unexpected error %v", "/a/lasting/itch", err)
		} else if got, want := newf.Stat().Mode, fs.FileMode(0); got != want {
			t.Errorf("Wrong mode for %q: got %v, want %v", path, got, want)
		}
	}

	// Prefixes of an existing path should exist.
	openPath("/a", nil)
	openPath("/a/lasting", nil)

	// Non-existing siblings should report an error.
	openPath("/a/lasting/war", file.ErrChildNotFound)

	// Creating a sibling should work, and not disturb its neighbors.
	createPath("/a/lasting/consequence", nil)
	openPath("/a/lasting/peace", nil)
	openPath("/a/lasting/consequence", nil)

	// Removing a path should make it unreachable.
	removePath("/a/lasting/peace", nil)
	openPath("/a/lasting/peace", file.ErrChildNotFound)

	createPath("/a/lasting/war/of/words", nil)
	subtree := openPath("/a/lasting/war", nil)
	openPath("/a/lasting/war/of", nil)
	openPath("/a/lasting/war/of/words", nil)

	// Removing an intermediate node drops the whole subtree, but not its ancestor.
	removePath("/a/lasting/war", nil)
	openPath("/a/lasting/war", file.ErrChildNotFound)
	openPath("/a/lasting/war/of", file.ErrChildNotFound)
	openPath("/a/lasting/war/of/words", file.ErrChildNotFound)

	// A subtree can be spliced in, and preserve its structure.
	createPath("/a/boring", nil)
	setPath("/a/boring/sludge", subtree, nil)
	openPath("/a/boring/sludge", nil)
	openPath("/a/boring/sludge/of", nil)
	openPath("/a/boring/sludge/of/words", nil)
	createPath("/a/boring/song", nil)

	setPath("", subtree, fpath.ErrEmptyPath)

	// Verify that opening a path produces the right files.
	if fp, err := fpath.OpenPath(ctx, root, "a/boring/sludge/of/words"); err != nil {
		t.Errorf("OpenPath failed: %v", err)
	} else {
		want := []string{"a", "boring", "sludge", "of", "words"}
		var got []string
		for i, elt := range fp {
			st := elt.Stat()
			st.Mode = fs.ModeDir | 0750
			st.Update()
			elt.XAttr().Set("index", strconv.Itoa(i+1))
			got = append(got, elt.Name())
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Path names (-want, +got)\n%s", diff)
		}
	}

	// Verify that walk is depth-first and respects its filter.
	{
		want := []string{
			"", "a",
			"a/boring", "a/boring/sludge", "a/boring/song",
			"a/lasting", "a/lasting/consequence", "a/lasting/itch",
			"okay",
		}
		var got []string
		if err := fpath.Walk(ctx, root, func(e fpath.Entry) error {
			got = append(got, e.Path)
			if e.Err != nil {
				return e.Err
			} else if e.File == subtree {
				return fpath.ErrSkipChildren
			}
			return nil
		}); err != nil {
			t.Errorf("Walk failed: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Walk paths (-want, +got)\n%s", diff)
		}
	}

	rk, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush root: %v", err)
	}
	t.Logf("Root key: %x", rk)
}

func TestFS(t *testing.T) {
	cas := mustNewCAS(t, sha1.New)
	ctx := t.Context()

	root := file.New(cas, &file.NewOptions{
		Stat: &file.Stat{Mode: fs.ModeDir | 0755},
	})
	kid, err := fpath.Set(ctx, root, "kid", &fpath.SetOptions{
		Create:  true,
		SetStat: func(s *file.Stat) { s.Mode = 0644 },
	})
	if err != nil {
		t.Fatalf("Create child: %v", err)
	}

	fp := fpath.NewFS(ctx, root)
	t.Run("Open", func(t *testing.T) {
		got, err := fp.Open("kid")
		if err != nil {
			t.Fatalf("Open kid: %v", err)
		}
		fi, err := got.Stat()
		if err != nil {
			t.Fatalf("Stat kid: %v", err)
		}
		if sys, ok := fi.Sys().(*file.File); !ok || sys != kid {
			t.Fatalf("Stat sys: got %+v, want %+v", fi.Sys(), kid)
		}
	})

	t.Run("Stat", func(t *testing.T) {
		fi, err := fp.Stat("kid")
		if err != nil {
			t.Fatalf("Stat kid: %v", err)
		}
		if sys, ok := fi.Sys().(*file.File); !ok || sys != kid {
			t.Fatalf("Stat sys: got %+v, want %+v", fi.Sys(), kid)
		}
	})

	t.Run("ReadDir", func(t *testing.T) {
		des, err := fp.ReadDir(".") // "." denotes the root, see fs.ValidPath
		if err != nil {
			t.Fatalf("ReadDir root: %v", err)
		}
		if len(des) != 1 {
			t.Fatalf("Got %+v, wanted 1 entry", des)
		}
		if n := des[0].Name(); n != "kid" {
			t.Errorf("Name: got %q, want %q", n, "kid")
		}
		if des[0].IsDir() {
			t.Error("IsDir is true, want false")
		}
	})

	rk, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush root: %v", err)
	}
	t.Logf("Root key: %x", rk)
}

func errorOK(err, werr error) bool {
	if werr == nil {
		return err == nil
	}
	return errors.Is(err, werr)
}
