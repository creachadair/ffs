package fpath_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"os"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/filestore"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/file"
	"bitbucket.org/creachadair/ffs/fpath"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/xerrors"
)

var saveStore = flag.String("save", "", "Save blobs to a filestore at this path")

func TestPaths(t *testing.T) {
	var bs blob.Store = memstore.New()
	if *saveStore != "" {
		fs, err := filestore.New(*saveStore)
		if err != nil {
			t.Fatalf("Opening filestore %q: %v", *saveStore, err)
		}
		bs = fs
		t.Logf("Saving test output to filestore %q", *saveStore)
	}
	cas := blob.NewCAS(bs, sha1.New)

	ctx := context.Background()
	root := file.New(cas, nil)
	createPath := func(path string, werr error) *file.File {
		got, err := fpath.Create(ctx, root, path)
		if !errorOK(err, werr) {
			t.Errorf("CreatePath %q: got error %v, want %v", path, err, werr)
		}
		return got
	}
	removePath := func(path string, werr error) {
		err := fpath.Remove(ctx, root, path)
		if !errorOK(err, werr) {
			t.Errorf("RemovePath %q: got error %v, want %v", path, err, werr)
		}
	}
	openPath := func(path string, werr error) *file.File {
		got, err := fpath.Open(ctx, root, path)
		if !errorOK(err, werr) {
			t.Errorf("OpenPath %q: got error %v, want %v", path, err, werr)
		}
		return got
	}
	setPath := func(path string, f *file.File, werr error) {
		err := fpath.Set(ctx, root, path, f)
		if !errorOK(err, werr) {
			t.Errorf("SetPath %q: got error %v, want %v", path, err, werr)
		}
	}

	// Opening the empty path should return the root.
	if got := openPath("", nil); got != root {
		t.Errorf("Open empty path: got %p, want %p", got, root)
	}

	// Removing or creating an empty path should quietly do nothing.
	removePath("", nil)
	removePath("/", nil)
	createPath("", nil)
	createPath("/", nil)

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

	setPath("", subtree, fpath.ErrEmptyPath)
	setPath("/a/dog", nil, fpath.ErrNilFile)

	{
		want := []string{"a", "boring", "sludge", "of", "words"}
		var got []string
		if err := fpath.Visit(ctx, root, "/a/boring/sludge/of/words", func(f *file.File) error {
			got = append(got, f.Name())
			f.SetStat(func(s *file.Stat) {
				s.Mode = os.ModeDir | 0755
			})
			return nil
		}); err != nil {
			t.Errorf("Visit: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Visited names (-want, +got)\n%s", diff)
		}
	}

	rkey, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush root: %v", err)
	}
	t.Logf("Root key: %s", hex.EncodeToString([]byte(rkey)))
}

func errorOK(err, werr error) bool {
	if werr == nil {
		return err == nil
	}
	return xerrors.Is(err, werr)
}
