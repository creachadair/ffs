// Copyright 2023 Michael J. Fromberger. All Rights Reserved.
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

package zipstore_test

import (
	"archive/zip"
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/ffs/storage/zipstore"
	"github.com/google/go-cmp/cmp"
)

func TestZipStore(t *testing.T) {
	fpath := t.TempDir()

	// Create a filestore with some keys in it.
	fs, err := filestore.New(fpath)
	if err != nil {
		t.Fatalf("Create filestore: %v", err)
	}

	ctx := context.Background()
	mustPut := func(key, value string) {
		if err := fs.Put(ctx, blob.PutOptions{
			Key:     key,
			Data:    []byte(value),
			Replace: true,
		}); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}

	mustPut("1000", "one-thousand")
	mustPut("2048", "two-thousand forty-eight")
	mustPut("0", "zero")
	mustPut("8675309", "jenny")
	mustPut("68000", "sixty-eight thousand")

	// Create a ZIP archive containin the contents of the filestore.
	zpath := filepath.Join(t.TempDir(), "test.zip")
	cmd := exec.Command("zip", "-r", zpath, filepath.Base(fpath))
	cmd.Dir = filepath.Dir(fpath)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Creating ZIP archive: %v", err)
	}
	zf, err := zip.OpenReader(zpath)
	if err != nil {
		t.Fatalf("Open ZIP archive: %v", err)
	}

	// Open the ZIP archive with a zipstore and verify that it has the
	// appropriate contents.
	st := zipstore.New(zf, nil)
	mustGet := func(key, want string, ok bool) {
		data, err := st.Get(ctx, key)
		if (err == nil) != ok {
			t.Errorf("Get %q: err=%v, want error %v", key, err, ok)
		}
		if got := string(data); got != want {
			t.Errorf("Get %q: got %q, want %q", key, got, want)
		}
	}

	if n, err := st.Len(ctx); err != nil {
		t.Errorf("Len: unexpected error: %v", err)
	} else if n != 5 {
		t.Errorf("Len: got %d, want 5", n)
	}

	mustGet("1000", "one-thousand", true)
	mustGet("8675309", "jenny", true)
	mustGet("bogus", "", false)
	mustGet("68000", "sixty-eight thousand", true)
	mustGet("--------", "", false)

	wantKeys := []string{"1000", "2048", "68000", "8675309"} // N.B. excludes "0"
	var gotKeys []string
	if err := st.List(ctx, "1", func(key string) error {
		gotKeys = append(gotKeys, key)
		return nil
	}); err != nil {
		t.Errorf("List: unexpected error: %v", err)
	}
	if diff := cmp.Diff(gotKeys, wantKeys); diff != "" {
		t.Errorf("List: wrong keys (+got, -want):\n%s", diff)
	}

	// Verify that the write methods of the interface report errors.
	if err := st.Delete(ctx, "0"); err == nil {
		t.Error("Delete should report an error, but did not")
	}
	if err := st.Put(ctx, blob.PutOptions{Key: "Q"}); err == nil {
		t.Error("Put should report an error, but did not")
	}

	if err := st.Close(ctx); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
