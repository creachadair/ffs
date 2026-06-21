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

package filestore_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/filestore"
)

var keepOutput = flag.Bool("keep", false, "Keep test output after running")

func TestStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "filestore")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	t.Logf("Test store: %s", dir)
	if !*keepOutput {
		defer os.RemoveAll(dir) // best effort cleanup
	}

	s, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	storetest.Run(t, s)
}

func TestNesting(t *testing.T) {
	dir := t.TempDir()
	t.Logf("Test store: %s", dir)

	s, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}

	ctx := t.Context()
	k1 := storetest.SubKV(t, ctx, s, "foo", "bar")
	k2 := storetest.SubKV(t, ctx, s, "foo/_bar")

	if k1d, k2d := k1.(filestore.KV).Dir(), k2.(filestore.KV).Dir(); k1d == k2d {
		t.Fatalf("Equal directories: %q", k1d)
	}
}

func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	s, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}
	kv := storetest.SubKV(t, t.Context(), s, "non-empty", "stuff")
	if err := kv.Put(t.Context(), blob.PutOptions{
		Key:     "something",
		Data:    []byte("hello"),
		Replace: true,
	}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run a standard test in a substore, to generate some directories.
	// The test deletes all its values, so those directories should wind up empty.
	sub, err := s.Sub(t.Context(), "non-empty")
	if err != nil {
		t.Fatalf("Sub failed: %v", err)
	}
	storetest.Run(t, storetest.NopCloser(sub))

	// Now close the store to trigger the cleanup pass.
	if err := s.Close(t.Context()); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify that an empty KV from the test is gone.
	if _, err := os.Stat(filepath.Join(dir, "kv_6f6e65")); err == nil {
		t.Error("Found empty KV")
	}

	// Verify that a non-empty key unaffected by the test is still present.
	if got, err := kv.Get(t.Context(), "something"); err != nil {
		t.Errorf("Get failed: %v", err)
	} else if string(got) != "hello" {
		t.Errorf("Get returned %q, want hello", got)
	}
}

func BenchmarkStore(b *testing.B) {
	s, err := filestore.New(b.TempDir())
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	kv, err := s.KV(b.Context(), "benchmark")
	if err != nil {
		b.Fatalf("KV: %v", err)
	}
	storetest.BenchmarkKV(b, kv)
}
