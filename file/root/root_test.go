// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package root_test

import (
	"io/fs"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestRoot(t *testing.T) {
	kv := memstore.NewKV()
	cas := blob.CASFromKV(kv)
	ctx := t.Context()

	r := root.New(kv, &root.Options{
		Description: "Test root",
		IndexKey:    "hey you get off of my cloud",
		ChainKey:    "->>",
	})

	// Create a new empty file to use as the root file.
	rfKey, err := file.New(cas, &file.NewOptions{
		Stat:        &file.Stat{Mode: fs.ModeDir | 0755},
		PersistStat: true,
	}).Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing root file: %v", err)
	}

	// Saving the root blob to storage should fail if there is no file key set.
	if err := r.Save(ctx, "test-root"); err == nil {
		t.Error("Save should not have succeeded with an empty FileKey")
	}

	// Saving the root should succeed once the file key is present.
	r.FileKey = rfKey
	if err := r.Save(ctx, "test-root"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load a copy of the root back in and make sure it looks sensible.
	rc, err := root.Open(ctx, kv, "test-root")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Check the reloaded contents match.
	if diff := cmp.Diff(rc, r, cmpopts.IgnoreUnexported(root.Root{})); diff != "" {
		t.Errorf("Loaded root (-got, +want):\n%s", diff)
	}

	// Verify that we can save and reload a chained root.
	ckey, err := r.SaveChain(ctx, cas)
	if err != nil {
		t.Fatalf("SaveChain failed: %v", err)
	}
	rc.ChainKey = ckey

	// Verify that we got the expected root back from this.
	rcc, err := rc.Chain(ctx, cas)
	if err != nil {
		t.Fatalf("Load chain failed: %v", err)
	}
	if diff := cmp.Diff(rcc, r, cmpopts.IgnoreUnexported(root.Root{})); diff != "" {
		t.Errorf("Loaded chained root (-got, +want):\n%s", diff)
	}
}
