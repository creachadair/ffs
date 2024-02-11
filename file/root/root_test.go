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
	"context"
	"crypto/sha1"
	"io/fs"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
)

func TestRoot(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()

	r := root.New(cas, &root.Options{
		Description: "Test root",
		IndexKey:    "hey you get off of my cloud",
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
	if err := r.Save(ctx, "test-root", true); err == nil {
		t.Error("Save should not have succeeded with an empty FileKey")
	}

	// Saving the root should succeed once the file key is present.
	r.FileKey = rfKey
	if err := r.Save(ctx, "test-root", true); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load a copy of the root back in and make sure it looks sensible.
	rc, err := root.Open(ctx, cas, "test-root")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Check the root file.
	if rfc, err := rc.File(ctx, nil); err != nil {
		t.Errorf("Loading root file: %v", err)
	} else if rfcKey, err := rfc.Flush(ctx); err != nil {
		t.Errorf("Flush failed: %v", err)
	} else if rfcKey != rfKey {
		t.Errorf("Loaded root file key: got %q, want %q", rfcKey, rfKey)
	}

	// Check exported fields.
	if rc.Description != r.Description {
		t.Errorf("Loaded desc: got %q, want %q", rc.Description, r.Description)
	}
	if rc.IndexKey != r.IndexKey {
		t.Errorf("Loaded index key: got %q, want %q", rc.IndexKey, r.IndexKey)
	}
}
