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
	"os"
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
		OwnerKey:    "whatever",
	})

	// Create a new empty file to use as the root file.
	rfKey, err := file.New(cas, &file.NewOptions{
		Stat: &file.Stat{Mode: os.ModeDir | 0755},
	}).Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing root file: %v", err)
	}

	// Verify that setting the root file gives the right values.
	if _, err = r.SetFile(ctx, rfKey); err != nil {
		t.Errorf("SetFile: unexpected error: %v", err)
	} else if r.FileKey != rfKey {
		t.Errorf("SetFile: got key %q, want %q", r.FileKey, rfKey)
	}

	// Save the root blob to storage.
	if err := r.Save(ctx, "test-root"); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load a copy of the root back in and make sure it looks sensible.
	rc, err := root.Open(ctx, cas, "test-root")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Check the root file.
	if rfc, err := rc.File(ctx); err != nil {
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
	if rc.OwnerKey != r.OwnerKey {
		t.Errorf("Loaded owner key: got %q, want %q", rc.OwnerKey, rc.OwnerKey)
	}
}
