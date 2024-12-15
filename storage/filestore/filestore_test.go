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
	"context"
	"flag"
	"os"
	"testing"

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
	storetest.Run(t, storetest.NopCloser(s))
}

func TestNesting(t *testing.T) {
	dir := t.TempDir()
	t.Logf("Test store: %s", dir)

	s, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("Creating store in %q: %v", dir, err)
	}

	ctx := context.Background()
	k1 := storetest.SubKeyspace(t, ctx, s, "foo", "bar")
	k2 := storetest.SubKeyspace(t, ctx, s, "foo/_bar")

	if k1d, k2d := k1.(filestore.KV).Dir(), k2.(filestore.KV).Dir(); k1d == k2d {
		t.Fatalf("Equal directories: %q", k1d)
	}
}
