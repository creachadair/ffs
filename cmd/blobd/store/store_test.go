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

package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/store"
)

var errBadAddress = errors.New("bad memstore address")

func newMemStore(_ context.Context, addr string) (blob.Store, error) {
	if addr != "" {
		return nil, errBadAddress
	}
	return memstore.New(), nil
}

func TestRegistryOpen(t *testing.T) {
	r := store.Registry{
		"mem": newMemStore,
	}

	ctx := context.Background()

	// Opening an existing tag should succeed.
	if s, err := r.Open(ctx, "mem"); err != nil {
		t.Errorf("Open(ctx, mem) failed: %v", err)
	} else {
		t.Logf("Open OK, store=%[1]T (%[1]p)", s)
	}

	// Errors reported by the opener should be propagated.
	if s, err := r.Open(ctx, "mem:garbage"); !errors.Is(err, errBadAddress) {
		t.Errorf("Open(ctx, mem:garbage): got (%[1]T (%[1]p), %v), want (nil, %v)", s, err, errBadAddress)
	}

	// Opening a non-existing tag should fail.
	s, err := r.Open(ctx, "http://localhost:8080")
	if !errors.Is(err, store.ErrInvalidAddress) || s != nil {
		t.Errorf("Open(ctx, URL): got (%[1]T (%[1]p), %v), want (nil, %v)",
			s, err, store.ErrInvalidAddress)
	}
}
