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

func TestRegisterOpen(t *testing.T) {
	r := new(store.Registry)

	// Registering an invalid tag should fail.
	for _, tag := range []string{"", ":", "foo::", "foo:bar", "foo:bar:", ":baz"} {
		if err := r.Register(tag, newMemStore); !errors.Is(err, store.ErrInvalidTag) {
			t.Errorf("Register(%q, ...): got %v, want %v", tag, err, store.ErrInvalidTag)
		}
	}

	// Registering a fresh name should succeed.
	if err := r.Register("mem", newMemStore); err != nil {
		t.Errorf("Register(mem, ...) failed: %v", err)
	}

	// Registering a fresh name with a nil Opener should fail.
	if err := r.Register("bogus", nil); err == nil {
		t.Error("Register(bogus, nil) did not fail")
	} else {
		t.Logf("Register(bogus, nil) correctly failed: %v", err)
	}

	// Re-registering an existing name should fail.
	if err := r.Register("mem:", newMemStore); !errors.Is(err, store.ErrDuplicateTag) {
		t.Errorf("Register(mem:, ...): got %v, want %v", err, store.ErrDuplicateTag)
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
