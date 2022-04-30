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

package prefixed_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/prefixed"
	"github.com/google/go-cmp/cmp"
)

func TestStore(t *testing.T) {
	m := memstore.New()
	p := prefixed.New(m).Derive("POG:")
	storetest.Run(t, p)
}

func mustPut(t *testing.T, s blob.Store, key, val string) {
	t.Helper()
	if err := s.Put(context.Background(), blob.PutOptions{
		Key:     key,
		Data:    []byte(val),
		Replace: true,
	}); err != nil {
		t.Errorf("Put %q=%q failed: %v", key, val, err)
	}
}

func TestPrefixes(t *testing.T) {
	m := memstore.New()
	p1 := prefixed.New(m).Derive("A:")
	p2 := p1.Derive("B:")

	// Verify that the keys that arrive in the underlying store reflect the
	// correct prefixes, and that the namespaces are disjoint as long as the
	// prefixes are disjoint.
	mustPut(t, p1, "foo", "bar")
	mustPut(t, p2, "foo", "baz")
	mustPut(t, p1, "xyzzy", "plugh")
	mustPut(t, p2, "foo", "quux")

	snap := m.Snapshot(make(map[string]string))

	if diff := cmp.Diff(map[string]string{
		"A:foo":   "bar",
		"B:foo":   "quux",
		"A:xyzzy": "plugh",
	}, snap); diff != "" {
		t.Errorf("Prefixed store: wrong content (-want, +got)\n%s", diff)
	}
}
