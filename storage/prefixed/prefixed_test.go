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

func runList(p blob.Store, want ...string) func(t *testing.T) {
	return func(t *testing.T) {
		var got []string
		if err := p.List(context.Background(), "", func(key string) error {
			got = append(got, key)
			return nil
		}); err != nil {
			t.Fatalf("List p1: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("p keys: (-want, +got)\n%s", diff)
		}
	}
}

func TestPrefixes(t *testing.T) {
	m := memstore.New()
	p1 := prefixed.New(m).Derive("A:")
	p2 := p1.Derive("B:")
	p3 := prefixed.NewCAS(selfCAS{m}).Derive("C:")

	// Verify that the keys that arrive in the underlying store reflect the
	// correct prefixes, and that the namespaces are disjoint as long as the
	// prefixes are disjoint.
	mustPut(t, p1, "foo", "bar")
	mustPut(t, p2, "foo", "baz")
	mustPut(t, p1, "xyzzy", "plugh")
	mustPut(t, p2, "foo", "quux")
	mustPut(t, p2, "bar", "plover")
	mustPut(t, p3, "foo", "bizzle")
	mustPut(t, p3, "zuul", "dana")

	// Verify that a CAS key is properly prefixed, but that the key returned
	// does not have the prefix.
	ckey, err := p3.CASPut(context.Background(), []byte("hexxus"))
	if err != nil {
		t.Errorf("p3 CAS put: %v", err)
	}

	t.Run("Snapshot", func(t *testing.T) {
		snap := m.Snapshot(make(map[string]string))

		if diff := cmp.Diff(map[string]string{
			"A:foo":     "bar",
			"A:xyzzy":   "plugh",
			"B:foo":     "quux",
			"B:bar":     "plover",
			"C:foo":     "bizzle",
			"C:zuul":    "dana",   // from p3.Put
			"C:" + ckey: "hexxus", // from p3.CASPut
		}, snap); diff != "" {
			t.Errorf("Prefixed store: wrong content (-want, +got)\n%s", diff)
		}
	})

	t.Run("List-1", runList(p1, "foo", "xyzzy"))
	t.Run("List-2", runList(p2, "bar", "foo"))
	t.Run("List-3", runList(p3, "foo", "hexxus", "zuul"))
}

type selfCAS struct {
	blob.Store
}

func (selfCAS) CASKey(_ context.Context, data []byte) (string, error) {
	return string(data), nil
}

func (s selfCAS) CASPut(ctx context.Context, data []byte) (string, error) {
	key := string(data)
	err := s.Put(ctx, blob.PutOptions{Key: key, Data: data})
	if err != nil && !blob.IsKeyExists(err) {
		return key, err
	}
	return key, nil
}
