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

package suffixed_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/suffixed"
	"github.com/google/go-cmp/cmp"
)

var (
	_ blob.Store = suffixed.Store{}
	_ blob.CAS   = suffixed.CAS{}
)

func TestStore(t *testing.T) {
	m := memstore.New()
	p := suffixed.New(m).Derive(":POG")
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

func TestSuffixes(t *testing.T) {
	m := memstore.New()
	p1 := suffixed.New(m).Derive(":A")
	p2 := p1.Derive(":B")
	p3 := suffixed.NewCAS(selfCAS{m}).Derive(":C")

	// Verify that the keys that arrive in the underlying store reflect the
	// correct suffixes, and that the namespaces are disjoint as long as the
	// suffixes are disjoint.
	mustPut(t, p1, "foo", "bar")
	mustPut(t, p2, "foo", "baz")
	mustPut(t, p1, "xyzzy", "plugh")
	mustPut(t, p2, "foo", "quux")
	mustPut(t, p2, "bar", "plover")
	mustPut(t, p3, "foo", "bizzle")
	mustPut(t, p3, "zuul", "dana")

	// Verify that a CAS key is properly suffixed, but that the key returned
	// does not have the suffixed.
	ckey, err := p3.CASPut(context.Background(), []byte("hexxus"))
	if err != nil {
		t.Errorf("p3 CAS put: %v", err)
	}

	t.Run("Snapshot", func(t *testing.T) {
		snap := m.Snapshot(make(map[string]string))

		if diff := cmp.Diff(map[string]string{
			"foo:A":     "bar",
			"xyzzy:A":   "plugh",
			"foo:B":     "quux",
			"bar:B":     "plover",
			"foo:C":     "bizzle",
			"zuul:C":    "dana",   // from p3.Put
			ckey + ":C": "hexxus", // from p3.CASPut
		}, snap); diff != "" {
			t.Errorf("Suffixed store: wrong content (-want, +got)\n%s", diff)
		}
	})

	t.Run("List-1", runList(p1, "foo", "xyzzy"))
	t.Run("List-2", runList(p2, "bar", "foo"))
	t.Run("List-3", runList(p3, "foo", "hexxus", "zuul"))
}

func TestLen(t *testing.T) {
	m := memstore.New()
	p0 := suffixed.New(m)
	p1 := p0.Derive(":X")
	p2 := p1.Derive(":Y")

	mustPut(t, p0, "starfruit", "0")
	mustPut(t, p1, "apple", "1")
	mustPut(t, p1, "pear", "1")
	mustPut(t, p2, "plum", "2")
	mustPut(t, p2, "cherry", "2")
	mustPut(t, p2, "mango", "2")

	tests := []struct {
		store blob.Store
		want  int64
	}{
		{m, 6},  // base store: all the keys
		{p0, 6}, // empty suffix, equivalent to base
		{p1, 2},
		{p2, 3},
	}
	for _, test := range tests {
		got, err := test.store.Len(context.Background())
		if err != nil {
			t.Errorf("Len failed: %v", err)
			continue
		}
		if got != test.want {
			t.Errorf("Len: got %v, want %v", got, test.want)
		}
	}
}

func TestNesting(t *testing.T) {
	m := memstore.New()
	p1 := suffixed.New(m)
	c1 := suffixed.NewCAS(selfCAS{m})

	t.Run("Renew", func(t *testing.T) {
		if p2 := suffixed.New(p1); p2 != p1 {
			t.Errorf("Wrapped new: got %v, want %v", p2, p1)
		}
	})
	t.Run("RenewCAS", func(t *testing.T) {
		if c2 := suffixed.NewCAS(c1); c2 != c1 {
			t.Errorf("Wrapped new: got %v, want %v", c2, c1)
		}
	})

	p2 := p1.Derive(":X")
	p3 := p1.Derive(":Y")
	p4 := p2.Derive(":Z") // derivation replaces existing keys
	p5 := p2.Derive("")   // empty suffix goes back to the original

	mustPut(t, p1, "foo", "1")
	mustPut(t, p2, "foo", "2")
	mustPut(t, p3, "foo", "3")
	mustPut(t, p4, "foo", "4")
	mustPut(t, p5, "bar", "5")

	t.Run("Snapshot", func(t *testing.T) {
		snap := m.Snapshot(make(map[string]string))

		if diff := cmp.Diff(map[string]string{
			"foo":   "1", // unsuffixed from p1
			"foo:X": "2", // from p2
			"foo:Y": "3", // from p3
			"foo:Z": "4", // from p4
			"bar":   "5", // from p5 (eqv. to p1)
		}, snap); diff != "" {
			t.Errorf("Suffixed store: wrong content (-want, +got)\n%s", diff)
		}
	})
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