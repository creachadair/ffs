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

package affixed_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/affixed"
	"github.com/google/go-cmp/cmp"
)

var (
	_ blob.KV  = affixed.KV{}
	_ blob.CAS = affixed.CAS{}
)

func TestKV(t *testing.T) {
	m := memstore.New()
	p := affixed.New(m).Derive("POG:", ":CHAMP")
	storetest.Run(t, p)
}

func mustPut(t *testing.T, s blob.KV, key, val string) {
	t.Helper()
	if err := s.Put(context.Background(), blob.PutOptions{
		Key:     key,
		Data:    []byte(val),
		Replace: true,
	}); err != nil {
		t.Errorf("Put %q=%q failed: %v", key, val, err)
	}
}

func mustGet(t *testing.T, s blob.KV, key, val string) {
	t.Helper()
	if got, err := s.Get(context.Background(), key); err != nil {
		t.Errorf("Get %q failed: %v", key, err)
	} else if string(got) != val {
		t.Errorf("Get %q: got %q, want %q", key, got, val)
	}
}

func runList(p blob.KV, want ...string) func(t *testing.T) {
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

func TestAffixes(t *testing.T) {
	m := memstore.New()
	p1 := affixed.New(m).Derive("A:", ":A")
	p2 := p1.Derive("B:", ":B")
	p3 := affixed.NewCAS(selfCAS{m}).Derive("C:", ":C")

	// Verify that the keys that arrive in the underlying store reflect the
	// correct affixes, and that the namespaces are disjoint as long as the
	// affixes are disjoint.
	mustPut(t, p1, "foo", "bar")
	mustPut(t, p2, "foo", "baz")
	mustPut(t, p1, "xyzzy", "plugh")
	mustPut(t, p2, "foo", "quux")
	mustPut(t, p2, "bar", "plover")
	mustPut(t, p3, "foo", "bizzle")
	mustPut(t, p3, "zuul", "dana")

	// Make sure the values round-trip.
	mustGet(t, p1, "foo", "bar")
	mustGet(t, p1, "xyzzy", "plugh")
	mustGet(t, p2, "foo", "quux")
	mustGet(t, p2, "bar", "plover")
	mustGet(t, p3, "foo", "bizzle")
	mustGet(t, p3, "zuul", "dana")

	// Verify that a CAS key is properly affixed.
	ckey, err := p3.CASPut(context.Background(), blob.CASPutOptions{Data: []byte("hexxus")})
	if err != nil {
		t.Errorf("p3 CAS put: %v", err)
	}

	mustGet(t, p3, ckey, "hexxus")

	t.Run("Snapshot", func(t *testing.T) {
		snap := m.Snapshot(make(map[string]string))

		if diff := cmp.Diff(map[string]string{
			"A:foo:A":          "bar",
			"A:xyzzy:A":        "plugh",
			"B:foo:B":          "quux",
			"B:bar:B":          "plover",
			"C:foo:C":          "bizzle",
			"C:zuul:C":         "dana",   // from p3.Put
			"C:" + ckey + ":C": "hexxus", // from p3.CASPut
		}, snap); diff != "" {
			t.Errorf("Affixed store: wrong content (-want, +got)\n%s", diff)
		}
	})

	t.Run("List-1", runList(p1, "foo", "xyzzy"))
	t.Run("List-2", runList(p2, "bar", "foo"))
	t.Run("List-3", runList(p3, "foo", "hexxus", "zuul"))
}

func TestLen(t *testing.T) {
	m := memstore.New()
	p0 := affixed.New(m)
	p1 := p0.Derive("X:", ":X")
	p2 := p1.WithPrefix("Y:")

	mustPut(t, p0, "starfruit", "0")
	mustPut(t, p1, "apple", "1")
	mustPut(t, p1, "pear", "1")
	mustPut(t, p2, "plum", "2")
	mustPut(t, p2, "cherry", "2")
	mustPut(t, p2, "mango", "2")

	tests := []struct {
		store blob.KV
		want  int64
	}{
		{m, 6},  // base store: all the keys
		{p0, 6}, // empty prefix, equivalent to base
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
			t.Errorf("Len %+v: got %v, want %v", test.store, got, test.want)
		}
	}
}

func TestNesting(t *testing.T) {
	m := memstore.New()
	p1 := affixed.New(m)
	c1 := affixed.NewCAS(selfCAS{m})

	t.Run("Renew", func(t *testing.T) {
		if p2 := affixed.New(p1); p2 != p1 {
			t.Errorf("Wrapped new: got %v, want %v", p2, p1)
		}
	})
	t.Run("RenewCAS", func(t *testing.T) {
		if c2 := affixed.NewCAS(c1); c2 != c1 {
			t.Errorf("Wrapped new: got %v, want %v", c2, c1)
		}
	})

	p2 := p1.Derive("X:", ":X")
	p3 := p1.Derive("Y:", ":Y")
	p4 := p2.Derive("Z:", ":Z") // derivation replaces existing keys
	p5 := p2.Derive("", "")     // empty affix goes back to the original
	p6 := p2.WithPrefix("Q:")   // as p2, with prefix altered
	p7 := p2.WithSuffix(":Q")   // as P2, with suffix altered

	mustPut(t, p1, "foo", "1")
	mustPut(t, p2, "foo", "2")
	mustPut(t, p3, "foo", "3")
	mustPut(t, p4, "foo", "4")
	mustPut(t, p5, "bar", "5")
	mustPut(t, p6, "bar", "6")
	mustPut(t, p7, "bar", "7")

	t.Run("Snapshot", func(t *testing.T) {
		snap := m.Snapshot(make(map[string]string))

		if diff := cmp.Diff(map[string]string{
			"foo":     "1", // unaffixed from p1
			"X:foo:X": "2", // from p2
			"Y:foo:Y": "3", // from p3
			"Z:foo:Z": "4", // from p4
			"bar":     "5", // from p5 (eqv. to p1)
			"Q:bar:X": "6", // from p6
			"X:bar:Q": "7", // from p7
		}, snap); diff != "" {
			t.Errorf("Affixed store: wrong content (-want, +got)\n%s", diff)
		}
	})
}

type selfCAS struct {
	blob.KV
}

func (selfCAS) CASKey(_ context.Context, opts blob.CASPutOptions) (string, error) {
	return opts.Prefix + string(opts.Data) + opts.Suffix, nil
}

func (s selfCAS) CASPut(ctx context.Context, opts blob.CASPutOptions) (string, error) {
	key := opts.Prefix + string(opts.Data) + opts.Suffix
	err := s.Put(ctx, blob.PutOptions{Key: key, Data: opts.Data})
	if err != nil && !blob.IsKeyExists(err) {
		return key, err
	}
	return key, nil
}
