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

package dbkey_test

import (
	"testing"

	"github.com/creachadair/ffs/storage/dbkey"
)

type keyOp = func(dbkey.Prefix) dbkey.Prefix

func subs(names ...string) keyOp {
	return func(p dbkey.Prefix) dbkey.Prefix {
		for _, name := range names {
			p = p.Sub(name)
		}
		return p
	}
}

func subKV(names ...string) keyOp {
	return func(p dbkey.Prefix) dbkey.Prefix {
		for _, name := range names[:len(names)-1] {
			p = p.Sub(name)
		}
		return p.Keyspace(names[len(names)-1])
	}
}

func TestPrefix(t *testing.T) {
	tests := []struct {
		base    dbkey.Prefix
		op      keyOp
		wantHex string
	}{
		{"", subs(), ""},
		{"ABC", subs(), "414243"},
		{"", subs("sub1"), "6e3617aaf658"},
		{"", subKV("sub1"), "fea73fac92bd"}, // KV does not collide with sub

		// Verify that we get to the same place regardless where we start.
		{"", subs("sub1", "sub2"), "d404af12ddfb"},
		{"\xd4\x04\xaf\x12\xdd\xfb", subKV("ks"), "47b256a71b00"},
		{"", subKV("sub1", "sub2", "ks"), "47b256a71b00"},
		{"\x6e\x36\x17\xaa\xf6\x58", subKV("sub2", "ks"), "47b256a71b00"},
	}
	for _, tc := range tests {
		got := tc.op(tc.base)
		if got.String() != tc.wantHex {
			t.Errorf("Prefix %q derivation: got %q, want %q", tc.base, got, tc.wantHex)
		}
	}
}

func TestAddRemove(t *testing.T) {
	tests := []struct {
		prefix dbkey.Prefix
		input  string
		added  string
	}{
		{"", "", ""},
		{"", "apple", "apple"},
		{"pear", "apple", "pearapple"},
		{"plum", "", "plum"},
		{"cherry:", "plum", "cherry:plum"},
	}
	for _, tc := range tests {
		added := tc.prefix.Add(tc.input)
		if added != tc.added {
			t.Errorf("%q.Add(%q): got %q, want %q", tc.prefix, tc.input, added, tc.added)
		}

		// Verify that Remove round-trips the key.
		rem := tc.prefix.Remove(added)
		if rem != tc.input {
			t.Errorf("%q.Remove(%q): got %q, want %q", tc.prefix, added, rem, tc.input)
		}
	}
}
