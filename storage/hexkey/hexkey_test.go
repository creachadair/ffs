// Copyright 2024 Michael J. Fromberger. All Rights Reserved.
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

package hexkey_test

import (
	"strings"
	"testing"

	"github.com/creachadair/ffs/storage/hexkey"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      hexkey.Config
		input, want string
	}{
		{"PlainEmpty", hexkey.Config{}, "", ""},
		{"PlainKey", hexkey.Config{}, "\x01\x02\x03", "010203"},
		{"Prefix", hexkey.Config{Prefix: "foo"}, "0123", "foo/30313233"},
		{"Shard1", hexkey.Config{Shard: 1}, "\xab\xcd", "a/abcd"},
		{"Shard2", hexkey.Config{Shard: 2}, "\xab\xcd\xef", "ab/abcdef"},
		{"Shard3", hexkey.Config{Shard: 3}, "\x01\x02\x03\x04", "010/01020304"},
		{"EmptyShard", hexkey.Config{Shard: 3}, "", "---/-"},
		{"ShortShard", hexkey.Config{Shard: 3}, "\x01", "01-/01"},
		{"PrefixShard", hexkey.Config{Prefix: "foo", Shard: 4}, "ABCDE", "foo/4142/4142434445"},
		{"LongShard", hexkey.Config{Shard: 8}, "ABC", "414243--/414243"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := tc.config.Encode(tc.input)
			if enc != tc.want {
				t.Errorf("Encode %q: got %q, want %q", tc.input, enc, tc.want)
			}

			dec, err := tc.config.Decode(enc)
			if err != nil {
				t.Errorf("Decode %q: unexpected error: %v", enc, err)
			} else if dec != tc.input {
				t.Errorf("Decode %q: got %q, want %q", enc, dec, tc.input)
			}
		})
	}
}

func TestStart(t *testing.T) {
	tests := []struct {
		name        string
		config      hexkey.Config
		input, want string
	}{
		{"AllEmpty", hexkey.Config{}, "", ""},
		{"NoShard", hexkey.Config{}, "\x01\x23", "0123"},
		{"Shard2", hexkey.Config{Shard: 2}, "\x01\x23\x45", "01/012345"},
		{"Shard3", hexkey.Config{Shard: 3}, "\x01\x23\x45", "012/012345"},
		{"Shard10", hexkey.Config{Shard: 10}, "\x01\x23\x45", "012345"},
		{"Prefix0", hexkey.Config{Prefix: "ok"}, "\xab\xcd", "ok/abcd"},
		{"Prefix2", hexkey.Config{Prefix: "ok", Shard: 2}, "\xab\xcd\xef", "ok/ab/abcdef"},
		{"Prefix5", hexkey.Config{Prefix: "ok", Shard: 5}, "\xab\xcd\xef", "ok/abcde/abcdef"},
		{"Prefix10", hexkey.Config{Prefix: "ok", Shard: 10}, "\xab\xcd\xef", "ok/abcdef"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.config.Start(tc.input)
			if got != tc.want {
				t.Errorf("Start %q: got %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	estr := hexkey.ErrNotMyKey.Error()
	tests := []struct {
		name    string
		config  hexkey.Config
		input   string
		errtext string
	}{
		{"NonHex", hexkey.Config{}, "garbage", "invalid byte"},
		{"NoPrefix", hexkey.Config{Prefix: "foo"}, "010203", estr},
		{"BadShard", hexkey.Config{Shard: 3}, "0a/0b0c0d", estr},
		{"EmptyTail", hexkey.Config{Shard: 3}, "0a0/", estr},
		{"BadHex", hexkey.Config{Shard: 3}, "0a0/0a0", "odd length hex"},
		{"PrefixBadHex", hexkey.Config{Prefix: "foo", Shard: 3}, "foo/abc/abcdefgh", "invalid byte"},
		{"PrefixShort", hexkey.Config{Prefix: "bar", Shard: 3}, "foo/012", estr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dec, err := tc.config.Decode(tc.input)
			if err == nil {
				t.Errorf("Decode %q: got %q, want error", tc.input, dec)
			} else if got := err.Error(); !strings.Contains(got, tc.errtext) {
				t.Errorf("Decode %q: got %v, want %q", tc.input, err, tc.errtext)
			}
		})
	}
}
