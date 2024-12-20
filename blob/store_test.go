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

package blob_test

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	gocmp "github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSentinelErrors(t *testing.T) {
	plain := errors.New("it's not for you")
	keyExists := fmt.Errorf("test: %w", blob.ErrKeyExists)
	keyNotFound := fmt.Errorf("test: %w", blob.ErrKeyNotFound)

	t.Run("ErrorsIs", func(t *testing.T) {
		tests := []struct {
			input error
			is    error
			want  bool
		}{
			{nil, blob.ErrKeyExists, false},
			{nil, blob.ErrKeyNotFound, false},
			{plain, blob.ErrKeyExists, false},
			{plain, blob.ErrKeyNotFound, false},
			{keyExists, blob.ErrKeyExists, true},
			{keyExists, blob.ErrKeyNotFound, false},
			{keyNotFound, blob.ErrKeyExists, false},
			{keyNotFound, blob.ErrKeyNotFound, true},
			{blob.KeyExists("x"), blob.ErrKeyExists, true},
			{blob.KeyExists("x"), blob.ErrKeyNotFound, false},
			{blob.KeyNotFound("y"), blob.ErrKeyExists, false},
			{blob.KeyNotFound("y"), blob.ErrKeyNotFound, true},
		}
		for _, test := range tests {
			got := errors.Is(test.input, test.is)
			if got != test.want {
				t.Errorf("Error %q is %q: got %v, want %v", test.input, test.is, got, test.want)
			}
		}
	})

	t.Run("ErrorChecks", func(t *testing.T) {
		tests := []struct {
			input error
			check func(error) bool
			want  bool
		}{
			{nil, blob.IsKeyExists, false},
			{nil, blob.IsKeyNotFound, false},
			{plain, blob.IsKeyExists, false},
			{plain, blob.IsKeyNotFound, false},
			{keyExists, blob.IsKeyExists, true},
			{keyExists, blob.IsKeyNotFound, false},
			{keyNotFound, blob.IsKeyExists, false},
			{keyNotFound, blob.IsKeyNotFound, true},
		}
		for i, test := range tests {

			got := test.check(test.input)
			if got != test.want {
				t.Errorf("[%d] Error %q check %q: got %v, want %v",
					i+1, test.input, funcBaseName(test.check), got, test.want)
			}
		}
	})
}

func funcBaseName(v any) string {
	_, name := path.Split(runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name())
	return name
}

func TestKeyError(t *testing.T) {
	const needle = "magic test key"

	tests := []struct {
		input error
		base  error
	}{
		{blob.KeyExists(needle), blob.ErrKeyExists},
		{blob.KeyNotFound(needle), blob.ErrKeyNotFound},
	}
	for _, test := range tests {
		v, ok := test.input.(*blob.KeyError)
		if !ok {
			t.Errorf("Error %q is not a KeyError", test.input)
			continue
		}
		if v.Key != needle {
			t.Errorf("Error %q: got key %q, want %q", test.input, v.Key, needle)
		}
		if v.Err != test.base {
			t.Errorf("Error %q: got base %v, want %v", test.input, v.Err, test.base)
		}
	}
}

func TestListSyncKeyer(t *testing.T) {
	kv := memstore.NewKV().Init(map[string]string{
		"1": "one",
		"2": "two",
		"3": "three",
		"4": "four",
		"5": "five",
	})
	sk := blob.ListSyncKeyer{KV: kv}
	ctx := context.Background()
	check := func(t *testing.T, keys []string, want ...string) {
		t.Helper()
		got, err := sk.SyncKeys(ctx, keys)
		if err != nil {
			t.Fatalf("SyncKeys: unexpected error: %v", err)
		} else if diff := gocmp.Diff(got, want, cmpopts.EquateEmpty()); diff != "" {
			// N.B. We do care about order here, because the wrapper promises it.
			t.Fatalf("SyncKeys (-got, +want):\n%s", diff)
		}
	}

	t.Run("Empty", func(t *testing.T) {
		check(t, nil)
	})
	t.Run("NoneMissing", func(t *testing.T) {
		check(t, []string{"1", "3", "4"})
	})
	t.Run("SomeMissing", func(t *testing.T) {
		check(t, []string{"1", "6", "4", "7"}, "6", "7")
	})
	t.Run("AllMissing", func(t *testing.T) {
		check(t, []string{"10", "50", "90", "0", "8"}, "0", "10", "50", "8", "90")
	})
}
