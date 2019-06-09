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

// Package storetest provides correctness tests for impelmentations of the
// blob.Store interface.
package storetest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/xerrors"
)

type op = func(context.Context, *testing.T, blob.Store)

var script = []op{
	// Verify that the store is initially empty.
	opList(""),
	opLen(0),

	// Get for a non-existing key should report an error.
	opGet("nonesuch", "", blob.ErrKeyNotFound),

	// Delete for a non-existing key should report an error.
	opDelete("nonesuch", blob.ErrKeyNotFound),

	// Put a value in and verify that it is recorded.
	opPut("fruit", "apple", false, nil),
	opSize("fruit", 5, nil),
	opGet("fruit", "apple", nil),

	// Put for an existing key fails when replace is false.
	opPut("fruit", "pear", false, blob.ErrKeyExists),

	// Put for an existing key works when replace is true.
	opPut("fruit", "pear", true, nil),
	opSize("fruit", 4, nil),
	opGet("fruit", "pear", nil),

	opList("", "fruit"),
	opLen(1),

	// Add some additional keys.
	opPut("nut", "hazelnut", false, nil),
	opPut("animal", "cat", false, nil),
	opPut("beverage", "piña colada", false, nil),

	opList("", "animal", "beverage", "fruit", "nut"),
	opLen(4),

	// Verify that deletion works as expected.
	opGet("animal", "cat", nil),
	opDelete("animal", nil),
	opDelete("animal", blob.ErrKeyNotFound),
	opGet("animal", "", blob.ErrKeyNotFound),

	opList("", "beverage", "fruit", "nut"),
	opLen(3),

	// Verify that sizes are reported correctly.
	opSize("beverage", 12, nil),
	opSize("fruit", 4, nil),
	opSize("nut", 8, nil),

	// Re-inserting a deleted key does not report a conflict.
	opPut("animal", "badger", false, nil),
	opSize("animal", 6, nil),

	opDelete("beverage", nil),
	opList("", "animal", "fruit", "nut"),
	opLen(3),

	// An empty key is valid and works normally.
	opPut("", "ahoy there", false, nil),
	opList("", "", "animal", "fruit", "nut"),
	opGet("", "ahoy there", nil),
	opSize("", 10, nil),

	// Check list starting points.
	opList("a", "animal", "fruit", "nut"),
	opList("animal", "animal", "fruit", "nut"),
	opList("animated", "fruit", "nut"),
	opList("goofy", "nut"),
	opList("nutty"),

	// Clean up.
	opLen(4),
	opDelete("", nil),
	opLen(3),
	opDelete("animal", nil),
	opLen(2),
	opDelete("fruit", nil),
	opLen(1),
	opDelete("nut", nil),
	opLen(0),
	opList(""),
}

func opGet(key, want string, werr error) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		got, err := s.Get(ctx, key)
		if !errorOK(err, werr) {
			t.Errorf("s.Get(%q): got error: %v, want: %v", key, err, werr)
		} else if v := string(got); v != want {
			t.Errorf("s.Get(%q): got %#q, want %#q", key, v, want)
		}
	}
}

func opPut(key, data string, replace bool, werr error) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		err := s.Put(ctx, blob.PutOptions{
			Key:     key,
			Data:    []byte(data),
			Replace: replace,
		})
		if !errorOK(err, werr) {
			t.Errorf("s.Put(%q, %q, %v): got error: %v, want: %v", key, data, replace, err, werr)
		}
	}
}

func opSize(key string, want int64, werr error) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		got, err := s.Size(ctx, key)
		if !errorOK(err, werr) {
			t.Errorf("s.Size(%q): got error: %v, want: %v", key, err, werr)
		} else if got != want {
			t.Errorf("s.Size(%q): got %d, want %d", key, got, want)
		}
	}
}

func opDelete(key string, werr error) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		err := s.Delete(ctx, key)
		if !errorOK(err, werr) {
			t.Errorf("s.Delete(%q): got error: %v, want: %v", key, err, werr)
		}
	}
}

func opList(from string, want ...string) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		var got []string
		err := s.List(ctx, from, func(key string) error {
			got = append(got, key)
			return nil
		})
		if err != nil {
			t.Errorf("s.List(...): unexpected error: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("s.List(...): wrong keys (-want, +got):\n%s", diff)
		}
	}
}

func opLen(want int64) op {
	return func(ctx context.Context, t *testing.T, s blob.Store) {
		got, err := s.Len(ctx)
		if err != nil {
			t.Errorf("s.Len(): unexpected error: %v", err)
		}
		if got != want {
			t.Errorf("s.Len(): got %d, want %d", got, want)
		}
	}
}

func errorOK(err, werr error) bool {
	if werr == nil {
		return err == nil
	}
	return xerrors.Is(err, werr)
}

// Run applies the test script to empty store s, and reports any errors to t.
// After Run returns, the contents of s are garbage.
func Run(t *testing.T, s blob.Store) {
	ctx := context.Background()
	for _, op := range script {
		op(ctx, t, s)
	}

	// Exercise concurrency.
	const numWorkers = 16
	const numKeys = 16

	taskKey := func(task, key int) string {
		return fmt.Sprintf("task-%d-key-%d", task, key)
	}

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()

			for k := 1; k <= numKeys; k++ {
				key := taskKey(i, k)
				value := strconv.Itoa(k)
				if err := s.Put(ctx, blob.PutOptions{
					Key:     key,
					Data:    []byte(value),
					Replace: true,
				}); err != nil {
					t.Errorf("Task %d: s.Put(%q=%q) failed: %v", i, key, value, err)
				}
			}

			// List all the keys currently in the store, and pick out all those
			// that belong to this task.
			mine := fmt.Sprintf("task-%d-", i)
			got := make(map[string]bool)
			if err := s.List(ctx, "", func(key string) error {
				if strings.HasPrefix(key, mine) {
					got[key] = true
				}
				return nil
			}); err != nil {
				t.Errorf("Task %d: s.List failed: %v", i, err)
			}

			for k := 1; k <= numKeys; k++ {
				key := taskKey(i, k)
				if _, err := s.Get(ctx, key); err != nil {
					t.Errorf("Task %d: s.Get(%q) failed: %v", i, key, err)
				}

				// Verify that List did not miss any of this task's keys.
				if !got[key] {
					t.Errorf("Task %d: s.List missing key %q", i, key)
				}
			}

			for k := 1; k <= numKeys; k++ {
				key := taskKey(i, k)
				if err := s.Delete(ctx, key); err != nil {
					t.Errorf("Task %d: s.Delete(%q) failed: %v", i, key, err)
				}
			}
		}()
	}
	wg.Wait()
}
