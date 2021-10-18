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

package index_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffs/index/indexpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
)

func TestIndex(t *testing.T) {
	keyData, err := ioutil.ReadFile("testdata/keys.txt")
	if err != nil {
		t.Fatalf("Reading keys: %v", err)
	}
	keys := strings.Split(strings.TrimSpace(string(keyData)), "\n")
	t.Logf("Read %d bytes (%d keys) from keys.txt", len(keyData), len(keys))

	idx := index.New(len(keys), &index.Options{
		FalsePositiveRate: 0.01,
	})

	// Add keys at even offsets, skip keys at odd ones.
	// Thus we expect half the keys to be missing.
	var numAdded, totalKeyBytes int
	for i, key := range keys {
		if i%2 == 0 {
			idx.Add(key)
			numAdded++
			totalKeyBytes += len(key)
		}
	}
	t.Logf("Added %d keys to the index", numAdded)

	stats := idx.Stats()
	t.Logf("Index stats: %d keys, %d filter bits (m), %d hash seeds",
		stats.NumKeys, stats.FilterBits, stats.NumHashes)
	if stats.NumKeys != numAdded {
		t.Errorf("Wrong number of keys: got %d, want %d", stats.NumKeys, numAdded)
	}
	if n := idx.Len(); n != stats.NumKeys || n != numAdded {
		t.Errorf("Len: got %d, wanted %d == %d", n, stats.NumKeys, numAdded)
	}
	t.Logf("Total indexed key size: %d bytes", totalKeyBytes)

	falses := make(map[bool]int)
	for i, key := range keys {
		want := i%2 == 0
		got := idx.Has(key)
		if got != want {
			falses[got]++
		}
	}

	// We expect there to be false positives.
	t.Logf("False positives: %d (%.2f%%)", falses[true], percent(falses[true], len(keys)))

	// There should be no false negatives.
	if neg := falses[false]; neg != 0 {
		t.Errorf("False negatives: %d (%.2f%%)", neg, percent(neg, len(keys)))
	}

	// Marshal the index into protobuf wire format.
	pbits, err := proto.Marshal(index.Encode(idx))
	if err != nil {
		t.Errorf("Marshal failed: %v", err)
	}
	t.Logf("Encoded index: %d bytes in wire format", len(pbits))

	// Unmarshal the wire format and make sure it round-trips.
	var dpb indexpb.Index
	if err := proto.Unmarshal(pbits, &dpb); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	opts := []cmp.Option{
		cmp.AllowUnexported(index.Index{}),
		cmpopts.IgnoreFields(index.Index{}, "hash"), // function, non-comparable
	}

	dec := index.Decode(&dpb)
	if diff := cmp.Diff(dec, idx, opts...); diff != "" {
		t.Errorf("Decoded index: (-want, +got)\n%s", diff)
	}
}

func percent(x, n int) float64 { return 100 * (float64(x) / float64(n)) }
