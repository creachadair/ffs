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

package storetest

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/creachadair/ffs/blob"
)

// BenchmarkKV runs a series of simple benchmark exercises against kv.
// The benchmark does not make any assumptions about the contents of kv, but
// after it runs its contents will be garbage.
func BenchmarkKV(b *testing.B, kv blob.KV) {
	smallValue := bytes.Repeat([]byte{1}, 8)
	mediumValue := bytes.Repeat([]byte{1}, 16000)  // about 16K
	largeValue := bytes.Repeat([]byte{1}, 1500000) // about 1M

	// Set up a bunch of keys to use for the benchmark.
	// The Put benchmark actually stores them.
	const numSeedValues = 2_000
	vs := [][]byte{smallValue, mediumValue, largeValue}
	keys := make([]string, numSeedValues)
	for i := range numSeedValues {
		keys[i] = strconv.Itoa(i + 1)
	}

	b.Run("Put", func(b *testing.B) {
		var cur int
		for b.Loop() {
			if err := kv.Put(b.Context(), blob.PutOptions{
				Key:     keys[cur],
				Data:    vs[cur%3],
				Replace: true,
			}); err != nil {
				b.Fatalf("Put %q: %v", cur+1, err)
			}
			cur = (cur + 1) % len(keys)
		}
	})

	b.Run("Get", func(b *testing.B) {
		var cur int
		for b.Loop() {
			_, err := kv.Get(b.Context(), keys[cur])
			if err != nil {
				b.Fatalf("Get %q failed: %v", keys[cur], err)
			}
			cur = (cur + 1) % len(keys)
		}
	})

	b.Run("List", func(b *testing.B) {
		for b.Loop() {
			for _, err := range kv.List(b.Context(), "") {
				if err != nil {
					b.Fatalf("List failed; %v", err)
				}
			}
		}
	})

	b.Run("Has", func(b *testing.B) {
		b.Run("Present", func(b *testing.B) {
			var cur int
			for b.Loop() {
				ks, err := kv.Has(b.Context(), keys[cur])
				if err != nil {
					b.Fatalf("Has %q failed: %v", keys[cur], err)
				} else if !ks.Has(keys[cur]) {
					b.Fatalf("Has %q wrong answer", keys[cur])
				}
				cur = (cur + 1) % len(keys)
			}
		})
		b.Run("Absent", func(b *testing.B) {
			for b.Loop() {
				ks, err := kv.Has(b.Context(), "nonesuch")
				if err != nil {
					b.Fatalf("Has nonesuch failed: %v", err)
				} else if ks.Has("nonesuch") {
					b.Fatal("Has nonesuch wrong answer")
				}
			}
		})
	})

	b.Run("Len", func(b *testing.B) {
		for b.Loop() {
			n, err := kv.Len(b.Context())
			if err != nil {
				b.Fatalf("Len failed: %v", err)
			} else if n < numSeedValues {
				// N.B. Use < here because the store might not have been empty.
				b.Fatalf("Len = %d, want at least %d", n, numSeedValues)
			}
		}
	})
}
