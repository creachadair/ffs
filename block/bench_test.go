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

package block_test

import (
	"math/rand"
	"testing"

	"github.com/creachadair/ffs/block"
)

func BenchmarkSplitter_Next(b *testing.B) {
	src := rand.New(rand.NewSource(202109111241))
	s := block.NewSplitter(src, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Next(); err != nil {
			b.Fatalf("Error from Next at i=%d of %d: %v", i, b.N, err)
		}
	}
}

func BenchmarkHasher_Update(b *testing.B) {
	const windowSize = 48
	b.Run("RabinKarp", func(b *testing.B) {
		h := block.RabinKarpHasher(1031, 2147483659, windowSize).Hash()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Update(196)
		}
	})
}
