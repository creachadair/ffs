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
