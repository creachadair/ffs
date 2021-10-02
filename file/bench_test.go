package file_test

import (
	"fmt"
	"testing"
	"unsafe"
)

func isZeroUnsafe(data []byte) bool {
	n := len(data)
	m := n &^ 7

	i := 0
	for ; i < m; i += 8 {
		v := *(*uint64)(unsafe.Pointer(&data[i]))
		if v != 0 {
			return false
		}
	}
	for ; i < n; i++ {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

func isZeroSafe(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func BenchmarkZeroTest(b *testing.B) {
	// N.B. Sizes chosen for the worst case of the unsafe implementation,
	// leaving a 7-byte tail.
	sizes := []int{103, 1007, 10007, 100007}

	for _, size := range sizes {
		buf := make([]byte, size)
		b.Run(fmt.Sprintf("Unsafe-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				isZeroUnsafe(buf)
			}
		})
		b.Run(fmt.Sprintf("Safe-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				isZeroSafe(buf)
			}
		})
	}
}
