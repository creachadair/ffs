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

package file

import (
	"io"
	"unsafe"
)

// splitExtent splits ext into possibly-multiple extents by removing
// zero-valued data blocks. If there are no zero blocks, the return slice
// contains just the original extent.
func splitExtent(ext *extent) []*extent {
	var chunks [][]cblock
	var bases []int64
	var sizes []int64

	// Do a two-finger walk of the blocks. The left finger (lo) scans for the
	// next non-zero block, and the right finger (hi) scans forward from there
	// to find the end of the non-zero range. Along the way, we keep track of
	// the base and size of each non-zero range, to pack into extents.

	base := ext.base
	lo := 0

nextChunk:
	for lo < len(ext.blocks) {
		// Scan for a nonzero block.
		if ext.blocks[lo].key == "" {
			base += ext.blocks[lo].bytes
			lo++
			continue
		}

		// Scan forward for a zero block.
		nextBase := base + ext.blocks[lo].bytes
		for hi := lo + 1; hi < len(ext.blocks); hi++ {
			blk := ext.blocks[hi]

			// If we found a zero-value block, the non-zero blocks since the last
			// marker are an extent.
			if blk.key == "" {
				chunks = append(chunks, ext.blocks[lo:hi])
				bases = append(bases, base)
				sizes = append(sizes, nextBase-base)
				base = nextBase
				lo = hi
				continue nextChunk
			}
			nextBase += blk.bytes
		}

		// If we get here, hi reached the end of the blocks without finding
		// another zero-value block, so the rest of the blocks are an extent.
		// In the typical case where nothing happened, return without packing.
		if lo == 0 {
			return []*extent{ext}
		}
		chunks = append(chunks, ext.blocks[lo:])
		bases = append(bases, base)
		sizes = append(sizes, nextBase-base)
		break
	}

	exts := make([]*extent, len(chunks))
	for i, chunk := range chunks {
		exts[i] = &extent{
			base:   bases[i],
			bytes:  sizes[i],
			blocks: chunk,
		}
	}
	return exts
}

// A blockReader implements io.Reader for the concatenation of a slice of byte
// slices. This avoids the overhead of constructing a bytes.Reader for each
// blob plus an io.MultiReader to concatenate them.
type blockReader struct {
	cur    int
	blocks [][]byte
}

func newBlockReader(blocks [][]byte) *blockReader {
	return &blockReader{blocks: blocks}
}

func (r *blockReader) Read(data []byte) (int, error) {
	var nr int
	for nr < len(data) && r.cur < len(r.blocks) {
		curBlock := r.blocks[r.cur]
		cp := copy(data[nr:], curBlock)
		if cp == len(curBlock) {
			r.blocks[r.cur] = nil
			r.cur++
		} else {
			r.blocks[r.cur] = curBlock[cp:]
		}
		nr += cp
	}
	if nr == 0 && r.cur >= len(r.blocks) {
		return 0, io.EOF
	}
	return nr, nil
}

// zero sets all of data to zeroes and returns its length.
func zero(data []byte) int {
	n := len(data)
	m := n &^ 7

	i := 0
	for ; i < m; i += 8 {
		v := (*uint64)(unsafe.Pointer(&data[i]))
		*v = 0
	}
	for ; i < n; i++ {
		data[i] = 0
	}
	return n
}

// zeroCheck returns the length of the longest prefix and suffix of data that
// comprise all zeroes, along with the length of data. If data consists of all
// zero bytes, zeroCheck returns len(data), 0, len(data).
//
// Otherwise, zhead is the count of zero bytes prior to the first non-zero
// byte, and ztail is the count of zero bytes after the last non-zero byte.
func zeroCheck(data []byte) (zhead, ztail, n int) {
	// Benchmarks for this implementation vs. naive loop.
	// Sizes in bytes, times in ns/op (from go test -bench).
	//
	//   Size     Unsafe  Naive  Speedup
	//   103      11      41     2.72x
	//   1007     73      267    2.66x
	//   10007    646     2529   2.91x
	//   100007   6320    25248  2.99x
	//
	n = len(data)
	m := n &^ 7 // count of full 64-bit strides

	i := 0
	for ; i < m; i += 8 {
		v := *(*uint64)(unsafe.Pointer(&data[i]))
		if v != 0 {
			// Not zero: Count prefix and suffix zeroes.
			zhead = i
			for data[zhead] == 0 {
				zhead++
			}
			for j := n - 1; j > i && data[j] == 0; j-- {
				ztail++
			}
			return zhead, ztail, n
		}
	}
	for ; i < n; i++ {
		if data[i] != 0 {
			zhead = i
			for j := n - 1; j > i && data[j] == 0; j-- {
				ztail++
			}
			return zhead, ztail, n
		}
	}
	return n, 0, n
}

func min(z0 int, zs ...int) int {
	for _, z := range zs {
		if z < z0 {
			z0 = z
		}
	}
	return z0
}
