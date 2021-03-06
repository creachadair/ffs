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

package file

import (
	"bytes"
	"context"
	"io"

	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/split"
)

// A data value represents an ordered sequence of bytes stored in a blob.Store.
// Other than length, no metadata are preserved. File data are recorded as a
// flat array of discontiguous extents.
type fileData struct {
	sc         *split.Config
	totalBytes int64
	extents    []*extent
}

// toWireType converts d to wire encoding.
func (d *fileData) toWireType() *wiretype.Index {
	if d.totalBytes == 0 && len(d.extents) == 0 {
		return nil
	}
	w := &wiretype.Index{
		TotalBytes: uint64(d.totalBytes),
		Extents:    make([]*wiretype.Extent, len(d.extents)),
	}
	for i, ext := range d.extents {
		x := &wiretype.Extent{
			Base:   uint64(ext.base),
			Bytes:  uint64(ext.bytes),
			Blocks: make([]*wiretype.Block, len(ext.blocks)),
		}
		for j, blk := range ext.blocks {
			x.Blocks[j] = &wiretype.Block{
				Bytes: uint64(blk.bytes),
				Key:   []byte(blk.key),
			}
		}
		w.Extents[i] = x
	}
	w.Normalize()
	return w
}

// fromWireType replaces the contents of d from the wire encoding pb.
func (d *fileData) fromWireType(pb *wiretype.Index) {
	if pb == nil {
		return
	}
	pb.Normalize()
	d.totalBytes = int64(pb.TotalBytes)
	d.extents = make([]*extent, len(pb.Extents))

	for i, ext := range pb.Extents {
		d.extents[i] = &extent{
			base:   int64(ext.Base),
			bytes:  int64(ext.Bytes),
			blocks: make([]block, len(ext.Blocks)),
		}
		for j, blk := range ext.Blocks {
			d.extents[i].blocks[j] = block{
				bytes: int64(blk.Bytes),
				key:   string(blk.Key),
			}
		}
	}
}

// size reports the size of the data in bytes.
func (d *fileData) size() int64 { return d.totalBytes }

// blocks calls f once for each block used by d, giving the key and the size of
// the blob. If the same blob is repeated, f will be called multiple times for
// the same key.
func (d *fileData) blocks(f func(int64, string)) {
	for _, ext := range d.extents {
		for _, blk := range ext.blocks {
			f(blk.bytes, blk.key)
		}
	}
}

// truncate modifies the length of the file to end at offset, extending or
// contracting it as necessary. Contraction may require splitting a block.
func (d *fileData) truncate(ctx context.Context, s CAS, offset int64) error {
	if offset >= d.totalBytes {
		d.totalBytes = offset
		return nil
	}
	pre, span, _ := d.splitSpan(0, offset)
	if len(span) != 0 {
		n := len(span) - 1
		last := span[n]
		span = span[:n]

		// If the offset transects a block, read that block and write back its
		// prefix. If the offset is exactly at the start of the block, we can
		// skip that step and discard the whole block.
		if i, pos := last.findBlock(offset); i >= 0 && offset > pos {
			keep := last.blocks[:i]
			bits, err := s.Get(ctx, last.blocks[i].key)
			if err != nil {
				return err
			}
			blks, err := d.splitBlobs(ctx, s, bits[:int(offset-pos)])
			if err != nil {
				return err
			}
			span = append(span, &extent{
				base:   last.base,
				bytes:  offset - last.base,
				blocks: append(keep, blks...),
			})
		}
	}
	d.extents = append(pre, span...)
	d.totalBytes = offset
	return nil
}

// writeAt writes the contents of data at the specified offset in d.  It
// returns the number of bytes successfully written, and satisfies the
// semantics of io.WriterAt.
func (d *fileData) writeAt(ctx context.Context, s CAS, data []byte, offset int64) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	end := offset + int64(len(data))
	pre, span, post := d.splitSpan(offset, end)

	var left, right []block
	var parts [][]byte
	newBase := offset
	newEnd := end

	// If this write does not span any existing extents, create a new one
	// containing just this write.
	if len(span) == 0 {
		parts = append(parts, data)
	} else {
		if span[0].base < newBase {
			// The first extent starts before the write. Find the first block
			// split by or contiguous to the write, preserve everything before
			// that, and read in the contents to set up the split.
			newBase = span[0].base

			pos := span[0].base
			for _, blk := range span[0].blocks {
				next := pos + blk.bytes
				if next < offset {
					left = append(left, blk)
					pos = next
					continue
				}

				bits, err := s.Get(ctx, blk.key)
				if err != nil {
					return 0, err
				}
				parts = append(parts, bits[:int(offset-pos)])
				break
			}
		}

		// Insert the main body of the write.
		parts = append(parts, data)

		if last := span[len(span)-1]; last.base+last.bytes >= newEnd {
			// The last extent ends after the write. Find the last block split by
			// or contiguous to the write, preserve everything after that, and
			// read in the contents to set up the split.
			newEnd = last.base + last.bytes

			pos := last.base
			for i, blk := range last.blocks {
				if pos > end {
					// Preserve the rest of this extent
					right = append(right, last.blocks[i:]...)
					break
				}
				next := pos + blk.bytes
				if next <= end {
					pos = next
					continue // skip overwritten block
				}

				bits, err := s.Get(ctx, blk.key)
				if err != nil {
					return 0, err
				}

				parts = append(parts, bits[int(end-pos):])
				pos = next
			}
		}
	}

	// Now write out the combined data and assemble the new index.
	body, err := d.splitBlobs(ctx, s, parts...)
	if err != nil {
		return 0, err
	}

	merged := &extent{
		base:   newBase,
		bytes:  newEnd - newBase,
		blocks: append(left, append(body, right...)...),
	}

	// Check whether we have created contiguous extents, and merge them if so.
	if n := len(pre); n > 0 && pre[n-1].abuts(merged) {
		merged.base = pre[n-1].base
		merged.bytes += pre[n-1].bytes
		merged.blocks = append(pre[n-1].blocks, merged.blocks...)
		pre = pre[:n]
	}
	if len(post) > 0 && merged.abuts(post[0]) {
		merged.bytes += post[0].bytes
		merged.blocks = append(merged.blocks, post[0].blocks...)
		post = post[1:]
	}

	d.extents = append(append(pre, merged), post...)
	if end > d.totalBytes {
		d.totalBytes = end
	}

	return len(data), nil
}

// readAt reads the content of d into data from the specified offset, returning
// the number of bytes successfully read. It satisfies the semantics of the
// io.ReaderAt interface.
func (d *fileData) readAt(ctx context.Context, s CAS, data []byte, offset int64) (int, error) {
	if offset > d.totalBytes {
		return 0, io.EOF
	}
	end := offset + int64(len(data))
	if end > d.totalBytes {
		end = d.totalBytes
	}
	_, span, _ := d.splitSpan(offset, end)

	nr := 0

	fill := func(ext *extent) {
		if ext.base > offset {
			size := ext.base - offset
			nr += zero(data[nr : nr+int(size)])
			offset += size
		}
	}

	// If the range begins in unstored space, fill the uncovered prefix.
	if len(span) == 0 {
		nr += zero(data[:int(end-offset)])
	}

	// Copy data out of the spanning extents.
	for _, ext := range span {
		fill(ext)

		i, pos := ext.findBlock(offset)
		for _, blk := range ext.blocks[i:] {
			if pos > end {
				break // done with this extent, which must also be the last
			}

			// Fetch the block contents and copy whatever portion we need.
			bits, err := s.Get(ctx, blk.key)
			if err != nil {
				return 0, err
			}

			lo := int(offset - pos)
			cp := copy(data[nr:], bits[lo:])
			nr += cp
			offset += int64(cp)
			pos += blk.bytes
		}
	}

	// If the range ends in unstored space, fill the uncovered suffix.
	fill(&extent{base: end})

	// The contract for io.ReaderAt requires an error if we return fewer bytes
	// than requested.
	if nr < len(data) {
		return nr, io.EOF
	}
	return nr, nil
}

func (d *fileData) splitBlobs(ctx context.Context, s CAS, blobs ...[]byte) ([]block, error) {
	rs := make([]io.Reader, len(blobs))
	for i, b := range blobs {
		rs[i] = bytes.NewReader(b)
	}

	var blks []block
	if err := split.New(io.MultiReader(rs...), d.sc).Split(func(blk []byte) error {
		key, err := s.PutCAS(ctx, blk)
		if err != nil {
			return err
		}
		blks = append(blks, block{bytes: int64(len(blk)), key: key})
		return nil
	}); err != nil {
		return nil, err
	}
	return blks, nil
}

func zero(data []byte) int {
	for i := range data {
		data[i] = 0
	}
	return len(data)
}

// splitSpan returns three subslices of the extents of d, those which end
// entirely before offset lo, those fully containing the range from lo to hi,
// and those which begin entirely at or after offset hi.
//
// If span is empty, the range fully spans unstored data. Otherwise, the first
// and last elements of span are "split" by the range.
func (d *fileData) splitSpan(lo, hi int64) (pre, span, post []*extent) {
	for i, ext := range d.extents {
		if lo > ext.base+ext.bytes {
			pre = append(pre, ext)
		} else if hi < ext.base {
			post = append(post, d.extents[i:]...)
			break // nothing more to do; everything else is bigger
		} else {
			span = append(span, ext)
		}
	}

	return
}

// An extent represents a single contiguous stored subrange of a file. The
// blocks record the offsets and block storage keys for the extent.
type extent struct {
	base   int64   // offset of the first byte within the file
	bytes  int64   // number of bytes in the extent
	blocks []block // continguous extent blocks
	starts []int64 // block starting offsets, for search
}

// findBlock returns the index and base offset of the first block in e that
// contains offset. It returns -1, -1 if no block in e contains offset.
func (e *extent) findBlock(offset int64) (int, int64) {
	// After a change, do a linear scan to (re)initialize the offsets cache.
	// Subsequent lookups will fall through to binary search below.
	if len(e.starts) != len(e.blocks) {
		var idx int
		var base int64

		e.starts = make([]int64, len(e.blocks))
		pos := e.base
		for i, blk := range e.blocks {
			e.starts[i] = pos
			pos += blk.bytes
			if e.starts[i] <= offset && offset < pos {
				idx = i
				base = e.starts[i]

				// we found the needle, but finish the loop to populate the
				// remainder of the offsets cache.
			}
		}
		return idx, base
	}

	// Subsequent searches binary search.
	lo, hi := 0, len(e.starts)
	for lo < hi {
		mid := (lo + hi) / 2
		base := e.starts[mid]
		if offset < base {
			hi = mid
		} else if offset > base+e.blocks[mid].bytes {
			lo = mid + 1
		} else {
			return mid, base
		}
	}
	return -1, -1
}

// abuts reports whether the end of e is contiguous with the beginning of o.
func (e *extent) abuts(o *extent) bool { return e.base+e.bytes == o.base }

// A block represents a single content-addressable block of file data.
type block struct {
	bytes int64  // number of bytes in the block
	key   string // storage key for this block
}
