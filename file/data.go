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
	"context"
	"errors"
	"io"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/block"
	"github.com/creachadair/ffs/file/wiretype"
)

// A data value represents an ordered sequence of bytes stored in a blob.Store.
// Other than length, no metadata are preserved. File data are recorded as a
// flat array of discontiguous extents.
type fileData struct {
	sc         *block.SplitConfig
	totalBytes int64
	extents    []*extent
}

// toWireType converts d to wire encoding.
func (d *fileData) toWireType() *wiretype.Index {
	if d.totalBytes == 0 && len(d.extents) == 0 {
		// No data in this file.
		return nil
	} else if len(d.extents) == 1 && d.extents[0].base == 0 && len(d.extents[0].blocks) == 1 {
		// There is exactly one block in this file, and it starts at offset zero.
		// Store it as a single key, and no normalization is needed.
		return &wiretype.Index{
			TotalBytes: uint64(d.totalBytes),
			Single:     []byte(d.extents[0].blocks[0].key),
		}
	}

	// At this point we have multiple blocks, so we actually have to do some
	// work to pack and normalize the extents.
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
func (d *fileData) fromWireType(pb *wiretype.Index) error {
	if pb == nil {
		return nil
	}

	d.totalBytes = int64(pb.TotalBytes)
	if len(pb.Single) != 0 {
		if len(pb.Extents) != 0 {
			return errors.New("invalid index: single-block and extents both set")
		}
		d.extents = []*extent{{
			base:   0,
			bytes:  d.totalBytes,
			blocks: []cblock{{key: string(pb.Single), bytes: d.totalBytes}},
		}}
		return nil
	}

	pb.Normalize()
	d.extents = make([]*extent, len(pb.Extents))
	for i, ext := range pb.Extents {
		d.extents[i] = &extent{
			base:   int64(ext.Base),
			bytes:  int64(ext.Bytes),
			blocks: make([]cblock, len(ext.Blocks)),
		}
		for j, blk := range ext.Blocks {
			d.extents[i].blocks[j] = cblock{
				bytes: int64(blk.Bytes),
				key:   string(blk.Key),
			}
		}
	}
	return nil
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
func (d *fileData) truncate(ctx context.Context, s blob.CAS, offset int64) error {
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
			span = append(span, splitExtent(&extent{
				base:   last.base,
				bytes:  offset - last.base,
				blocks: append(keep, blks...),
			})...)
		}
	}
	d.extents = append(pre, span...)
	d.totalBytes = offset
	return nil
}

// writeAt writes the contents of data at the specified offset in d.  It
// returns the number of bytes successfully written, and satisfies the
// semantics of io.WriterAt.
func (d *fileData) writeAt(ctx context.Context, s blob.CAS, data []byte, offset int64) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	end := offset + int64(len(data))
	pre, span, post := d.splitSpan(offset, end)

	var left, right []cblock
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

	// N.B. It is possible that this write has created contiguous extents.
	// Rather than fix it here, we rely on the normalization that happens during
	// conversion to wire format, which includes this merge check.

	d.extents = make([]*extent, 0, len(pre)+1+len(post))
	//
	// d.extents = [ ...pre... | ...merged ... | ...post... ]
	//
	d.extents = append(d.extents, pre...)
	d.extents = append(d.extents, splitExtent(&extent{
		base:   newBase,
		bytes:  newEnd - newBase,
		blocks: append(left, append(body, right...)...),
	})...)
	d.extents = append(d.extents, post...)
	if end > d.totalBytes {
		d.totalBytes = end
	}

	return len(data), nil
}

// readAt reads the content of d into data from the specified offset, returning
// the number of bytes successfully read. It satisfies the semantics of the
// io.ReaderAt interface.
func (d *fileData) readAt(ctx context.Context, s blob.CAS, data []byte, offset int64) (int, error) {
	if offset > d.totalBytes {
		return 0, io.EOF
	}
	end := offset + int64(len(data))
	if end > d.totalBytes {
		end = d.totalBytes
	}
	_, span, _ := d.splitSpan(offset, end)

	// If the entire requested range is unstored, zero as much as we can
	// attribute given the total file size. Note that io.ReaderAt requires we
	// report an error if the total is less than requested.
	if len(span) == 0 {
		nr := zero(data[:int(end-offset)])
		if nr < len(data) {
			return nr, io.EOF
		}
		return nr, nil
	}

	// At this point, at least some of the data overlap a stored range.  Walk
	// through the extents copying data into the output till we have enough or
	// we run out of spaces.
	nr := 0
walkSpan:
	for _, ext := range span {
		// This extent starts after the current offset, zero-fill up to the
		// beginning of the extent, or we run out ouf space.
		if offset < ext.base {
			cp := min(int(ext.base-offset), len(data)-nr)
			nr += zero(data[nr : nr+cp])
			if nr == len(data) {
				break walkSpan
			}
			offset += int64(cp)
		}

		// The output is not full, and offset at or past the start of this extent.
		// Find the first block containing offset and walk forward.
		i, base := ext.findBlock(offset)
		for _, blk := range ext.blocks[i:] {
			if base > end {
				break walkSpan
			}

			// Fetch the block contents and copy whatever we can.
			bits, err := s.Get(ctx, blk.key)
			if err != nil {
				return 0, err
			}

			pos := int(offset - base)
			cp := min(len(bits)-pos, len(data)-nr)
			nr += copy(data[nr:], bits[pos:pos+cp])
			if nr == len(data) {
				break walkSpan
			}
			offset += int64(cp)
			base += blk.bytes
		}

		// Reaching here, data is not yet full and we have not yet gone past the
		// end of the requested range. Go back for another extent, if there is one.
	}

	// At this point we have all the stored data we can take.  If there is still
	// space in the output, the remaining portions of the range are unstored.
	if nr < len(data) && end > offset {
		cp := int(end - offset)
		if max := len(data) - nr; cp > max {
			cp = max
		}
		nr += zero(data[nr : nr+cp])
	}

	if nr < len(data) {
		return nr, io.EOF
	}
	return nr, nil
}

// splitBlobs re-blocks the concatenation of the specified blobs and returns
// the resulting blocks. Zero-valued blocks are not stored, the caller can
// detect this by looking for a key of "".
func (d *fileData) splitBlobs(ctx context.Context, s blob.CAS, blobs ...[]byte) ([]cblock, error) {
	data := newBlockReader(blobs)

	var blks []cblock
	if err := block.NewSplitter(data, d.sc).Split(func(blk []byte) error {
		// We do not store blocks of zeroes. They count against the total file
		// size, but we do not explicitly record them.
		if isZero(blk) {
			blks = append(blks, cblock{bytes: int64(len(blk))})
			return nil
		}

		// If a block has a lot of zeroes at its head or tail, treat them as if
		// they were separate zero blocks.
		zhead, ztail, n := zeroAffixes(blk)
		if zhead > n/8 {
			// Inject a "fake" zero block for the prefix, and chop it.
			blks = append(blks, cblock{bytes: int64(zhead)})
			blk = blk[zhead:]
		}
		if ztail > n/8 {
			// Chop off the suffix; see below for the block injection.
			blk = blk[:len(blk)-ztail]
		}

		key, err := s.PutCAS(ctx, blk)
		if err != nil {
			return err
		}
		blks = append(blks, cblock{bytes: int64(len(blk)), key: key})

		if ztail > n/8 {
			// Inject a "fake" zero block for the suffix.
			blks = append(blks, cblock{bytes: int64(ztail)})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return blks, nil
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

// newfileData constructs a new fileData value containing exactly the data from
// s.  For each data block, newFileData calls put to store the block and return
// its key. An error from put stops construction and is reported to the caller.
func newFileData(s *block.Splitter, put func([]byte) (string, error)) (fileData, error) {
	fd := fileData{sc: s.Config()}

	ext := new(extent)
	push := func() {
		if len(ext.blocks) != 0 {
			fd.extents = append(fd.extents, ext)
		}
		ext = &extent{base: fd.totalBytes}
	}

	err := s.Split(func(data []byte) error {
		dlen := int64(len(data))

		// A block of zeroes ends the current extent. We count the block against
		// the total file size, but do not explicitly store it.
		if isZero(data) {
			// N.B. We have to update the total length first, so that push will
			// see the correct new value for the next extent.
			fd.totalBytes += dlen
			push()
			return nil
		}

		// If a block has a lot of zeroes at its head or tail, chop them.  We
		// define "a lot" as a fraction of the block size.
		zhead, ztail, n := zeroAffixes(data)
		if zhead > n/8 {
			fd.totalBytes += int64(zhead)
			push()
			data = data[zhead:]
			dlen = int64(len(data))
		}

		// Update the total length regardless whether we have trailing zeroes to
		// remove from the block. Do this BEFORE adjusting the block.
		fd.totalBytes += dlen
		if ztail > n/8 {
			data = data[:len(data)-ztail]
			dlen = int64(len(data))
			defer push() // start a new extent after this block
		}
		ext.bytes += dlen

		key, err := put(data)
		if err != nil {
			return err
		}
		ext.blocks = append(ext.blocks, cblock{
			bytes: dlen,
			key:   key,
		})

		return nil
	})
	if err != nil {
		return fileData{}, err
	}
	push() // flush any trailing extent

	return fd, nil
}

// An extent represents a single contiguous stored subrange of a file. The
// blocks record the offsets and block storage keys for the extent.
type extent struct {
	base   int64    // offset of the first byte within the file
	bytes  int64    // number of bytes in the extent
	blocks []cblock // continguous extent blocks
	starts []int64  // block starting offsets, for search
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

// A block represents a single content-addressable block of file data.
type cblock struct {
	bytes int64  // number of bytes in the block
	key   string // storage key for this block
}
