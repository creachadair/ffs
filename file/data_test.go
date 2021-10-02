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
	"crypto/sha1"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/block"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/google/go-cmp/cmp"
)

func hashOf(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	return string(h.Sum(nil))
}

func TestIndex(t *testing.T) {
	mem := memstore.New()
	cas := blob.NewCAS(mem, sha1.New)
	d := &fileData{
		sc: &block.SplitConfig{Min: 1024}, // in effect, "don't split"
	}
	ctx := context.Background()
	writeString := func(s string, at int64) {
		nw, err := d.writeAt(ctx, cas, []byte(s), at)
		t.Logf("Write %q at offset %d (%d, %v)", s, at, nw, err)
		if err != nil {
			t.Fatalf("writeAt(ctx, %q, %d): got (%d, %v), unexpected error", s, at, nw, err)
		} else if nw != len(s) {
			t.Errorf("writeAt(ctx, %q, %d): got %d, want %d", s, at, nw, len(s))
		}
	}
	checkString := func(at, nb int64, want string) {
		buf := make([]byte, nb)
		nr, err := d.readAt(ctx, cas, buf, at)
		t.Logf("Read %d from offset %d (%d, %v)", nb, at, nr, err)
		if err != nil && err != io.EOF {
			t.Fatalf("readAt(ctx, #[%d], %d): got (%d, %v), unexpected error", nb, at, nr, err)
		} else if got := string(buf[:nr]); got != want {
			t.Errorf("readAt(ctx, #[%d], %d): got %q, want %q", nb, at, got, want)
		}
	}
	truncate := func(at int64) {
		err := d.truncate(ctx, cas, at)
		t.Logf("truncate(ctx, %d) %v", at, err)
		if err != nil {
			t.Fatalf("truncate(ctx, %d): unexpected error: %v", at, err)
		}
	}
	type index struct {
		totalBytes int64
		extents    []*extent
	}
	checkIndex := func(want index) {
		// We have to tell cmp that it's OK to look at unexported fields on these types.
		opt := cmp.AllowUnexported(index{}, extent{}, cblock{})
		got := index{totalBytes: d.totalBytes, extents: d.extents}
		if diff := cmp.Diff(want, got, opt); diff != "" {
			t.Errorf("Incorrect index (-want, +got)\n%s", diff)
		}
	}

	// Write some discontiguous regions into the file and verify that the
	// resulting index is correct.
	checkString(0, 10, "")

	writeString("foobar", 0)
	checkString(0, 6, "foobar")
	checkString(3, 6, "bar")
	// foobar

	writeString("foobar", 10)
	checkString(10, 6, "foobar")
	checkString(0, 16, "foobar\x00\x00\x00\x00foobar")
	// foobar----foobar

	writeString("aliquot", 20)
	checkString(0, 100, "foobar\x00\x00\x00\x00foobar\x00\x00\x00\x00aliquot")
	// foobar----foobar----aliquot

	checkIndex(index{
		totalBytes: 27,
		extents: []*extent{
			{base: 0, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{0}},
			{base: 10, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{10}},
			{base: 20, bytes: 7, blocks: []cblock{{7, hashOf("aliquot")}}, starts: []int64{20}},
		},
	})

	writeString("barbarossa", 3)
	checkString(0, 100, "foobarbarossabar\x00\x00\x00\x00aliquot")
	// foo..........bar----aliquot
	// ^^^barbarossa^^^  preserved block contents outside overlap (^)

	truncate(6)
	// foobar

	checkString(0, 16, "foobar")
	checkIndex(index{
		totalBytes: 6,
		extents: []*extent{
			{base: 0, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{0}},
		},
	})

	writeString("kinghell", 3)
	checkString(0, 11, "fookinghell")
	// fookinghell

	checkIndex(index{
		totalBytes: 11,
		extents: []*extent{
			{base: 0, bytes: 11, blocks: []cblock{{11, hashOf("fookinghell")}}, starts: []int64{0}},
		},
	})

	writeString("mate", 11)
	checkString(0, 15, "fookinghellmate")
	// fookinghellmate

	checkIndex(index{
		totalBytes: 15,
		extents: []*extent{ // these adjacent blocks should be merged (with no split)
			{base: 0, bytes: 15, blocks: []cblock{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
		},
	})

	writeString("cor", 20)
	checkString(0, 100, "fookinghellmate\x00\x00\x00\x00\x00cor")
	// fookinghellmate-----cor

	checkIndex(index{
		totalBytes: 23,
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []cblock{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
			{base: 20, bytes: 3, blocks: []cblock{{3, hashOf("cor")}}, starts: []int64{20}},
		},
	})
}

func TestWireEncoding(t *testing.T) {
	opts := []cmp.Option{cmp.AllowUnexported(fileData{}, extent{}, cblock{})}
	t.Run("SingleBlock", func(t *testing.T) {
		d := &fileData{totalBytes: 10, extents: []*extent{
			{bytes: 10, blocks: []cblock{{bytes: 10, key: "foo"}}},
		}}
		idx := d.toWireType()
		if idx.TotalBytes != 10 {
			t.Errorf("Index total bytes: got %d, want 10", idx.TotalBytes)
		}
		if s := string(idx.Single); s != "foo" {
			t.Errorf("Index single key: got %q, want foo", s)
		}
		if len(idx.Extents) != 0 {
			t.Errorf("Index has %d extents, want 0", len(idx.Extents))
		}

		dx := new(fileData)
		if err := dx.fromWireType(idx); err != nil {
			t.Errorf("Decoding index failed: %v", err)
		}
		if diff := cmp.Diff(*d, *dx, opts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})

	t.Run("MultipleBlocks", func(t *testing.T) {
		d := &fileData{totalBytes: 15, extents: []*extent{
			{bytes: 15, blocks: []cblock{
				{bytes: 10, key: "foo"},
				{bytes: 5, key: "bar"},
			}},
		}}
		idx := d.toWireType()
		if idx.TotalBytes != 15 {
			t.Errorf("Index total bytes: got %d, want 15", idx.TotalBytes)
		}
		if len(idx.Single) != 0 {
			t.Errorf("Index single key: got %q, want empty", string(idx.Single))
		}
		if len(idx.Extents) != 1 || len(idx.Extents[0].Blocks) != 2 {
			t.Errorf("Index extents=%d, blocks=%d; want 1, 2",
				len(idx.Extents), len(idx.Extents[0].Blocks))
		}

		dx := new(fileData)
		if err := dx.fromWireType(idx); err != nil {
			t.Errorf("Decoding index failed: %v", err)
		}
		if diff := cmp.Diff(d, dx, opts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})

	t.Run("DecodeMergesExtents", func(t *testing.T) {
		idx := &wiretype.Index{
			TotalBytes: 10,
			Extents: []*wiretype.Extent{
				{Base: 0, Bytes: 3, Blocks: []*wiretype.Block{{Bytes: 3, Key: []byte("1")}}},
				{Base: 3, Bytes: 7, Blocks: []*wiretype.Block{{Bytes: 7, Key: []byte("2")}}},
			},
		}

		dx := new(fileData)
		if err := dx.fromWireType(idx); err != nil {
			t.Errorf("Decoding index failed: %v", err)
		}
		want := &fileData{
			totalBytes: 10,
			extents: []*extent{
				{base: 0, bytes: 10, blocks: []cblock{{bytes: 3, key: "1"}, {bytes: 7, key: "2"}}},
			},
		}
		if diff := cmp.Diff(want, dx, opts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})
}

func TestReblocking(t *testing.T) {
	mem := memstore.New()
	cas := blob.NewCAS(mem, sha1.New)
	d := &fileData{
		sc: &block.SplitConfig{Min: 200, Size: 1024, Max: 8192},
	}

	rand.Seed(1) // change to update test data

	const alphabet = "0123456789abcdef"
	var buf bytes.Buffer
	for buf.Len() < 4000 {
		buf.WriteByte(alphabet[rand.Intn(len(alphabet))])
	}
	fileData := buf.Bytes()

	ctx := context.Background()
	// Write the data in a bunch of small contiguous chunks, and verify that the
	// result reblocks adjacent chunks.
	i, nb := 0, 0
	for i < len(fileData) {
		end := i + 25
		if end > len(fileData) {
			end = len(fileData)
		}
		if _, err := d.writeAt(ctx, cas, fileData[i:end], int64(i)); err != nil {
			t.Fatalf("writeAt(ctx, %#q, %d): unexpected error: %v", string(fileData[i:end]), i, err)
		}
		i = end
		nb++
	}

	check := func(want ...int64) {
		var total int64
		var got []int64
		for _, ext := range d.extents {
			for _, blk := range ext.blocks {
				total += blk.bytes
				got = append(got, blk.bytes)
			}
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Wrong block sizes (-want, +got)\n%s", diff)
		}
		if int(total) != len(fileData) {
			t.Errorf("Wrong total size: got %d, want %d", total, len(fileData))
		}
	}
	check(481, 2329, 413, 255, 522) // manually checked

	// Now exactly overwrite one block, and verify that it updated its neighbor.
	// Note that the tail of the original blocks should not be modified.
	//
	// Before: xxxx xxxxxxxxxxxxxxxxxxxxxxx xxxx xx xxxxx
	// After:  AAAAAAAAAAAAAAAAAAAAxxxxxxxx xxxx xx xxxxx
	// Write:  ^^^^^^^^^^^^^^^^^^^^         \----\--\---- unchanged
	//
	if _, err := d.writeAt(ctx, cas, bytes.Repeat([]byte("A"), 2000), 0); err != nil {
		t.Fatalf("writeAt(ctx, A*2977, 0): unexpected error: %v", err)
	}
	check(2810, 413, 255, 522) // manually checked; note tail is stable

	t.Log("Block manifest:")
	d.blocks(func(size int64, key string) {
		t.Logf("%-4d\t%x", size, []byte(key))
	})
}

type lineHash struct{}

func (h lineHash) Hash() block.Hash { return h }

func (lineHash) Update(b byte) uint64 {
	if b == '\x00' {
		return 1
	}
	return 2
}

func TestNewFileData(t *testing.T) {
	const input = "This is the first line" + // ext 1, block 1
		"\x00This is the second" + // ext 1, block 2
		"\x00This is the third" + // ext 1, block 3
		"\x00\x00\x00\x00\x00" + // zeroes (not stored)
		"\x00And the fourth line then beckoned" // ext 2, block 1

	s := block.NewSplitter(strings.NewReader(input), &block.SplitConfig{
		Hasher: lineHash{},
		Min:    5,
		Max:    100,
		Size:   16,
	})
	fd, err := newFileData(s, func(data []byte) (string, error) {
		t.Logf("Block: %q", string(data))
		h := sha1.New()
		h.Write(data)
		return string(h.Sum(nil)), nil
	})
	if err != nil {
		t.Fatalf("newFileData failed: %v", err)
	}
	t.Logf("Input size: %d, total bytes: %d", len(input), fd.totalBytes)
	if fd.totalBytes != int64(len(input)) {
		t.Errorf("TotalBytes: got %d, want %d", fd.totalBytes, len(input))
	}

	type extinfo struct {
		Base, Bytes int64
		Blocks      int
	}
	want := []extinfo{
		{0, 59, 3},
		{64, 34, 1},
	}
	var got []extinfo
	for i, ext := range fd.extents {
		got = append(got, extinfo{
			Base:   ext.base,
			Bytes:  ext.bytes,
			Blocks: len(ext.blocks),
		})
		for j, b := range ext.blocks {
			t.Logf("Extent %d block %d: %d bytes, key=%x", i+1, j+1, b.bytes, b.key)
		}
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Wrong extents: (-want, +got):\n%s", diff)
	}
}
