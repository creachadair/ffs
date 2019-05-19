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
	"encoding/base64"
	"io"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/split"
	"github.com/golang/protobuf/proto"
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
		sc: split.Config{Min: 1024}, // in effect, "don't split"
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
		opt := cmp.AllowUnexported(index{}, extent{}, block{})
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
			{base: 0, bytes: 6, blocks: []block{{6, hashOf("foobar")}}, starts: []int64{0}},
			{base: 10, bytes: 6, blocks: []block{{6, hashOf("foobar")}}, starts: []int64{10}},
			{base: 20, bytes: 7, blocks: []block{{7, hashOf("aliquot")}}, starts: []int64{20}},
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
			{base: 0, bytes: 6, blocks: []block{{6, hashOf("foobar")}}, starts: []int64{0}},
		},
	})

	writeString("kinghell", 3)
	checkString(0, 11, "fookinghell")
	// fookinghell

	checkIndex(index{
		totalBytes: 11,
		extents: []*extent{
			{base: 0, bytes: 11, blocks: []block{{11, hashOf("fookinghell")}}, starts: []int64{0}},
		},
	})

	writeString("mate", 11)
	checkString(0, 15, "fookinghellmate")
	// fookinghellmate

	checkIndex(index{
		totalBytes: 15,
		extents: []*extent{ // these adjacent blocks should be merged (with no split)
			{base: 0, bytes: 15, blocks: []block{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
		},
	})

	writeString("cor", 20)
	checkString(0, 100, "fookinghellmate\x00\x00\x00\x00\x00cor")
	// fookinghellmate-----cor

	checkIndex(index{
		totalBytes: 23,
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []block{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
			{base: 20, bytes: 3, blocks: []block{{3, hashOf("cor")}}, starts: []int64{20}},
		},
	})
}

func TestReblocking(t *testing.T) {
	mem := memstore.New()
	cas := blob.NewCAS(mem, sha1.New)
	d := &fileData{
		sc: split.Config{Min: 100, Size: 512, Max: 8192},
	}
	ctx := context.Background()
	fileData := bytes.Repeat([]byte("0123456789abcdef"), 285)

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
	check(2977, 485, 595, 503) // manually checked
	t.Log("Index 1:\n", proto.CompactTextString(d.toProto()))

	// Now exactly overwrite one block, and verify that it updated its neighbor.
	// Note that the tail of the original blocks should not be modified.
	if _, err := d.writeAt(ctx, cas, bytes.Repeat([]byte("A"), 2977), 0); err != nil {
		t.Fatalf("writeAt(ctx, A*2977, 0): unexpected error: %v", err)
	}
	check(771, 216, 2164, 311, 595, 503) // manually checked
	t.Log("Index 2:\n", proto.CompactTextString(d.toProto()))

	t.Log("Block manifest:")
	d.blocks(func(size int64, key string) {
		t.Logf("%-4d\t%s", size, base64.RawURLEncoding.EncodeToString([]byte(key)))
	})
}
