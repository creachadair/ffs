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
	"strconv"
	"strings"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/block"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var cmpFileDataOpts = []cmp.Option{
	cmp.AllowUnexported(fileData{}, extent{}, cblock{}),
	cmpopts.IgnoreFields(fileData{}, "sc"),
}

func TestIndex(t *testing.T) {
	d := newDataTester(t, &block.SplitConfig{Min: 1024}) // in effect, "don't split"

	type index struct {
		totalBytes int64
		extents    []*extent
	}
	checkIndex := func(want index) {
		// We have to tell cmp that it's OK to look at unexported fields on these types.
		opt := cmp.AllowUnexported(index{}, extent{}, cblock{})
		got := index{totalBytes: d.fd.totalBytes, extents: d.fd.extents}
		if diff := cmp.Diff(want, got, opt); diff != "" {
			t.Errorf("Incorrect index (-want, +got)\n%s", diff)
		}
	}

	// Write some discontiguous regions into the file and verify that the
	// resulting index is correct.
	d.checkString(0, 10, "")

	d.writeString("foobar", 0)
	d.checkString(0, 6, "foobar")
	d.checkString(3, 6, "bar")
	// foobar

	d.writeString("foobar", 10)
	d.checkString(10, 6, "foobar")
	d.checkString(0, 16, "foobar\x00\x00\x00\x00foobar")
	// foobar----foobar

	d.writeString("aliquot", 20)
	d.checkString(0, 100, "foobar\x00\x00\x00\x00foobar\x00\x00\x00\x00aliquot")
	// foobar----foobar----aliquot

	checkIndex(index{
		totalBytes: 27,
		extents: []*extent{
			{base: 0, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{0}},
			{base: 10, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{10}},
			{base: 20, bytes: 7, blocks: []cblock{{7, hashOf("aliquot")}}, starts: []int64{20}},
		},
	})

	d.writeString("barbarossa", 3)
	d.checkString(0, 100, "foobarbarossabar\x00\x00\x00\x00aliquot")
	// foo..........bar----aliquot
	// ^^^barbarossa^^^  preserved block contents outside overlap (^)

	d.truncate(6)
	// foobar

	d.checkString(0, 16, "foobar")
	checkIndex(index{
		totalBytes: 6,
		extents: []*extent{
			{base: 0, bytes: 6, blocks: []cblock{{6, hashOf("foobar")}}, starts: []int64{0}},
		},
	})

	d.writeString("kinghell", 3)
	d.checkString(0, 11, "fookinghell")
	// fookinghell

	checkIndex(index{
		totalBytes: 11,
		extents: []*extent{
			{base: 0, bytes: 11, blocks: []cblock{{11, hashOf("fookinghell")}}, starts: []int64{0}},
		},
	})

	d.writeString("mate", 11)
	d.checkString(0, 15, "fookinghellmate")
	// fookinghellmate

	checkIndex(index{
		totalBytes: 15,
		extents: []*extent{ // these adjacent blocks should be merged (with no split)
			{base: 0, bytes: 15, blocks: []cblock{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
		},
	})

	d.writeString("cor", 20)
	d.checkString(0, 100, "fookinghellmate\x00\x00\x00\x00\x00cor")
	// fookinghellmate-----cor

	checkIndex(index{
		totalBytes: 23,
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []cblock{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
			{base: 20, bytes: 3, blocks: []cblock{{3, hashOf("cor")}}, starts: []int64{20}},
		},
	})

	d.writeString("THEEND", 30)
	d.checkString(0, 100, "fookinghellmate\x00\x00\x00\x00\x00cor\x00\x00\x00\x00\x00\x00\x00THEEND")
	checkIndex(index{
		totalBytes: 36,
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []cblock{{15, hashOf("fookinghellmate")}}, starts: []int64{0}},
			{base: 20, bytes: 3, blocks: []cblock{{3, hashOf("cor")}}, starts: []int64{20}},
			{base: 30, bytes: 6, blocks: []cblock{{6, hashOf("THEEND")}}, starts: []int64{30}},
		},
	})

	// Verify read boundary cases.
	d.checkString(24, 3, "\x00\x00\x00")                 // entirely unstored
	d.checkString(11, 6, "mate\x00\x00")                 // partly stored, partly unstored
	d.checkString(25, 100, "\x00\x00\x00\x00\x00THEEND") // partly unstored, partly stored
	d.checkString(18, 7, "\x00\x00cor\x00\x00")          // unstored, stored, unstored
}

func TestWireEncoding(t *testing.T) {

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
		if diff := cmp.Diff(*d, *dx, cmpFileDataOpts...); diff != "" {
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
		if diff := cmp.Diff(d, dx, cmpFileDataOpts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})

	t.Run("NormalizeMergesExtents", func(t *testing.T) {
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
		if diff := cmp.Diff(want, dx, cmpFileDataOpts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})

	t.Run("NormalizeDropsEmpty", func(t *testing.T) {
		idx := &wiretype.Index{
			TotalBytes: 20,
			Extents: []*wiretype.Extent{
				{Base: 0, Bytes: 0},
				{Base: 3, Bytes: 7, Blocks: []*wiretype.Block{{Bytes: 7, Key: []byte("X")}}},
				{Base: 12, Bytes: 0},
				{Base: 15, Bytes: 5, Blocks: []*wiretype.Block{{Bytes: 5, Key: []byte("Y")}}},
				{Base: 144, Bytes: 0},
			},
		}

		dx := new(fileData)
		if err := dx.fromWireType(idx); err != nil {
			t.Errorf("Decoding index failed: %v", err)
		}
		want := &fileData{
			totalBytes: 20,
			extents: []*extent{
				{base: 3, bytes: 7, blocks: []cblock{{bytes: 7, key: "X"}}},
				{base: 15, bytes: 5, blocks: []cblock{{bytes: 5, key: "Y"}}},
			},
		}
		if diff := cmp.Diff(want, dx, cmpFileDataOpts...); diff != "" {
			t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
		}
	})
}

func TestWriteBlocking(t *testing.T) {
	ti := newTestInput("\x00\x00\x00\x00foo\x00\x00\x00\x00|barf\x00\x00\x00|\x00\x00\x00bazzu")
	d := newDataTester(t, &block.SplitConfig{
		Hasher: ti, Min: 5, Size: 16, Max: 100,
	})
	d.writeString(ti.input, 0)
	want := &fileData{
		totalBytes: int64(ti.inputLen()),
		extents: []*extent{
			{base: 4, bytes: 3, blocks: []cblock{{bytes: 3, key: hashOf("foo")}}},
			{base: 11, bytes: 4, blocks: []cblock{{bytes: 4, key: hashOf("barf")}}},
			{base: 21, bytes: 5, blocks: []cblock{{bytes: 5, key: hashOf("bazzu")}}},
		},
	}
	if diff := cmp.Diff(want, d.fd, cmpFileDataOpts...); diff != "" {
		t.Errorf("Wrong decoded block (-want, +got)\n%s", diff)
	}
}

func TestReblocking(t *testing.T) {
	d := newDataTester(t, &block.SplitConfig{Min: 200, Size: 1024, Max: 8192})
	rand.Seed(1) // change to update test data

	const alphabet = "0123456789abcdef"
	var buf bytes.Buffer
	for buf.Len() < 4000 {
		buf.WriteByte(alphabet[rand.Intn(len(alphabet))])
	}
	fileData := buf.String()

	// Write the data in a bunch of small contiguous chunks, and verify that the
	// result reblocks adjacent chunks.
	i, nb := 0, 0
	for i < len(fileData) {
		end := i + 25
		if end > len(fileData) {
			end = len(fileData)
		}
		d.writeString(fileData[i:end], int64(i))
		i = end
		nb++
	}

	check := func(want ...int64) {
		var total int64
		var got []int64
		for _, ext := range d.fd.extents {
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
	d.writeString(strings.Repeat("AAAA", 500), 0)
	check(2810, 413, 255, 522) // manually checked; note tail is stable

	t.Log("Block manifest:")
	d.fd.blocks(func(size int64, key string) {
		t.Logf("%-4d\t%x", size, []byte(key))
	})
}

type testInput struct {
	template string
	splits   map[int]bool
	input    string
	pos      int
}

func TestNewFileData(t *testing.T) {
	type extinfo struct {
		Base, Bytes int64
		Blocks      int
	}
	tests := []struct {
		input string
		want  []extinfo
	}{
		{
			//     |<--------------- 43 bytes ------------------>|                    |<- 15 bytes -->|
			//     |   block 1    ^   block 2     ^   block 3    |   ...unstored...   |   block 1     |
			input: "The first line|The second line|The third line|\x00\x00\x00\x00\x00|The fourth line",
			want:  []extinfo{{0, 43, 3}, {48, 15, 1}},
		},
		{
			// sizes:            3               3               3
			//       unstored  |   | unstored  |   | unstored  |   |   unstored    |
			input: "\x00\x00\x00foo|\x00\x00\x00bar\x00\x00\x00|baz\x00\x00\x00\x00",
			want:  []extinfo{{3, 3, 1}, {9, 3, 1}, {15, 3, 1}},
		},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			ti := newTestInput(test.input)
			s := block.NewSplitter(ti.reader(), &block.SplitConfig{
				Hasher: ti, Min: 5, Max: 100, Size: 16,
			})

			// Generate a new data index from the input. We don't actually store
			// any data here, just generate some plausible keys as if we did.
			fd, err := newFileData(s, func(data []byte) (string, error) {
				t.Logf("Block: %q", string(data))
				h := sha1.New()
				h.Write(data)
				return string(h.Sum(nil)), nil
			})
			if err != nil {
				t.Fatalf("newFileData failed: %v", err)
			}

			// Verify that the construction preserved all the input.
			t.Logf("Input size: %d, total bytes: %d", ti.inputLen(), fd.totalBytes)
			if want := ti.inputLen(); fd.totalBytes != int64(want) {
				t.Errorf("TotalBytes: got %d, want %d", fd.totalBytes, want)
			}

			// Verify that the created extents match the template.
			var got []extinfo
			for i, ext := range fd.extents {
				t.Logf("Extent %d base %d bytes %d", i+1, ext.base, ext.bytes)
				got = append(got, extinfo{
					Base:   ext.base,
					Bytes:  ext.bytes,
					Blocks: len(ext.blocks),
				})
				for j, b := range ext.blocks {
					t.Logf("- E%d block %d: %d bytes, key=%x", i+1, j+1, b.bytes, b.key)
				}
			}
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("Wrong extents: (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestBlockReader(t *testing.T) {
	const message = "you are not the person we thought you were"

	input := bytes.SplitAfter([]byte(message), []byte(" "))
	r := newBlockReader(input)
	var data []byte
	buf := make([]byte, 8)
	for {
		nr, err := r.Read(buf)
		data = append(data, buf[:nr]...)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Read failed: nr=%d, err=%v", nr, err)
		}
	}
	got := string(data)
	if got != message {
		t.Errorf("Block reader:\n- got  %q\n- want %q", got, message)
	}
}

func TestSetZero(t *testing.T) {
	isZero := func(data []byte) bool {
		for _, b := range data {
			if b != 0 {
				return false
			}
		}
		return true
	}

	for i := 1; i <= 2048; i += 7 {
		buf := make([]byte, i)
		rand.Read(buf)
		n := zero(buf)
		if !isZero(buf) {
			t.Errorf("zero(#[%d]) failed: %+v", i, buf)
		}
		if n != len(buf) {
			t.Errorf("Wrong size returned: got %d, want %d", n, len(buf))
		}
	}
}

func hashOf(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	return string(h.Sum(nil))
}

type dataTester struct {
	t   *testing.T
	ctx context.Context
	cas blob.CAS
	fd  *fileData
}

func newDataTester(t *testing.T, sc *block.SplitConfig) *dataTester {
	return &dataTester{
		t:   t,
		ctx: context.Background(),
		cas: blob.NewCAS(memstore.New(), sha1.New),
		fd:  &fileData{sc: sc},
	}
}

func (d *dataTester) writeString(s string, at int64) {
	d.t.Helper()
	nw, err := d.fd.writeAt(d.ctx, d.cas, []byte(s), at)
	d.t.Logf("Write %q at offset %d (%d, %v)", s, at, nw, err)
	if err != nil {
		d.t.Fatalf("writeAt(ctx, %q, %d): got (%d, %v), unexpected error", s, at, nw, err)
	} else if nw != len(s) {
		d.t.Errorf("writeAt(ctx, %q, %d): got %d, want %d", s, at, nw, len(s))
	}
}

func (d *dataTester) checkString(at, nb int64, want string) {
	d.t.Helper()
	buf := make([]byte, nb)
	nr, err := d.fd.readAt(d.ctx, d.cas, buf, at)
	d.t.Logf("Read %d from offset %d (%d, %v)", nb, at, nr, err)
	if err != nil && err != io.EOF {
		d.t.Fatalf("readAt(ctx, #[%d], %d): got (%d, %v), unexpected error", nb, at, nr, err)
	} else if got := string(buf[:nr]); got != want {
		d.t.Errorf("readAt(ctx, #[%d], %d): got %q, want %q", nb, at, got, want)
	}
}

func (d *dataTester) truncate(at int64) {
	d.t.Helper()
	err := d.fd.truncate(d.ctx, d.cas, at)
	d.t.Logf("truncate(ctx, %d) %v", at, err)
	if err != nil {
		d.t.Fatalf("truncate(ctx, %d): unexpected error: %v", at, err)
	}
}

func newTestInput(template string) *testInput {
	parts := strings.Split(template, "|")
	splits := make(map[int]bool)
	pos := 0
	for _, p := range parts {
		pos += len(p)
		splits[pos] = true
		pos++
	}
	return &testInput{
		template: template,
		input:    strings.Join(parts, ""),
		splits:   splits,
	}
}

func (ti *testInput) reader() *strings.Reader { return strings.NewReader(ti.input) }
func (ti *testInput) inputLen() int           { return len(ti.input) }
func (ti *testInput) inc()                    { ti.pos++ }

func (ti *testInput) Hash() block.Hash { return ti }
func (ti *testInput) Update(b byte) uint64 {
	defer ti.inc()
	if ti.splits[ti.pos] {
		return 1
	}
	return 2
}
