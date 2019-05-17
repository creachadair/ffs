package file

import (
	"context"
	"crypto/sha1"
	"io"
	"testing"

	"bitbucket.org/creachadair/ffs/blob"
	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/splitter"
	"github.com/google/go-cmp/cmp"
)

func hashOf(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	return string(h.Sum(nil))
}

func TestIndex(t *testing.T) {
	mem := memstore.New()
	d := &Data{
		s:  blob.NewCAS(mem, sha1.New),
		sc: &splitter.Config{Min: 1024}, // in effect, "don't split"
	}
	ctx := context.Background()
	writeString := func(s string, at int64) {
		nw, err := d.writeAt(ctx, []byte(s), at)
		t.Logf("Write %q at offset %d (%d, %v)", s, at, nw, err)
		if err != nil {
			t.Fatalf("writeAt(ctx, %q, %d): got (%d, %v), unexpected error", s, at, nw, err)
		} else if nw != len(s) {
			t.Errorf("writeAt(ctx, %q, %d): got %d, want %d", s, at, nw, len(s))
		}
	}
	checkString := func(at, nb int64, want string) {
		buf := make([]byte, nb)
		nr, err := d.readAt(ctx, buf, at)
		t.Logf("Read %d from offset %d (%d, %v)", nb, at, nr, err)
		if err != nil && err != io.EOF {
			t.Fatalf("readAt(ctx, #[%d], %d): got (%d, %v), unexpected error", nb, at, nr, err)
		} else if got := string(buf[:nr]); got != want {
			t.Errorf("readAt(ctx, #[%d], %d): got %q, want %q", nb, at, got, want)
		}
	}
	truncate := func(at int64) {
		err := d.truncate(ctx, at)
		t.Logf("truncate(ctx, %d) %v", at, err)
		if err != nil {
			t.Fatalf("truncate(ctx, %d): unexpected error: %v", at, err)
		}
	}
	checkIndex := func(want index) {
		// We have to tell cmp that it's OK to look at unexported fields on these types.
		opt := cmp.AllowUnexported(index{}, extent{}, block{})
		if diff := cmp.Diff(want, d.index, opt); diff != "" {
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
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []block{
				{11, hashOf("fookinghell")},
				{4, hashOf("mate")},
			}, starts: []int64{0, 11}},
		},
	})

	writeString("cor", 20)
	checkString(0, 100, "fookinghellmate\x00\x00\x00\x00\x00cor")
	// fookinghellmate-----cor

	checkIndex(index{
		totalBytes: 23,
		extents: []*extent{
			{base: 0, bytes: 15, blocks: []block{
				{11, hashOf("fookinghell")},
				{4, hashOf("mate")},
			}, starts: []int64{0, 11}},
			{base: 20, bytes: 3, blocks: []block{
				{3, hashOf("cor")},
			}, starts: []int64{20}},
		},
	})
}
