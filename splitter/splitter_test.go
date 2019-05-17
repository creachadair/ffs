package splitter

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

// burstyReader implements io.Reader, returning chunks from r whose size is
// bounded above by the specified byte lengths, to simulate a reader that does
// not always deliver all that was requested.
type burstyReader struct {
	r   io.Reader
	len []int
	pos int
}

func (b *burstyReader) Read(buf []byte) (int, error) {
	cap := len(buf)
	if len(b.len) > b.pos {
		if n := b.len[b.pos]; n < cap {
			cap = b.len[b.pos]
		}
		b.pos = (b.pos + 1) % len(b.len)
	}
	return b.r.Read(buf[:cap])
}

func newBurstyReader(s string, sizes ...int) io.Reader {
	return &burstyReader{strings.NewReader(s), sizes, 0}
}

// dummyHash is a mock Hash implementation used for testing Splitter.  It
// returns a fixed value for all updates except a designated value.
type dummyHash struct {
	magic byte
	hash  uint
	size  int
}

func (dummyHash) Reset() {}

func (d dummyHash) Update(in byte) uint {
	if in == d.magic {
		return 1
	}
	return d.hash
}

func (d dummyHash) Size() int { return d.size }

func TestSplitterMin(t *testing.T) {
	const minBytes = 10
	d := dummyHash{
		magic: '|',
		hash:  12345,
		size:  1,
	}
	c := Config{Hash: d, Min: minBytes}
	s := c.New(strings.NewReader("abc|def|ghi|jkl|mno"))
	b, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < minBytes {
		t.Errorf("len(b): got %d, want at least %d", len(b), minBytes)
	}
	t.Logf("b=%q", string(b))
}

func TestSplitterMax(t *testing.T) {
	const maxBytes = 10
	d := dummyHash{
		hash: 12345,
		size: 1,
	}
	c := Config{Hash: d, Max: maxBytes}
	s := c.New(strings.NewReader("abc|def|ghi|jkl|mno"))
	b, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > maxBytes {
		t.Errorf("len(b): got %d, want at most %d", len(b), maxBytes)
	}
	t.Logf("b=%q", string(b))
}

func TestSplitterBlocks(t *testing.T) {
	tests := []struct {
		input    string
		min, max int
		blocks   []string
	}{
		// In these test cases, any "|" in the input triggers a hash cut.  This
		// permits us to verify the various corner cases of when a cut occurs
		// vs. the length constraints.
		{"", 5, 15, nil},
		{"abc", 5, 15, []string{"abc"}},
		{"|", 0, 15, []string{"|"}},
		{"x||y", 1, 15, []string{"x", "|", "|y"}},
		{"|||x", 1, 5, []string{"|", "|", "|x"}},
		{"a|bc|defg|hijklmno|pqrst", 2, 8, []string{"a|bc", "|defg", "|hijklmn", "o|pqrst"}},
		{"abcdefgh|ijklmnop|||q", 5, 100, []string{"abcdefgh", "|ijklmnop", "|||q"}},
		{"a|b|c|d|e|", 1, 2, []string{"a", "|b", "|c", "|d", "|e", "|"}},
		{"abcdefghijk", 4, 4, []string{"abcd", "efgh", "ijk"}},
	}
	d := dummyHash{
		magic: '|',
		hash:  12345,
		size:  5,
	}
	for _, test := range tests {
		c := Config{Hash: d, Min: test.min, Max: test.max}
		s := c.New(newBurstyReader(test.input, 3, 5, 1, 4, 17, 20))
		var bs []string
		if err := s.Split(func(b []byte) error {
			bs = append(bs, string(b))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(bs, test.blocks) {
			t.Errorf("split %q: got %+q, want %+q", test.input, bs, test.blocks)
		}
	}
}
