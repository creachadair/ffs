package index

import (
	"io/ioutil"
	"strings"
	"testing"
)

func TestBuilder(t *testing.T) {
	keyData, err := ioutil.ReadFile("testdata/keys.txt")
	if err != nil {
		t.Fatalf("Reading keys: %v", err)
	}
	keys := strings.Split(string(keyData), "\n")
	b := NewBuilder(nil)

	const startOffset = 10
	for _, key := range keys[startOffset:] {
		if key != "" {
			b.AddKey(key)
		}
	}
	idx := b.Build()
	t.Logf("Index has %d ranks and %d tail keys", len(idx.table), len(idx.tail))

	for i, key := range keys {
		want := key != "" && i >= startOffset
		got := idx.Has(key)
		if got != want {
			t.Errorf("idx.Has(%q) at offset %d: got %v, want %v", key, i, got, want)
		}
	}
}
