package index_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/creachadair/ffs/index"
)

func TestBuilder(t *testing.T) {
	keyData, err := ioutil.ReadFile("testdata/keys.txt")
	if err != nil {
		t.Fatalf("Reading keys: %v", err)
	}
	keys := strings.Split(strings.TrimSpace(string(keyData)), "\n")
	b := index.NewBuilder(&index.BuilderOpts{
		BitsPerKey: 16,
	})

	// Add keys at even offsets, skip keys at odd ones.  Thus we expect half the
	// keys to be missing.
	var numAdded, totalKeyBytes int
	for i, key := range keys {
		if i%2 == 0 {
			b.AddKey(key)
			numAdded++
			totalKeyBytes += len(key)
		}
	}
	t.Logf("Added %d keys to the builder out of %d total", numAdded, len(keys))

	idx := b.Build()
	t.Logf("Index has %d keys", idx.NumKeys())
	t.Logf("Index data size: %d bytes", idx.Size())
	t.Logf("Total key size:  %d bytes", totalKeyBytes)

	falses := make(map[bool]int)
	for i, key := range keys {
		want := i%2 == 0
		got := idx.Has(key)
		if got != want {
			falses[got]++
		}
	}

	// We expect there to be false positives.
	t.Logf("False positives: %d (%.2f%%)", falses[true], percent(falses[true], len(keys)))

	// There should be no false negatives.
	if neg := falses[false]; neg != 0 {
		t.Errorf("False negatives: %d (%.2f%%)", neg, percent(neg, len(keys)))
	}
}

func percent(x, n int) float64 { return 100 * (float64(x) / float64(n)) }
