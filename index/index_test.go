package index_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/creachadair/ffs/index"
)

func TestIndex(t *testing.T) {
	keyData, err := ioutil.ReadFile("testdata/keys.txt")
	if err != nil {
		t.Fatalf("Reading keys: %v", err)
	}
	keys := strings.Split(strings.TrimSpace(string(keyData)), "\n")
	t.Logf("Read %d bytes (%d keys) from keys.txt", len(keyData), len(keys))

	idx := index.New(len(keys), &index.Options{
		FalsePositiveRate: 0.001,
	})

	// Add keys at even offsets, skip keys at odd ones.
	// Thus we expect half the keys to be missing.
	var numAdded, totalKeyBytes int
	for i, key := range keys {
		if i%2 == 0 {
			idx.Add(key)
			numAdded++
			totalKeyBytes += len(key)
		}
	}
	t.Logf("Added %d keys to the index", numAdded)

	stats := idx.Stats()
	t.Logf("Index stats: %d keys, %d filter bits (m), %d hash seeds",
		stats.NumKeys, stats.FilterBits, stats.NumHashes)
	if stats.NumKeys != numAdded {
		t.Errorf("Wrong number of keys: got %d, want %d", stats.NumKeys, numAdded)
	}
	t.Logf("Total indexed key size: %d bytes", totalKeyBytes)
	approxFilterBytes := (stats.FilterBits+7)/8 + 8*stats.NumHashes
	t.Logf("Approximate index size: %d bytes (%.2f%%)", approxFilterBytes,
		percent(approxFilterBytes, totalKeyBytes))

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
