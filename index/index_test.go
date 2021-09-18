package index

import (
	"strings"
	"testing"
)

func TestBuilder(t *testing.T) {
	input := strings.Fields("garbage humans suck penniless nerbleweefers")

	b := NewBuilder(nil)
	for _, key := range input {
		b.AddKey(key)
	}
	idx := b.Build()

	t.Logf("Keys:  %+q", b.keys)
	t.Logf("Table: %+x", idx.table)

	for _, key := range input {
		if !idx.Has(key) {
			t.Errorf("Has(%q): got false, want true)", key)
		}
	}

	wrong := strings.Fields("wrong things are sucking as much as right ones")
	for _, key := range wrong {
		if idx.Has(key) {
			t.Errorf("Has(%q): got true, want false", key)
		}
	}
}
