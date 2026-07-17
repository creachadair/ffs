package filetree_test

import (
	"strings"
	"testing"

	"github.com/creachadair/ffs/filetree"
)

func TestParseKey(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		errText string
	}{
		{"", "", ""},  // empty
		{"@", "", ""}, // literal
		{"@x", "x", ""},
		{"@@xyz", "@xyz", ""},

		{"3120322033", "1 2 3", ""}, // hex, even length
		{"YXBwbGU=", "apple", ""},   // base64 with padding

		{filetree.FormatKey32("cherry"), "cherry", ""}, // key32, i.e., base32 no padding

		{"12340", "", "invalid key"}, // looks hex, but odd length
		{"bozo", "", "invalid key"},  // uses unsupported key32
	}
	for _, tc := range tests {
		got, err := filetree.ParseKey(tc.input)
		if tc.errText == "" && err != nil {
			t.Errorf("ParseKey(%q): unexpected error: %v", tc.input, err)
		} else if tc.errText != "" && (err == nil || !strings.Contains(err.Error(), tc.errText)) {
			t.Errorf("ParseKey(%q): got err=%v, want %q", tc.input, err, tc.errText)
		}
		if got != tc.want {
			t.Errorf("ParseKey(%q): got %q, want %q", tc.input, got, tc.want)
		}
	}
}
