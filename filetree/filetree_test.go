package filetree_test

import (
	"strings"
	"testing"
	"time"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/filetree/filetreetest"
)

func TestParseKey(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		errText string
	}{
		{"", "", "empty key is invalid"},
		{"@", "", "empty key is invalid"},

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

func TestStore(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		var zero filetree.Store
		if zero.IsValid() {
			t.Error("A zero Store should not report as valid")
		}
	})

	t.Run("Valid", func(t *testing.T) {
		mem := memstore.New(nil)
		s, err := filetree.NewStore(t.Context(), mem)
		if err != nil {
			t.Fatalf("Creating store: %v", err)
		}
		if !s.IsValid() {
			t.Error("New store should be valid")
		}
		if err := s.Close(t.Context()); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		s, err := filetree.NewStore(t.Context(), memstore.New(nil))
		if err != nil {
			t.Fatalf("Creating store: %v", err)
		}

		// Set up a root pointer with an empty file.
		filetreetest.SetRoot(t, s, "testing", nil)

		// Set up a file to add to the tree under a path.
		// Attach an extended attribute so it has some shape we can verify later.
		const testPath = "testing/a/b/target"
		const testAttr = "test.attr"
		const testValue = "hello, world!"
		ctime := time.Now()
		pi := filetreetest.SetFile(t, s, filetreetest.FileInfo{
			Path:    testPath,
			ModTime: ctime,
			XAttr:   map[string]string{testAttr: testValue},
		})
		t.Logf("Target key: %s", filetree.FormatKey32(pi.File.Key()))

		// There should be a root, and its base pointer should be what SetPath reported.
		rp := filetreetest.GetRoot(t, s, "testing")
		if pi.Root == nil {
			t.Error("Missing root pointer")
		} else if pi.Root.FileKey != rp.FileKey {
			t.Errorf("Root base file: got %s, want %s",
				filetree.FormatKey32(pi.Root.FileKey), filetree.FormatKey32(rp.FileKey))
		}

		// The root key should match what we created.
		if pi.RootKey != "testing" {
			t.Errorf("Root key: got %q, want testing", pi.RootKey)
		}

		// The filename should have been properly set.
		if got, want := pi.File.Name(), "target"; got != want {
			t.Errorf("Target file name: got %q, want %q", got, want)
		}

		// Make sure we got the same file back out, at least to the extent that
		// it has the stats and attributes we attached to it.
		if got := pi.File.XAttr().Get(testAttr); got != testValue {
			t.Errorf("Target file %q: got %q, want %q", testAttr, got, testValue)
		}
		if got := pi.File.Stat().ModTime; !got.Equal(ctime) {
			t.Errorf("Target file mod time: got %v, want %v", got, ctime)
		}
	})
}
