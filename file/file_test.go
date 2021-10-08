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

package file_test

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/block"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// Interface satisfaction checks.
var (
	_ fs.File     = (*file.Cursor)(nil)
	_ fs.FileInfo = file.Info{}
)

func TestRoundTrip(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)

	// Construct a new file and write it to storage, then read it back and
	// verify that the original state was correctly restored.
	f := file.New(cas, &file.NewOptions{
		Stat:  &file.Stat{Mode: 0640},
		Split: &block.SplitConfig{Min: 17, Size: 84, Max: 500},
	})
	if n := f.Size(); n != 0 {
		t.Errorf("Size: got %d, want 0", n)
	}
	ctx := context.Background()

	wantx := map[string]string{
		"fruit": "apple",
		"nut":   "hazelnut",
	}
	for k, v := range wantx {
		f.XAttr().Set(k, v)
	}

	const testMessage = "Four fat fennel farmers fell feverishly for Felicia Frances"
	fmt.Fprint(f.Cursor(ctx), testMessage)
	if n := f.Size(); n != int64(len(testMessage)) {
		t.Errorf("Size: got %d, want %d", n, len(testMessage))
	}
	fkey, err := f.Flush(ctx)
	if err != nil {
		t.Fatalf("Flushing failed: %v", err)
	}

	g, err := file.Open(ctx, cas, fkey)
	if err != nil {
		t.Fatalf("Open %x: %v", fkey, err)
	}

	// Verify that file contents were preserved.
	bits, err := ioutil.ReadAll(g.Cursor(ctx))
	if err != nil {
		t.Errorf("Reading %x: %v", fkey, err)
	}
	if got := string(bits); got != testMessage {
		t.Errorf("Reading %x: got %q, want %q", fkey, got, testMessage)
	}

	// Verify that extended attributes were preserved.
	gotx := make(map[string]string)
	g.XAttr().List(func(key, val string) {
		if v, ok := g.XAttr().Get(key); !ok || v != val {
			t.Errorf("GetXAttr(%q): got (%q, %v), want (%q, true)", key, v, ok, val)
		}
		gotx[key] = val
	})
	if diff := cmp.Diff(wantx, gotx); diff != "" {
		t.Errorf("XAttr (-want, +got)\n%s", diff)
	}

	// Verify that file stat was preserved.
	ignoreUnexported := cmpopts.IgnoreUnexported(file.Stat{})
	if diff := cmp.Diff(f.Stat(), g.Stat(), ignoreUnexported); diff != "" {
		t.Errorf("Stat (-want, +got)\n%s", diff)
	}
	if got, want := g.Stat().Persistent(), f.Stat().Persistent(); got != want {
		t.Errorf("Stat persist: got %v, want %v", got, want)
	}

	// Verify that seek and truncation work.
	if err := g.Truncate(ctx, 15); err != nil {
		t.Errorf("Truncate(15): unexpected error: %v", err)
	} else if pos, err := g.Cursor(ctx).Seek(0, io.SeekStart); err != nil {
		t.Errorf("Seek(0): unexpected error: %v", err)
	} else if pos != 0 {
		t.Errorf("Pos after Seek(0): got %d, want 0", pos)
	} else if bits, err := ioutil.ReadAll(g.Cursor(ctx)); err != nil {
		t.Errorf("Read failed: %v", err)
	} else if got, want := string(bits), testMessage[:15]; got != want {
		t.Errorf("Truncated message: got %q, want %q", got, want)
	}

	// Exercise the scanner.
	if err := f.Scan(ctx, func(key string, isFile bool) bool {
		if isFile && key != fkey {
			t.Errorf("File key: got %x, want %x", key, fkey)
		} else {
			t.Logf("Data key %x OK", key)
		}
		return true
	}); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
}

func TestChild(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	root := file.New(cas, nil)

	names := []string{"all.txt", "your.go", "base.exe"}
	for _, name := range names {
		root.Child().Set(name, root.New(nil))
	}

	// Names should come out in lexicographic order.
	sort.Strings(names)

	// Child names should be correct even without a flush.
	if diff := cmp.Diff(names, root.Child().Names()); diff != "" {
		t.Errorf("Wrong children (-want, +got):\n%s", diff)
	}

	// Flushing shouldn't disturb the names.
	rkey, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("root.Flush failed: %v", err)
	}
	t.Logf("Flushed root to %x", rkey)

	if diff := cmp.Diff(names, root.Child().Names()); diff != "" {
		t.Errorf("Wrong children (-want, +got):\n%s", diff)
	}
}

func TestCycleCheck(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	root := file.New(cas, nil)

	kid := file.New(cas, nil)
	root.Child().Set("harmless", kid)
	kid.Child().Set("harmful", root)

	key, err := root.Flush(ctx)
	if err == nil {
		t.Errorf("Cyclic flush: got %q, nil, want error", key)
	} else {
		t.Logf("Cyclic flush correctly failed: %v", err)
	}
}

func TestSetData(t *testing.T) {
	cas := blob.NewCAS(memstore.New(), sha1.New)
	ctx := context.Background()
	root := file.New(cas, &file.NewOptions{
		Split: &block.SplitConfig{
			Hasher: lineHash{},
			Min:    5,
			Max:    100,
			Size:   16,
		},
	})

	// Flush out the block, so that we can check below that updating the content
	// invalidates the key.
	okey, err := root.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Old root key: %x", okey)

	const input = `My name is Ozymandias
King of Kings!
Look up on my works, ye mighty
and despair!`
	if err := root.SetData(ctx, strings.NewReader(input)); err != nil {
		t.Fatalf("SetData unexpectedly failed: %v", err)
	}
	key, err := root.Flush(ctx)
	if err != nil {
		t.Errorf("Flush failed: %v", err)
	}
	t.Logf("Root key: %x", key)

	// Make sure we invalidated the file key by setting its data.
	if okey == key {
		t.Errorf("File data was not invalidated: key is %x", key)
	}

	// As a reality check, read the node back in and check that we got the right
	// number of blocks.
	data, err := cas.Get(ctx, key)
	if err != nil {
		t.Fatalf("Block fetch: %v", err)
	}
	var obj wiretype.Object
	if err := proto.Unmarshal(data, &obj); err != nil {
		t.Fatalf("Unmarshal object: %v", err)
	}
	pb, ok := obj.Value.(*wiretype.Object_Node)
	if !ok {
		t.Fatal("Object does not contain a node")
	}

	// Make sure we stored the right amount of data.
	if got, want := pb.Node.Index.TotalBytes, uint64(len(input)); got != want {
		t.Logf("Stored total bytes: got %d, want %d", got, want)
	}

	// Make sure we stored the expected number of blocks.
	// The artificial hasher splits on newlines, so we can just count.
	var gotBlocks int
	for _, ext := range pb.Node.Index.Extents {
		gotBlocks += len(ext.Blocks)
	}
	wantBlocks := len(strings.Split(input, "\n"))
	if gotBlocks != wantBlocks {
		t.Errorf("Stored blocks: got %d, want %d", gotBlocks, wantBlocks)
	}

	t.Logf("Encoded node:\n%s", prototext.Format(pb.Node))
}

type lineHash struct{}

func (h lineHash) Hash() block.Hash { return h }

func (lineHash) Update(b byte) uint64 {
	if b == '\n' {
		return 1
	}
	return 2
}
