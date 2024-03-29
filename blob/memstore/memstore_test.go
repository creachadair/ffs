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

package memstore_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/google/go-cmp/cmp"
)

func TestStore(t *testing.T) {
	m := memstore.New()
	storetest.Run(t, m)
}

func TestSnapshot(t *testing.T) {
	m := memstore.New()
	m.Put(context.Background(), blob.PutOptions{
		Key:  "foo",
		Data: []byte("bar"),
	})
	m.Put(context.Background(), blob.PutOptions{
		Key:  "baz",
		Data: []byte("quux"),
	})
	m.Delete(context.Background(), "baz")

	got := m.Snapshot(nil)
	want := map[string]string{"foo": "bar"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Wrong snapshot: (-want, +got):\n%s", diff)
	}
}
