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

package monitor_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/ffs/storage/monitor"
)

type kvStub struct{ blob.KV }

var _ blob.Store = (*monitor.M[any, kvStub])(nil)

type testDB = filestore.Store
type testKV = filestore.KV

type testStore struct {
	*monitor.M[testDB, testKV]
}

func (t testStore) Close(ctx context.Context) error { return t.M.DB.Close(ctx) }

func TestMonitor(t *testing.T) {
	fs, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatalf("Create base store: %v", err)
	}
	s := testStore{M: monitor.New(monitor.Config[testDB, testKV]{
		DB: fs,
		NewKV: func(ctx context.Context, fs testDB, _ dbkey.Prefix, name string) (testKV, error) {
			kv, err := fs.KV(ctx, name)
			if err != nil {
				return testKV{}, err
			}
			return kv.(testKV), nil
		},
		NewSub: func(ctx context.Context, fs testDB, _ dbkey.Prefix, name string) (testDB, error) {
			sub, err := fs.Sub(ctx, name)
			if err != nil {
				return testDB{}, err
			}
			return sub.(testDB), nil
		},
	})}
	storetest.Run(t, s)
}
