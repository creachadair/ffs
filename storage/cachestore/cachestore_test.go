// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
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

package cachestore_test

import (
	"crypto/sha1"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/cachestore"
)

func TestStore(t *testing.T) {
	m := memstore.New()
	c := cachestore.New(m, 100)
	storetest.Run(t, c)
}

func TestCAS(t *testing.T) {
	bs := blob.NewCAS(memstore.New(), sha1.New)
	c := cachestore.NewCAS(bs, 100)
	storetest.Run(t, c)
}
