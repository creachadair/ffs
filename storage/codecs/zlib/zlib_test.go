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

package zlib_test

import (
	"testing"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/codecs/zlib"
	"github.com/creachadair/ffs/storage/encoded"
)

func TestStore(t *testing.T) {
	m := encoded.New(memstore.New(nil), zlib.NewCodec(zlib.LevelDefault))
	storetest.Run(t, storetest.NopCloser(m))
}
