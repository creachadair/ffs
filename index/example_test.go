// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package index_test

import (
	"fmt"
	"strings"

	"github.com/creachadair/ffs/index"
)

func ExampleIndex() {
	words := strings.Fields("a foolish consistency is the hobgoblin of little minds")

	idx := index.New(32, nil)
	for _, word := range words {
		idx.Add(word)
	}

	fmt.Printf("Has %q: %v\n", "foolish", idx.Has("foolish"))
	fmt.Printf("Has %q: %v\n", "cabbage", idx.Has("cabbage"))

	info := idx.Stats()
	fmt.Printf("%d keys, %d filter bits, %d hash seeds\n",
		info.NumKeys, info.FilterBits, info.NumHashes)

	// Output:
	// Has "foolish": true
	// Has "cabbage": false
	// 9 keys, 256 filter bits, 6 hash seeds
}
