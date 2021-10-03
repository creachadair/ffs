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

package block_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/creachadair/ffs/block"
)

func Example() {
	// Note that these two strings are similar, but the second one has been
	// edited. The edit changes the block splits around the point of the edit,
	// but the sliding window allows them to resynchronize after the break.
	const input1 = `abcdefg-hijklmnop-qrstuv-wxyz-abcdefg-hijklmnop-qrstuv-wxyz-abcdefghijklmnopqrstuv`
	const input2 = `abcdefg-hijklmnop-qrstuv-wxyz-*-abcdefg-hijklmnop-qrstuv-wxyz-abcdefghijklmnopqrstuv`

	opts := &block.SplitConfig{
		Min:    5,  // no blocks shorter than this
		Size:   10, // desired mean block size
		Max:    20, // no blocks longer than this
		Hasher: block.RabinKarpHasher(23, 997, 13),
	}

	for _, v := range []string{input1, input2} {
		s := block.NewSplitter(strings.NewReader(v), opts)
		var i int
		if err := s.Split(func(data []byte) error {
			i++
			fmt.Printf("%d. %s\n", i, string(data))
			return nil
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println()
	}

	// Output:
	//
	// 1. abcdefg-h
	// 2. ijklmnop-qrstu
	// 3. v-wxyz-abcdefg
	// 4. -hijklmnop-qrstu
	// 5. v-wxyz-abcdefghijklm
	// 6. nopqrstuv
	//
	// 1. abcdefg-h
	// 2. ijklmnop-qrstu
	// 3. v-wxyz-*-
	// 4. abcdefg-hi
	// 5. jklmnop-qrstu
	// 6. v-wxyz-abcdefghijklm
	// 7. nopqrstuv
}
