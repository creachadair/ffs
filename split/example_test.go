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

package split_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/creachadair/ffs/split"
)

func Example() {
	// Return blocks no larger than 10 bytes.  Otherwise, use default settings,
	// which includes a Rabin-Karp rolling hash.
	r := strings.NewReader("Four score and seven years ago...")
	s := split.New(r, &split.Config{Max: 10})
	if err := s.Split(func(data []byte) error {
		fmt.Println(string(data))
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Output:
	// Four score
	//  and seven
	//  years ago
	// ...
}
