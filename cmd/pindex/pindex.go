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

// Program pindex prints a wire-format file index as text.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"bitbucket.org/creachadair/ffs/file/wirepb"
	"github.com/golang/protobuf/proto"
)

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatalf("Usage: %s <index-file-path>", filepath.Base(os.Args[0]))
	}
	data, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		log.Fatalf("Reading input: %v", err)
	}
	var pb wirepb.Node
	if err := proto.Unmarshal(data, &pb); err != nil {
		log.Fatalf("Decoding input: %v", err)
	}
	fmt.Println(proto.MarshalTextString(&pb))
}
