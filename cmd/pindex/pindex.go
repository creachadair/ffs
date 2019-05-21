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
