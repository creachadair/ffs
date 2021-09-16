// Copyright (C) 2019 Michael J. Fromberger. All Rights Reserved.

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/creachadair/ffs/file/wiretype"
	"google.golang.org/protobuf/proto"
)

func main() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Read: %v", err)
	}
	var pb wiretype.Node
	if err := proto.Unmarshal(data, &pb); err != nil {
		log.Fatalf("Decode: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(&pb)
}
