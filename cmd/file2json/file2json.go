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
	out := make(map[string]proto.Message)
	{ // Check whether the message is a valid root blob.
		var rpb wiretype.Root
		if err := proto.Unmarshal(data, &rpb); err == nil {
			if err := rpb.CheckValid(); err == nil {
				out["root"] = &rpb
			}
		}
	}

	if len(out) == 0 {
		// If it wasn't a root blob, it has to be a node, or the game is off.
		var npb wiretype.Node
		if err := proto.Unmarshal(data, &npb); err != nil {
			log.Fatalf("Decode: %v", err)
		}
		out["node"] = &npb
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
