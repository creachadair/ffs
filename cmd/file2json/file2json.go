// Copyright (C) 2019 Michael J. Fromberger. All Rights Reserved.

package main

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/creachadair/ffs/file/wiretype"
	"google.golang.org/protobuf/proto"
)

func main() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Read: %v", err)
	}
	out := make(map[string]proto.Message)

	var obj wiretype.Object
	if err := proto.Unmarshal(data, &obj); err != nil {
		log.Fatalf("Decode: %v", err)
	}
	switch t := obj.Value.(type) {
	case *wiretype.Object_Node:
		out["node"] = t.Node
	case *wiretype.Object_Root:
		out["root"] = t.Root
	case *wiretype.Object_Index:
		out["index"] = t.Index
	default:
		log.Fatalf("Unknown blob format (%d bytes)", len(data))
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
