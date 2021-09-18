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

// Package wiretype defines the encoding types for the ffs package.
// Most of this package is generated by the protocol buffer compiler from the
// schema in file/wiretype/wiretype.proto.
package wiretype

// Requires: google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate protoc --go_out=. --go_opt=paths=source_relative wiretype.proto

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"
)

// MarshalJSON implements the json.Marshaler interface for a *Node, by
// delegating to the protojson marshaler. This allows a node to be encoded
// using the encoding/json package transparently.
func (n *Node) MarshalJSON() ([]byte, error) { return protojson.Marshal(n) }

// Normalize updates n in-place so that all fields are in canonical order.
func (n *Node) Normalize() {
	n.Index.Normalize()
	sort.Slice(n.XAttrs, func(i, j int) bool {
		return n.XAttrs[i].Name < n.XAttrs[j].Name
	})
	sort.Slice(n.Children, func(i, j int) bool {
		return n.Children[i].Name < n.Children[j].Name
	})
}

// Normalize updates n in-place so that all fields are in canonical order.
func (x *Index) Normalize() {
	if x == nil {
		return
	}
	sort.Slice(x.Extents, func(i, j int) bool {
		return x.Extents[i].Base < x.Extents[j].Base
	})
}

// CheckValid checks whether r is a valid root message, meaning that it has a
// non-empty root file key and a valid checksum. It returns nil if the message
// is valid; otherwise a descriptive error.
func (r *Root) CheckValid() error {
	if len(r.RootFileKey) == 0 {
		return errors.New("invalid root: missing file key")
	}
	if want := r.ComputeChecksum(); want != r.Checksum {
		return fmt.Errorf("invalid root: wrong checksum %x", r.Checksum)
	}
	return nil
}

// SetChecksum computes and sets the checksum field of r, returning r.
func (r *Root) SetChecksum() *Root { r.Checksum = r.ComputeChecksum(); return r }

// ComputeChecksum computes and returns the checksum of r from its contents.
func (r *Root) ComputeChecksum() uint32 {
	crc := crc32.NewIEEE()
	crc.Write(r.RootFileKey)
	crc.Write([]byte(r.Description))
	crc.Write(r.BlobIndexKey)
	crc.Write(r.OwnerKey)
	return crc.Sum32()
}
