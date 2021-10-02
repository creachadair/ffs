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
	"context"
	"fmt"
	"sort"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalJSON implements the json.Marshaler interface for a *Node, by
// delegating to the protojson marshaler. This allows a node to be encoded
// using the encoding/json package transparently.
func (n *Node) MarshalJSON() ([]byte, error) { return protojson.Marshal(n) }

// MarshalJSON implements the json.Marshaler interface for a *Root, by
// delegating to the protojson marshaler. This allows a node to be encoded
// using the encoding/json package transparently.
func (r *Root) MarshalJSON() ([]byte, error) { return protojson.Marshal(r) }

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
	if x == nil || len(x.Extents) == 0 {
		return
	}
	sort.Slice(x.Extents, func(i, j int) bool {
		return x.Extents[i].Base < x.Extents[j].Base
	})
	i, j := 0, 1
	for j < len(x.Extents) {
		// If two adjacent extents abut, merge them into the first.
		if x.Extents[i].Base+x.Extents[i].Bytes == x.Extents[j].Base {
			x.Extents[i].Bytes += x.Extents[j].Bytes
			x.Extents[i].Blocks = append(x.Extents[i].Blocks, x.Extents[j].Blocks...)
		} else {
			i++
			x.Extents[i] = x.Extents[j]
		}
		j++
	}
	x.Extents = x.Extents[:i+1]
}

// Store is the interface to storage used by the Load and Save functions.
type Store interface {
	Get(context.Context, string) ([]byte, error)
	PutCAS(context.Context, []byte) (string, error)
}

// Load reads the specified blob from s and decodes it into msg.
func Load(ctx context.Context, s Store, key string, msg proto.Message) error {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("loading message: %w", err)
	}
	return proto.Unmarshal(bits, msg)
}

// Save encodes msg in wire format and writes it to s, returning the storage key.
func Save(ctx context.Context, s Store, msg proto.Message) (string, error) {
	bits, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("encoding message: %w", err)
	}
	return s.PutCAS(ctx, bits)
}
