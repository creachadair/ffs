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
package wiretype

// Requires: google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate protoc --go_out=. --go_opt=paths=source_relative wiretype.proto

import "sort"

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
