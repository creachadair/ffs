// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package index

import (
	"github.com/cespare/xxhash/v2"
	"github.com/creachadair/ffs/index/indexpb"
)

// Encode encodes idx as a protocol buffer message for storage.
func Encode(idx *Index) *indexpb.EncodedIndex {
	return &indexpb.EncodedIndex{
		NumKeys:  uint64(idx.numKeys),
		Seeds:    idx.seeds,
		Segments: []uint64(idx.bits),
	}
}

// Decode decodes a compressed index from protobuf.
func Decode(pb *indexpb.EncodedIndex) *Index {
	return &Index{
		numKeys: int(pb.NumKeys),
		bits:    bitVector(pb.Segments),
		nbits:   64 * uint64(len(pb.Segments)),
		seeds:   pb.Seeds,
		hash:    xxhash.Sum64String,

		// TODO(creachadair): Check the hash_func value.
	}
}
