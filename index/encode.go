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
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/creachadair/ffs/index/indexpb"
)

// Encode encodes idx as a protocol buffer message for storage.
func Encode(idx *Index) *indexpb.Index {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	for _, seg := range idx.bits {
		var val [8]byte
		binary.BigEndian.PutUint64(val[:], seg)
		w.Write(val[:])
	}
	w.Close()
	return &indexpb.Index{
		NumKeys:     uint64(idx.numKeys),
		Seeds:       idx.seeds,
		NumSegments: uint64(len(idx.bits)),
		SegmentData: buf.Bytes(),
	}
}

// Decode decodes an encoded index from protobuf.
func Decode(pb *indexpb.Index) (*Index, error) {
	idx := &Index{
		numKeys: int(pb.NumKeys),
		seeds:   pb.Seeds,
		hash:    (*Options)(nil).hashFunc(), // the default

		// TODO(creachadair): Check the hash_func value.
	}

	//lint:file-ignore SA1019 Support the old format until usage is updated.

	// Explicit segments, no compression.
	if len(pb.Segments) != 0 {
		idx.bits = bitVector(pb.Segments)
		idx.nbits = 64 * uint64(len(pb.Segments))
		return idx, nil
	}

	// Compressed segments.
	rc, err := zlib.NewReader(bytes.NewReader(pb.SegmentData))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	bits, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	nseg := int(pb.NumSegments)
	if len(bits) != 8*nseg {
		return nil, fmt.Errorf("invalid segment data: got %d bytes, want %d", len(bits), 8*nseg)
	}
	idx.bits = make(bitVector, nseg)
	idx.nbits = 64 * pb.NumSegments
	for i := 0; i < nseg; i++ {
		idx.bits[i] = binary.BigEndian.Uint64(bits[8*i:])
	}
	return idx, nil
}
