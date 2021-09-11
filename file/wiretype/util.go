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

package wiretype

import "github.com/creachadair/ffs/split"

// NewIndex constructs an Index by consuming blocks from s.  For each data
// block, NewIndex calls put to store the block and return its key. An error
// from put stops index construction and is returned to the caller.
func NewIndex(s *split.Splitter, put func([]byte) (string, error)) (*Index, error) {
	var idx Index
	var ext *Extent
	push := func() {
		if ext == nil {
			return
		}
		for _, b := range ext.Blocks {
			ext.Bytes += b.Bytes
		}
		idx.Extents = append(idx.Extents, ext)
		ext = nil
	}

	if err := s.Split(func(data []byte) error {
		// A block of zeroes ends the current extent. We count the block against
		// the total file size, but do not explicitly store it.
		if isZero(data) {
			push()
			idx.TotalBytes += uint64(len(data))
			return nil
		}

		// Otherwise, we have real data to store. Start a fresh extent if do not
		// already have one, store the block, and append it to the extent.
		if ext == nil {
			// N.B. We need the total from BEFORE the new block is added.
			ext = &Extent{Base: idx.TotalBytes}
		}

		idx.TotalBytes += uint64(len(data))
		key, err := put(data)
		if err != nil {
			return err
		}
		ext.Blocks = append(ext.Blocks, &Block{
			Bytes: uint64(len(data)),
			Key:   []byte(key),
		})
		return nil
	}); err != nil {
		return nil, err
	}
	push()
	return &idx, nil
}

func isZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
