// Copyright 2019 Michael J. Fromberger. All Rights Reserved.
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

// Package zlib implements the encoded.Codec interface to apply zlib
// compression to blobs.
package zlib

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
)

// A Codec implements the encoded.Codec interface to provide zlib compression
// of blob data. A zero value is ready for use, but performs no compression.
// For most uses prefer NewCodec.
type Codec struct{ level Level }

// Level determines the compression level to use.
type Level int

// Compression level constants forwarded from compress/flate.
const (
	LevelNone     Level = flate.NoCompression
	LevelFastest  Level = flate.BestSpeed
	LevelSmallest Level = flate.BestCompression
	LevelDefault  Level = flate.DefaultCompression
)

// NewCodec returns a Codec using the specified compression level.
// Note that a zero level means no compression, not default compression.
func NewCodec(level Level) Codec { return Codec{level} }

// Encode compresses src via zlib and writes it to w.
func (c Codec) Encode(w io.Writer, src []byte) error {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], uint64(len(src)))
	if _, err := w.Write(buf[:n]); err != nil {
		return err
	}

	z, err := zlib.NewWriterLevel(w, int(c.level))
	if err != nil {
		return err
	} else if _, err := z.Write(src); err != nil {
		return err
	}
	return z.Close()
}

// Decode decompresses src via zlib and writes it to w.
func (Codec) Decode(w io.Writer, src []byte) error {
	_, n := binary.Uvarint(src)
	if n <= 0 {
		return errors.New("invalid length prefix")
	}
	z, err := zlib.NewReader(bytes.NewReader(src[n:]))
	if err != nil {
		return err
	}
	defer z.Close()
	_, err = io.Copy(w, z)
	return err
}

// DecodedLen reports the decoded length of src.
func (c Codec) DecodedLen(src []byte) (int, error) {
	v, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, errors.New("invalid length prefix")
	}
	return int(v), nil
}
