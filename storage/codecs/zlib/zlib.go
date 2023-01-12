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
	z, err := zlib.NewWriterLevel(w, int(c.level))
	if err != nil {
		return err
	}
	_, err = z.Write(src)
	cerr := z.Close()
	if err != nil {
		return err
	}
	return cerr
}

// Decode decompresses src via zlib and writes it to w.
func (c Codec) Decode(w io.Writer, src []byte) error {
	z, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return err
	}
	defer z.Close()
	_, err = io.Copy(w, z)
	return err
}

// DecodedLen reports the decoded length of src.
func (c Codec) DecodedLen(src []byte) (int, error) {
	var n int
	err := c.Decode(lengthWriter{&n}, src)
	return n, err
}

type lengthWriter struct{ z *int }

func (w lengthWriter) Write(data []byte) (int, error) {
	*w.z += len(data)
	return len(data), nil
}
