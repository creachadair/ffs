// Copyright 2024 Michael J. Fromberger. All Rights Reserved.
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

// Package hexkey implements utilities for hexadecimal encoding of blob store keys.
package hexkey

import (
	"encoding/hex"
	"errors"
	"path"
	"strings"
)

// Config carries settings for the encoding and decoding of hex keys.  The zero
// value is ready for use and encodes keys as plain hexadecimal strings.
type Config struct {
	// Prefix, if set, is prepended to all keys, separated from the remainder of
	// the key by "/".
	Prefix string

	// Shard, if positive, specifies a prefix of each hex-encoded key that will
	// be separated from the key by an intervening "/".
	Shard int
}

// ErrNotMyKey is a sentinel error reported by Decode when given a key that
// does not match the parameters of the config.
var ErrNotMyKey = errors.New("key does not match config")

// Encode encodes the specified key as hexadecimal according to c.
func (c Config) Encode(key string) string {
	if c.Shard <= 0 {
		return path.Join(c.Prefix, hex.EncodeToString([]byte(key)))
	}
	tail := hex.EncodeToString([]byte(key))

	// Pad out the key so it is at least one byte longer than the shard prefix.
	// This ensures _ is not empty in a key like "P/xxx/_".
	for len(tail) <= c.Shard {
		tail += "-"
	}
	return path.Join(c.Prefix, tail[:c.Shard], tail[c.Shard:])
}

// Decode decodes the specified hex-encoded key according to c.
// If ekey does not match the expected format, it reports ErrNotMyKey.
// Otherwise, any error results from decoding the hexadecimal digits.
func (c Config) Decode(ekey string) (string, error) {
	if c.Prefix != "" {
		tail, ok := strings.CutPrefix(ekey, c.Prefix+"/")
		if !ok {
			return "", ErrNotMyKey
		}
		ekey = tail
	}

	// If no shard prefix is expected, the key is complete.
	if c.Shard <= 0 {
		key, err := hex.DecodeString(ekey)
		return string(key), err
	}

	// Otherwise, make sure we have a matching shard prefix and non-empty suffix.
	pre, post, ok := strings.Cut(ekey, "/")
	if !ok || len(pre) != c.Shard || post == "" {
		return "", ErrNotMyKey
	}
	key, err := hex.DecodeString(strings.TrimRight(pre+post, "-"))
	return string(key), err
}
