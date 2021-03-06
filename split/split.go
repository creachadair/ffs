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

// Package split implements a content-sensitive block splitter based on a
// rolling hash function.
//
// The algorithm used to split data into blocks is based on the one from LBFS:
//  http://pdos.csail.mit.edu/lbfs/
// As described in the SOSP 2001 paper "A Low-Bandwidth Network File System":
//  https://pdos.csail.mit.edu/papers/lbfs:sosp01/lbfs.pdf
//
// This package provides a rolling hash using the Rabin-Karp construction, and
// alternative implementations can be plugged in via the RollingHash interface.
//
package split

import (
	"io"
)

// These values are the defaults used if none are specified in the config.
// They are exported as variables so that they can be overridden with flags.
var (
	// DefaultMin is the default minimum block size, in bytes.
	DefaultMin = 2048

	// DefaultSize is the default target block size, in bytes.
	DefaultSize = 16384

	// DefaultMax is the default maximum block size, in bytes.
	DefaultMax = 65536
)

// A Config contains the settings to construct a splitter.
type Config struct {
	// Construct a rolling hash to use for splitting. If nil, use DefaultHash.
	Hash func() RollingHash

	// Minimum block size, in bytes. The splitter will not split a block until
	// it is at least this size.
	Min int

	// Desired block size, in bytes. The splitter will attempt to generate
	// blocks of approximately this average size.
	Size int

	// Maximum block size, in bytes. The splitter will split any block that
	// exceeds this size, even if the rolling hash does not find a break.
	Max int
}

func (c *Config) newHash() RollingHash {
	if c == nil || c.Hash == nil {
		return DefaultHash()
	}
	return c.Hash()
}

func (c *Config) min() int {
	if c == nil || c.Min <= 0 {
		return DefaultMin
	}
	return c.Min
}

func (c *Config) size() int {
	if c == nil || c.Size <= 0 {
		return DefaultSize
	}
	return c.Size
}

func (c *Config) max() int {
	if c == nil || c.Max <= 0 {
		return DefaultMax
	}
	return c.Max
}

// New returns a splitter that reads its data from r and partitions it into
// blocks using the rolling hash from c. A nil *Config is ready for use with
// default sizes and hash settings.
func New(r io.Reader, c *Config) *Splitter {
	return &Splitter{
		reader: r,
		hash:   c.newHash(),
		min:    c.min(),
		exp:    c.size(),
		buf:    make([]byte, c.max()),
	}
}

// A Splitter wraps an underlying io.Reader to split the data from the reader
// into blocks using a rolling hash.
type Splitter struct {
	reader io.Reader // The underlying source of block data.

	hash RollingHash // The rolling hash used to find breakpoints.
	min  int         // Minimum block size in bytes.
	exp  int         // Expected block size in bytes.
	next int         // Next unused offset in buf.
	end  int         // End of previous block.
	buf  []byte      // Incoming data buffer.
}

// Next returns the next available block, or an error.  The slice returned is
// only valid until a subsequent call of Next.  Returns nil, io.EOF when no
// further blocks are available.
func (s *Splitter) Next() ([]byte, error) {
	// Shift out the previous block, if any.  This invalidates any previous
	// slice returned by this method, as the data have moved.
	if s.end > 0 {
		copy(s.buf, s.buf[s.end:])
		s.next -= s.end
		s.end = 0
	}

	i := s.end // The position of the next potential block boundary
	for {
		// Try to read more data into the buffer.  An EOF at this point is not
		// an error, since there may be data left in the buffer from earlier.
		nr, err := s.reader.Read(s.buf[s.next:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		s.next += nr

		// Look for a block boundary: A point where the hash value goes to 1
		// modulo the desired block size, or we run out of buffered data.
		isCut := false
		for ; i < s.next; i++ {
			u := s.hash.Update(s.buf[i])
			isCut = u%uint(s.exp) == 1 && i-s.end >= s.min
			if isCut {
				break
			}
		}

		// If we found a block cut, or have reached the maximum block size, or
		// there is no input left, update state and return the block.
		if isCut || i >= len(s.buf) || (i > s.end && err == io.EOF) {
			block := s.buf[s.end:i]
			s.end = i
			return block, nil
		}

		// We didn't find a cut, and there's room for more data in the buffer.
		// If there's still something left to read, go back for another chunk.
		if err == io.EOF {
			break
		}
	}
	// No more blocks available, end of input.
	return nil, io.EOF
}

// Split splits blocks from s and passes each block in sequence to f, until
// there are no further blocks or until f returns an error.  If f returns an
// error, processing stops and that error is returned to the caller of Split.
//
// The slice passed to f is only valid while f is active; if f wishes to store
// a block for later use, it must be copied.
func (s *Splitter) Split(f func(data []byte) error) error {
	for {
		block, err := s.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		} else if err := f(block); err != nil {
			return err
		}
	}
}

/*
 Implementation notes:

 The Splitter maintains a buffer big enough to hold a full maximum-length block
 of data.  The buffer is organized as follows:

    0                                                          len(buf)
   |abcdefghijklmnopqrs----------------------------------------|
            ^end       ^next

 All the bytes in buf[:end] belong to the previous block. If end > 0, the first
 step is to shift out those old bytes. Note that in doing so, we invalidate the
 previous buffer reported to the caller, if any:

   |ijklmnopqrs------------------------------------------------|
    ^end       ^next

 Now, if next < len(buf), try to fill the buffer with new data:

   |ijklmnopqrsAAAAAAAAAAAAAAAAAAAAAAAAAAA---------------------|
    ^end                                  ^next

 Now we scan forward from i = end until we reach next or find a block boundary.
 For a position to count as a block boundary, it must be on a hash cut at least
 minBytes greater than end; or, it must be at the maximum block size.

   |ijklmnopqrsAAAAAAAAAA*AAAAAAAAAAAAAAAA---------------------|
    ^end                 ^i               ^next

 There are now four possibilities to consider:

  (a) If i is at a hash cut at least min greater than end:
      This is a normal block, which we must return.
  (b) If i == len(buf):
      This is a long block, capped by the max block size, which we must return.
  (c) If i == next, i > end, and input is at EOF:
      This is a non-empty tail block, which we must return.

 If none of (a)-(c) apply, it means we have not seen a block boundary and have
 space left in the buffer. If the input is not exhausted, we go back and try to
 read another chunk from the input; otherwise we report EOF.

 If we do have a block to return, its data are in buf[0:i]. We update end to i,
 to mark the end of the block for the next call.

   [*********************]<< returned block
   |ijklmnopqrsAAAAAAAAAA*AAAAAAAAAAAAAAAA---------------------|
                         ^end             ^next
                         ^i

 At this point, the buffer is in a clean state for the next iteration.
*/
