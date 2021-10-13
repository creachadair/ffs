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

// Package store provides an interface to open blob.Store instances named by
// string addresses or URLs.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/creachadair/ffs/blob"
)

// An Opener opens a blob.Store instance associated with the given address.
// The address passed to the Opener has its dispatch tag removed.  The format
// of the address is opaque to the registry, and the opener is responsible for
// checking its validity.
type Opener func(ctx context.Context, addr string) (blob.Store, error)

// A Registry maintains a mapping from dispatch tags to Opener values.
type Registry map[string]Opener

// Open opens a blob.Store for the specified address of the form "tag" or
// "tag:value".  If the address does not have this form, or if the tag does not
// correspond to any known implementation, Open reports ErrInvalidAddress.
func (r Registry) Open(ctx context.Context, addr string) (blob.Store, error) {
	tag, target := addr, ""
	if i := strings.Index(addr, ":"); i > 0 {
		tag, target = addr[:i], addr[i+1:]
	}

	open, ok := r[tag]
	if !ok {
		return nil, fmt.Errorf("open %q: %w", addr, ErrInvalidAddress)
	}
	s, err := open(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("open [%s] %q: %w", tag, target, err)
	}
	return s, nil
}

var (
	// ErrInvalidAddress is reported by Open when given an address that is
	// syntactically invalid or has no corresponding Opener.
	ErrInvalidAddress = errors.New("invalid address")
)
