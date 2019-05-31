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
	"strings"
	"sync"

	"bitbucket.org/creachadair/ffs/blob"
	"golang.org/x/xerrors"
)

// Default is the default store registry.
var Default = &Registry{}

// An Opener opens a blob.Store instance associated with the given address.
// The address passed to the Opener has its dispatch tag removed. An Opener
// must be safe for concurrent use by multiple goroutines.
type Opener func(ctx context.Context, addr string) (blob.Store, error)

// A Registry maintains a mapping from addresses to Opener values.  The methods
// of a Registry are safe for concurrent use by multiple goroutines.
type Registry struct {
	μ sync.RWMutex
	m map[string]Opener
}

// Register associates the specified address tag with the given Opener.  It is
// an error (ErrDuplicateTag) if tag is already registered.
// A tag may end with ":" but must not otherwise contain any ":" characters.
func (r *Registry) Register(tag string, o Opener) error {
	clean := strings.TrimSuffix(tag, ":")
	if clean == "" || strings.Contains(clean, ":") {
		return xerrors.Errorf("register %q: %w", tag, ErrInvalidTag)
	} else if o == nil {
		return xerrors.Errorf("register %q: opener is nil", tag)
	}

	r.μ.Lock()
	defer r.μ.Unlock()
	if r.m == nil {
		r.m = make(map[string]Opener)
	} else if _, ok := r.m[clean]; ok {
		return xerrors.Errorf("register %q: %w", clean, ErrDuplicateTag)
	}
	r.m[clean] = o
	return nil
}

// Open opens a blob.Store for the specified address of the form "tag" or
// "tag:value".  If the address does not have this form, or if the tag does not
// correspond to any known implementation, Open reports ErrInvalidAddress.
func (r *Registry) Open(ctx context.Context, addr string) (blob.Store, error) {
	tag := addr
	if i := strings.Index(addr, ":"); i > 0 {
		tag, addr = addr[:i], addr[i+1:]
	} else {
		addr = ""
	}

	r.μ.RLock()
	open, ok := r.m[tag]
	r.μ.RUnlock()

	if !ok {
		return nil, xerrors.Errorf("open %q: %w", addr, ErrInvalidAddress)
	}
	s, err := open(ctx, addr)
	if err != nil {
		return nil, xerrors.Errorf("open %q: %w", addr, err)
	}
	return s, nil
}

var (
	// ErrInvalidTag is reported by Register when given an invalid tag.
	ErrInvalidTag = xerrors.New("invalid tag")

	// ErrDuplicateTag is reported by Register when given a tag which was
	// already previously registered with a different value.
	ErrDuplicateTag = xerrors.New("duplicate tag")

	// ErrInvalidAddress is reported by Open when given an address that is
	// syntactically invalid or has no corresponding Opener.
	ErrInvalidAddress = xerrors.New("invalid address")
)
