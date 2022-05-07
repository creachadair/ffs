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

// Package root defines a storage representation for pointers to file trees and
// associated metadata.
package root

import (
	"context"
	"errors"
	"fmt"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/wiretype"
	"google.golang.org/protobuf/proto"
)

// ErrNoData indicates that the requested data do not exist.
var ErrNoData = errors.New("requested data not found")

// A Root records the location of the root of a file tree.
type Root struct {
	cas blob.CAS

	OwnerKey    string // the key of an owner metadata blob
	Description string // a human-readable description
	FileKey     string // the storage key of the file node
	IndexKey    string // the storage key of the blob index
}

// New constructs a new empty Root associated with the given store.
// If opts != nil, initial values are set from its contents.
func New(s blob.CAS, opts *Options) *Root {
	if opts == nil {
		opts = new(Options)
	}
	return &Root{
		cas: s,

		OwnerKey:    opts.OwnerKey,
		Description: opts.Description,
		FileKey:     opts.FileKey,
		IndexKey:    opts.IndexKey,
	}
}

// Open opens a stored root record given its storage key in s.
func Open(ctx context.Context, s blob.CAS, key string) (*Root, error) {
	var obj wiretype.Object
	if err := wiretype.Load(ctx, s, key, &obj); err != nil {
		return nil, fmt.Errorf("loading root %q: %w", key, err)
	}
	return Decode(s, &obj)
}

// File loads and returns the root file of r from s, if one exists.  If no file
// exists, it returns ErrNoData. If s == nil, it uses the same store as r.
func (r *Root) File(ctx context.Context, s blob.CAS) (*file.File, error) {
	if r.FileKey == "" {
		return nil, ErrNoData
	}
	if s == nil {
		s = r.cas
	}
	return file.Open(ctx, s, r.FileKey)
}

// Save writes r in wire format to the given storage key in s.
func (r *Root) Save(ctx context.Context, key string, replace bool) error {
	if r.FileKey == "" {
		return errors.New("missing file key")
	}
	bits, err := proto.Marshal(Encode(r))
	if err != nil {
		return err
	}
	return r.cas.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: replace,
	})
}

// Encode encodes r as a protobuf message for storage.
func Encode(r *Root) *wiretype.Object {
	return &wiretype.Object{
		Value: &wiretype.Object_Root{
			Root: &wiretype.Root{
				FileKey:     []byte(r.FileKey),
				Description: r.Description,
				OwnerKey:    []byte(r.OwnerKey),
				IndexKey:    []byte(r.IndexKey),
			},
		},
	}
}

// Decode decodes a protobuf-encoded root record and associates it with the
// storage in s.
func Decode(s blob.CAS, obj *wiretype.Object) (*Root, error) {
	pb, ok := obj.Value.(*wiretype.Object_Root)
	if !ok {
		return nil, errors.New("object does not contain a root")
	}
	return &Root{
		cas: s,

		OwnerKey:    string(pb.Root.OwnerKey),
		Description: pb.Root.Description,
		FileKey:     string(pb.Root.FileKey),
		IndexKey:    string(pb.Root.IndexKey),
	}, nil
}

// Options are configurable settings for creating a Root.  A nil options
// pointer provides zero values for all fields.
type Options struct {
	FileKey     string
	Description string
	OwnerKey    string
	IndexKey    string
}
