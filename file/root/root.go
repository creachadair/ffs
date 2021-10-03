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
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffs/index/indexpb"
	"google.golang.org/protobuf/proto"
)

// ErrNoData indicates that the requested data do not exist.
var ErrNoData = errors.New("requested data not found")

// A Root records the location of the root of a file tree.
type Root struct {
	cas file.CAS

	OwnerKey    string // the key of an owner metadata blob
	Description string // a human-readable description

	fileKey string // the storage key of the file node
	file    *file.File

	indexKey string // the key of the blob index
	idx      *index.Index
}

// New constructs a new empty Root associated with the given store.
// If opts != nil, initial values are set from its contents.
func New(s file.CAS, opts *Options) *Root {
	if opts == nil {
		opts = new(Options)
	}
	return &Root{
		cas: s,

		OwnerKey:    opts.OwnerKey,
		Description: opts.Description,
		fileKey:     opts.FileKey,
		indexKey:    opts.IndexKey,
	}
}

// Open opens a stored root record given its storage key in s.
func Open(ctx context.Context, s file.CAS, key string) (*Root, error) {
	var obj wiretype.Object
	if err := wiretype.Load(ctx, s, key, &obj); err != nil {
		return nil, fmt.Errorf("loading root %q: %w", key, err)
	}
	return Decode(s, &obj)
}

// File loads and returns the root file of r, if one exists.
// If no file exists, it returns ErrNoData.
func (r *Root) File(ctx context.Context) (*file.File, error) {
	if r.file != nil {
		return r.file, nil
	} else if r.fileKey == "" {
		return nil, ErrNoData
	}
	f, err := file.Open(ctx, r.cas, r.fileKey)
	if err != nil {
		return nil, err
	}
	r.file = f
	return f, nil
}

// NewFile replaces the root file of r with a newly-created file using the
// given options.
func (r *Root) NewFile(opts *file.NewOptions) *file.File {
	r.fileKey = ""
	r.file = file.New(r.cas, opts)
	return r.file
}

// OpenFile replaces the root file of r by opening the specified file key.
func (r *Root) OpenFile(ctx context.Context, key string) (*file.File, error) {
	f, err := file.Open(ctx, r.cas, key)
	if err != nil {
		return nil, err
	}
	r.fileKey = key
	r.file = f
	return f, nil
}

// Index loads and returns the blob index for r, if one exists.
// If no index exists, it returns ErrNoData.
func (r *Root) Index(ctx context.Context) (*index.Index, error) {
	if r.idx != nil {
		return r.idx, nil
	} else if r.indexKey == "" {
		return nil, ErrNoData
	}
	var pb indexpb.EncodedIndex
	if err := wiretype.Load(ctx, r.cas, r.indexKey, &pb); err != nil {
		return nil, err
	}
	r.idx = index.Decode(&pb)
	return r.idx, nil
}

// SetIndex stores and updates the blob index for r to idx.
// If idx == nil, the blob index for r is cleared.
func (r *Root) SetIndex(idx *index.Index) {
	r.indexKey = ""
	r.idx = idx
}

func (r *Root) saveIndex(ctx context.Context) error {
	if r.idx == nil {
		return nil // nothing to do
	}
	bits, err := proto.Marshal(index.Encode(r.idx))
	if err != nil {
		return err
	}
	ikey, err := r.cas.PutCAS(ctx, bits)
	if err != nil {
		return err
	}
	r.indexKey = ikey
	return nil
}

// Save writes r in wire format to the given storage key in s.
func (r *Root) Save(ctx context.Context, key string) error {
	// If there is a cached file, flush it and update the storage key.
	// Otherwise, it is an error if there is no storage key set.
	if r.file != nil {
		fkey, err := r.file.Flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing file: %w", err)
		}
		r.fileKey = fkey
	} else if r.fileKey == "" {
		return errors.New("missing file key")
	}

	// If there is a blob index, flush it and update its storage key.
	if err := r.saveIndex(ctx); err != nil {
		return fmt.Errorf("writing index: %w", err)
	}

	bits, err := proto.Marshal(Encode(r))
	if err != nil {
		return err
	}
	return r.cas.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: true,
	})
}

// Encode encodes r as a protobuf message for storage.
func Encode(r *Root) *wiretype.Object {
	return &wiretype.Object{
		Value: &wiretype.Object_Root{
			Root: &wiretype.Root{
				FileKey:     []byte(r.fileKey),
				Description: r.Description,
				IndexKey:    []byte(r.indexKey),
				OwnerKey:    []byte(r.OwnerKey),
			},
		},
	}
}

// Decode decodes a protobuf-encoded root record and associates it with the
// storage in s.
func Decode(s file.CAS, obj *wiretype.Object) (*Root, error) {
	pb, ok := obj.Value.(*wiretype.Object_Root)
	if !ok {
		return nil, errors.New("object does not contain a root")
	}
	return &Root{
		cas: s,

		OwnerKey:    string(pb.Root.OwnerKey),
		Description: pb.Root.Description,
		fileKey:     string(pb.Root.FileKey),
		indexKey:    string(pb.Root.IndexKey),
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