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

// ErrNoIndex is returned by Index when no blob index is defined.
var ErrNoIndex = errors.New("no blob index")

// A Root records the location of the root of a file tree.
type Root struct {
	cas file.CAS

	FileKey     string // the storage key of the file node
	OwnerKey    string // the key of an owner metadata blob
	Description string // a human-readable description

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

		FileKey:     opts.FileKey,
		OwnerKey:    opts.OwnerKey,
		Description: opts.Description,
		indexKey:    opts.IndexKey,
	}
}

// Open opens a stored root record given its storage key in s.
func Open(ctx context.Context, s file.CAS, key string) (*Root, error) {
	var pb wiretype.Root
	if err := loadWireType(ctx, s, key, &pb); err != nil {
		return nil, fmt.Errorf("loading root %q: %w", key, err)
	} else if err := pb.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid root %q: %w", key, err)
	}
	return &Root{
		cas: s,

		FileKey:     string(pb.RootFileKey),
		OwnerKey:    string(pb.OwnerKey),
		Description: pb.Description,
		indexKey:    string(pb.BlobIndexKey),
	}, nil
}

// Index loads and returns the blob index for r, if one exists.
// If no index exists, it returns ErrNoIndex.
func (r *Root) Index(ctx context.Context) (*index.Index, error) {
	if r.idx != nil {
		return r.idx, nil
	} else if r.indexKey == "" {
		return nil, ErrNoIndex
	}
	var pb indexpb.EncodedIndex
	if err := loadWireType(ctx, r.cas, r.indexKey, &pb); err != nil {
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
	if r.indexKey != "" || r.idx == nil {
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
	if err := r.saveIndex(ctx); err != nil {
		return fmt.Errorf("writing index: %w", err)
	}
	bits, err := proto.Marshal(r.toWireType())
	if err != nil {
		return err
	}
	return r.cas.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: true,
	})
}

func (r *Root) toWireType() *wiretype.Root {
	return (&wiretype.Root{
		RootFileKey:  []byte(r.FileKey),
		Description:  r.Description,
		BlobIndexKey: []byte(r.indexKey),
		OwnerKey:     []byte(r.OwnerKey),
	}).SetChecksum()
}

func loadWireType(ctx context.Context, s file.CAS, key string, msg proto.Message) error {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("loading message: %w", err)
	}
	return proto.Unmarshal(bits, msg)
}

// Options are configurable settings for creating a Root.  A nil options
// pointer provides zero values for all fields.
type Options struct {
	FileKey     string
	Description string
	OwnerKey    string
	IndexKey    string
}
