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

// Package rpcstore implements the blob.Store that delegates to an underlying
// store via a JSON-RPC interface.
package rpcstore

import (
	"context"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
)

// Service implements a service that adapts RPC requests to a blob.Store.
type Service struct {
	st blob.Store
}

// NewService constructs a Service that delegates to the given blob.Store.
func NewService(st blob.Store) Service { return Service{st: st} }

// Get handles the corresponding method of blob.Store.
func (s Service) Get(ctx context.Context, req *KeyRequest) ([]byte, error) {
	data, err := s.st.Get(ctx, string(req.Key))
	return data, filterErr(err)
}

// Put handles the corresponding method of blob.Store.
func (s Service) Put(ctx context.Context, req *PutRequest) error {
	return filterErr(s.st.Put(ctx, blob.PutOptions{
		Key:     string(req.Key),
		Data:    req.Data,
		Replace: req.Replace,
	}))
}

// Delete handles the corresponding method of blob.Store.
func (s Service) Delete(ctx context.Context, req *KeyRequest) error {
	return filterErr(s.st.Delete(ctx, string(req.Key)))
}

// Size handles the corresponding method of blob.Store.
func (s Service) Size(ctx context.Context, req *KeyRequest) (int64, error) {
	size, err := s.st.Size(ctx, string(req.Key))
	return size, filterErr(err)
}

// List handles the corresponding method of blob.Store.
func (s Service) List(ctx context.Context, req *ListRequest) (*ListReply, error) {
	var rsp ListReply

	limit := req.Count
	if limit <= 0 {
		limit = 32
	}
	if err := s.st.List(ctx, string(req.Start), func(key string) error {
		if len(rsp.Keys) == limit {
			rsp.Next = []byte(key)
			return blob.ErrStopListing
		}
		rsp.Keys = append(rsp.Keys, []byte(key))
		return nil
	}); err != nil {
		return nil, err
	}
	return &rsp, nil
}

// Len handles the corresponding method of blob.Store.
func (s Service) Len(ctx context.Context) (int64, error) { return s.st.Len(ctx) }

// Store implements the blob.Store interface by calling a JSON-RPC service.
type Store struct {
	cli    *jrpc2.Client
	prefix string
}

// NewClient constructs a Store that delegates through the given client.
// If prefix != "" it is prepended to the service method names.
func NewClient(cli *jrpc2.Client, prefix string) Store {
	return Store{cli: cli, prefix: prefix}
}

// Get implements a method of blob.Store.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := s.cli.CallResult(ctx, s.prefix+"Get", KeyRequest{Key: []byte(key)}, &data)
	return data, unfilterErr(err)
}

// Put implements a method of blob.Store.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	_, err := s.cli.Call(ctx, s.prefix+"Put", &PutRequest{
		Key:     []byte(opts.Key),
		Data:    opts.Data,
		Replace: opts.Replace,
	})
	return unfilterErr(err)
}

// Delete implements a method of blob.Store.
func (s Store) Delete(ctx context.Context, key string) error {
	_, err := s.cli.Call(ctx, s.prefix+"Delete", KeyRequest{Key: []byte(key)})
	return unfilterErr(err)
}

// Size implements a method of blob.Store.
func (s Store) Size(ctx context.Context, key string) (int64, error) {
	var size int64
	err := s.cli.CallResult(ctx, s.prefix+"Size", KeyRequest{Key: []byte(key)}, &size)
	return size, unfilterErr(err)
}

// List implements a method of blob.Store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	next := start
	for {
		// Fetch another batch of keys.
		var rsp ListReply
		err := s.cli.CallResult(ctx, s.prefix+"List", ListRequest{Start: []byte(next)}, &rsp)
		if err != nil {
			return err
		} else if len(rsp.Keys) == 0 {
			break
		}

		// Deliver keys to the callback.
		for _, key := range rsp.Keys {
			if err := f(string(key)); err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
		if len(rsp.Next) == 0 {
			break
		}
		next = string(rsp.Next)
	}
	return nil
}

// Len implements a method of blob.Store.
func (s Store) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.cli.CallResult(ctx, s.prefix+"Len", nil, &count)
	return count, err
}
