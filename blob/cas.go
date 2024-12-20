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

package blob

import (
	"context"

	"golang.org/x/crypto/sha3"
)

// CASStore is a [blob.Store] implementation that wraps each [blob.KV] produced
// by an underlying store so that it implements [blob.CAS].
type CASStore struct {
	Store
}

func NewCASStore(base Store) CASStore { return CASStore{Store: base} }

func (c CASStore) Keyspace(ctx context.Context, name string) (KV, error) {
	return c.CASKeyspace(ctx, name)
}

func (c CASStore) CASKeyspace(ctx context.Context, name string) (CAS, error) {
	kv, err := c.Store.Keyspace(ctx, name)
	if err != nil {
		return nil, err
	}
	if cas, ok := kv.(CAS); ok {
		return cas, nil
	}
	return NewCAS(kv, sha3.New256), nil
}

func (c CASStore) Sub(ctx context.Context, name string) (Store, error) {
	sub, err := c.Store.Sub(ctx, name)
	if err != nil {
		return nil, err
	}
	return CASStore{Store: sub}, nil
}

func (c CASStore) Close(ctx context.Context) error {
	if c, ok := c.Store.(Closer); ok {
		return c.Close(ctx)
	}
	return nil
}
