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

package rpcstore_test

import (
	"context"
	"testing"

	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/rpcstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
)

func TestStore(t *testing.T) {
	mem := memstore.New()
	svc := rpcstore.NewService(mem)

	loc := server.NewLocal(handler.ServiceMap{
		"Blob": handler.NewService(svc),
	}, nil)

	si, err := jrpc2.RPCServerInfo(context.Background(), loc.Client)
	if err != nil {
		t.Fatalf("Server info: %v", err)
	}
	t.Logf("Server methods: %+q", si.Methods)

	rs := rpcstore.NewClient(loc.Client, "Blob.")
	storetest.Run(t, rs)
	if err := loc.Close(); err != nil {
		t.Fatalf("Server close: %v", err)
	}
}
