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

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"hash"
	"log"
	"net"
	"os"

	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/cachestore"
	"github.com/creachadair/ffs/blob/codecs/encrypted"
	"github.com/creachadair/ffs/blob/codecs/zlib"
	"github.com/creachadair/ffs/blob/encoded"
	"github.com/creachadair/getpass"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/server"
	"github.com/creachadair/keyfile"
	"github.com/creachadair/wbstore"
	"golang.org/x/crypto/sha3"
)

type closer = func()

type startConfig struct {
	Address       string
	Methods       jrpc2.Assigner
	ServerOptions *jrpc2.ServerOptions
}

func startNetServer(ctx context.Context, opts startConfig) (closer, <-chan error) {
	lst, err := net.Listen(jrpc2.Network(opts.Address))
	if err != nil {
		ctrl.Fatalf("Listen: %v", err)
	}
	isUnix := lst.Addr().Network() == "unix"
	if isUnix {
		os.Chmod(opts.Address, 0600) // best-effort
	}

	log.Printf("Service: %q", opts.Address)
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		acc := server.NetAccepter(lst, channel.Line)
		errc <- server.Loop(acc, server.Static(opts.Methods), &server.LoopOptions{
			ServerOptions: opts.ServerOptions,
		})
	}()

	return func() {
		lst.Close()
		if isUnix {
			defer os.Remove(opts.Address)
		}
	}, errc
}

func mustOpenStore(ctx context.Context) (cas blob.CAS) {
	var buf blob.Store
	defer func() {
		if buf != nil {
			cas = wbstore.New(ctx, cas, buf)
		}
	}()

	bs, err := stores.Open(ctx, *storeAddr)
	if err != nil {
		ctrl.Fatalf("Opening store: %v", err)
	}

	if *bufferDB != "" {
		buf, err = stores.Open(ctx, *bufferDB)
		if err != nil {
			ctrl.Fatalf("Opening buffer store: %v", err)
		}
	}
	if *zlibLevel > 0 {
		bs = encoded.New(bs, zlib.NewCodec(zlib.Level(*zlibLevel)))
	}
	if *cacheSize > 0 {
		bs = cachestore.New(bs, *cacheSize<<20)
	}
	if *keyFile == "" {
		return blob.NewCAS(bs, sha3.New256)
	}

	key, err := keyfile.LoadKey(*keyFile, func() (string, error) {
		return getpass.Prompt("Passphrase: ")
	})
	if err != nil {
		ctrl.Fatalf("Loading encryption key: %v", err)
	}

	c, err := aes.NewCipher(key)
	if err != nil {
		ctrl.Fatalf("Creating cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		ctrl.Fatalf("Creating GCM instance: %v", err)
	}
	bs = encoded.New(bs, encrypted.New(gcm, nil))
	return blob.NewCAS(bs, func() hash.Hash {
		return hmac.New(sha3.New256, key)
	})
}
