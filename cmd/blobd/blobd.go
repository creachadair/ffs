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

// Program blobd exports a blob.Store via JSON-RPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/blobd/store"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/metrics"
	"github.com/creachadair/rpcstore"

	// Storage implementations (see the stores registry below).
	"github.com/creachadair/badgerstore"
	"github.com/creachadair/bitcaskstore"
	"github.com/creachadair/boltstore"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/gcsstore"
	"github.com/creachadair/leveldbstore"
	"github.com/creachadair/pebblestore"
	"github.com/creachadair/pogrebstore"
	"github.com/creachadair/sqlitestore"
)

var (
	listenAddr = flag.String("listen", "", "Service address (required)")
	storeAddr  = flag.String("store", "", "Store address (required)")
	keyFile    = flag.String("keyfile", "", "Encryption key file")
	bufferDB   = flag.String("buffer", "", "Write-behind buffer database")
	cacheSize  = flag.Int("cache", 0, "Memory cache size in MiB (0 means no cache)")
	doDebug    = flag.Bool("debug", false, "Enable server debug logging")
	zlibLevel  = flag.Int("zlib", 0, "Enable ZLIB compression (0 means no compression)")

	stores = store.Registry{
		"badger":  badgerstore.Opener,
		"bitcask": bitcaskstore.Opener,
		"bolt":    boltstore.Opener,
		"file":    filestore.Opener,
		"gcs":     gcsstore.Opener,
		"leveldb": leveldbstore.Opener,
		"memory":  memstore.Opener,
		"pebble":  pebblestore.Opener,
		"pogreb":  pogrebstore.Opener,
		"sqlite":  sqlitestore.Opener,
	}
)

func init() {
	var keys []string
	for key := range stores {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %[1]s [options] -store <spec> -listen <addr>

Start a JSON-RPC server that serves content from the blob.Store described by the -store
spec. The server listens at the specified address, which may be a host:port or the path
of a Unix-domain socket.

A store spec is a storage type and address: type:address
The types understood are: %[2]s

JSON-RPC requests are delimited by newlines.

With -keyfile, the store is opened with AES encryption.
Use -cache to enable a memory cache over the underlying store.

Options:
`, filepath.Base(os.Args[0]), strings.Join(keys, ", "))
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	ctrl.Run(func() error {
		switch {
		case *listenAddr == "":
			ctrl.Exitf(1, "You must provide a non-empty -listen address")
		case *storeAddr == "":
			ctrl.Exitf(1, "You must provide a non-empty -store address")
		}

		ctx := context.Background()
		bs := mustOpenStore(ctx)
		defer func() {
			if err := blob.CloseStore(ctx, bs); err != nil {
				log.Printf("Warning: closing store: %v", err)
			}
		}()
		log.Printf("Store address: %q", *storeAddr)
		if *zlibLevel > 0 {
			log.Printf("Compression enabled: ZLIB level %d", *zlibLevel)
			if *keyFile != "" {
				log.Printf(">> WARNING: Compression and encryption are both enabled")
			}
		}
		if *cacheSize > 0 {
			log.Printf("Memory cache size: %d MiB", *cacheSize)
		}
		if *keyFile != "" {
			log.Printf("Encryption key: %q", *keyFile)
		}

		mx := metrics.New()
		mx.SetLabel("blobd.store", *storeAddr)
		mx.SetLabel("blobd.pid", os.Getpid())
		mx.SetLabel("blobd.encrypted", *keyFile != "")
		if *keyFile != "" {
			mx.SetLabel("blobd.encrypted.keyfile", *keyFile)
		}
		mx.SetLabel("blobd.compressed", *zlibLevel > 0)
		mx.SetLabel("blobd.cacheSize", *cacheSize)
		if *bufferDB != "" {
			mx.SetLabel("blobd.buffer.db", *bufferDB)
			buf := bs.(*wbstore.Store).Buffer()
			mx.SetLabel("blobd.buffer.len", func() interface{} {
				n, err := buf.Len(ctx)
				if err != nil {
					return "unknown"
				}
				return n
			})
		}

		var debug *log.Logger
		if *doDebug {
			debug = log.New(os.Stderr, "[blobd] ", log.LstdFlags)
		}
		closer, errc := startNetServer(ctx, startConfig{
			Address: *listenAddr,
			Methods: rpcstore.NewService(bs, nil).Methods(),

			ServerOptions: &jrpc2.ServerOptions{
				Logger:    debug,
				Metrics:   mx,
				StartTime: time.Now().In(time.UTC),
			},
		})

		sig := make(chan os.Signal, 2)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			s, ok := <-sig
			if ok {
				log.Printf("Received signal: %v, closing listener", s)
				closer()
				signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			}
		}()
		return <-errc
	})
}