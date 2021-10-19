//go:build !minimal && windows

package main

import (
	"github.com/creachadair/badgerstore"
	"github.com/creachadair/bitcaskstore"
	"github.com/creachadair/boltstore"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/cmd/blobd/store"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/gcsstore"
	"github.com/creachadair/pogrebstore"
	"github.com/creachadair/s3store"
)

var stores = store.Registry{
	"badger":  badgerstore.Opener,
	"bitcask": bitcaskstore.Opener,
	"bolt":    boltstore.Opener,
	"file":    filestore.Opener,
	"gcs":     gcsstore.Opener,
	"memory":  memstore.Opener,
	"pogreb":  pogrebstore.Opener,
	"s3":      s3store.Opener,
}
