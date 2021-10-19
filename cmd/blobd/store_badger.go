//go:build all || badger

package main

import "github.com/creachadair/badgerstore"

func init() { stores["badger"] = badgerstore.Opener }
