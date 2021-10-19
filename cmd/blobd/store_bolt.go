//go:build all || bolt

package main

import "github.com/creachadair/boltstore"

func init() { stores["bolt"] = boltstore.Opener }
