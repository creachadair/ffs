//go:build all || memory

package main

import "github.com/creachadair/ffs/blob/memstore"

func init() { stores["memory"] = memstore.Opener }
