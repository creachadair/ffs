//go:build all || pebble

package main

import "github.com/creachadair/pebblestore"

func init() { stores["pebble"] = pebblestore.Opener }
