//go:build all || pogreb

package main

import "github.com/creachadair/pogrebstore"

func init() { stores["pogreb"] = pogrebstore.Opener }
