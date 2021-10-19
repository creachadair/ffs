//go:build all || bitcask

package main

import "github.com/creachadair/bitcaskstore"

func init() { stores["bitcask"] = bitcaskstore.Opener }
