//go:build all || leveldb

package main

import "github.com/creachadair/leveldbstore"

func init() { stores["leveldb"] = leveldbstore.Opener }
