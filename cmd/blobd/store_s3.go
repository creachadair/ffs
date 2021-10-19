//go:build all || s3

package main

import "github.com/creachadair/s3store"

func init() { stores["s3"] = s3store.Opener }
