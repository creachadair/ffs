//go:build all || gcs

package main

import "github.com/creachadair/gcsstore"

func init() { stores["gcs"] = gcsstore.Opener }
