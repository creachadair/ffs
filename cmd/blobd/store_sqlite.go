//go:build all || sqlite

package main

import "github.com/creachadair/sqlitestore"

func init() { stores["sqlite"] = sqlitestore.Opener }
