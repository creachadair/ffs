//go:build go_mod_tidy_deps

// Package deps depends on tools needed for build and CI that are not otherwise
// direct dependencies of the module.
package deps

import (
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
	_ "honnef.co/go/tools/staticcheck"
)
