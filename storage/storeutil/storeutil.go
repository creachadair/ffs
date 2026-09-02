// Copyright 2026 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package storeutil implements utilities for working with [blob.Store].
package storeutil

import (
	"context"
	"errors"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/taskgroup"
)

// ListAllKeys lists all the keys in kv in arbitrary order, calling kf for each
// key found.  Unlike [blob.KVCore.List], keys are not reported in lexicographic
// order, but each key is reported exactly once. If kf reports an error, the
// list ends and that error is returned.
func ListAllKeys(ctx context.Context, kv blob.KVCore, kf func(string) error) error {
	var g taskgroup.Group

	// The keymap is not safe for concurrent use by multiple goroutines, so
	// serialize insertions through a collector.
	ctx, cancelCause := context.WithCancelCause(ctx)
	defer cancelCause(nil)
	coll := taskgroup.Gather(g.Go, func(key string) {
		if err := kf(key); err != nil {
			cancelCause(err)
		}
	})

	for i := range 256 {
		if ctx.Err() != nil {
			break
		}
		pfx := string([]byte{byte(i)})
		coll.Report(func(report func(string)) error {
			for key, err := range kv.List(ctx, pfx) {
				if err != nil {
					return err
				} else if !strings.HasPrefix(key, pfx) {
					break
				}
				report(key)
			}
			return nil
		})
	}
	return errors.Join(g.Wait(), ctx.Err())
}
