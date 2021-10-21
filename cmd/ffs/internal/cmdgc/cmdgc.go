// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package cmdgc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/ffs/config"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/taskgroup"
)

var Command = &command.C{
	Name:  "gc",
	Usage: "<root-key> <root-key>...",
	Help:  "Garbage-collect blobs not reachable from known roots",

	Run: func(env *command.Env, args []string) error {
		keys, err := config.RootKeys(args)
		if err != nil {
			return err
		} else if len(keys) == 0 {
			return errors.New("at least one root key is required")
		}

		cfg := env.Config.(*config.Settings)
		ctx, cancel := context.WithCancel(cfg.Context)
		return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
			n, err := s.Len(ctx)
			if err != nil {
				return err
			} else if n == 0 {
				return errors.New("the store is empty")
			}
			var idxs []*index.Index
			idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.01})
			fmt.Fprintf(env, "Begin GC of %d blobs, roots=%+q\n", n, keys)

			// Mark phase: Scan all roots.
			for _, key := range keys {
				rp, err := root.Open(cfg.Context, s, key)
				if err != nil {
					return fmt.Errorf("opening %q: %w", key, err)
				}
				idx.Add(key)

				// If this root has a cached index, use that instead of scanning.
				if rp.IndexKey != "" {
					var obj wiretype.Object
					if err := wiretype.Load(cfg.Context, s, rp.IndexKey, &obj); err != nil {
						return fmt.Errorf("loading index: %w", err)
					}
					ridx := obj.GetIndex()
					if ridx == nil {
						return fmt.Errorf("no index in %x", rp.IndexKey)
					}

					rpi, err := index.Decode(ridx)
					if err != nil {
						return fmt.Errorf("decoding index for %q: %w", key, err)
					}
					idxs = append(idxs, rpi)
					idx.Add(rp.IndexKey)
					fmt.Fprintf(env, "Loaded cached index for %q (%x)\n", key, rp.IndexKey)
					continue
				}

				// Otherwise, we need to compute the reachable set.
				// TODO(creachadair): Maybe cache the results here too.
				rf, err := rp.File(cfg.Context)
				if err != nil {
					return fmt.Errorf("opening %q: %w", rp.FileKey, err)
				}
				idx.Add(rp.FileKey)

				fmt.Fprintf(env, "Scanning data reachable from %q (%x)...\n", key, rp.FileKey)
				start := time.Now()
				var numKeys int
				if err := rf.Scan(cfg.Context, func(key string, isFile bool) bool {
					numKeys++
					idx.Add(key)
					return true
				}); err != nil {
					return fmt.Errorf("scanning %q: %w", key, err)
				}
				fmt.Fprintf(env, "Finished scanning %d blobs [%v elapsed]\n",
					numKeys, time.Since(start).Truncate(10*time.Millisecond))
			}
			idxs = append(idxs, idx)

			// Sweep phase: Remove blobs not indexed.
			g := taskgroup.New(taskgroup.Trigger(cancel))

			fmt.Fprintf(env, "Begin sweep over %d blobs...\n", n)
			start := time.Now()
			var numKeep, numDrop uint32
			for i := 0; i < 256; i++ {
				pfx := string([]byte{byte(i)})
				g.Go(func() error {
					defer fmt.Fprint(env, ".")
					return s.List(cfg.Context, pfx, func(key string) error {
						if !strings.HasPrefix(key, pfx) {
							return blob.ErrStopListing
						}
						for _, idx := range idxs {
							if idx.Has(key) {
								atomic.AddUint32(&numKeep, 1)
								return nil
							}
						}
						atomic.AddUint32(&numDrop, 1)
						return s.Delete(ctx, key)
					})
				})
			}
			fmt.Fprintln(env, "All key ranges listed, waiting for cleanup...")
			if err := g.Wait(); err != nil {
				return fmt.Errorf("sweeping failed: %w", err)
			}
			fmt.Fprintln(env, "*")
			fmt.Fprintf(env, "GC complete: keep %d, drop %d [%v elapsed]\n",
				numKeep, numDrop, time.Since(start).Truncate(10*time.Millisecond))
			return nil
		})
	},
}
