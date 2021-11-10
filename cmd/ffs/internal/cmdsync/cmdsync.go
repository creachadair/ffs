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

package cmdsync

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/ffs/config"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/taskgroup"
)

var syncFlags struct {
	Verbose bool
}

func debug(msg string, args ...interface{}) {
	if syncFlags.Verbose {
		log.Printf(msg, args...)
	}
}

var Command = &command.C{
	Name:  "sync",
	Usage: `<target-store> (<file-key>|root:<root-key>) ...`,
	Help: `Synchronize file trees between stores.

Transfer all the blobs reachable from the specified file or root
trees into the given target store.
`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&syncFlags.Verbose, "v", false, "Enable verbose logging")
	},
	Run: runSync,
}

func runSync(env *command.Env, args []string) error {
	if len(args) < 2 {
		return env.Usagef("missing target store and source keys")
	}
	addr, keys := args[0], args[1:]

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(src blob.CAS) error {
		return config.WithStore(cfg.Context, addr, func(tgt blob.CAS) error {
			debug("Target store: %q", addr)
			for _, elt := range keys {
				var err error
				if strings.HasPrefix(elt, "root:") {
					debug("Copying root %q...", elt)
					err = copyRoot(cfg.Context, src, tgt, elt)
				} else if pk, perr := config.ParseKey(elt); perr != nil {
					return perr
				} else if fp, oerr := file.Open(cfg.Context, src, pk); oerr != nil {
					return oerr
				} else {
					debug("Copying file %x...", pk)
					err = copyFile(cfg.Context, src, tgt, fp)
				}

				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func copyRoot(ctx context.Context, src, tgt blob.CAS, key string) error {
	rp, err := root.Open(ctx, src, key)
	if err != nil {
		return err
	}
	if err := copyBlob(ctx, src, tgt, rp.OwnerKey, true); err != nil {
		return fmt.Errorf("copying owner: %w", err)
	}
	if err := copyBlob(ctx, src, tgt, rp.IndexKey, false); err != nil {
		return fmt.Errorf("copying index: %w", err)
	}
	if rp.Predecessor != "" {
		if err := copyRoot(ctx, src, tgt, rp.Predecessor); err != nil {
			return err
		}
	}
	fp, err := rp.File(ctx)
	if err != nil {
		return err
	}
	if err := copyFile(ctx, src, tgt, fp); err != nil {
		return fmt.Errorf("copying root file: %w", err)
	}
	return root.New(tgt, &root.Options{
		OwnerKey:    rp.OwnerKey,
		Description: rp.Description,
		FileKey:     rp.FileKey,
		IndexKey:    rp.IndexKey,
		Predecessor: rp.Predecessor,
	}).Save(ctx, key, true)
}

func copyFile(ctx context.Context, src, tgt blob.CAS, fp *file.File) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, run := taskgroup.New(taskgroup.Trigger(cancel)).Limit(64)
	start := time.Now()

	var nb int64
	if err := fp.Scan(ctx, func(key string, isFile bool) bool {
		run(func() error {
			defer atomic.AddInt64(&nb, 1)
			return copyBlob(ctx, src, tgt, key, false)
		})
		return true
	}); err != nil {
		return err
	}
	cerr := g.Wait()
	debug("Copied %d blobs [%v elapsed]", nb, time.Since(start).Truncate(10*time.Millisecond))
	return cerr
}

func copyBlob(ctx context.Context, src, tgt blob.CAS, key string, replace bool) error {
	if key == "" {
		return nil
	}
	bits, err := src.Get(ctx, key)
	if err != nil {
		return err
	}
	err = tgt.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: replace,
	})
	if blob.IsKeyExists(err) {
		err = nil
	}
	return err
}
