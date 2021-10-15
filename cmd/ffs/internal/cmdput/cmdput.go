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

package cmdput

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/ffs/config"
	"github.com/creachadair/ffs/file"
	"github.com/pkg/xattr"
)

var putFlags struct {
	Stat  bool
	XAttr bool
}

var Command = &command.C{
	Name:  "put",
	Usage: "<path> ...",
	Help: `Write file and directory contents to the store.

Add each specified path to the store and print its storage key.
`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&putFlags.Stat, "stat", false, "Capture file and directory stat")
		fs.BoolVar(&putFlags.XAttr, "xattr", false, "Capture extended attributes")
	},
	Run: runPut,
}

func runPut(env *command.Env, args []string) error {
	if len(args) == 0 {
		return errors.New("missing required path")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		keys := make([]string, len(args))
		for i, path := range args {
			f, err := putDir(cfg.Context, s, path)
			if err != nil {
				return err
			}
			key, err := f.Flush(cfg.Context)
			if err != nil {
				return err
			}
			keys[i] = key
		}
		for _, key := range keys {
			fmt.Printf("%x\n", key)
		}
		return nil
	})
}

// putFile puts a single file or symlink into the store.
// The caller is responsible for closing in after putFile returns.
func putFile(ctx context.Context, s blob.CAS, path string, in *os.File) (*file.File, error) {
	fi, err := in.Stat()
	if err != nil {
		return nil, err
	}

	// File or symbolic link.
	f := file.New(s, &file.NewOptions{
		Name: fi.Name(),
		Stat: fileInfoToStat(fi),
	})

	// Extended attributes (if -xattr is set)
	if err := addExtAttrs(in, f); err != nil {
		return nil, err
	}

	if fi.Mode().IsRegular() {
		if err := f.SetData(ctx, in); err != nil {
			return nil, fmt.Errorf("copying data: %w", err)
		}
	} else if fi.Mode()&fs.ModeSymlink != 0 {
		tgt, err := os.Readlink(path)
		if err != nil {
			return nil, err
		} else if err := f.SetData(ctx, strings.NewReader(tgt)); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// putDir puts a single file, directory, or symlink into the store.
// If path names a plain file or symlin, it calls putFile.
func putDir(ctx context.Context, s blob.CAS, path string) (*file.File, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	fi, err := in.Stat()
	if err != nil {
		return nil, err
	} else if !fi.IsDir() {
		// Non-directory files, symlinks, etc.
		return putFile(ctx, s, path, in)
	}

	// Directory
	d := file.New(s, &file.NewOptions{
		Name: fi.Name(),
		Stat: fileInfoToStat(fi),
	})

	// Extended attributes (if -xattr is set)
	if err := addExtAttrs(in, d); err != nil {
		return nil, err
	}

	// Children
	in.Close()
	elts, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, elt := range elts {
		var kid *file.File
		var err error

		sub := filepath.Join(path, elt.Name())
		if elt.IsDir() {
			kid, err = putDir(ctx, s, sub)
		} else if t := elt.Type(); t != 0 && (t&fs.ModeSymlink == 0) {
			continue // e.g., socket, pipe, device, fifo, etc.
		} else if in, err = os.Open(sub); err != nil {
			return nil, err
		} else {
			kid, err = putFile(ctx, s, sub, in)
			in.Close()
		}
		if err != nil {
			return nil, err
		}
		d.Child().Set(elt.Name(), kid)
	}
	return d, nil
}

func addExtAttrs(in *os.File, f *file.File) error {
	if !putFlags.XAttr {
		return nil
	}
	names, err := xattr.FList(in)
	if err != nil {
		return fmt.Errorf("listing xattr: %w", err)
	}
	xa := f.XAttr()
	for _, name := range names {
		data, err := xattr.FGet(in, name)
		if err != nil {
			return fmt.Errorf("get xattr %q: %w", name, err)
		}
		xa.Set(name, string(data))
	}
	return nil
}

func fileInfoToStat(fi fs.FileInfo) *file.Stat {
	if !putFlags.Stat {
		return nil
	}
	var owner, group int
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		owner = int(st.Uid)
		group = int(st.Gid)
	}
	return &file.Stat{
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
		OwnerID: owner,
		GroupID: group,
	}
}
