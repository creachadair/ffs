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

package cmdfile

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/ffs/config"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/fpath"
)

const fileCmdUsage = `root:<root-key> [path]
<file-key> [path]`

var Command = &command.C{
	Name: "file",
	Help: `Manipulate file and directory objects

File objects are addressed by storage keys. The storage key for
a file may be specified in the following formats:

  root:<root-name>              : the file key from a root pointer
  74686973206973206d79206b6579  : hexadecimal encoded
  dGhpcyBpcyBteSBrZXk=          : base64 encoded
`,

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: fileCmdUsage,
			Help:  "Print the representation of a file object",

			Run: runShow,
		},
		{
			Name:  "read",
			Usage: fileCmdUsage,
			Help:  "Read the binary contents of a file object",

			Run: runRead,
		},
		{
			Name: "set",
			Usage: `root:<root-key> <path> <target-key>
<origin-key> <path> <file-key>`,
			Help: `Set the specified path beneath the origin to the given target

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.
`,

			Run: runSet,
		},
		{
			Name: "remove",
			Usage: `root:<root-key> <path>
<origin-key> <path>`,
			Help: `Remove the specified path from beneth the origin

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.
`,

			Run: runRemove,
		},
	},
}

func runShow(env *command.Env, args []string) error {
	if len(args) == 0 {
		return command.Usagef("missing required storage key")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := openFile(cfg.Context, s, args[0], args[1:]...)
		if err != nil {
			return err
		}

		msg := file.Encode(of.targetFile).Value.(*wiretype.Object_Node).Node
		fmt.Println(config.ToJSON(map[string]interface{}{
			"storageKey": []byte(of.targetKey),
			"node":       msg,
		}))
		return nil
	})
}

func runRead(env *command.Env, args []string) error {
	if len(args) == 0 {
		return command.Usagef("missing required storage key")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := openFile(cfg.Context, s, args[0], args[1:]...)
		if err != nil {
			return err
		}
		_, err = io.Copy(os.Stdout, of.targetFile.Cursor(cfg.Context))
		return err
	})
}

func runSet(env *command.Env, args []string) error {
	if len(args) != 3 {
		return command.Usagef("got %d arguments, wanted origin, path, target", len(args))
	}
	path := path.Clean(args[1])
	if path == "" {
		return command.Usagef("path must not be empty")
	}
	targetKey, err := config.ParseKey(args[2])
	if err != nil {
		return fmt.Errorf("target key: %w", err)
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		tf, err := file.Open(cfg.Context, s, targetKey)
		if err != nil {
			return fmt.Errorf("target file: %w", err)
		}
		of, err := openFile(cfg.Context, s, args[0]) // N.B. No path; see below.
		if err != nil {
			return err
		}

		if _, err := fpath.Set(cfg.Context, of.rootFile, path, &fpath.SetOptions{
			Create: true,
			SetStat: func(st *file.Stat) {
				if st.Mode == 0 {
					st.Mode = fs.ModeDir | 0755
				}
			},
			File: tf,
		}); err != nil {
			return err
		}
		key, err := of.flushRoot(cfg.Context, s)
		if err != nil {
			return err
		}
		fmt.Printf("%x\n", key)
		return nil
	})
}

func runRemove(env *command.Env, args []string) error {
	if len(args) != 2 {
		return command.Usagef("got %d arguments, wanted origin, path", len(args))
	}
	path := path.Clean(args[1])
	if path == "" {
		return command.Usagef("path must not be empty")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := openFile(cfg.Context, s, args[0]) // N.B. No path; see below
		if err != nil {
			return err
		}

		if err := fpath.Remove(cfg.Context, of.rootFile, path); err != nil {
			return err
		}
		key, err := of.flushRoot(cfg.Context, s)
		if err != nil {
			return err
		}
		fmt.Printf("%x\n", key)
		return nil
	})
}

type openInfo struct {
	root       *root.Root // set if the spec is a root key
	rootKey    string     // set if the spec is a root key
	rootFile   *file.File // the starting file, whether or not there is a root
	targetFile *file.File // the target file (== rootFile if there is no path)
	targetKey  string     // the target file storage key
}

func (o *openInfo) flushRoot(ctx context.Context, s blob.CAS) (string, error) {
	key, err := o.rootFile.Flush(ctx)
	if err != nil {
		return "", err
	}
	if o.root != nil {
		o.root.FileKey = key
		if err := o.root.Save(ctx, o.rootKey, true); err != nil {
			return "", err
		}
	}
	return key, nil
}

func openFile(ctx context.Context, s blob.CAS, spec string, path ...string) (*openInfo, error) {
	var out openInfo

	if strings.HasPrefix(spec, "root:") {
		rp, err := root.Open(ctx, s, spec)
		if err != nil {
			return nil, err
		}
		rf, err := rp.File(ctx)
		if err != nil {
			return nil, err
		}
		out.root = rp
		out.rootKey = spec
		out.rootFile = rf
		out.targetKey = rp.FileKey
	} else if fk, err := config.ParseKey(spec); err != nil {
		return nil, err
	} else if fp, err := file.Open(ctx, s, fk); err != nil {
		return nil, err
	} else {
		out.rootFile = fp
		out.targetKey = fk
	}

	if len(path) == 0 {
		out.targetFile = out.rootFile
		return &out, nil
	}

	var err error
	out.targetFile, err = fpath.Open(ctx, out.rootFile, path[0])
	if err != nil {
		return nil, err
	}
	out.targetKey, _ = out.targetFile.Flush(ctx)
	return &out, nil
}
