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
	"errors"
	"fmt"
	"io"
	"os"
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
	},
}

func runShow(env *command.Env, args []string) error {
	if len(args) == 0 {
		return errors.New("missing required storage key")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		fp, fileKey, err := openFile(cfg.Context, s, args[0], args[1:]...)
		if err != nil {
			return err
		}

		msg := file.Encode(fp).Value.(*wiretype.Object_Node).Node
		fmt.Println(config.ToJSON(map[string]interface{}{
			"fileKey": []byte(fileKey),
			"node":    msg,
		}))
		return nil
	})
}

func runRead(env *command.Env, args []string) error {
	if len(args) == 0 {
		return errors.New("missing required storage key")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		fp, _, err := openFile(cfg.Context, s, args[0], args[1:]...)
		if err != nil {
			return err
		}
		_, err = io.Copy(os.Stdout, fp.Cursor(cfg.Context))
		return err
	})
}

func openFile(ctx context.Context, s blob.CAS, spec string, path ...string) (*file.File, string, error) {
	var fileKey string
	if strings.HasPrefix(spec, "root:") {
		rp, err := root.Open(ctx, s, spec)
		if err != nil {
			return nil, "", err
		}
		fileKey = rp.FileKey
	} else if fk, err := config.ParseKey(spec); err != nil {
		return nil, "", err
	} else {
		fileKey = fk
	}

	fp, err := file.Open(ctx, s, fileKey)
	if err != nil {
		return nil, fileKey, err
	} else if len(path) == 0 {
		return fp, fileKey, nil
	}

	tp, err := fpath.Open(ctx, fp, path[0])
	if err != nil {
		return nil, "", err
	}
	fileKey, _ = tp.Flush(ctx)
	return tp, fileKey, nil
}
