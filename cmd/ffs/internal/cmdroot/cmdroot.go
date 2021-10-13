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

package cmdroot

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/cmd/ffs/config"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
)

var Command = &command.C{
	Name: "root",
	Help: "Manipulate filesystem root pointers",

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: "<root-key>",
			Help:  "Print the representation of a filesystem root",

			Run: runView,
		},
		{
			Name: "list",
			Help: "List the root keys known in the store",

			Run: runList,
		},
		{
			Name:  "create",
			Usage: "<name> <description>...",
			Help:  "Create a new empty root pointer",

			SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
				fs.BoolVar(&createFlags.Replace, "replace", false, "Replace an existing root name")
			},
			Run: runCreate,
		},
		{
			Name:  "copy",
			Usage: "<source-name> <target-name>",
			Help:  "Duplicate a root pointer under a new name",

			Run: runCopy,
		},
		{
			Name:  "set-description",
			Usage: "<name> <description>...",
			Help:  "Edit the description of the given root",

			Run: runEditDesc,
		},
		{
			Name:  "set-file",
			Usage: "<name> <file-key>",
			Help:  "Edit the file key of the given root",

			Run: runEditFile,
		},
	},
}

func runView(env *command.Env, args []string) error {
	keys, err := config.RootKeys(args)
	if err != nil {
		return err
	} else if len(keys) == 0 {
		return errors.New("missing required <root-key>")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		rp, err := root.Open(cfg.Context, s, keys[0])
		if err != nil {
			return err
		}
		msg := root.Encode(rp).Value.(*wiretype.Object_Root).Root
		fmt.Println(config.ToJSON(map[string]interface{}{
			"rootKey": keys[0],
			"root":    msg,
		}))
		return nil
	})
}

func runList(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("extra arguments after command")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		return s.List(cfg.Context, "root:", func(key string) error {
			if !strings.HasPrefix(key, "root:") {
				return blob.ErrStopListing
			}
			fmt.Println(key)
			return nil
		})
	})
}

var createFlags struct {
	Replace bool
}

func runCreate(env *command.Env, args []string) error {
	if len(args) < 2 {
		return errors.New("usage is: <name> <description>...") //lint:ignore ST1005 User message.
	}
	key := config.RootKey(args[0])
	desc := strings.Join(args[1:], " ")
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		fk, err := file.New(s, &file.NewOptions{
			Stat: &file.Stat{Mode: os.ModeDir | 0755},
		}).Flush(cfg.Context)
		if err != nil {
			return fmt.Errorf("creating new file: %w", err)
		}
		return root.New(s, &root.Options{
			Description: desc,
			FileKey:     fk,
		}).Save(cfg.Context, key, createFlags.Replace)
	})
}

func runCopy(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	}
	defer na.Close()
	key := config.RootKey(na.Args[0])
	return na.Root.Save(na.Context, key, false)
}

func runEditDesc(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	}
	defer na.Close()
	na.Root.Description = strings.Join(na.Args, " ")
	return na.Root.Save(na.Context, na.Key, true)
}

func runEditFile(env *command.Env, args []string) error {
	na, err := getNameArgs(env, args)
	if err != nil {
		return err
	}
	defer na.Close()

	key, err := config.ParseKey(na.Args[0])
	if err != nil {
		return err
	} else if _, err := file.Open(na.Context, na.Store, key); err != nil {
		return err
	}
	na.Root.FileKey = key
	return na.Root.Save(na.Context, na.Key, true)
}

type rootArgs struct {
	Context context.Context
	Key     string
	Args    []string
	Root    *root.Root
	Store   blob.CAS
	Close   func()
}

func getNameArgs(env *command.Env, args []string) (*rootArgs, error) {
	if len(args) < 2 {
		return nil, errors.New("usage: " + env.Command.Name + " " + env.Command.Usage)
	}
	key := config.RootKey(args[0])
	cfg := env.Config.(*config.Settings)
	bs, err := cfg.OpenStore(cfg.Context)
	if err != nil {
		return nil, err
	}
	rp, err := root.Open(cfg.Context, bs, key)
	if err != nil {
		blob.CloseStore(cfg.Context, bs)
		return nil, err
	}
	return &rootArgs{
		Context: cfg.Context,
		Key:     key,
		Args:    args[1:],
		Root:    rp,
		Store:   bs,
		Close:   func() { blob.CloseStore(cfg.Context, bs) },
	}, nil
}
