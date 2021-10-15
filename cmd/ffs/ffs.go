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

package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/cmd/ffs/config"

	// Subcommands.
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdfile"
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdgc"
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdput"
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdroot"
)

var (
	configPath = "$HOME/.config/ffs/config.yml"
	storeAddr  string
)

func main() {
	root := &command.C{
		Name: filepath.Base(os.Args[0]),
		Usage: `<command> [arguments]
help [<command>]`,
		Help: `A command-line tool to manage FFS file trees.`,

		SetFlags: func(env *command.Env, fs *flag.FlagSet) {
			if cf, ok := os.LookupEnv("FFS_CONFIG"); ok && cf != "" {
				configPath = cf
			}
			fs.StringVar(&configPath, "config", configPath, "Configuration file path")
			fs.StringVar(&storeAddr, "store", storeAddr, "Store service address (overrides config)")
		},

		Init: func(env *command.Env) error {
			cfg, err := config.Load(os.ExpandEnv(configPath))
			if err != nil {
				return err
			}
			if storeAddr != "" {
				cfg.StoreAddress = storeAddr
			}
			cfg.Context = context.Background()
			config.ExpandString(&cfg.StoreAddress)
			env.Config = cfg
			return nil
		},

		Commands: []*command.C{
			cmdroot.Command,
			cmdfile.Command,
			cmdput.Command,
			cmdgc.Command,
			command.HelpCommand(nil),
		},
	}
	command.RunOrFail(root.NewEnv(nil), os.Args[1:])
}
