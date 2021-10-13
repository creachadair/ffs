package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/cmd/ffs/config"

	// Subcommands.
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdfile"
	"github.com/creachadair/ffs/cmd/ffs/internal/cmdgc"
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
			cmdgc.Command,
			command.HelpCommand(nil),
		},
	}
	if err := command.Execute(root.NewEnv(nil), os.Args[1:]); err != nil {
		if errors.Is(err, command.ErrUsage) {
			os.Exit(2)
		}
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}
