// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/creachadair/rpcstore"
)

func getContext(env *command.Env) context.Context {
	return env.Config.(*settings).Context
}

func getCmd(env *command.Env, args []string) error {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: get <key>...")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer blob.CloseStore(getContext(env), bs)

	nctx := getContext(env)
	for _, arg := range args {
		key, err := parseKey(arg)
		if err != nil {
			return err
		}
		data, err := bs.Get(nctx, key)
		if err != nil {
			return err
		}
		os.Stdout.Write(data)
	}
	return nil
}

func sizeCmd(env *command.Env, args []string) error {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: size <key>...")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer blob.CloseStore(getContext(env), bs)

	nctx := getContext(env)
	for _, arg := range args {
		key, err := parseKey(arg)
		if err != nil {
			return err
		}
		size, err := bs.Size(nctx, key)
		if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString([]byte(key)), size)
	}
	return nil
}

func delCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 {
		//lint:ignore ST1005 The punctuation signifies repetition to the user.
		return errors.New("usage is: delete <key>...")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	nctx := getContext(env)
	defer func() {
		if cerr := blob.CloseStore(nctx, bs); err == nil {
			err = cerr
		}
	}()
	missingOK := env.Config.(*settings).MissingOK
	for _, arg := range args {
		key, err := parseKey(arg)
		if err != nil {
			return err
		}
		if err := bs.Delete(nctx, key); blob.IsKeyNotFound(err) && missingOK {
			continue
		} else if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString([]byte(key)))
	}
	return nil
}

func listCmd(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("usage is: list")
	}
	cfg := env.Config.(*settings)
	start, err := parseKey(cfg.Start)
	if err != nil {
		return err
	}
	pfx, err := parseKey(cfg.Prefix)
	if err != nil {
		return err
	}
	if pfx != "" && start == "" {
		start = pfx
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer blob.CloseStore(getContext(env), bs)

	return bs.List(getContext(env), start, func(key string) error {
		if !strings.HasPrefix(key, pfx) {
			if key > pfx {
				return blob.ErrStopListing
			}
			return nil
		} else if cfg.Raw {
			fmt.Println(key)
		} else {
			fmt.Printf("%x\n", key)
		}
		return nil
	})
}

func lenCmd(env *command.Env, args []string) error {
	if len(args) != 0 {
		return errors.New("usage is: len")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer blob.CloseStore(getContext(env), bs)
	n, err := bs.Len(getContext(env))
	if err != nil {
		return err
	}
	fmt.Println(n)
	return nil
}

func casPutCmd(env *command.Env, args []string) (err error) {
	cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := blob.CloseStore(getContext(env), cas); err == nil {
			err = cerr
		}
	}()
	data, err := readData(getContext(env), "put", args)
	if err != nil {
		return err
	}
	key, err := cas.CASPut(getContext(env), data)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", key)
	return nil
}

func casKeyCmd(env *command.Env, args []string) error {
	cas, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	data, err := readData(getContext(env), "key", args)
	if err != nil {
		return err
	}
	key, err := cas.CASKey(getContext(env), data)
	if err != nil {
		return err
	}
	fmt.Printf("%x\n", key)
	return nil
}

func copyCmd(env *command.Env, args []string) error {
	if len(args) != 2 {
		return errors.New("usage is: copy <src> <dst>")
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	ctx := getContext(env)
	defer blob.CloseStore(ctx, bs)

	srcKey, err := parseKey(args[0])
	if err != nil {
		return err
	}
	dstKey, err := parseKey(args[1])
	if err != nil {
		return err
	}
	src, err := bs.Get(ctx, srcKey)
	if err != nil {
		return err
	}
	return bs.Put(ctx, blob.PutOptions{
		Key:     dstKey,
		Data:    src,
		Replace: env.Config.(*settings).Replace,
	})
}

func statCmd(env *command.Env, args []string) error {
	s, err := storeFromEnv(env)
	if err != nil {
		return err
	}
	si, err := s.ServerInfo(getContext(env))
	if err != nil {
		return err
	}
	msg, err := json.Marshal(si)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	json.Indent(&buf, msg, "", "  ")
	fmt.Println(buf.String())
	return nil
}

func putCmd(env *command.Env, args []string) (err error) {
	if len(args) == 0 || len(args) > 2 {
		return errors.New("usage is: put <key> [<path>]")
	}
	key, err := parseKey(args[0])
	if err != nil {
		return err
	}
	bs, err := storeFromEnv(env)
	if err != nil {
		return nil
	}
	defer func() {
		if cerr := blob.CloseStore(getContext(env), bs); err == nil {
			err = cerr
		}
	}()
	data, err := readData(getContext(env), "put", args[1:])
	if err != nil {
		return err
	}

	return bs.Put(getContext(env), blob.PutOptions{
		Key:     key,
		Data:    data,
		Replace: env.Config.(*settings).Replace,
	})
}

func readData(ctx context.Context, cmd string, args []string) (data []byte, err error) {
	if len(args) == 0 {
		data, err = ioutil.ReadAll(os.Stdin)
	} else if len(args) == 1 {
		data, err = ioutil.ReadFile(args[0])
	} else {
		return nil, fmt.Errorf("usage is: %s [<path>]", cmd)
	}
	return
}

func storeFromEnv(env *command.Env) (rpcstore.CAS, error) {
	t := env.Config.(*settings)
	if t.Store == "" {
		return rpcstore.CAS{}, errors.New("no -store address was specified")
	}
	var ch channel.Channel
	if isHTTP(t.Store) {
		ch = jhttp.NewChannel(t.Store, nil)
	} else if conn, err := net.Dial(jrpc2.Network(t.Store)); err != nil {
		return rpcstore.CAS{}, fmt.Errorf("dialing: %w", err)
	} else {
		ch = channel.Line(conn, conn)
	}
	var logger *log.Logger
	if t.Debug {
		logger = log.New(os.Stderr, "[client] ", log.LstdFlags)
	}
	cli := jrpc2.NewClient(ch, &jrpc2.ClientOptions{Logger: logger})
	return rpcstore.NewCAS(cli, nil), nil
}

func isAllHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func parseKey(s string) (string, error) {
	if strings.HasPrefix(s, "@") {
		return s[1:], nil
	}
	var key []byte
	var err error
	if isAllHex(s) {
		key, err = hex.DecodeString(s)
	} else if strings.HasSuffix(s, "=") {
		key, err = base64.StdEncoding.DecodeString(s)
	} else {
		key, err = base64.RawStdEncoding.DecodeString(s) // tolerate missing padding
	}
	if err != nil {
		return "", fmt.Errorf("invalid key %q: %w", s, err)
	}
	return string(key), nil
}

func isHTTP(addr string) bool {
	return strings.HasPrefix(addr, "http:") || strings.HasPrefix(addr, "https:")
}
