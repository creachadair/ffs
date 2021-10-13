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

// Package config defines the configuration settings shared by the
// subcommands of the ffs command-line tool.
package config

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/rpcstore"
	yaml "gopkg.in/yaml.v3"
)

// Settings represents the stored configuration settings for the ffs tool.
type Settings struct {
	// Context value governing the execution of the tool.
	Context context.Context `json:"-" yaml:"-"`

	// The default address for the blob store service (required).
	StoreAddress string `json:"storeAddress" yaml:"store-address"`
}

// OpenStore connects to the store service address in the configuration.  The
// caller is responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStore(_ context.Context) (blob.CAS, error) {
	if s.StoreAddress == "" {
		return nil, errors.New("no store service address")
	}
	conn, err := net.Dial(jrpc2.Network(s.StoreAddress))
	if err != nil {
		return nil, fmt.Errorf("dialing store: %w", err)
	}
	ch := channel.Line(conn, conn)
	return rpcstore.NewCAS(jrpc2.NewClient(ch, nil), nil), nil
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(blob.CAS) error) error {
	bs, err := s.OpenStore(ctx)
	if err != nil {
		return err
	}
	defer blob.CloseStore(ctx, bs)
	return f(bs)
}

// ParseKey parses the string encoding of a key.  By default, s must be hex
// encoded. If s begins with "@", it is taken literally. If s begins with "+"
// it is taken as base64.
func ParseKey(s string) (string, error) {
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

// RootKeys returns a slice of root keys from args, or an error if any of the
// keys is invalid.
func RootKeys(args []string) ([]string, error) {
	keys := make([]string, len(args))
	for i, arg := range args {
		if arg == "" {
			return nil, errors.New("empty root key")
		}
		keys[i] = RootKey(arg)
	}
	return keys, nil
}

// RootKey converts s into a root key.
func RootKey(s string) string {
	if strings.HasPrefix(s, "root:") {
		return s
	}
	return "root:" + s
}

// ExpandString calls os.ExpandEnv to expand environment variables in *s.
// The value of *s is replaced.
func ExpandString(s *string) { *s = os.ExpandEnv(*s) }

// Load reads and parses the contents of a config file from path.  If the
// specified path does not exist, an empty config is returned without error.
func Load(path string) (*Settings, error) {
	data, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return new(Settings), nil
	} else if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	cfg := new(Settings)
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	return cfg, nil
}

// ToJSON converts a value to indented JSON.
func ToJSON(msg interface{}) string {
	bits, err := json.Marshal(msg)
	if err != nil {
		return "null"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, bits, "", "  "); err != nil {
		return "null"
	}
	return buf.String()
}

func isAllHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
