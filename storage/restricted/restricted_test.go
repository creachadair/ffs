// Copyright 2026 Michael J. Fromberger. All Rights Reserved.
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

package restricted_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffs/storage/restricted"
)

// policyFunc implements the [Policy] interface with a function.  The flag
// argument reports whether the check is for a keyspace (true), or a substore.
type policyFunc func(ctx context.Context, isKV bool, path []string, name string) error

// CheckSub implements [Policy] by calling p with the isKV argument set false.
func (p policyFunc) CheckSub(ctx context.Context, path []string, name string) error {
	return p(ctx, false, path, name)
}

// CheckKV implements [Policy] by calling p with the isKV argument set true.
func (p policyFunc) CheckKV(ctx context.Context, path []string, name string) error {
	return p(ctx, true, path, name)
}

func TestStore(t *testing.T) {
	s := restricted.NewStore(memstore.New(nil), restricted.AllowAll)
	storetest.Run(t, s)
}

func TestPolicyChecks(t *testing.T) {
	testPolicy := policyFunc(func(ctx context.Context, isKV bool, path []string, name string) error {
		t.Logf("Check isKV=%v path=%q name=%q", isKV, path, name)
		if isKV {
			if len(path) == 0 && name == "rootkv" {
				return nil
			}
			if slices.Contains(path, "rootsub") && name == "subkv" {
				return nil
			}
		} else {
			if len(path) == 0 && name == "rootsub" {
				return nil
			}
			if slices.Contains(path, "rootsub") && name == "subsub" {
				return nil
			}
		}
		switch name {
		case "wrapped": // report an error that wraps ErrNoAccess
			return fmt.Errorf("computer says no: %w", restricted.ErrNoAccess)
		case "direct": // report ErrNoAccess directly
			return restricted.ErrNoAccess
		default: // report an error unrelated to ErrNoAccess
			return errors.New("hail no my person")
		}
	})
	wantNoError := func(label string) func(any, error) any {
		return func(got any, err error) any {
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", label, err)
			}
			return got
		}
	}
	wantNoAccess := func(label string) func(any, error) {
		return func(got any, err error) {
			if !errors.Is(err, restricted.ErrNoAccess) {
				t.Errorf("%s: got (%v, %v), want %v", label, got, err, restricted.ErrNoAccess)
			}
		}
	}

	s := restricted.NewStore(memstore.New(nil), testPolicy)

	// Check that errors in various shapes are properly wrapped.
	wantNoAccess("Root KV other")(s.KV(t.Context(), "other"))
	wantNoAccess("Root KV other")(s.KV(t.Context(), "wrapped"))
	wantNoAccess("Root KV other")(s.KV(t.Context(), "direct"))

	wantNoError("Root KV OK")(s.KV(t.Context(), "rootkv"))

	wantNoAccess("Root sub other")(s.Sub(t.Context(), "other"))
	sub := wantNoError("Root sub OK")(s.Sub(t.Context(), "rootsub")).(blob.Store)

	wantNoAccess("Sub KV other")(sub.KV(t.Context(), "other"))
	wantNoError("Sub KV OK")(sub.KV(t.Context(), "subkv"))

	wantNoAccess("Sub sub other")(sub.Sub(t.Context(), "other"))
	ssub := wantNoError("Sub sub OK")(sub.Sub(t.Context(), "subsub")).(blob.Store)
	wantNoAccess("Sub sub KV rootkv")(ssub.KV(t.Context(), "rootkv"))
	wantNoAccess("Sub sub KV other")(ssub.KV(t.Context(), "other"))
	wantNoError("Sub sub KV subkv")(ssub.KV(t.Context(), "subkv"))
}
