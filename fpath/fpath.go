// Copyright 2019 Michael J. Fromberger. All Rights Reserved.
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

// Package fpath implements path traversal relative to a *file.File.  A path is
// a slash-separated string,
package fpath

import (
	"context"
	"strings"

	"bitbucket.org/creachadair/ffs/file"
	"golang.org/x/xerrors"
)

var (
	// ErrEmptyPath is reported by Set when given an empty path.
	ErrEmptyPath = xerrors.New("empty path")

	// ErrNilFile is reported by Set when passed a nil file.
	ErrNilFile = xerrors.New("nil file")
)

// Open traverses the given slash-separated path sequentially from root, and
// returns the resulting file or ErrChildNotFound. An empty path yields root
// without error.
func Open(ctx context.Context, root *file.File, path string) (*file.File, error) {
	fp, err := findPath(ctx, query{root: root, path: path})
	return fp.target, err
}

// Set traverses the given slash-separated path sequentially from root and
// inserts f at the end of it. An empty path is an error, and if any element of
// the path except the last does not exist it reports ErrChildNotFound.
func Set(ctx context.Context, root *file.File, path string, f *file.File) error {
	if f == nil {
		return xerrors.Errorf("Set %q: %w", path, ErrNilFile)
	}
	dir, base := "", path
	if i := strings.LastIndex(path, "/"); i > 0 {
		dir, base = path[:i], path[i+1:]
	}
	if base == "" {
		return xerrors.Errorf("set: %w", ErrEmptyPath)
	}
	fp, err := findPath(ctx, query{root: root, path: dir})
	if err != nil {
		return err
	}
	fp.target.Set(base, f)
	return nil
}

// Create creates a file at the given slash-separated path beneath root,
// creating any necessary parents, and returns the final element named. An
// empty path yields root without error.
func Create(ctx context.Context, root *file.File, path string) (*file.File, error) {
	fp, err := findPath(ctx, query{
		root: root,
		path: path,
		ef: func(fp *foundPath, err error) error {
			if xerrors.Is(err, file.ErrChildNotFound) {
				c := fp.target.New(&file.NewOptions{Name: fp.targetName})
				fp.target.Set(fp.targetName, c)
				fp.parent, fp.target = fp.target, c
				return nil
			}
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	return fp.target, nil
}

// Remove removes the file at the given slash-separated path beneath root.
// If any component of the path does not exist, it returns ErrChildNotFound.
func Remove(ctx context.Context, root *file.File, path string) error {
	fp, err := findPath(ctx, query{root: root, path: path})
	if err != nil {
		return err
	} else if fp.parent != nil {
		fp.parent.Remove(fp.targetName)
	}
	return nil
}

type errFilter = func(*foundPath, error) error

func findPath(ctx context.Context, q query) (foundPath, error) {
	fp := foundPath{
		parent: nil,
		target: q.root,
	}
	for _, name := range parsePath(q.path) {
		fp.targetName = name
		c, err := fp.target.Open(ctx, name)
		if err == nil {
			fp.parent, fp.target = fp.target, c
		} else if q.ef == nil {
			return fp, err
		} else if ferr := q.ef(&fp, err); ferr != nil {
			return fp, ferr
		}
	}
	return fp, nil
}

type query struct {
	root *file.File
	path string
	ef   errFilter
}

type foundPath struct {
	parent     *file.File
	target     *file.File
	targetName string
}

func parsePath(path string) []string {
	clean := strings.TrimPrefix(path, "/")
	if clean == "" {
		return nil
	}
	return strings.Split(clean, "/")
}
