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
	"path"
	"strings"

	"bitbucket.org/creachadair/ffs/file"
	"golang.org/x/xerrors"
)

var (
	// ErrEmptyPath is reported by Set when given an empty path.
	ErrEmptyPath = xerrors.New("empty path")

	// ErrNilFile is reported by Set when passed a nil file.
	ErrNilFile = xerrors.New("nil file")

	// ErrSkipChildren signals to the Walk function that the children of the
	// current node should not be visited.
	ErrSkipChildren = xerrors.New("skip child files")
)

// Open traverses the given slash-separated path sequentially from root, and
// returns the resulting file or ErrChildNotFound. An empty path yields root
// without error.
func Open(ctx context.Context, root *file.File, path string) (*file.File, error) {
	fp, err := findPath(ctx, query{root: root, path: path})
	return fp.target, err
}

// View traverses the given slash-separated path sequentially from root, and
// returns a slice of all the files along the path not including root itself.
// If any element of the path does not exist, View returns the prefix that was
// found along with an file.ErrChildNotFound error.
func View(ctx context.Context, root *file.File, path string) ([]*file.File, error) {
	var out []*file.File
	cur := root
	for _, name := range parsePath(path) {
		c, err := cur.Open(ctx, name)
		if err != nil {
			return out, err
		}
		out = append(out, c)
		cur = c
	}
	return out, nil
}

// SetOptions control the behaviour of the Set function. A nil *SetOptions
// behaves as a zero-valued options structure.
type SetOptions struct {
	// If true, create any path elements that do not exist along the path.
	Create bool

	// If not nil, insert this element at the end of the path.  If nil, a new
	// empty file with default options is created.
	File *file.File
}

func (s *SetOptions) create() bool { return s != nil && s.Create }

func (s *SetOptions) target() *file.File {
	if s == nil {
		return nil
	} else {
		return s.File
	}
}

// Set traverses the given slash-separated path sequentially from root and
// inserts a file at the end of it. An empty path is an error.  If opts.Create
// is true, any missing path entries are created; otherwise if any path element
// except the last does not exist it reports file.ErrChildNotFound.
//
// If opts.File is not nil, it is inserted at the end of the path; otherwise if
// opts.Create is true, a new empty file is inserted. If neither of these is
// true, it reports ErrNilFile.
func Set(ctx context.Context, root *file.File, path string, opts *SetOptions) error {
	if opts.target() == nil && !opts.create() {
		return xerrors.Errorf("set %q: %w", path, ErrNilFile)
	}
	dir, base := "", path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		dir, base = path[:i], path[i+1:]
	}
	if base == "" {
		return xerrors.Errorf("set %q: %w", path, ErrEmptyPath)
	}
	fp, err := findPath(ctx, query{
		root: root,
		path: dir,
		ef: func(fp *foundPath, err error) error {
			if xerrors.Is(err, file.ErrChildNotFound) && opts.create() {
				c := fp.target.New(&file.NewOptions{Name: fp.targetName})
				fp.target.Set(fp.targetName, c)
				fp.parent, fp.target = fp.target, c
				return nil
			}
			return err
		},
	})
	if err != nil {
		return err
	}
	if last := opts.target(); last != nil {
		fp.target.Set(base, last)
	} else {
		fp.target.Set(base, root.New(nil))
	}
	return nil
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

// An Entry is the argument to the visit callback for the Walk function.
type Entry struct {
	Path string     // the path of this entry relative to the root
	File *file.File // the file for this entry (nil on error)
	Err  error
}

// Walk walks the file tree rooted at root, depth-first, and calls visit with
// an entry for each file in the tree. The entry.Path gives the path of the
// file relative to the root. If an error occurred opening the file at that
// path, entry.File is nil and entry.Err contains the error; otherwise
// entry.File contains the file addressed by the path.
//
// If visit reports an error other than ErrSkipChildren, traversal stops and
// that error is returned to the caller of Walk.  If it returns ErrSkipChildren
// the walk continues but skips the descendant files of the current entry.
func Walk(ctx context.Context, root *file.File, visit func(Entry) error) error {
	q := []string{""}
	for len(q) != 0 {
		next := q[len(q)-1]
		q = q[:len(q)-1]

		f, err := Open(ctx, root, next)
		err = visit(Entry{
			Path: next,
			File: f,
			Err:  err,
		})
		if err == nil {
			if f == nil {
				continue // the error was suppressed
			}
			kids := f.Children()
			for i, name := range kids {
				kids[i] = path.Join(next, name)
			}
			for i, j := 0, len(kids)-1; i < j; i++ {
				kids[i], kids[j] = kids[j], kids[i]
				j--
			}
			q = append(q, kids...)
		} else if err != ErrSkipChildren {
			return err
		}
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
