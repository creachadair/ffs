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
// a slash-separated string, which may optionally begin with "/".
package fpath

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/creachadair/ffs/file"
)

var (
	// ErrEmptyPath is reported by Set when given an empty path.
	ErrEmptyPath = errors.New("empty path")

	// ErrNilFile is reported by Set when passed a nil file.
	ErrNilFile = errors.New("nil file")

	// ErrSkipChildren signals to the Walk function that the children of the
	// current node should not be visited.
	ErrSkipChildren = errors.New("skip child files")
)

// Open traverses the given slash-separated path sequentially from root, and
// returns the resulting file or file.ErrChildNotFound. An empty path yields
// root without error.
func Open(ctx context.Context, root *file.File, path string) (*file.File, error) {
	fp, err := findPath(ctx, query{root: root, path: path})
	return fp.target, err
}

// OpenPath traverses the given slash-separated path sequentially from root,
// and returns a slice of all the files along the path, not including root
// itself.  If any element of the path does not exist, OpenPath returns the
// prefix that was found along with an file.ErrChildNotFound error.
func OpenPath(ctx context.Context, root *file.File, path string) ([]*file.File, error) {
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

	// If not nil, this function is called for any intermediate path elements
	// created along the path. It is also called for the final element if a new
	// final element is not provided as File.
	SetStat func(*file.Stat)

	// If not nil, insert this element at the end of the path.  If nil, a new
	// empty file with default options is created.
	File *file.File
}

func (s *SetOptions) create() bool { return s != nil && s.Create }

func (s *SetOptions) target() *file.File {
	if s == nil {
		return nil
	}
	return s.File
}

func (s *SetOptions) setStat(f *file.File) *file.File {
	if s != nil && s.SetStat != nil {
		fs := f.Stat()
		s.SetStat(&fs)
		fs.Update()
	}
	return f
}

// Set traverses the given slash-separated path sequentially from root and
// inserts a file at the end of it. An empty path is an error (ErrEmptyPath).
//
// If opts.Create is true, any missing path entries are created; otherwise it
// is an error (file.ErrChildNotFound) if any path element except the last does
// not exist.
//
// If opts.File != nil, that file is inserted at the end of the path; otherwise
// if opts.Create is true, a new empty file is inserted. If neither of these is
// true, Set reports ErrNilFile.
func Set(ctx context.Context, root *file.File, path string, opts *SetOptions) (*file.File, error) {
	if opts.target() == nil && !opts.create() {
		return nil, fmt.Errorf("set %q: %w", path, ErrNilFile)
	}
	dir, base := "", path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		dir, base = path[:i], path[i+1:]
	}
	if base == "" {
		return nil, fmt.Errorf("set %q: %w", path, ErrEmptyPath)
	}
	fp, err := findPath(ctx, query{
		root: root,
		path: dir,
		ef: func(fp *foundPath, err error) error {
			if errors.Is(err, file.ErrChildNotFound) && opts.create() {
				c := opts.setStat(fp.target.New(&file.NewOptions{Name: fp.targetName}))
				fp.target.Child().Set(fp.targetName, c)
				fp.parent, fp.target = fp.target, c
				return nil
			}
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	if last := opts.target(); last != nil {
		fp.target.Child().Set(base, last)
		return last, nil
	}
	newf := root.New(nil)
	fp.target.Child().Set(base, opts.setStat(newf))
	return newf, nil
}

// Remove removes the file at the given slash-separated path beneath root.  If
// any component of the path does not exist, it returns file.ErrChildNotFound.
func Remove(ctx context.Context, root *file.File, path string) error {
	fp, err := findPath(ctx, query{root: root, path: path})
	if err != nil {
		return err
	} else if fp.parent != nil {
		fp.parent.Child().Remove(fp.targetName)
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
	for ctx.Err() == nil && len(q) != 0 {
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
			kids := f.Child().Names()
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
	return ctx.Err()
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
	if clean == "" || path == "." {
		return nil
	}
	return strings.Split(clean, "/")
}
