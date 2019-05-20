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

package file

import (
	"os"
	"time"

	"golang.org/x/xerrors"
)

// Stat implements the os.FileInfo interface for *File values.
type Stat struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
}

// Name reports the attributed name of f. Note that a *File does not persist
// its name; the attributed name is assigned either when the file is created or
// when it is opened as a child of another file.
func (s Stat) Name() string { return s.name }

// Size reports the total size of the file's content, in bytes.
func (s Stat) Size() int64 { return s.size }

// Mode reports the file mode of the file. Note that the mode is persisted but
// otherwise ignored.
func (s Stat) Mode() os.FileMode { return s.mode }

// ModTime reports the last modified time of the file.
func (s Stat) ModTime() time.Time { return s.mtime }

// IsDir reports whether the file is a directory based on its attributed mode.
func (s Stat) IsDir() bool { return s.mode.IsDir() }

// Sys returns nil to satisfy the os.FileInfo interface.
func (Stat) Sys() interface{} { return nil }

var (
	// ErrChildNotFound indicates that a requested child file does not exist.
	ErrChildNotFound = xerrors.New("child file not found")
)
