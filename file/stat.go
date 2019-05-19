package file

import (
	"os"
	"time"
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
