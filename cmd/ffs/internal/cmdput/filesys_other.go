//go:build !windows

package cmdput

import (
	"io/fs"
	"syscall"
)

func ownerAndGroup(fi fs.FileInfo) (owner, group int) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if ok {
		return int(st.Uid), int(st.Gid)
	}
	return 0, 0
}
