package cmdput

import "io/fs"

func ownerAndGroup(fi fs.FileInfo) (owner, group int) {
	return 0, 0
}
