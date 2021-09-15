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

	"github.com/creachadair/ffs/file/wiretype"
)

// A Stat is a view into the stat metadata for a file.
type Stat struct {
	f *File // set for stat views of an existing file; nil OK

	Mode    os.FileMode `json:"mode,omitempty"`
	ModTime time.Time   `json:"mod_time,omitempty"`

	// Numeric ID and name of file owner.
	OwnerID   int    `json:"owner_id,omitempty"`
	OwnerName string `json:"owner_name,omitempty"`

	// Numeric ID and name of file group.
	GroupID   int    `json:"group_id,omitempty"`
	GroupName string `json:"group_name,omitempty"`

	// To add additional metadata, add a field to this type and a corresponding
	// field to wiretype.Stat, then update the toWireType and fromWireType
	// methods to encode and decode the value.
}

// Clear clears the current stat metadata for the file associated with s.
// Calling this method does not change whether stat is persisted, nor does it
// modify the current contents of s, so calling Update on the same s will
// restore the cleared values.
func (s Stat) Clear() { s.f.setStat(Stat{}) }

// Update updates the stat metadata for the file associated with s to the
// current contents of s. Calling this method does not change whether stat is
// persisted.
func (s Stat) Update() { s.f.setStat(s) }

// Edit calls f to edit the contents of s in-place. It returns the modified s.
func (s Stat) Edit(edit func(*Stat)) Stat { save := s.f; edit(&s); s.f = save; return s }

// Persist enables (ok == true) or disables (ok == false) stat persistence for
// the file associated with s. The contents of s are not changed. It returns s.
func (s Stat) Persist(ok bool) Stat { s.f.saveStat = ok; s.f.inval(); return s }

// Persistent reports whether the file associated with s persists stat.
func (s Stat) Persistent() bool { return s.f.saveStat }

const (
	bitSetuid = 04000
	bitSetgid = 02000
	bitSticky = 01000
)

// toWireType encodes s as an equivalent wiretype.Stat.
func (s Stat) toWireType() *wiretype.Stat {
	perm := s.Mode & os.ModePerm
	if s.Mode&os.ModeSetuid != 0 {
		perm |= bitSetuid
	}
	if s.Mode&os.ModeSetgid != 0 {
		perm |= bitSetgid
	}
	if s.Mode&os.ModeSticky != 0 {
		perm |= bitSticky
	}
	pb := &wiretype.Stat{
		Permissions: uint32(perm),
		FileType:    modeToType(s.Mode),
	}
	if !s.ModTime.IsZero() {
		pb.ModTime = &wiretype.Timestamp{
			Seconds: uint64(s.ModTime.Unix()),
			Nanos:   uint32(s.ModTime.Nanosecond()),
		}
	}
	if s.OwnerID != 0 || s.OwnerName != "" {
		pb.Owner = &wiretype.Stat_Ident{
			Id:   uint64(s.OwnerID),
			Name: s.OwnerName,
		}
	}
	if s.GroupID != 0 || s.GroupName != "" {
		pb.Group = &wiretype.Stat_Ident{
			Id:   uint64(s.GroupID),
			Name: s.GroupName,
		}
	}
	return pb
}

func modeToType(mode os.FileMode) wiretype.Stat_FileType {
	switch {
	case mode&os.ModeType == 0:
		return wiretype.Stat_REGULAR
	case mode&os.ModeDir != 0:
		return wiretype.Stat_DIRECTORY
	case mode&os.ModeSymlink != 0:
		return wiretype.Stat_SYMLINK
	case mode&os.ModeSocket != 0:
		return wiretype.Stat_SOCKET
	case mode&os.ModeNamedPipe != 0:
		return wiretype.Stat_NAMED_PIPE
	case mode&os.ModeDevice != 0:
		if mode&os.ModeCharDevice != 0 {
			return wiretype.Stat_CHAR_DEVICE
		}
		return wiretype.Stat_DEVICE
	default:
		return wiretype.Stat_UNKNOWN
	}
}

var ftypeMode = [...]os.FileMode{
	wiretype.Stat_REGULAR:     0,
	wiretype.Stat_DIRECTORY:   os.ModeDir,
	wiretype.Stat_SYMLINK:     os.ModeSymlink,
	wiretype.Stat_SOCKET:      os.ModeSocket,
	wiretype.Stat_NAMED_PIPE:  os.ModeNamedPipe,
	wiretype.Stat_DEVICE:      os.ModeDevice,
	wiretype.Stat_CHAR_DEVICE: os.ModeDevice | os.ModeCharDevice,
}

func typeToMode(ftype wiretype.Stat_FileType) os.FileMode {
	if n := int(ftype); n >= 0 && n < len(ftypeMode) {
		return ftypeMode[n]
	}
	return os.ModeIrregular
}

// fromWireType decodes a wiretype.Stat into s. If pb == nil, s is unmodified.
func (s *Stat) fromWireType(pb *wiretype.Stat) {
	if pb == nil {
		return // no stat was persisted for this file
	}
	mode := os.FileMode(pb.Permissions & 0777)
	if pb.Permissions&bitSetuid != 0 {
		mode |= os.ModeSetuid
	}
	if pb.Permissions&bitSetgid != 0 {
		mode |= os.ModeSetgid
	}
	if pb.Permissions&bitSticky != 0 {
		mode |= os.ModeSticky
	}
	s.Mode = mode | typeToMode(pb.FileType)
	if id := pb.Owner; id != nil {
		s.OwnerID = int(id.Id)
		s.OwnerName = id.Name
	}
	if id := pb.Group; id != nil {
		s.GroupID = int(id.Id)
		s.GroupName = id.Name
	}
	if t := pb.ModTime; t != nil {
		s.ModTime = time.Unix(int64(t.Seconds), int64(t.Nanos))
	}
}
