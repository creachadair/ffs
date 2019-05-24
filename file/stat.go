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

	"bitbucket.org/creachadair/ffs/file/wirepb"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
)

// Stat records file stat metadata.
type Stat struct {
	Mode      os.FileMode
	ModTime   time.Time
	OwnerID   int    // numeric ID of file owner
	OwnerName string // name of file owner
	GroupID   int    // numeric ID of the file's primary group
	GroupName string // name of the file's primary group

	// To add additional metadata, add a field to this type and a corresponding
	// field to the wirepb.Stat message, then update the toProto and fromProto
	// methods to encode and decode the value.
}

func (s Stat) toProto() *wirepb.Stat {
	pb := &wirepb.Stat{
		Mode:      uint32(s.Mode),
		OwnerId:   uint64(s.OwnerID),
		OwnerName: s.OwnerName,
		GroupId:   uint64(s.GroupID),
		GroupName: s.GroupName,
	}
	if !s.ModTime.IsZero() {
		ts, _ := ptypes.TimestampProto(s.ModTime)
		pb.ModTime = ts
	}
	return pb
}

func (s *Stat) fromProto(pb *wirepb.Stat) {
	s.Mode = os.FileMode(pb.GetMode())
	s.OwnerID = int(pb.GetOwnerId())
	s.OwnerName = pb.GetOwnerName()
	s.GroupID = int(pb.GetGroupId())
	s.GroupName = pb.GetGroupName()
	if ts, err := ptypes.Timestamp(pb.GetModTime()); err == nil {
		s.ModTime = ts
	}
}

var (
	// ErrChildNotFound indicates that a requested child file does not exist.
	ErrChildNotFound = xerrors.New("child file not found")
)
