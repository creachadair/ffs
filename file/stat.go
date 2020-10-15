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

// Stat records file stat metadata.
type Stat struct {
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

// toWireType encodes s as an equivalent wiretype.Stat.
func (s Stat) toWireType() *wiretype.Stat {
	pb := &wiretype.Stat{
		Mode:      uint32(s.Mode),
		OwnerID:   uint64(s.OwnerID),
		OwnerName: s.OwnerName,
		GroupID:   uint64(s.GroupID),
		GroupName: s.GroupName,
	}
	if !s.ModTime.IsZero() {
		pb.ModTime = &wiretype.Time{
			Seconds: s.ModTime.Unix(),
			Nanos:   int64(s.ModTime.Nanosecond()),
		}
	}
	return pb
}

// FromWireType decodes a wiretype.Stat into s. If pb == nil, s is unmodified.
func (s *Stat) FromWireType(pb *wiretype.Stat) {
	if pb == nil {
		return // no stat was persisted for this file
	}
	s.Mode = os.FileMode(pb.Mode)
	s.OwnerID = int(pb.OwnerID)
	s.OwnerName = pb.OwnerName
	s.GroupID = int(pb.GroupID)
	s.GroupName = pb.GroupName
	if t := pb.ModTime; t != nil {
		s.ModTime = time.Unix(t.Seconds, t.Nanos)
	}
}
