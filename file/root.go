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
	"context"
	"fmt"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file/wirepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A Root represents the state of a file tree at a moment in time.  It carries
// the key of its root file and a named collection of snapshots.
type Root struct {
	s    blob.CAS
	msg  *wirepb.Root // current root state
	file *File        // root file cache
}

// NewRoot constructs a root by creating a new file in s. The resulting root
// has an empty snapshots list.
func NewRoot(s blob.CAS, opts *NewOptions) *Root {
	return &Root{
		s:    s,
		msg:  new(wirepb.Root),
		file: New(s, opts),
	}
}

// OpenRoot opens a root from the given storage key in s.
func OpenRoot(ctx context.Context, s blob.CAS, key string) (*Root, error) {
	bits, err := s.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("reading root: %v", err)
	}
	out := &Root{s: s, msg: new(wirepb.Root)}
	if err := proto.Unmarshal(bits, out.msg); err != nil {
		return nil, err
	}
	out.file, err = Open(ctx, s, string(out.msg.Key))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// File returns the root file of r.
func (r *Root) File() *File { return r.file }

// Snapshot reports whether r has a snapshot with the given name, and if so
// returns its current contents.
func (r *Root) Snapshot(name string) (snap Snapshot, ok bool) {
	if i := r.findSnapshot(name); i >= 0 {
		snap.fromProto(r.msg.Snapshots[i])
		return snap, true
	}
	return
}

// SetSnapshot creates or updates a snapshot with the given name, pointing to
// the current state of the root file. This has the side-effect of flushing the
// root file. It returns the storage key of the root file that was saved.
func (r *Root) SetSnapshot(ctx context.Context, name string) (string, error) {
	key, err := r.file.Flush(ctx)
	if err != nil {
		return "", err
	}
	r.addSnapshot(Snapshot{
		Key:     key,
		Name:    name,
		Updated: time.Now(),
	}.toProto())
	return key, nil
}

// RemoveSnapshot removes a snapshot with the specified label from r, and
// reports whether any change was made.
func (r *Root) RemoveSnapshot(name string) bool {
	if i := r.findSnapshot(name); i >= 0 {
		r.msg.Snapshots = append(r.msg.Snapshots[:i], r.msg.Snapshots[i+1:]...)
		return true
	}
	return false
}

// Snapshots returns a slice of the snapshots of r.
func (r *Root) Snapshots() []Snapshot {
	out := make([]Snapshot, len(r.msg.Snapshots))
	for i, snap := range r.msg.Snapshots {
		out[i].fromProto(snap)
	}
	return out
}

// findSnapshot reports the location of the first snapshot with the specified
// name, or -1. If name == "", findSnapshot returns -1.
func (r *Root) findSnapshot(name string) int {
	if name == "" {
		return -1
	}
	for i, snap := range r.msg.Snapshots {
		if snap.Name == name {
			return i
		}
	}
	return -1
}

// addSnapshot appends snap to the snapshots of root and removes any existing
// snapshot with the same name, if applicable.
func (r *Root) addSnapshot(snap *wirepb.Root_Snapshot) {
	old := r.findSnapshot(snap.Name)
	if old >= 0 {
		// Slide the rest down, leaving the final element free for reuse.
		copy(r.msg.Snapshots[old:], r.msg.Snapshots[old+1:])
		r.msg.Snapshots[len(r.msg.Snapshots)-1] = snap
		return
	}

	// No matching snapshot was found; add the new one to the end.
	r.msg.Snapshots = append(r.msg.Snapshots, snap)
}

// Flush writes the current contents of r to its associated storage key.  This
// has the side-effect of flushing the root file. It returns the storage key of
// the root blob.
func (r *Root) Flush(ctx context.Context) (string, error) {
	// Flush the root directory.
	key, err := r.file.Flush(ctx)
	if err != nil {
		return "", err
	}

	// Update the root storage key with the new state.
	r.msg.Key = []byte(key)
	bits, err := proto.Marshal(r.msg)
	if err != nil {
		return "", err
	}
	return r.s.PutCAS(ctx, bits)
}

// A Snapshot represents a point-in-time snapshot of the root.
type Snapshot struct {
	Key     string    // the storage key of the root file
	Name    string    // the label assigned to identify the snapshot
	Updated time.Time // when the snapshot was created or updated
}

func (s Snapshot) toProto() *wirepb.Root_Snapshot {
	pb := &wirepb.Root_Snapshot{
		Key:  []byte(s.Key),
		Name: s.Name,
	}
	if !s.Updated.IsZero() {
		pb.SnapTime = &timestamppb.Timestamp{
			Seconds: int64(s.Updated.Unix()),
			Nanos:   int32(s.Updated.Nanosecond()),
		}
	}
	return pb
}

func (s *Snapshot) fromProto(pb *wirepb.Root_Snapshot) {
	s.Key = string(pb.GetKey())
	s.Name = pb.GetName()
	if ts := pb.SnapTime; ts != nil {
		s.Updated = time.Unix(ts.Seconds, int64(ts.Nanos))
	}
}
