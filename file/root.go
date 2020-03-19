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

// A Root represents the state of a filesystem at a moment in time, including
// the storage key of its root directory and any snapshots.
type Root struct {
	s    blob.CAS
	name string       // pointer file name, e.g. "ROOT".
	msg  *wirepb.Root // current root state
	file *File        // root file pointer
}

// NewRoot creates a new empty root stored in s. The name is the storage key of
// the pointer, which contains the content address of a *wirepb.Root message.
func NewRoot(s blob.CAS, name string) *Root {
	return &Root{s: s, name: name, msg: new(wirepb.Root), file: New(s, nil)}
}

// OpenRoot opens the root message indicated by the given pointer name in s.
func OpenRoot(ctx context.Context, s blob.CAS, name string) (*Root, error) {
	key, err := s.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("reading pointer: %w", err)
	}
	bits, err := s.Get(ctx, string(key))
	if err != nil {
		return nil, fmt.Errorf("reading root: %w", err)
	}
	out := &Root{s: s, name: name, msg: new(wirepb.Root)}
	if err := proto.Unmarshal(bits, out.msg); err != nil {
		return nil, err
	}
	out.file, err = Open(ctx, s, string(out.msg.Key))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// File returns the root directory of r.
func (r *Root) File() *File { return r.file }

// Snapshot adds a snapshot with the specified label pointing to the current
// filesystem root.  This has the side-effect of flushing the root directory.
// The storage key of the updated root is returned.
func (r *Root) Snapshot(ctx context.Context, name string) (string, error) {
	key, err := r.file.Flush(ctx)
	if err != nil {
		return "", err
	}
	now := time.Now()
	r.addSnapshot(&wirepb.Root_Snapshot{
		Key:  []byte(key),
		Name: name,
		SnapTime: &timestamppb.Timestamp{
			Seconds: int64(now.Unix()),
			Nanos:   int32(now.Nanosecond()),
		},
	})
	return r.Flush(ctx)
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

// Flush flushes the root directory of r, writes the current state of the root
// to storage, and updates the pointer. The storage key of the root is returned.
func (r *Root) Flush(ctx context.Context) (string, error) {
	// Flush the root directory.
	key, err := r.file.Flush(ctx)
	if err != nil {
		return "", err
	}

	// Update the root message and obtain its storage key.
	r.msg.Key = []byte(key)
	bits, err := proto.Marshal(r.msg)
	if err != nil {
		return "", err
	}
	rkey, err := r.s.PutCAS(ctx, bits)
	if err != nil {
		return "", err
	}

	// Update the pointer and return the storage key.
	return rkey, r.s.Put(ctx, blob.PutOptions{
		Key:     r.name,
		Data:    []byte(rkey),
		Replace: true,
	})
}
