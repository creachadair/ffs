# Flexible Filesystem

[![GoDoc](https://img.shields.io/static/v1?label=godoc&message=reference&color=yellowgreen)](https://pkg.go.dev/github.com/creachadair/ffs)

An work-in-progress experimental storage-agnostic filesystem representation.

## Summary

A file in FFS is represented as a Merkle tree encoded in a [content-addressable
blob store](./blob). Unlike files in POSIX style filesystems, all files in FFS
have the same structure, consisting of binary content, children, and
metadata. In other words, every "file" is also potentially a "directory", and
vice versa.

Files are encoded in storage using wire-format [protocol
buffer](https://developers.google.com/protocol-buffers) messages as defined in
[`wiretype.proto`](./file/wiretype/wiretype.proto). The key messages are:

- A [`Node`](./file/wiretype/wiretype.proto#L53) is the top-level encoding of a
  file. The storage key for a file is the content address (**storage key**) of
  its wire-encoded node message.

- An [`Index`](./file/wiretype/wiretype.proto#L111) records the binary content
  of a file, if any. An index records the total size of the file along with the
  sizes, offsets, and storage keys of its data blocks.

- A [`Child`](./file/wiretype/wiretype.proto#L156) records the name and storage
  key of a child of a file. Children are ordered lexicographically by name.

### Data Storage

Binary file content is stored in discrete blocks.  The block size is not fixed,
but varies over a (configurable) predefined range of sizes. Block boundaries
are chosen by splitting the file data with a [rolling hash](./block), similar
to the technique used in rsync or LBFS, and contents are stored as raw blobs.

The blocks belonging to a particular file are recorded in _extents_, where each
extent represents an ordered, contiguous sequence of blocks. Ranges of file
content that consist of all zero-valued bytes are not stored, allowing sparse
files to be stored compactly.

### Children

The children of a file are themselves files. Within the node, each child is
recorded as a pair comprising a non-empty string _name_ and the storage key of
another file. Each name must be unique among the children of a given file, but
it is fine for multiple children to share the same storage key.

### Metadata

Files have no required metadata, but for convenience the node representation
includes optional [`Stat`](./file/wiretype/wiretype.proto#L65) and
[`XAttr`](./file/wiretype/wiretype.proto#L148) messages that encode typical
filesystem metadata like POSIX permissions, file type, modification timestamp,
and ownership. These fields are persisted in the encoding of a node, and thus
affect its storage key, but are not otherwise interpreted.

## Related Tools

- The [`ffuse`](https://github.com/creachadair/ffuse) repository defines a FUSE
  filesystem that exposes the FFS data format.

- The [`blobd`](https://github.com/creachadair/ffs/tree/default/cmd/blobd)
  tool defines a JSON-RPC service that implements the FFS blob store interface
  over various underlying key-value storage implementations.

- The [`blob`](https://github.com/creachadair/ffs/tree/default/cmd/blob) tool
  is a client that communicates with the `blobd` service to manipulate the
  contents of a blob store as opaque data.

- The [`ffs`](https://github.com/creachadair/ffs/tree/default/cmd/ffs) tool
  also communicates with the `blobd` service and provides commands to
  manipulate the contents of the store as FFS specific messages.

- The [`file2json`](https://github.com/creachadair/ffs/tree/default/cmd/file2json)
  tool decodes wire-format node messages and translates them to JSON for easier
  reading by humans.
