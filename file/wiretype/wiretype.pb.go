// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

// This schema defines the encoding types for the ffs package.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v5.29.3
// source: wiretype.proto

package wiretype

import (
	indexpb "github.com/creachadair/ffs/index/indexpb"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// A FileType abstracts the type of a file.
type Stat_FileType int32

const (
	Stat_REGULAR     Stat_FileType = 0   // a regular file
	Stat_DIRECTORY   Stat_FileType = 1   // a directory
	Stat_SYMLINK     Stat_FileType = 2   // a symbolic link
	Stat_SOCKET      Stat_FileType = 3   // a Unix-domain socket
	Stat_NAMED_PIPE  Stat_FileType = 4   // a named pipe
	Stat_DEVICE      Stat_FileType = 5   // a (block) device file
	Stat_CHAR_DEVICE Stat_FileType = 6   // a (character) device file
	Stat_UNKNOWN     Stat_FileType = 404 // nothing is known about the type of this file
)

// Enum value maps for Stat_FileType.
var (
	Stat_FileType_name = map[int32]string{
		0:   "REGULAR",
		1:   "DIRECTORY",
		2:   "SYMLINK",
		3:   "SOCKET",
		4:   "NAMED_PIPE",
		5:   "DEVICE",
		6:   "CHAR_DEVICE",
		404: "UNKNOWN",
	}
	Stat_FileType_value = map[string]int32{
		"REGULAR":     0,
		"DIRECTORY":   1,
		"SYMLINK":     2,
		"SOCKET":      3,
		"NAMED_PIPE":  4,
		"DEVICE":      5,
		"CHAR_DEVICE": 6,
		"UNKNOWN":     404,
	}
)

func (x Stat_FileType) Enum() *Stat_FileType {
	p := new(Stat_FileType)
	*p = x
	return p
}

func (x Stat_FileType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Stat_FileType) Descriptor() protoreflect.EnumDescriptor {
	return file_wiretype_proto_enumTypes[0].Descriptor()
}

func (Stat_FileType) Type() protoreflect.EnumType {
	return &file_wiretype_proto_enumTypes[0]
}

func (x Stat_FileType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Stat_FileType.Descriptor instead.
func (Stat_FileType) EnumDescriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{3, 0}
}

// An Object is the top-level wrapper for encoded objects.
type Object struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Value:
	//
	//	*Object_Node
	//	*Object_Root
	//	*Object_Index
	Value isObject_Value `protobuf_oneof:"value"`
	// A version marker for the stored object.
	// Currently 0 is the only known value.
	Version       uint64 `protobuf:"varint,15,opt,name=version,proto3" json:"version,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Object) Reset() {
	*x = Object{}
	mi := &file_wiretype_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Object) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Object) ProtoMessage() {}

func (x *Object) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Object.ProtoReflect.Descriptor instead.
func (*Object) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{0}
}

func (x *Object) GetValue() isObject_Value {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Object) GetNode() *Node {
	if x != nil {
		if x, ok := x.Value.(*Object_Node); ok {
			return x.Node
		}
	}
	return nil
}

func (x *Object) GetRoot() *Root {
	if x != nil {
		if x, ok := x.Value.(*Object_Root); ok {
			return x.Root
		}
	}
	return nil
}

func (x *Object) GetIndex() *indexpb.Index {
	if x != nil {
		if x, ok := x.Value.(*Object_Index); ok {
			return x.Index
		}
	}
	return nil
}

func (x *Object) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

type isObject_Value interface {
	isObject_Value()
}

type Object_Node struct {
	Node *Node `protobuf:"bytes,1,opt,name=node,proto3,oneof"` // a structured file object
}

type Object_Root struct {
	Root *Root `protobuf:"bytes,2,opt,name=root,proto3,oneof"` // a root pointer
}

type Object_Index struct {
	Index *indexpb.Index `protobuf:"bytes,3,opt,name=index,proto3,oneof"` // a blob index
}

func (*Object_Node) isObject_Value() {}

func (*Object_Root) isObject_Value() {}

func (*Object_Index) isObject_Value() {}

// A Root records the location of a root node of a file tree.
type Root struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The storage key of the root of the tree.
	// The blob contains an Object holding a Node message.
	// This field must be non-empty for a root to be valid.
	FileKey []byte `protobuf:"bytes,1,opt,name=file_key,json=fileKey,proto3" json:"file_key,omitempty"`
	// A human-readable descriptive label for the root.
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// The storage key of a blob index for the root.
	// The blob contains a Object holding an ffs.index.Index message.
	IndexKey      []byte `protobuf:"bytes,4,opt,name=index_key,json=indexKey,proto3" json:"index_key,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Root) Reset() {
	*x = Root{}
	mi := &file_wiretype_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Root) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Root) ProtoMessage() {}

func (x *Root) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Root.ProtoReflect.Descriptor instead.
func (*Root) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{1}
}

func (x *Root) GetFileKey() []byte {
	if x != nil {
		return x.FileKey
	}
	return nil
}

func (x *Root) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Root) GetIndexKey() []byte {
	if x != nil {
		return x.IndexKey
	}
	return nil
}

// A Node is the top-level encoding of a file.
type Node struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Index         *Index                 `protobuf:"bytes,1,opt,name=index,proto3" json:"index,omitempty"`                 // file contents
	Stat          *Stat                  `protobuf:"bytes,2,opt,name=stat,proto3" json:"stat,omitempty"`                   // stat metadata (optional)
	XAttrs        []*XAttr               `protobuf:"bytes,3,rep,name=x_attrs,json=xAttrs,proto3" json:"x_attrs,omitempty"` // extended attributes
	Children      []*Child               `protobuf:"bytes,4,rep,name=children,proto3" json:"children,omitempty"`           // child file pointers
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Node) Reset() {
	*x = Node{}
	mi := &file_wiretype_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{2}
}

func (x *Node) GetIndex() *Index {
	if x != nil {
		return x.Index
	}
	return nil
}

func (x *Node) GetStat() *Stat {
	if x != nil {
		return x.Stat
	}
	return nil
}

func (x *Node) GetXAttrs() []*XAttr {
	if x != nil {
		return x.XAttrs
	}
	return nil
}

func (x *Node) GetChildren() []*Child {
	if x != nil {
		return x.Children
	}
	return nil
}

// Stat records POSIX style file metadata. Other than the modification time,
// these metadata are not interpreted by the file plumbing, but are preserved
// for the benefit of external tools.
type Stat struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The low-order 12 bits of this field hold the standard Unix permissions,
	// along with the sticky, setuid, and setgid bits. The rest are reserved and
	// must be set to zero. In binary:
	//
	//	           owner group other
	//	... +-+-+-+-----+-----+-----+   S: setuid
	//	    |S|G|T|r w x|r w x|r w x|   G: setgid
	//	... +-+-+-+-----+-----+-----+   T: sticky
	//	     B A 9     6     3     0  « bit
	Permissions   uint32        `protobuf:"varint,1,opt,name=permissions,proto3" json:"permissions,omitempty"`
	FileType      Stat_FileType `protobuf:"varint,2,opt,name=file_type,json=fileType,proto3,enum=ffs.file.Stat_FileType" json:"file_type,omitempty"`
	ModTime       *Timestamp    `protobuf:"bytes,3,opt,name=mod_time,json=modTime,proto3" json:"mod_time,omitempty"`
	Owner         *Stat_Ident   `protobuf:"bytes,4,opt,name=owner,proto3" json:"owner,omitempty"`
	Group         *Stat_Ident   `protobuf:"bytes,5,opt,name=group,proto3" json:"group,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Stat) Reset() {
	*x = Stat{}
	mi := &file_wiretype_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Stat) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stat) ProtoMessage() {}

func (x *Stat) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stat.ProtoReflect.Descriptor instead.
func (*Stat) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{3}
}

func (x *Stat) GetPermissions() uint32 {
	if x != nil {
		return x.Permissions
	}
	return 0
}

func (x *Stat) GetFileType() Stat_FileType {
	if x != nil {
		return x.FileType
	}
	return Stat_REGULAR
}

func (x *Stat) GetModTime() *Timestamp {
	if x != nil {
		return x.ModTime
	}
	return nil
}

func (x *Stat) GetOwner() *Stat_Ident {
	if x != nil {
		return x.Owner
	}
	return nil
}

func (x *Stat) GetGroup() *Stat_Ident {
	if x != nil {
		return x.Group
	}
	return nil
}

// Time is the encoding of a timestamp, in seconds and nanoseconds elapsed
// since the Unix epoch in UTC.
type Timestamp struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Seconds       uint64                 `protobuf:"varint,1,opt,name=seconds,proto3" json:"seconds,omitempty"`
	Nanos         uint32                 `protobuf:"varint,2,opt,name=nanos,proto3" json:"nanos,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Timestamp) Reset() {
	*x = Timestamp{}
	mi := &file_wiretype_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Timestamp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Timestamp) ProtoMessage() {}

func (x *Timestamp) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Timestamp.ProtoReflect.Descriptor instead.
func (*Timestamp) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{4}
}

func (x *Timestamp) GetSeconds() uint64 {
	if x != nil {
		return x.Seconds
	}
	return 0
}

func (x *Timestamp) GetNanos() uint32 {
	if x != nil {
		return x.Nanos
	}
	return 0
}

// An Index records the size and storage locations of file data.
type Index struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	TotalBytes    uint64                 `protobuf:"varint,1,opt,name=total_bytes,json=totalBytes,proto3" json:"total_bytes,omitempty"`
	Extents       []*Extent              `protobuf:"bytes,2,rep,name=extents,proto3" json:"extents,omitempty"` // multiple blocks
	Single        []byte                 `protobuf:"bytes,3,opt,name=single,proto3" json:"single,omitempty"`   // a single block
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Index) Reset() {
	*x = Index{}
	mi := &file_wiretype_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Index) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Index) ProtoMessage() {}

func (x *Index) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Index.ProtoReflect.Descriptor instead.
func (*Index) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{5}
}

func (x *Index) GetTotalBytes() uint64 {
	if x != nil {
		return x.TotalBytes
	}
	return 0
}

func (x *Index) GetExtents() []*Extent {
	if x != nil {
		return x.Extents
	}
	return nil
}

func (x *Index) GetSingle() []byte {
	if x != nil {
		return x.Single
	}
	return nil
}

// An Extent describes a single contiguous span of stored data.
type Extent struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Base          uint64                 `protobuf:"varint,1,opt,name=base,proto3" json:"base,omitempty"`   // the starting offset
	Bytes         uint64                 `protobuf:"varint,2,opt,name=bytes,proto3" json:"bytes,omitempty"` // the number of bytes in this extent
	Blocks        []*Block               `protobuf:"bytes,3,rep,name=blocks,proto3" json:"blocks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Extent) Reset() {
	*x = Extent{}
	mi := &file_wiretype_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Extent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Extent) ProtoMessage() {}

func (x *Extent) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Extent.ProtoReflect.Descriptor instead.
func (*Extent) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{6}
}

func (x *Extent) GetBase() uint64 {
	if x != nil {
		return x.Base
	}
	return 0
}

func (x *Extent) GetBytes() uint64 {
	if x != nil {
		return x.Bytes
	}
	return 0
}

func (x *Extent) GetBlocks() []*Block {
	if x != nil {
		return x.Blocks
	}
	return nil
}

// A Block describes the size and storage key of a data blob.
type Block struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Bytes         uint64                 `protobuf:"varint,1,opt,name=bytes,proto3" json:"bytes,omitempty"` // the number of bytes in this block
	Key           []byte                 `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`      // the storage key of the block data
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Block) Reset() {
	*x = Block{}
	mi := &file_wiretype_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Block) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Block) ProtoMessage() {}

func (x *Block) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Block.ProtoReflect.Descriptor instead.
func (*Block) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{7}
}

func (x *Block) GetBytes() uint64 {
	if x != nil {
		return x.Bytes
	}
	return 0
}

func (x *Block) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

// An XAttr records the name and value of an extended attribute.
// The contents of the value are not interpreted.
type XAttr struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value         []byte                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *XAttr) Reset() {
	*x = XAttr{}
	mi := &file_wiretype_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *XAttr) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*XAttr) ProtoMessage() {}

func (x *XAttr) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use XAttr.ProtoReflect.Descriptor instead.
func (*XAttr) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{8}
}

func (x *XAttr) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *XAttr) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

// A Child records the name and storage key of a child Node.
type Child struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Key           []byte                 `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Child) Reset() {
	*x = Child{}
	mi := &file_wiretype_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Child) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Child) ProtoMessage() {}

func (x *Child) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Child.ProtoReflect.Descriptor instead.
func (*Child) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{9}
}

func (x *Child) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Child) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

// An Ident represents the identity of a user or group.
type Stat_Ident struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            uint64                 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`    // numeric ID
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"` // human-readable name
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Stat_Ident) Reset() {
	*x = Stat_Ident{}
	mi := &file_wiretype_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Stat_Ident) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stat_Ident) ProtoMessage() {}

func (x *Stat_Ident) ProtoReflect() protoreflect.Message {
	mi := &file_wiretype_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stat_Ident.ProtoReflect.Descriptor instead.
func (*Stat_Ident) Descriptor() ([]byte, []int) {
	return file_wiretype_proto_rawDescGZIP(), []int{3, 0}
}

func (x *Stat_Ident) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Stat_Ident) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

var File_wiretype_proto protoreflect.FileDescriptor

const file_wiretype_proto_rawDesc = "" +
	"\n" +
	"\x0ewiretype.proto\x12\bffs.file\x1a\x19index/indexpb/index.proto\"\xa1\x01\n" +
	"\x06Object\x12$\n" +
	"\x04node\x18\x01 \x01(\v2\x0e.ffs.file.NodeH\x00R\x04node\x12$\n" +
	"\x04root\x18\x02 \x01(\v2\x0e.ffs.file.RootH\x00R\x04root\x12(\n" +
	"\x05index\x18\x03 \x01(\v2\x10.ffs.index.IndexH\x00R\x05index\x12\x18\n" +
	"\aversion\x18\x0f \x01(\x04R\aversionB\a\n" +
	"\x05value\"l\n" +
	"\x04Root\x12\x19\n" +
	"\bfile_key\x18\x01 \x01(\fR\afileKey\x12 \n" +
	"\vdescription\x18\x02 \x01(\tR\vdescription\x12\x1b\n" +
	"\tindex_key\x18\x04 \x01(\fR\bindexKeyJ\x04\b\x03\x10\x04J\x04\b\x05\x10\x06\"\xa8\x01\n" +
	"\x04Node\x12%\n" +
	"\x05index\x18\x01 \x01(\v2\x0f.ffs.file.IndexR\x05index\x12\"\n" +
	"\x04stat\x18\x02 \x01(\v2\x0e.ffs.file.StatR\x04stat\x12(\n" +
	"\ax_attrs\x18\x03 \x03(\v2\x0f.ffs.file.XAttrR\x06xAttrs\x12+\n" +
	"\bchildren\x18\x04 \x03(\v2\x0f.ffs.file.ChildR\bchildren\"\x8f\x03\n" +
	"\x04Stat\x12 \n" +
	"\vpermissions\x18\x01 \x01(\rR\vpermissions\x124\n" +
	"\tfile_type\x18\x02 \x01(\x0e2\x17.ffs.file.Stat.FileTypeR\bfileType\x12.\n" +
	"\bmod_time\x18\x03 \x01(\v2\x13.ffs.file.TimestampR\amodTime\x12*\n" +
	"\x05owner\x18\x04 \x01(\v2\x14.ffs.file.Stat.IdentR\x05owner\x12*\n" +
	"\x05group\x18\x05 \x01(\v2\x14.ffs.file.Stat.IdentR\x05group\x1a+\n" +
	"\x05Ident\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x04R\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\"z\n" +
	"\bFileType\x12\v\n" +
	"\aREGULAR\x10\x00\x12\r\n" +
	"\tDIRECTORY\x10\x01\x12\v\n" +
	"\aSYMLINK\x10\x02\x12\n" +
	"\n" +
	"\x06SOCKET\x10\x03\x12\x0e\n" +
	"\n" +
	"NAMED_PIPE\x10\x04\x12\n" +
	"\n" +
	"\x06DEVICE\x10\x05\x12\x0f\n" +
	"\vCHAR_DEVICE\x10\x06\x12\f\n" +
	"\aUNKNOWN\x10\x94\x03\";\n" +
	"\tTimestamp\x12\x18\n" +
	"\aseconds\x18\x01 \x01(\x04R\aseconds\x12\x14\n" +
	"\x05nanos\x18\x02 \x01(\rR\x05nanos\"l\n" +
	"\x05Index\x12\x1f\n" +
	"\vtotal_bytes\x18\x01 \x01(\x04R\n" +
	"totalBytes\x12*\n" +
	"\aextents\x18\x02 \x03(\v2\x10.ffs.file.ExtentR\aextents\x12\x16\n" +
	"\x06single\x18\x03 \x01(\fR\x06single\"[\n" +
	"\x06Extent\x12\x12\n" +
	"\x04base\x18\x01 \x01(\x04R\x04base\x12\x14\n" +
	"\x05bytes\x18\x02 \x01(\x04R\x05bytes\x12'\n" +
	"\x06blocks\x18\x03 \x03(\v2\x0f.ffs.file.BlockR\x06blocks\"/\n" +
	"\x05Block\x12\x14\n" +
	"\x05bytes\x18\x01 \x01(\x04R\x05bytes\x12\x10\n" +
	"\x03key\x18\x02 \x01(\fR\x03key\"1\n" +
	"\x05XAttr\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12\x14\n" +
	"\x05value\x18\x02 \x01(\fR\x05value\"-\n" +
	"\x05Child\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12\x10\n" +
	"\x03key\x18\x02 \x01(\fR\x03keyB*Z(github.com/creachadair/ffs/file/wiretypeb\x06proto3"

var (
	file_wiretype_proto_rawDescOnce sync.Once
	file_wiretype_proto_rawDescData []byte
)

func file_wiretype_proto_rawDescGZIP() []byte {
	file_wiretype_proto_rawDescOnce.Do(func() {
		file_wiretype_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_wiretype_proto_rawDesc), len(file_wiretype_proto_rawDesc)))
	})
	return file_wiretype_proto_rawDescData
}

var file_wiretype_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_wiretype_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_wiretype_proto_goTypes = []any{
	(Stat_FileType)(0),    // 0: ffs.file.Stat.FileType
	(*Object)(nil),        // 1: ffs.file.Object
	(*Root)(nil),          // 2: ffs.file.Root
	(*Node)(nil),          // 3: ffs.file.Node
	(*Stat)(nil),          // 4: ffs.file.Stat
	(*Timestamp)(nil),     // 5: ffs.file.Timestamp
	(*Index)(nil),         // 6: ffs.file.Index
	(*Extent)(nil),        // 7: ffs.file.Extent
	(*Block)(nil),         // 8: ffs.file.Block
	(*XAttr)(nil),         // 9: ffs.file.XAttr
	(*Child)(nil),         // 10: ffs.file.Child
	(*Stat_Ident)(nil),    // 11: ffs.file.Stat.Ident
	(*indexpb.Index)(nil), // 12: ffs.index.Index
}
var file_wiretype_proto_depIdxs = []int32{
	3,  // 0: ffs.file.Object.node:type_name -> ffs.file.Node
	2,  // 1: ffs.file.Object.root:type_name -> ffs.file.Root
	12, // 2: ffs.file.Object.index:type_name -> ffs.index.Index
	6,  // 3: ffs.file.Node.index:type_name -> ffs.file.Index
	4,  // 4: ffs.file.Node.stat:type_name -> ffs.file.Stat
	9,  // 5: ffs.file.Node.x_attrs:type_name -> ffs.file.XAttr
	10, // 6: ffs.file.Node.children:type_name -> ffs.file.Child
	0,  // 7: ffs.file.Stat.file_type:type_name -> ffs.file.Stat.FileType
	5,  // 8: ffs.file.Stat.mod_time:type_name -> ffs.file.Timestamp
	11, // 9: ffs.file.Stat.owner:type_name -> ffs.file.Stat.Ident
	11, // 10: ffs.file.Stat.group:type_name -> ffs.file.Stat.Ident
	7,  // 11: ffs.file.Index.extents:type_name -> ffs.file.Extent
	8,  // 12: ffs.file.Extent.blocks:type_name -> ffs.file.Block
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

func init() { file_wiretype_proto_init() }
func file_wiretype_proto_init() {
	if File_wiretype_proto != nil {
		return
	}
	file_wiretype_proto_msgTypes[0].OneofWrappers = []any{
		(*Object_Node)(nil),
		(*Object_Root)(nil),
		(*Object_Index)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_wiretype_proto_rawDesc), len(file_wiretype_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_wiretype_proto_goTypes,
		DependencyIndexes: file_wiretype_proto_depIdxs,
		EnumInfos:         file_wiretype_proto_enumTypes,
		MessageInfos:      file_wiretype_proto_msgTypes,
	}.Build()
	File_wiretype_proto = out.File
	file_wiretype_proto_goTypes = nil
	file_wiretype_proto_depIdxs = nil
}
