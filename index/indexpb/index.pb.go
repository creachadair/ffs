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

// This schema defines a storage encoding for a Bloom filter index.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v5.29.3
// source: index.proto

package indexpb

import (
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

type Index_HashFunc int32

const (
	Index_DEFAULT Index_HashFunc = 0
)

// Enum value maps for Index_HashFunc.
var (
	Index_HashFunc_name = map[int32]string{
		0: "DEFAULT",
	}
	Index_HashFunc_value = map[string]int32{
		"DEFAULT": 0,
	}
)

func (x Index_HashFunc) Enum() *Index_HashFunc {
	p := new(Index_HashFunc)
	*p = x
	return p
}

func (x Index_HashFunc) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Index_HashFunc) Descriptor() protoreflect.EnumDescriptor {
	return file_index_proto_enumTypes[0].Descriptor()
}

func (Index_HashFunc) Type() protoreflect.EnumType {
	return &file_index_proto_enumTypes[0]
}

func (x Index_HashFunc) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Index_HashFunc.Descriptor instead.
func (Index_HashFunc) EnumDescriptor() ([]byte, []int) {
	return file_index_proto_rawDescGZIP(), []int{0, 0}
}

// An Index is the storage encoding of a Bloom filter index.
type Index struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	NumKeys       uint64                 `protobuf:"varint,1,opt,name=num_keys,json=numKeys,proto3" json:"num_keys,omitempty"`                                  // total number of keys indexed
	Seeds         []uint64               `protobuf:"varint,2,rep,packed,name=seeds,proto3" json:"seeds,omitempty"`                                              // hash seeds used for lookup
	NumSegments   uint64                 `protobuf:"varint,5,opt,name=num_segments,json=numSegments,proto3" json:"num_segments,omitempty"`                      // number of bit vector segments
	SegmentData   []byte                 `protobuf:"bytes,6,opt,name=segment_data,json=segmentData,proto3" json:"segment_data,omitempty"`                       // zlib-compressed segment data
	HashFunc      Index_HashFunc         `protobuf:"varint,4,opt,name=hash_func,json=hashFunc,proto3,enum=ffs.index.Index_HashFunc" json:"hash_func,omitempty"` // which hash function was used
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Index) Reset() {
	*x = Index{}
	mi := &file_index_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Index) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Index) ProtoMessage() {}

func (x *Index) ProtoReflect() protoreflect.Message {
	mi := &file_index_proto_msgTypes[0]
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
	return file_index_proto_rawDescGZIP(), []int{0}
}

func (x *Index) GetNumKeys() uint64 {
	if x != nil {
		return x.NumKeys
	}
	return 0
}

func (x *Index) GetSeeds() []uint64 {
	if x != nil {
		return x.Seeds
	}
	return nil
}

func (x *Index) GetNumSegments() uint64 {
	if x != nil {
		return x.NumSegments
	}
	return 0
}

func (x *Index) GetSegmentData() []byte {
	if x != nil {
		return x.SegmentData
	}
	return nil
}

func (x *Index) GetHashFunc() Index_HashFunc {
	if x != nil {
		return x.HashFunc
	}
	return Index_DEFAULT
}

var File_index_proto protoreflect.FileDescriptor

const file_index_proto_rawDesc = "" +
	"\n" +
	"\vindex.proto\x12\tffs.index\"\xd5\x01\n" +
	"\x05Index\x12\x19\n" +
	"\bnum_keys\x18\x01 \x01(\x04R\anumKeys\x12\x14\n" +
	"\x05seeds\x18\x02 \x03(\x04R\x05seeds\x12!\n" +
	"\fnum_segments\x18\x05 \x01(\x04R\vnumSegments\x12!\n" +
	"\fsegment_data\x18\x06 \x01(\fR\vsegmentData\x126\n" +
	"\thash_func\x18\x04 \x01(\x0e2\x19.ffs.index.Index.HashFuncR\bhashFunc\"\x17\n" +
	"\bHashFunc\x12\v\n" +
	"\aDEFAULT\x10\x00J\x04\b\x03\x10\x04B*Z(github.com/creachadair/ffs/index/indexpbb\x06proto3"

var (
	file_index_proto_rawDescOnce sync.Once
	file_index_proto_rawDescData []byte
)

func file_index_proto_rawDescGZIP() []byte {
	file_index_proto_rawDescOnce.Do(func() {
		file_index_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_index_proto_rawDesc), len(file_index_proto_rawDesc)))
	})
	return file_index_proto_rawDescData
}

var file_index_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_index_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_index_proto_goTypes = []any{
	(Index_HashFunc)(0), // 0: ffs.index.Index.HashFunc
	(*Index)(nil),       // 1: ffs.index.Index
}
var file_index_proto_depIdxs = []int32{
	0, // 0: ffs.index.Index.hash_func:type_name -> ffs.index.Index.HashFunc
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_index_proto_init() }
func file_index_proto_init() {
	if File_index_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_index_proto_rawDesc), len(file_index_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_index_proto_goTypes,
		DependencyIndexes: file_index_proto_depIdxs,
		EnumInfos:         file_index_proto_enumTypes,
		MessageInfos:      file_index_proto_msgTypes,
	}.Build()
	File_index_proto = out.File
	file_index_proto_goTypes = nil
	file_index_proto_depIdxs = nil
}
