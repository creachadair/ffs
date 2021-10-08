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
// 	protoc-gen-go v1.27.1
// 	protoc        v3.17.3
// source: index.proto

package indexpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
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

// An Indexis the storage encoding of a Bloom filter index.
type Index struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NumKeys  uint64         `protobuf:"varint,1,opt,name=num_keys,json=numKeys,proto3" json:"num_keys,omitempty"`                                  // total number of keys indexed
	Seeds    []uint64       `protobuf:"varint,2,rep,packed,name=seeds,proto3" json:"seeds,omitempty"`                                              // hash seeds used for lookup
	Segments []uint64       `protobuf:"varint,3,rep,packed,name=segments,proto3" json:"segments,omitempty"`                                        // index vector segments
	HashFunc Index_HashFunc `protobuf:"varint,4,opt,name=hash_func,json=hashFunc,proto3,enum=ffs.index.Index_HashFunc" json:"hash_func,omitempty"` // which hash function was used
}

func (x *Index) Reset() {
	*x = Index{}
	if protoimpl.UnsafeEnabled {
		mi := &file_index_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Index) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Index) ProtoMessage() {}

func (x *Index) ProtoReflect() protoreflect.Message {
	mi := &file_index_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
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

func (x *Index) GetSegments() []uint64 {
	if x != nil {
		return x.Segments
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

var file_index_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x09, 0x66,
	0x66, 0x73, 0x2e, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x22, 0xa5, 0x01, 0x0a, 0x05, 0x49, 0x6e, 0x64,
	0x65, 0x78, 0x12, 0x19, 0x0a, 0x08, 0x6e, 0x75, 0x6d, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x6e, 0x75, 0x6d, 0x4b, 0x65, 0x79, 0x73, 0x12, 0x14, 0x0a,
	0x05, 0x73, 0x65, 0x65, 0x64, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x04, 0x52, 0x05, 0x73, 0x65,
	0x65, 0x64, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x18,
	0x03, 0x20, 0x03, 0x28, 0x04, 0x52, 0x08, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x12,
	0x36, 0x0a, 0x09, 0x68, 0x61, 0x73, 0x68, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x19, 0x2e, 0x66, 0x66, 0x73, 0x2e, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x49,
	0x6e, 0x64, 0x65, 0x78, 0x2e, 0x48, 0x61, 0x73, 0x68, 0x46, 0x75, 0x6e, 0x63, 0x52, 0x08, 0x68,
	0x61, 0x73, 0x68, 0x46, 0x75, 0x6e, 0x63, 0x22, 0x17, 0x0a, 0x08, 0x48, 0x61, 0x73, 0x68, 0x46,
	0x75, 0x6e, 0x63, 0x12, 0x0b, 0x0a, 0x07, 0x44, 0x45, 0x46, 0x41, 0x55, 0x4c, 0x54, 0x10, 0x00,
	0x42, 0x2a, 0x5a, 0x28, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63,
	0x72, 0x65, 0x61, 0x63, 0x68, 0x61, 0x64, 0x61, 0x69, 0x72, 0x2f, 0x66, 0x66, 0x73, 0x2f, 0x69,
	0x6e, 0x64, 0x65, 0x78, 0x2f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_index_proto_rawDescOnce sync.Once
	file_index_proto_rawDescData = file_index_proto_rawDesc
)

func file_index_proto_rawDescGZIP() []byte {
	file_index_proto_rawDescOnce.Do(func() {
		file_index_proto_rawDescData = protoimpl.X.CompressGZIP(file_index_proto_rawDescData)
	})
	return file_index_proto_rawDescData
}

var file_index_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_index_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_index_proto_goTypes = []interface{}{
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
	if !protoimpl.UnsafeEnabled {
		file_index_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Index); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_index_proto_rawDesc,
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
	file_index_proto_rawDesc = nil
	file_index_proto_goTypes = nil
	file_index_proto_depIdxs = nil
}
