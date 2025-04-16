// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v3.20.3
// source: google/showcase/v1beta1/rest_error.proto

package genproto

import (
	code "google.golang.org/genproto/googleapis/rpc/code"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// HTTP/JSON error representation as defined in
// https://google.aip.dev/193#http11json-representation,
type RestError struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Error *RestError_Status `protobuf:"bytes,1,opt,name=error,proto3" json:"error,omitempty"`
}

func (x *RestError) Reset() {
	*x = RestError{}
	if protoimpl.UnsafeEnabled {
		mi := &file_google_showcase_v1beta1_rest_error_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RestError) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RestError) ProtoMessage() {}

func (x *RestError) ProtoReflect() protoreflect.Message {
	mi := &file_google_showcase_v1beta1_rest_error_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RestError.ProtoReflect.Descriptor instead.
func (*RestError) Descriptor() ([]byte, []int) {
	return file_google_showcase_v1beta1_rest_error_proto_rawDescGZIP(), []int{0}
}

func (x *RestError) GetError() *RestError_Status {
	if x != nil {
		return x.Error
	}
	return nil
}

type RestError_Status struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The HTTP status code that corresponds to `google.rpc.Status.code`.
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	// This corresponds to `google.rpc.Status.message`.
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	// This is the enum version for `google.rpc.Status.code`.
	Status code.Code `protobuf:"varint,4,opt,name=status,proto3,enum=google.rpc.Code" json:"status,omitempty"`
	// This corresponds to `google.rpc.Status.details`.
	Details []*anypb.Any `protobuf:"bytes,5,rep,name=details,proto3" json:"details,omitempty"`
}

func (x *RestError_Status) Reset() {
	*x = RestError_Status{}
	if protoimpl.UnsafeEnabled {
		mi := &file_google_showcase_v1beta1_rest_error_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RestError_Status) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RestError_Status) ProtoMessage() {}

func (x *RestError_Status) ProtoReflect() protoreflect.Message {
	mi := &file_google_showcase_v1beta1_rest_error_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RestError_Status.ProtoReflect.Descriptor instead.
func (*RestError_Status) Descriptor() ([]byte, []int) {
	return file_google_showcase_v1beta1_rest_error_proto_rawDescGZIP(), []int{0, 0}
}

func (x *RestError_Status) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *RestError_Status) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *RestError_Status) GetStatus() code.Code {
	if x != nil {
		return x.Status
	}
	return code.Code(0)
}

func (x *RestError_Status) GetDetails() []*anypb.Any {
	if x != nil {
		return x.Details
	}
	return nil
}

var File_google_showcase_v1beta1_rest_error_proto protoreflect.FileDescriptor

var file_google_showcase_v1beta1_rest_error_proto_rawDesc = []byte{
	0x0a, 0x28, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x73, 0x68, 0x6f, 0x77, 0x63, 0x61, 0x73,
	0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x72, 0x65, 0x73, 0x74, 0x5f, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x17, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x73, 0x68, 0x6f, 0x77, 0x63, 0x61, 0x73, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x15,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x63, 0x6f, 0x64, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xdf, 0x01, 0x0a, 0x09, 0x52, 0x65, 0x73, 0x74, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x12, 0x3f, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x29, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x73, 0x68, 0x6f, 0x77,
	0x63, 0x61, 0x73, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x73,
	0x74, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x05, 0x65,
	0x72, 0x72, 0x6f, 0x72, 0x1a, 0x90, 0x01, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12,
	0x12, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x63,
	0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x28, 0x0a,
	0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x10, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6f, 0x64, 0x65, 0x52,
	0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x2e, 0x0a, 0x07, 0x64, 0x65, 0x74, 0x61, 0x69,
	0x6c, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x07,
	0x64, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x42, 0x71, 0x0a, 0x1b, 0x63, 0x6f, 0x6d, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x73, 0x68, 0x6f, 0x77, 0x63, 0x61, 0x73, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x50, 0x01, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x61, 0x70, 0x69, 0x73, 0x2f,
	0x67, 0x61, 0x70, 0x69, 0x63, 0x2d, 0x73, 0x68, 0x6f, 0x77, 0x63, 0x61, 0x73, 0x65, 0x2f, 0x73,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x67, 0x65, 0x6e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0xea, 0x02,
	0x19, 0x47, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x3a, 0x3a, 0x53, 0x68, 0x6f, 0x77, 0x63, 0x61, 0x73,
	0x65, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_google_showcase_v1beta1_rest_error_proto_rawDescOnce sync.Once
	file_google_showcase_v1beta1_rest_error_proto_rawDescData = file_google_showcase_v1beta1_rest_error_proto_rawDesc
)

func file_google_showcase_v1beta1_rest_error_proto_rawDescGZIP() []byte {
	file_google_showcase_v1beta1_rest_error_proto_rawDescOnce.Do(func() {
		file_google_showcase_v1beta1_rest_error_proto_rawDescData = protoimpl.X.CompressGZIP(file_google_showcase_v1beta1_rest_error_proto_rawDescData)
	})
	return file_google_showcase_v1beta1_rest_error_proto_rawDescData
}

var file_google_showcase_v1beta1_rest_error_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_google_showcase_v1beta1_rest_error_proto_goTypes = []interface{}{
	(*RestError)(nil),        // 0: google.showcase.v1beta1.RestError
	(*RestError_Status)(nil), // 1: google.showcase.v1beta1.RestError.Status
	(code.Code)(0),           // 2: google.rpc.Code
	(*anypb.Any)(nil),        // 3: google.protobuf.Any
}
var file_google_showcase_v1beta1_rest_error_proto_depIdxs = []int32{
	1, // 0: google.showcase.v1beta1.RestError.error:type_name -> google.showcase.v1beta1.RestError.Status
	2, // 1: google.showcase.v1beta1.RestError.Status.status:type_name -> google.rpc.Code
	3, // 2: google.showcase.v1beta1.RestError.Status.details:type_name -> google.protobuf.Any
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_google_showcase_v1beta1_rest_error_proto_init() }
func file_google_showcase_v1beta1_rest_error_proto_init() {
	if File_google_showcase_v1beta1_rest_error_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_google_showcase_v1beta1_rest_error_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RestError); i {
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
		file_google_showcase_v1beta1_rest_error_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RestError_Status); i {
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
			RawDescriptor: file_google_showcase_v1beta1_rest_error_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_google_showcase_v1beta1_rest_error_proto_goTypes,
		DependencyIndexes: file_google_showcase_v1beta1_rest_error_proto_depIdxs,
		MessageInfos:      file_google_showcase_v1beta1_rest_error_proto_msgTypes,
	}.Build()
	File_google_showcase_v1beta1_rest_error_proto = out.File
	file_google_showcase_v1beta1_rest_error_proto_rawDesc = nil
	file_google_showcase_v1beta1_rest_error_proto_goTypes = nil
	file_google_showcase_v1beta1_rest_error_proto_depIdxs = nil
}
