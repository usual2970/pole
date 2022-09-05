// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.7.1
// source: internal/pb/pole.proto

package pb

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

type LockRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockUri string `protobuf:"bytes,1,opt,name=lockUri,proto3" json:"lockUri,omitempty"`
}

func (x *LockRequest) Reset() {
	*x = LockRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockRequest) ProtoMessage() {}

func (x *LockRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockRequest.ProtoReflect.Descriptor instead.
func (*LockRequest) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{0}
}

func (x *LockRequest) GetLockUri() string {
	if x != nil {
		return x.LockUri
	}
	return ""
}

type LockResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    int32  `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *LockResponse) Reset() {
	*x = LockResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LockResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LockResponse) ProtoMessage() {}

func (x *LockResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LockResponse.ProtoReflect.Descriptor instead.
func (*LockResponse) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{1}
}

func (x *LockResponse) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *LockResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type UnlockRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LockUri string `protobuf:"bytes,1,opt,name=lockUri,proto3" json:"lockUri,omitempty"`
}

func (x *UnlockRequest) Reset() {
	*x = UnlockRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UnlockRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnlockRequest) ProtoMessage() {}

func (x *UnlockRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnlockRequest.ProtoReflect.Descriptor instead.
func (*UnlockRequest) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{2}
}

func (x *UnlockRequest) GetLockUri() string {
	if x != nil {
		return x.LockUri
	}
	return ""
}

type UnlockResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    int32  `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *UnlockResponse) Reset() {
	*x = UnlockResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UnlockResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UnlockResponse) ProtoMessage() {}

func (x *UnlockResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UnlockResponse.ProtoReflect.Descriptor instead.
func (*UnlockResponse) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{3}
}

func (x *UnlockResponse) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *UnlockResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type ExecRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Sql string `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
}

func (x *ExecRequest) Reset() {
	*x = ExecRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecRequest) ProtoMessage() {}

func (x *ExecRequest) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecRequest.ProtoReflect.Descriptor instead.
func (*ExecRequest) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{4}
}

func (x *ExecRequest) GetSql() string {
	if x != nil {
		return x.Sql
	}
	return ""
}

type ExecResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    int32  `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *ExecResponse) Reset() {
	*x = ExecResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_pb_pole_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecResponse) ProtoMessage() {}

func (x *ExecResponse) ProtoReflect() protoreflect.Message {
	mi := &file_internal_pb_pole_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecResponse.ProtoReflect.Descriptor instead.
func (*ExecResponse) Descriptor() ([]byte, []int) {
	return file_internal_pb_pole_proto_rawDescGZIP(), []int{5}
}

func (x *ExecResponse) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *ExecResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_internal_pb_pole_proto protoreflect.FileDescriptor

var file_internal_pb_pole_proto_rawDesc = []byte{
	0x0a, 0x16, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x62, 0x2f, 0x70, 0x6f,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x27, 0x0a, 0x0b, 0x4c, 0x6f, 0x63, 0x6b,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x55,
	0x72, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x55, 0x72,
	0x69, 0x22, 0x3c, 0x0a, 0x0c, 0x4c, 0x6f, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22,
	0x29, 0x0a, 0x0d, 0x55, 0x6e, 0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x18, 0x0a, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x55, 0x72, 0x69, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x07, 0x6c, 0x6f, 0x63, 0x6b, 0x55, 0x72, 0x69, 0x22, 0x3e, 0x0a, 0x0e, 0x55, 0x6e,
	0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04,
	0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65,
	0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x1f, 0x0a, 0x0b, 0x45, 0x78,
	0x65, 0x63, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x73, 0x71, 0x6c,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x73, 0x71, 0x6c, 0x22, 0x3c, 0x0a, 0x0c, 0x45,
	0x78, 0x65, 0x63, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x63,
	0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12,
	0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x32, 0x81, 0x01, 0x0a, 0x04, 0x50, 0x6f,
	0x6c, 0x65, 0x12, 0x25, 0x0a, 0x04, 0x45, 0x78, 0x65, 0x63, 0x12, 0x0c, 0x2e, 0x45, 0x78, 0x65,
	0x63, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0d, 0x2e, 0x45, 0x78, 0x65, 0x63, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x25, 0x0a, 0x04, 0x4c, 0x6f, 0x63,
	0x6b, 0x12, 0x0c, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x0d, 0x2e, 0x4c, 0x6f, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x12, 0x2b, 0x0a, 0x06, 0x55, 0x6e, 0x6c, 0x6f, 0x63, 0x6b, 0x12, 0x0e, 0x2e, 0x55, 0x6e, 0x6c,
	0x6f, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0f, 0x2e, 0x55, 0x6e, 0x6c,
	0x6f, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x0d, 0x5a,
	0x0b, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_pb_pole_proto_rawDescOnce sync.Once
	file_internal_pb_pole_proto_rawDescData = file_internal_pb_pole_proto_rawDesc
)

func file_internal_pb_pole_proto_rawDescGZIP() []byte {
	file_internal_pb_pole_proto_rawDescOnce.Do(func() {
		file_internal_pb_pole_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_pb_pole_proto_rawDescData)
	})
	return file_internal_pb_pole_proto_rawDescData
}

var file_internal_pb_pole_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_internal_pb_pole_proto_goTypes = []interface{}{
	(*LockRequest)(nil),    // 0: LockRequest
	(*LockResponse)(nil),   // 1: LockResponse
	(*UnlockRequest)(nil),  // 2: UnlockRequest
	(*UnlockResponse)(nil), // 3: UnlockResponse
	(*ExecRequest)(nil),    // 4: ExecRequest
	(*ExecResponse)(nil),   // 5: ExecResponse
}
var file_internal_pb_pole_proto_depIdxs = []int32{
	4, // 0: Pole.Exec:input_type -> ExecRequest
	0, // 1: Pole.Lock:input_type -> LockRequest
	2, // 2: Pole.Unlock:input_type -> UnlockRequest
	5, // 3: Pole.Exec:output_type -> ExecResponse
	1, // 4: Pole.Lock:output_type -> LockResponse
	3, // 5: Pole.Unlock:output_type -> UnlockResponse
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_internal_pb_pole_proto_init() }
func file_internal_pb_pole_proto_init() {
	if File_internal_pb_pole_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_pb_pole_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockRequest); i {
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
		file_internal_pb_pole_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LockResponse); i {
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
		file_internal_pb_pole_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UnlockRequest); i {
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
		file_internal_pb_pole_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UnlockResponse); i {
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
		file_internal_pb_pole_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecRequest); i {
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
		file_internal_pb_pole_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecResponse); i {
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
			RawDescriptor: file_internal_pb_pole_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_internal_pb_pole_proto_goTypes,
		DependencyIndexes: file_internal_pb_pole_proto_depIdxs,
		MessageInfos:      file_internal_pb_pole_proto_msgTypes,
	}.Build()
	File_internal_pb_pole_proto = out.File
	file_internal_pb_pole_proto_rawDesc = nil
	file_internal_pb_pole_proto_goTypes = nil
	file_internal_pb_pole_proto_depIdxs = nil
}
