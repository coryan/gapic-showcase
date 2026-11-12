// Copyright 2026 Google LLC
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

package datastore

import (
	"fmt"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReadSpecToPB converts a fixture ReadSpec into datastorepb.ReadOptions.
func ReadSpecToPB(rs *fixturepb.ReadSpec) *pb.ReadOptions {
	if rs == nil {
		return nil
	}
	if rs.ReadTime != nil {
		return &pb.ReadOptions{
			ConsistencyType: &pb.ReadOptions_ReadTime{
				ReadTime: timestamppb.New(rs.ReadTime.AsTime()),
			},
		}
	}
	switch rs.ReadConsistency {
	case fixturepb.ReadConsistencySpec_READ_CONSISTENCY_STRONG:
		return &pb.ReadOptions{
			ConsistencyType: &pb.ReadOptions_ReadConsistency_{ReadConsistency: pb.ReadOptions_STRONG},
		}
	case fixturepb.ReadConsistencySpec_READ_CONSISTENCY_EVENTUAL:
		return &pb.ReadOptions{
			ConsistencyType: &pb.ReadOptions_ReadConsistency_{ReadConsistency: pb.ReadOptions_EVENTUAL},
		}
	default:
		return nil
	}
}

// PropertyMaskToPB converts a fixture PropertyMaskSpec into datastorepb.PropertyMask.
func PropertyMaskToPB(pm *fixturepb.PropertyMaskSpec) *pb.PropertyMask {
	if pm == nil {
		return nil
	}
	paths := make([]string, len(pm.Paths))
	copy(paths, pm.Paths)
	return &pb.PropertyMask{Paths: paths}
}

// PropertyTransformToPB converts a fixture PropertyTransformSpec into datastorepb.PropertyTransform.
func PropertyTransformToPB(t *fixturepb.PropertyTransformSpec) *pb.PropertyTransform {
	if t == nil {
		return nil
	}
	out := &pb.PropertyTransform{Property: t.Property}
	switch tt := t.TransformType.(type) {
	case *fixturepb.PropertyTransformSpec_SetToServerValue:
		if tt.SetToServerValue == fixturepb.ServerValueSpec_SERVER_VALUE_REQUEST_TIME {
			out.TransformType = &pb.PropertyTransform_SetToServerValue{
				SetToServerValue: pb.PropertyTransform_REQUEST_TIME,
			}
		}
	case *fixturepb.PropertyTransformSpec_Increment:
		out.TransformType = &pb.PropertyTransform_Increment{Increment: ValueToPB(tt.Increment)}
	case *fixturepb.PropertyTransformSpec_Maximum:
		out.TransformType = &pb.PropertyTransform_Maximum{Maximum: ValueToPB(tt.Maximum)}
	case *fixturepb.PropertyTransformSpec_Minimum:
		out.TransformType = &pb.PropertyTransform_Minimum{Minimum: ValueToPB(tt.Minimum)}
	case *fixturepb.PropertyTransformSpec_AppendMissingElements:
		var vals []*pb.Value
		if tt.AppendMissingElements != nil {
			for _, v := range tt.AppendMissingElements.Values {
				vals = append(vals, ValueToPB(v))
			}
		}
		out.TransformType = &pb.PropertyTransform_AppendMissingElements{
			AppendMissingElements: &pb.ArrayValue{Values: vals},
		}
	case *fixturepb.PropertyTransformSpec_RemoveAllFromArray:
		var vals []*pb.Value
		if tt.RemoveAllFromArray != nil {
			for _, v := range tt.RemoveAllFromArray.Values {
				vals = append(vals, ValueToPB(v))
			}
		}
		out.TransformType = &pb.PropertyTransform_RemoveAllFromArray{
			RemoveAllFromArray: &pb.ArrayValue{Values: vals},
		}
	}
	return out
}

// MutationSpecToPB converts a fixture MutationSpec into datastorepb.Mutation.
func MutationSpecToPB(m *fixturepb.MutationSpec) *pb.Mutation {
	if m == nil {
		return nil
	}
	out := &pb.Mutation{
		PropertyMask: PropertyMaskToPB(m.PropertyMask),
	}
	switch m.Op {
	case fixturepb.MutationOp_MUTATION_OP_INSERT:
		out.Operation = &pb.Mutation_Insert{Insert: EntityToPB(m.Entity)}
	case fixturepb.MutationOp_MUTATION_OP_UPSERT:
		out.Operation = &pb.Mutation_Upsert{Upsert: EntityToPB(m.Entity)}
	case fixturepb.MutationOp_MUTATION_OP_UPDATE:
		out.Operation = &pb.Mutation_Update{Update: EntityToPB(m.Entity)}
	case fixturepb.MutationOp_MUTATION_OP_DELETE:
		dk := m.DeleteKey
		if dk == nil && m.Entity != nil {
			dk = m.Entity.Key
		}
		out.Operation = &pb.Mutation_Delete{Delete: KeyToPB(dk)}
	}
	for _, tr := range m.Transforms {
		out.PropertyTransforms = append(out.PropertyTransforms, PropertyTransformToPB(tr))
	}
	if m.BaseVersion != nil {
		out.ConflictDetectionStrategy = &pb.Mutation_BaseVersion{BaseVersion: m.GetBaseVersion()}
	} else if m.UpdateTime != nil {
		out.ConflictDetectionStrategy = &pb.Mutation_UpdateTime{UpdateTime: timestamppb.New(m.UpdateTime.AsTime())}
	}
	return out
}

// RpcCodePtr returns a pointer to the given RpcCode enum value.
func RpcCodePtr(c fixturepb.RpcCode) *fixturepb.RpcCode {
	return &c
}

// GRPCCodeToRpcCode converts a gRPC codes.Code to fixturepb.RpcCode.
func GRPCCodeToRpcCode(c codes.Code) fixturepb.RpcCode {
	return fixturepb.RpcCode(c)
}

// RpcCodeToGRPC converts a fixturepb.RpcCode to gRPC codes.Code.
func RpcCodeToGRPC(c fixturepb.RpcCode) codes.Code {
	return codes.Code(c)
}

// ExtractErrorInfo extracts the first errdetails.ErrorInfo from a gRPC status error.
func ExtractErrorInfo(err error) *fixturepb.ErrorInfoSpec {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			md := make(map[string]string, len(ei.Metadata))
			for k, v := range ei.Metadata {
				md[k] = v
			}
			return &fixturepb.ErrorInfoSpec{
				Reason:   ei.Reason,
				Domain:   ei.Domain,
				Metadata: md,
			}
		}
	}
	return nil
}

// FormatVeneerError formats a VeneerError concisely for diagnostics.
func FormatVeneerError(e *fixturepb.VeneerError) string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("kind=%s rpc_code=%v http_status=%v msg=%q", e.Kind, e.RpcCode, e.HttpStatus, e.Message)
}
