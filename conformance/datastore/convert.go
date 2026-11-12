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
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// KeyToPB converts a fixture KeySpec into a wire datastorepb.Key.
func KeyToPB(k *fixturepb.KeySpec) *pb.Key {
	if k == nil {
		return nil
	}
	out := &pb.Key{
		Path: make([]*pb.Key_PathElement, 0, len(k.Path)),
	}
	if p := k.PartitionId; p != nil && (p.ProjectId != "" || p.DatabaseId != "" || p.NamespaceId != "") {
		out.PartitionId = &pb.PartitionId{
			ProjectId:   p.ProjectId,
			DatabaseId:  p.DatabaseId,
			NamespaceId: p.NamespaceId,
		}
	}
	for _, el := range k.Path {
		pe := &pb.Key_PathElement{Kind: el.Kind}
		switch id := el.IdType.(type) {
		case *fixturepb.PathElementSpec_Id:
			pe.IdType = &pb.Key_PathElement_Id{Id: id.Id}
		case *fixturepb.PathElementSpec_Name:
			pe.IdType = &pb.Key_PathElement_Name{Name: id.Name}
		}
		out.Path = append(out.Path, pe)
	}
	return out
}

// KeyFromPB converts a wire datastorepb.Key into a fixture KeySpec.
func KeyFromPB(k *pb.Key) *fixturepb.KeySpec {
	if k == nil {
		return nil
	}
	out := &fixturepb.KeySpec{
		Path: make([]*fixturepb.PathElementSpec, 0, len(k.Path)),
	}
	if p := k.PartitionId; p != nil && (p.ProjectId != "" || p.DatabaseId != "" || p.NamespaceId != "") {
		out.PartitionId = &fixturepb.PartitionSpec{
			ProjectId:   p.ProjectId,
			DatabaseId:  p.DatabaseId,
			NamespaceId: p.NamespaceId,
		}
	}
	for _, el := range k.Path {
		pe := &fixturepb.PathElementSpec{Kind: el.Kind}
		switch id := el.IdType.(type) {
		case *pb.Key_PathElement_Id:
			pe.IdType = &fixturepb.PathElementSpec_Id{Id: id.Id}
		case *pb.Key_PathElement_Name:
			pe.IdType = &fixturepb.PathElementSpec_Name{Name: id.Name}
		}
		out.Path = append(out.Path, pe)
	}
	return out
}

// ValueToPB converts a fixture ValueSpec to a wire datastorepb.Value,
// encoding vector_value using the Datastore __vector__ entity representation
// and enforcing wire-spec array exclude_from_indexes placement.
func ValueToPB(v *fixturepb.ValueSpec) *pb.Value {
	if v == nil {
		return nil
	}
	out := &pb.Value{
		ExcludeFromIndexes: v.ExcludeFromIndexes,
		Meaning:            v.Meaning,
	}
	switch vt := v.ValueType.(type) {
	case *fixturepb.ValueSpec_IsNull:
		out.ValueType = &pb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}
	case *fixturepb.ValueSpec_BooleanValue:
		out.ValueType = &pb.Value_BooleanValue{BooleanValue: vt.BooleanValue}
	case *fixturepb.ValueSpec_IntegerValue:
		out.ValueType = &pb.Value_IntegerValue{IntegerValue: vt.IntegerValue}
	case *fixturepb.ValueSpec_DoubleValue:
		out.ValueType = &pb.Value_DoubleValue{DoubleValue: vt.DoubleValue}
	case *fixturepb.ValueSpec_TimestampValue:
		if vt.TimestampValue != nil {
			out.ValueType = &pb.Value_TimestampValue{TimestampValue: timestamppb.New(vt.TimestampValue.AsTime())}
		}
	case *fixturepb.ValueSpec_KeyValue:
		out.ValueType = &pb.Value_KeyValue{KeyValue: KeyToPB(vt.KeyValue)}
	case *fixturepb.ValueSpec_StringValue:
		out.ValueType = &pb.Value_StringValue{StringValue: vt.StringValue}
	case *fixturepb.ValueSpec_BlobValue:
		b := make([]byte, len(vt.BlobValue))
		copy(b, vt.BlobValue)
		out.ValueType = &pb.Value_BlobValue{BlobValue: b}
	case *fixturepb.ValueSpec_GeoPointValue:
		if vt.GeoPointValue != nil {
			out.ValueType = &pb.Value_GeoPointValue{
				GeoPointValue: &latlng.LatLng{
					Latitude:  vt.GeoPointValue.Latitude,
					Longitude: vt.GeoPointValue.Longitude,
				},
			}
		}
	case *fixturepb.ValueSpec_EntityValue:
		out.ValueType = &pb.Value_EntityValue{EntityValue: EntityToPB(vt.EntityValue)}
	case *fixturepb.ValueSpec_ArrayValue:
		// Per Datastore wire protocol, ArrayValue.exclude_from_indexes is false
		// and each array element carries the flag.
		out.ExcludeFromIndexes = false
		var vals []*pb.Value
		if vt.ArrayValue != nil {
			vals = make([]*pb.Value, 0, len(vt.ArrayValue.Values))
			for _, elem := range vt.ArrayValue.Values {
				pbElem := ValueToPB(elem)
				if v.ExcludeFromIndexes && pbElem != nil {
					pbElem.ExcludeFromIndexes = true
				}
				vals = append(vals, pbElem)
			}
		}
		out.ValueType = &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vals}}
	case *fixturepb.ValueSpec_VectorValue:
		out.ValueType = &pb.Value_EntityValue{EntityValue: vectorEntityToPB(vt.VectorValue, v.ExcludeFromIndexes)}
	}
	return out
}

func vectorEntityToPB(vec *fixturepb.VectorSpec, excludeFromIndexes bool) *pb.Entity {
	var vals []*pb.Value
	if vec != nil {
		vals = make([]*pb.Value, 0, len(vec.Elements))
		for _, el := range vec.Elements {
			vals = append(vals, &pb.Value{
				ExcludeFromIndexes: excludeFromIndexes,
				ValueType:          &pb.Value_DoubleValue{DoubleValue: el},
			})
		}
	}
	return &pb.Entity{
		Properties: map[string]*pb.Value{
			"__type__": {ValueType: &pb.Value_StringValue{StringValue: "__vector__"}},
			"value":    {ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vals}}},
		},
	}
}

// ValueFromPB converts a wire datastorepb.Value into a fixture ValueSpec,
// decoding __vector__ entities into vector_value.
func ValueFromPB(v *pb.Value) *fixturepb.ValueSpec {
	if v == nil {
		return nil
	}
	out := &fixturepb.ValueSpec{
		ExcludeFromIndexes: v.ExcludeFromIndexes,
		Meaning:            v.Meaning,
	}
	switch vt := v.ValueType.(type) {
	case *pb.Value_NullValue:
		out.ValueType = &fixturepb.ValueSpec_IsNull{IsNull: true}
	case *pb.Value_BooleanValue:
		out.ValueType = &fixturepb.ValueSpec_BooleanValue{BooleanValue: vt.BooleanValue}
	case *pb.Value_IntegerValue:
		out.ValueType = &fixturepb.ValueSpec_IntegerValue{IntegerValue: vt.IntegerValue}
	case *pb.Value_DoubleValue:
		out.ValueType = &fixturepb.ValueSpec_DoubleValue{DoubleValue: vt.DoubleValue}
	case *pb.Value_TimestampValue:
		if vt.TimestampValue != nil {
			out.ValueType = &fixturepb.ValueSpec_TimestampValue{TimestampValue: timestamppb.New(vt.TimestampValue.AsTime())}
		}
	case *pb.Value_KeyValue:
		out.ValueType = &fixturepb.ValueSpec_KeyValue{KeyValue: KeyFromPB(vt.KeyValue)}
	case *pb.Value_StringValue:
		out.ValueType = &fixturepb.ValueSpec_StringValue{StringValue: vt.StringValue}
	case *pb.Value_BlobValue:
		b := make([]byte, len(vt.BlobValue))
		copy(b, vt.BlobValue)
		out.ValueType = &fixturepb.ValueSpec_BlobValue{BlobValue: b}
	case *pb.Value_GeoPointValue:
		if vt.GeoPointValue != nil {
			out.ValueType = &fixturepb.ValueSpec_GeoPointValue{
				GeoPointValue: &fixturepb.LatLngSpec{
					Latitude:  vt.GeoPointValue.Latitude,
					Longitude: vt.GeoPointValue.Longitude,
				},
			}
		}
	case *pb.Value_EntityValue:
		if vec, ok := vectorFromEntityPB(vt.EntityValue); ok {
			out.ValueType = &fixturepb.ValueSpec_VectorValue{VectorValue: vec}
			break
		}
		out.ValueType = &fixturepb.ValueSpec_EntityValue{EntityValue: EntityFromPB(vt.EntityValue)}
	case *pb.Value_ArrayValue:
		var vals []*fixturepb.ValueSpec
		anyExcluded := false
		if vt.ArrayValue != nil {
			vals = make([]*fixturepb.ValueSpec, 0, len(vt.ArrayValue.Values))
			for _, elem := range vt.ArrayValue.Values {
				fe := ValueFromPB(elem)
				if fe != nil && fe.ExcludeFromIndexes {
					anyExcluded = true
				}
				vals = append(vals, fe)
			}
		}
		if anyExcluded {
			out.ExcludeFromIndexes = true
		}
		out.ValueType = &fixturepb.ValueSpec_ArrayValue{ArrayValue: &fixturepb.ArraySpec{Values: vals}}
	}
	return out
}

func vectorFromEntityPB(e *pb.Entity) (*fixturepb.VectorSpec, bool) {
	if e == nil || e.Key != nil || len(e.Properties) != 2 {
		return nil, false
	}
	typeVal, ok := e.Properties["__type__"]
	if !ok || typeVal.GetStringValue() != "__vector__" {
		return nil, false
	}
	arrVal, ok := e.Properties["value"]
	if !ok || arrVal.GetArrayValue() == nil {
		return nil, false
	}
	elems := make([]float64, 0, len(arrVal.GetArrayValue().Values))
	for _, item := range arrVal.GetArrayValue().Values {
		switch item.ValueType.(type) {
		case *pb.Value_DoubleValue:
			elems = append(elems, item.GetDoubleValue())
		case *pb.Value_IntegerValue:
			elems = append(elems, float64(item.GetIntegerValue()))
		default:
			return nil, false
		}
	}
	return &fixturepb.VectorSpec{Elements: elems}, true
}

// EntityToPB converts a fixture EntitySpec to a wire datastorepb.Entity.
func EntityToPB(e *fixturepb.EntitySpec) *pb.Entity {
	if e == nil {
		return nil
	}
	out := &pb.Entity{
		Key:        KeyToPB(e.Key),
		Properties: make(map[string]*pb.Value, len(e.Properties)),
	}
	for k, v := range e.Properties {
		out.Properties[k] = ValueToPB(v)
	}
	return out
}

// EntityFromPB converts a wire datastorepb.Entity to a fixture EntitySpec.
func EntityFromPB(e *pb.Entity) *fixturepb.EntitySpec {
	if e == nil {
		return nil
	}
	out := &fixturepb.EntitySpec{
		Key:        KeyFromPB(e.Key),
		Properties: make(map[string]*fixturepb.ValueSpec, len(e.Properties)),
	}
	for k, v := range e.Properties {
		out.Properties[k] = ValueFromPB(v)
	}
	return out
}
