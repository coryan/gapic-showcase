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

// Package datastore provides a DatastoreFixture gRPC test proxy wrapping cloud.google.com/go/datastore.
package datastore

import (
	"context"
	"fmt"
	"time"

	ds "cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AllocateIds allocates IDs for incomplete keys.
func (s *Server) AllocateIds(ctx context.Context, req *fixturepb.AllocateIdsFixtureRequest) (*fixturepb.AllocateIdsFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	keys := make([]*ds.Key, len(req.IncompleteKeys))
	for i, k := range req.IncompleteKeys {
		keys[i] = keyFromSpec(k)
	}
	allocated, err := entry.client.AllocateIDs(ctx, keys)
	if err != nil {
		return &fixturepb.AllocateIdsFixtureResponse{Error: toVeneerError(err)}, nil
	}
	out := make([]*fixturepb.KeySpec, len(allocated))
	for i, k := range allocated {
		out[i] = keyToSpec(k)
	}
	return &fixturepb.AllocateIdsFixtureResponse{AllocatedKeys: out}, nil
}

// ReserveIds reserves complete keys.
func (s *Server) ReserveIds(ctx context.Context, req *fixturepb.ReserveIdsFixtureRequest) (*fixturepb.ReserveIdsFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if req.SequentialCount > 0 {
		return &fixturepb.ReserveIdsFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not provide a sequential ID reservation helper"),
		}, nil
	}
	keys := make([]*ds.Key, len(req.Keys))
	for i, k := range req.Keys {
		keys[i] = keyFromSpec(k)
	}
	if err := entry.client.ReserveIDs(ctx, keys); err != nil {
		return &fixturepb.ReserveIdsFixtureResponse{Error: toVeneerError(err)}, nil
	}
	return &fixturepb.ReserveIdsFixtureResponse{}, nil
}

// Get retrieves a single entity by key.

type dynamicEntity struct {
	Key   *ds.Key
	Props []ds.Property
}

func (d *dynamicEntity) LoadKey(k *ds.Key) error {
	d.Key = k
	return nil
}

func (d *dynamicEntity) Load(ps []ds.Property) error {
	d.Props = append([]ds.Property(nil), ps...)
	return nil
}

func (d *dynamicEntity) Save() ([]ds.Property, error) {
	return d.Props, nil
}

func keyFromSpec(k *fixturepb.KeySpec) *ds.Key {
	if k == nil {
		return nil
	}
	ns := k.GetPartitionId().GetNamespaceId()
	var cur *ds.Key
	for _, pe := range k.Path {
		el := &ds.Key{
			Kind:      pe.Kind,
			Namespace: ns,
			Parent:    cur,
		}
		switch id := pe.IdType.(type) {
		case *fixturepb.PathElementSpec_Id:
			el.ID = id.Id
		case *fixturepb.PathElementSpec_Name:
			el.Name = id.Name
		}
		cur = el
	}
	return cur
}

func keyToSpec(k *ds.Key) *fixturepb.KeySpec {
	if k == nil {
		return nil
	}
	var chain []*ds.Key
	for cur := k; cur != nil; cur = cur.Parent {
		chain = append(chain, cur)
	}
	path := make([]*fixturepb.PathElementSpec, 0, len(chain))
	for i := len(chain) - 1; i >= 0; i-- {
		el := chain[i]
		pe := &fixturepb.PathElementSpec{Kind: el.Kind}
		if el.ID != 0 {
			pe.IdType = &fixturepb.PathElementSpec_Id{Id: el.ID}
		} else if el.Name != "" {
			pe.IdType = &fixturepb.PathElementSpec_Name{Name: el.Name}
		}
		path = append(path, pe)
	}
	out := &fixturepb.KeySpec{Path: path}
	if k.Namespace != "" {
		out.PartitionId = &fixturepb.PartitionSpec{NamespaceId: k.Namespace}
	}
	return out
}

func entityToDynamic(e *fixturepb.EntitySpec) (*ds.Key, *dynamicEntity, error) {
	if e == nil {
		return nil, &dynamicEntity{}, nil
	}
	props := make([]ds.Property, 0, len(e.Properties))
	for name, vs := range e.Properties {
		val, err := valueFromSpec(vs)
		if err != nil {
			return nil, nil, err
		}
		props = append(props, ds.Property{
			Name:    name,
			Value:   val,
			NoIndex: vs.GetExcludeFromIndexes(),
		})
	}
	k := keyFromSpec(e.Key)
	return k, &dynamicEntity{Key: k, Props: props}, nil
}

func dynamicToEntitySpec(d *dynamicEntity) *fixturepb.EntitySpec {
	if d == nil {
		return nil
	}
	props := make(map[string]*fixturepb.ValueSpec, len(d.Props))
	for _, p := range d.Props {
		props[p.Name] = valueToSpec(p.Value, p.NoIndex)
	}
	return &fixturepb.EntitySpec{
		Key:        keyToSpec(d.Key),
		Properties: props,
	}
}

func valueFromSpec(v *fixturepb.ValueSpec) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch vt := v.ValueType.(type) {
	case *fixturepb.ValueSpec_IsNull:
		return nil, nil
	case *fixturepb.ValueSpec_BooleanValue:
		return vt.BooleanValue, nil
	case *fixturepb.ValueSpec_IntegerValue:
		return vt.IntegerValue, nil
	case *fixturepb.ValueSpec_DoubleValue:
		return vt.DoubleValue, nil
	case *fixturepb.ValueSpec_TimestampValue:
		if vt.TimestampValue == nil {
			return nil, nil
		}
		return vt.TimestampValue.AsTime().UTC(), nil
	case *fixturepb.ValueSpec_KeyValue:
		return keyFromSpec(vt.KeyValue), nil
	case *fixturepb.ValueSpec_StringValue:
		return vt.StringValue, nil
	case *fixturepb.ValueSpec_BlobValue:
		b := make([]byte, len(vt.BlobValue))
		copy(b, vt.BlobValue)
		return b, nil
	case *fixturepb.ValueSpec_GeoPointValue:
		return ds.GeoPoint{Lat: vt.GeoPointValue.Latitude, Lng: vt.GeoPointValue.Longitude}, nil
	case *fixturepb.ValueSpec_EntityValue:
		if vt.EntityValue == nil {
			return nil, nil
		}
		_, dyn, err := entityToDynamic(vt.EntityValue)
		if err != nil {
			return nil, err
		}
		return &ds.Entity{Key: keyFromSpec(vt.EntityValue.Key), Properties: dyn.Props}, nil
	case *fixturepb.ValueSpec_ArrayValue:
		var vals []any
		if vt.ArrayValue != nil {
			vals = make([]any, 0, len(vt.ArrayValue.Values))
			for _, elem := range vt.ArrayValue.Values {
				gv, err := valueFromSpec(elem)
				if err != nil {
					return nil, err
				}
				vals = append(vals, gv)
			}
		} else {
			vals = []any{}
		}
		return vals, nil
	case *fixturepb.ValueSpec_VectorValue:
		return nil, fmt.Errorf("%w: cloud.google.com/go/datastore does not have a native Vector type", errFeatureNotImplemented)
	default:
		return nil, nil
	}
}

func valueToSpec(v any, noIndex bool) *fixturepb.ValueSpec {
	out := &fixturepb.ValueSpec{ExcludeFromIndexes: noIndex}
	switch x := v.(type) {
	case nil:
		out.ValueType = &fixturepb.ValueSpec_IsNull{IsNull: true}
	case bool:
		out.ValueType = &fixturepb.ValueSpec_BooleanValue{BooleanValue: x}
	case int:
		out.ValueType = &fixturepb.ValueSpec_IntegerValue{IntegerValue: int64(x)}
	case int32:
		out.ValueType = &fixturepb.ValueSpec_IntegerValue{IntegerValue: int64(x)}
	case int64:
		out.ValueType = &fixturepb.ValueSpec_IntegerValue{IntegerValue: x}
	case float32:
		out.ValueType = &fixturepb.ValueSpec_DoubleValue{DoubleValue: float64(x)}
	case float64:
		out.ValueType = &fixturepb.ValueSpec_DoubleValue{DoubleValue: x}
	case time.Time:
		out.ValueType = &fixturepb.ValueSpec_TimestampValue{TimestampValue: timestamppb.New(x.UTC())}
	case *ds.Key:
		if x == nil {
			out.ValueType = &fixturepb.ValueSpec_IsNull{IsNull: true}
		} else {
			out.ValueType = &fixturepb.ValueSpec_KeyValue{KeyValue: keyToSpec(x)}
		}
	case string:
		out.ValueType = &fixturepb.ValueSpec_StringValue{StringValue: x}
	case []byte:
		b := make([]byte, len(x))
		copy(b, x)
		out.ValueType = &fixturepb.ValueSpec_BlobValue{BlobValue: b}
	case ds.GeoPoint:
		out.ValueType = &fixturepb.ValueSpec_GeoPointValue{
			GeoPointValue: &fixturepb.LatLngSpec{Latitude: x.Lat, Longitude: x.Lng},
		}
	case *ds.Entity:
		props := make(map[string]*fixturepb.ValueSpec, len(x.Properties))
		for _, p := range x.Properties {
			props[p.Name] = valueToSpec(p.Value, p.NoIndex)
		}
		out.ValueType = &fixturepb.ValueSpec_EntityValue{
			EntityValue: &fixturepb.EntitySpec{
				Key:        keyToSpec(x.Key),
				Properties: props,
			},
		}
	case []any:
		vals := make([]*fixturepb.ValueSpec, len(x))
		for i, elem := range x {
			vals[i] = valueToSpec(elem, false)
		}
		out.ValueType = &fixturepb.ValueSpec_ArrayValue{
			ArrayValue: &fixturepb.ArraySpec{Values: vals},
		}
	}
	return out
}

func aggPBValueToSpec(v *pb.Value) *fixturepb.ValueSpec {
	if v == nil {
		return nil
	}
	switch vt := v.ValueType.(type) {
	case *pb.Value_IntegerValue:
		return &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: vt.IntegerValue}}
	case *pb.Value_DoubleValue:
		return &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: vt.DoubleValue}}
	case *pb.Value_NullValue:
		return &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IsNull{IsNull: true}}
	default:
		return &fixturepb.ValueSpec{}
	}
}
