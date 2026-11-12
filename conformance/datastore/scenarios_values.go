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
	"context"
	"fmt"
	"math"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func valueTypeScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "ValueAllCoreTypesRoundTrip",
			Category: "values",
			SpecRef:  "Spec §5.1, §5.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-vals", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("AllTypes", "e1")
				ts := timestamppb.New(time.Unix(1700000000, 123456000).UTC())
				refKey := intKey("Ref", 99)

				embeddedComplete := &fixturepb.EntitySpec{
					Key: strKey("Sub", "complete"),
					Properties: map[string]*fixturepb.ValueSpec{
						"s": {ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "inner"}},
					},
				}
				embeddedIncomplete := &fixturepb.EntitySpec{
					Key: incompleteKey("SubInc"),
					Properties: map[string]*fixturepb.ValueSpec{
						"n": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 7}},
					},
				}
				embeddedKeyless := &fixturepb.EntitySpec{
					Properties: map[string]*fixturepb.ValueSpec{
						"flag": {ValueType: &fixturepb.ValueSpec_BooleanValue{BooleanValue: true}},
					},
				}

				entity := &fixturepb.EntitySpec{
					Key: k,
					Properties: map[string]*fixturepb.ValueSpec{
						"null_p":   {ValueType: &fixturepb.ValueSpec_IsNull{IsNull: true}},
						"bool_p":   {ValueType: &fixturepb.ValueSpec_BooleanValue{BooleanValue: true}},
						"int_p":    {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: -9001}},
						"dbl_p":    {ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: 3.14159}},
						"negz_p":   {ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.Copysign(0, -1)}},
						"nan_p":    {ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.NaN()}},
						"pinf_p":   {ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.Inf(1)}},
						"ninf_p":   {ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.Inf(-1)}},
						"ts_p":     {ValueType: &fixturepb.ValueSpec_TimestampValue{TimestampValue: ts}},
						"str_p":    {ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "hello datastore"}},
						"blob_p":   {ValueType: &fixturepb.ValueSpec_BlobValue{BlobValue: []byte{0x00, 0x01, 0xfe, 0xff}}},
						"key_p":    {ValueType: &fixturepb.ValueSpec_KeyValue{KeyValue: refKey}},
						"geo_p":    {ValueType: &fixturepb.ValueSpec_GeoPointValue{GeoPointValue: &fixturepb.LatLngSpec{Latitude: 37.422, Longitude: -122.084}}},
						"emb_c":    {ValueType: &fixturepb.ValueSpec_EntityValue{EntityValue: embeddedComplete}},
						"emb_inc":  {ValueType: &fixturepb.ValueSpec_EntityValue{EntityValue: embeddedIncomplete}},
						"emb_none": {ValueType: &fixturepb.ValueSpec_EntityValue{EntityValue: embeddedKeyless}},
						"arr_p": {
							ValueType: &fixturepb.ValueSpec_ArrayValue{
								ArrayValue: &fixturepb.ArraySpec{
									Values: []*fixturepb.ValueSpec{
										{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "a"}},
										{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 2}},
									},
								},
							},
						},
					},
				}

				pbEntity := EntityToPB(entity)
				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpsert() == nil {
								return fmt.Errorf("expected 1 Upsert mutation")
							}
							gotSpec := EntityFromPB(cr.Mutations[0].GetUpsert())
							if tr == fixturepb.Transport_TRANSPORT_HTTP_JSON {
								// JSON cannot preserve IEEE 754 negative zero separately from 0.
								if v, ok := gotSpec.Properties["negz_p"]; ok && v.GetDoubleValue() == 0 {
									v.ValueType = &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.Copysign(0, -1)}
								}
							}
							return AssertEntitySpecEqual(gotSpec, entity)
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{Version: 1}},
						},
					},
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{Entity: pbEntity, Version: 1}},
						},
					},
				)

				putResp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity:   entity,
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(putResp.Error, "Put"); err != nil {
					return err
				}

				getResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId: cid,
					Key:      k,
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(getResp.Error, "Get"); err != nil {
					return err
				}
				if !getResp.GetResult().GetFound() {
					return fmt.Errorf("expected found=true")
				}
				got := getResp.GetResult().GetEntity()
				if env.Transport == fixturepb.Transport_TRANSPORT_HTTP_JSON {
					if v, ok := got.Properties["negz_p"]; ok && v.GetDoubleValue() == 0 {
						v.ValueType = &fixturepb.ValueSpec_DoubleValue{DoubleValue: math.Copysign(0, -1)}
					}
				}
				return AssertEntitySpecEqual(got, entity)
			},
		},
		{
			Name:     "ValueVectorEmbeddingRoundTrip",
			Category: "values",
			SpecRef:  "Spec §5.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-vec", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Doc", "d1")
				entity := &fixturepb.EntitySpec{
					Key: k,
					Properties: map[string]*fixturepb.ValueSpec{
						"embedding": {
							ValueType: &fixturepb.ValueSpec_VectorValue{
								VectorValue: &fixturepb.VectorSpec{Elements: []float64{0.25, -0.5, 1.5}},
							},
						},
					},
				}

				pbEntity := EntityToPB(entity)
				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							up := cr.Mutations[0].GetUpsert()
							if up == nil {
								return fmt.Errorf("expected Upsert")
							}
							ev := up.Properties["embedding"].GetEntityValue()
							vec, ok := vectorFromEntityPB(ev)
							if !ok {
								return fmt.Errorf("wire embedding property is not a valid __vector__ entity: %v", up.Properties["embedding"])
							}
							if len(vec.Elements) != 3 || vec.Elements[0] != 0.25 || vec.Elements[1] != -0.5 || vec.Elements[2] != 1.5 {
								return fmt.Errorf("unexpected vector elements: %v", vec.Elements)
							}
							return nil
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{Version: 1}},
						},
					},
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{Entity: pbEntity, Version: 1}},
						},
					},
				)

				putResp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{ClientId: cid, Entity: entity})
				if err != nil {
					return err
				}
				if err := CheckNoError(putResp.Error, "Put"); err != nil {
					return err
				}
				getResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(getResp.Error, "Get"); err != nil {
					return err
				}
				return AssertEntitySpecEqual(getResp.GetResult().GetEntity(), entity)
			},
		},
	}
}
