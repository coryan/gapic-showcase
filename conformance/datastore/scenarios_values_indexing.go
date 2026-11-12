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
	"strings"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func valueIndexingScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "ValueExcludeFromIndexesScalarAndLargeUnindexed",
			Category: "values",
			SpecRef:  "Spec §5.2, §5.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-idx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				bigStr := strings.Repeat("x", 2048)
				bigBlob := make([]byte, 2048)
				for i := range bigBlob {
					bigBlob[i] = byte(i % 251)
				}
				k := strKey("Doc", "large")
				entity := &fixturepb.EntitySpec{
					Key: k,
					Properties: map[string]*fixturepb.ValueSpec{
						"big_str":  {ExcludeFromIndexes: true, ValueType: &fixturepb.ValueSpec_StringValue{StringValue: bigStr}},
						"big_blob": {ExcludeFromIndexes: true, ValueType: &fixturepb.ValueSpec_BlobValue{BlobValue: bigBlob}},
						"indexed":  {ExcludeFromIndexes: false, ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "short"}},
					},
				}

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							up := cr.Mutations[0].GetUpsert()
							if !up.Properties["big_str"].ExcludeFromIndexes || !up.Properties["big_blob"].ExcludeFromIndexes {
								return fmt.Errorf("expected exclude_from_indexes=true on large properties")
							}
							if up.Properties["indexed"].ExcludeFromIndexes {
								return fmt.Errorf("expected exclude_from_indexes=false on indexed property")
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
							Found: []*pb.EntityResult{{Entity: EntityToPB(entity), Version: 1}},
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
		{
			Name:     "ValueExcludeFromIndexesArrayPropagation",
			Category: "values",
			SpecRef:  "Spec §5.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-idx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Doc", "arr-idx")
				entity := &fixturepb.EntitySpec{
					Key: k,
					Properties: map[string]*fixturepb.ValueSpec{
						"tags": {
							ExcludeFromIndexes: true,
							ValueType: &fixturepb.ValueSpec_ArrayValue{
								ArrayValue: &fixturepb.ArraySpec{
									Values: []*fixturepb.ValueSpec{
										{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "a"}},
										{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "b"}},
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
							up := cr.Mutations[0].GetUpsert()
							tagVal := up.Properties["tags"]
							if tagVal == nil || tagVal.GetArrayValue() == nil {
								return fmt.Errorf("missing tags ArrayValue")
							}
							if tagVal.ExcludeFromIndexes {
								return fmt.Errorf("wire outer ArrayValue Value.exclude_from_indexes MUST be false")
							}
							if len(tagVal.GetArrayValue().Values) != 2 {
								return fmt.Errorf("expected 2 array elements")
							}
							for i, el := range tagVal.GetArrayValue().Values {
								if !el.ExcludeFromIndexes {
									return fmt.Errorf("wire array element[%d].exclude_from_indexes must be true", i)
								}
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
				gotTags := getResp.GetResult().GetEntity().Properties["tags"]
				if gotTags == nil || !gotTags.ExcludeFromIndexes {
					return fmt.Errorf("expected round-tripped array ExcludeFromIndexes=true, got %v", gotTags)
				}
				elems := gotTags.GetArrayValue().GetValues()
				if len(elems) != 2 || elems[0].GetStringValue() != "a" || elems[1].GetStringValue() != "b" {
					return fmt.Errorf("unexpected round-tripped array elements: %v", elems)
				}
				return nil
			},
		},
		{
			Name:     "ValueRejectDirectlyNestedArray",
			Category: "values",
			SpecRef:  "Spec §5.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-idx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				nested := &fixturepb.EntitySpec{
					Key: strKey("Bad", "nested"),
					Properties: map[string]*fixturepb.ValueSpec{
						"bad": {
							ValueType: &fixturepb.ValueSpec_ArrayValue{
								ArrayValue: &fixturepb.ArraySpec{
									Values: []*fixturepb.ValueSpec{{
										ValueType: &fixturepb.ValueSpec_ArrayValue{
											ArrayValue: &fixturepb.ArraySpec{
												Values: []*fixturepb.ValueSpec{
													{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}},
												},
											},
										},
									}},
								},
							},
						},
					},
				}

				resp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity:   nested,
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION)
			},
		},
	}
}
