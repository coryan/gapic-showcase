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

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func queryProjectionScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "QueryProjectionAndDistinctOn",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "p1")
				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						q := req.(*pb.RunQueryRequest).GetQuery()
						if len(q.Projection) != 2 || q.Projection[0].Property.Name != "category" || q.Projection[1].Property.Name != "prio" {
							return fmt.Errorf("unexpected projection: %v", q.Projection)
						}
						if len(q.DistinctOn) != 1 || q.DistinctOn[0].Name != "category" {
							return fmt.Errorf("unexpected distinct_on: %v", q.DistinctOn)
						}
						return nil
					},
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_PROJECTION,
							EntityResults: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key: KeyToPB(k),
									Properties: map[string]*pb.Value{
										"category": {ValueType: &pb.Value_StringValue{StringValue: "work"}, Meaning: 18},
										"prio":     {ValueType: &pb.Value_IntegerValue{IntegerValue: 2}, Meaning: 18},
									},
								},
							}},
							MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:        "Task",
							Projections: []string{"category", "prio"},
							DistinctOn:  []string{"category"},
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "projection query"); err != nil {
					return err
				}
				if len(resp.Entities) != 1 || resp.Entities[0].Properties["category"].GetStringValue() != "work" {
					return fmt.Errorf("unexpected projection result: %v", resp.Entities)
				}
				return nil
			},
		},
		{
			Name:     "QueryProjectionKeylessEntity",
			Category: "queries",
			SpecRef:  "Spec §5.1, §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_PROJECTION,
							EntityResults: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key: nil, // Explicitly allowed for PROJECTION results per Datastore wire spec.
									Properties: map[string]*pb.Value{
										"score": {ValueType: &pb.Value_IntegerValue{IntegerValue: 99}, Meaning: 18},
									},
								},
							}},
							MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:        "Score",
							Projections: []string{"score"},
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "keyless projection"); err != nil {
					return err
				}
				if len(resp.Entities) != 1 || resp.Entities[0].Properties["score"].GetIntegerValue() != 99 {
					return fmt.Errorf("expected 1 keyless projected entity with score=99, got %v", resp.Entities)
				}
				return nil
			},
		},
		{
			Name:     "QueryProjectionMeaning18RoundTripStripsMeaning",
			Category: "queries",
			SpecRef:  "Spec §5.3, §9.1, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-m18", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "m1")
				env.Server.Expect(
					Expectation{
						Method: "RunQuery",
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_PROJECTION,
								EntityResults: []*pb.EntityResult{{
									Entity: &pb.Entity{
										Key: KeyToPB(k),
										Properties: map[string]*pb.Value{
											"tag":   {ValueType: &pb.Value_StringValue{StringValue: "alpha"}, Meaning: 18},
											"count": {ValueType: &pb.Value_IntegerValue{IntegerValue: 42}, Meaning: 18},
										},
									},
								}},
								MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpsert() == nil {
								return fmt.Errorf("expected 1 Upsert on round-trip write-back")
							}
							return AssertNoMeaning18(cr.Mutations[0].GetUpsert())
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 1}}},
					},
				)

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:        "Task",
							Projections: []string{"tag", "count"},
						},
					},
					RoundTripUpsertResults: true,
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "meaning=18 projection round-trip")
			},
		},
		{
			Name:     "QueryKeysOnlyOrderLimitOffset",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				lim := int32(2)
				off := int32(1)
				k1 := strKey("Task", "t1")
				k2 := strKey("Task", "t2")

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						q := req.(*pb.RunQueryRequest).GetQuery()
						if len(q.Projection) != 1 || q.Projection[0].Property.Name != "__key__" {
							return fmt.Errorf("expected __key__ projection for KeysOnly, got %v", q.Projection)
						}
						if len(q.Order) != 2 ||
							q.Order[0].Property.Name != "prio" || q.Order[0].Direction != pb.PropertyOrder_DESCENDING ||
							q.Order[1].Property.Name != "name" || q.Order[1].Direction != pb.PropertyOrder_ASCENDING {
							return fmt.Errorf("unexpected orders: %v", q.Order)
						}
						if q.GetLimit().GetValue() != 2 || q.GetOffset() != 1 {
							return fmt.Errorf("expected limit=2 offset=1, got limit=%v offset=%d", q.Limit, q.Offset)
						}
						return nil
					},
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_KEY_ONLY,
							SkippedResults:   1,
							SkippedCursor:    []byte("skip1"),
							EntityResults: []*pb.EntityResult{
								{Entity: &pb.Entity{Key: KeyToPB(k1)}},
								{Entity: &pb.Entity{Key: KeyToPB(k2)}},
							},
							MoreResults: pb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:     "Task",
							KeysOnly: true,
							Orders: []*fixturepb.OrderSpec{
								{Property: "prio", Direction: fixturepb.OrderDirection_ORDER_DIRECTION_DESCENDING},
								{Property: "name", Direction: fixturepb.OrderDirection_ORDER_DIRECTION_ASCENDING},
							},
							Limit:  &lim,
							Offset: &off,
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "keys-only query"); err != nil {
					return err
				}
				if len(resp.Keys) != 2 {
					return fmt.Errorf("expected 2 keys, got %d", len(resp.Keys))
				}
				if err := AssertFixtureKeyEqual(resp.Keys[0], k1); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(resp.Keys[1], k2)
			},
		},
		{
			Name:     "QueryRejectNegativeOffset",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				negOff := int32(-1)
				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task", Offset: &negOff},
					},
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION)
			},
		},
	}
}
