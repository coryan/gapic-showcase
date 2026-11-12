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
	"bytes"
	"context"
	"fmt"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func queryAggregationScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "QueryFindNearestVectorSearch",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-knn", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				lim := int32(5)
				thresh := 0.75
				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						fn := req.(*pb.RunQueryRequest).GetQuery().GetFindNearest()
						if fn == nil {
							return fmt.Errorf("missing FindNearest on wire Query")
						}
						if fn.GetVectorProperty().GetName() != "embedding" ||
							fn.DistanceMeasure != pb.FindNearest_COSINE ||
							fn.GetLimit().GetValue() != 5 ||
							fn.DistanceResultProperty != "dist" ||
							fn.GetDistanceThreshold().GetValue() != 0.75 {
							return fmt.Errorf("unexpected FindNearest fields: %v", fn)
						}
						vec, ok := vectorFromEntityPB(fn.GetQueryVector().GetEntityValue())
						if !ok || len(vec.Elements) != 2 || vec.Elements[0] != 1.0 || vec.Elements[1] != 2.0 {
							return fmt.Errorf("unexpected FindNearest query_vector: %v", fn.GetQueryVector())
						}
						return nil
					},
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_FULL,
							MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind: "Doc",
							FindNearest: &fixturepb.FindNearestSpec{
								VectorProperty:         "embedding",
								QueryVector:            &fixturepb.VectorSpec{Elements: []float64{1.0, 2.0}},
								DistanceMeasure:        fixturepb.DistanceMeasureSpec_DISTANCE_MEASURE_COSINE,
								Limit:                  &lim,
								DistanceResultProperty: "dist",
								DistanceThreshold:      &thresh,
							},
						},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "FindNearest")
			},
		},
		{
			Name:     "QueryGqlWithBindingsAndContinuation",
			Category: "queries",
			SpecRef:  "Spec §9.2, §9.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-gql", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Task", "g1")
				k2 := strKey("Task", "g2")
				compiledQuery := &pb.Query{
					Kind: []*pb.KindExpression{{Name: "Task"}},
				}

				env.Server.Expect(
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							gq := req.(*pb.RunQueryRequest).GetGqlQuery()
							if gq == nil || gq.AllowLiterals {
								return fmt.Errorf("expected GqlQuery with allow_literals=false, got %v", req)
							}
							if gq.NamedBindings["tag"].GetValue().GetStringValue() != "ops" {
								return fmt.Errorf("unexpected named binding: %v", gq.NamedBindings)
							}
							if len(gq.PositionalBindings) != 1 || !bytes.Equal(gq.PositionalBindings[0].GetCursor(), []byte("start1")) {
								return fmt.Errorf("unexpected positional cursor binding: %v", gq.PositionalBindings)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Query: compiledQuery,
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults:    []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k1)}, Cursor: []byte("c1")}},
								EndCursor:        []byte("ec1"),
								MoreResults:      pb.QueryResultBatch_NOT_FINISHED,
							},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							rq := req.(*pb.RunQueryRequest)
							q := rq.GetQuery()
							if q == nil || len(q.Kind) != 1 || q.Kind[0].Name != "Task" || !bytes.Equal(q.StartCursor, []byte("ec1")) {
								return fmt.Errorf("expected compiled structured Query with start_cursor=ec1 on GQL continuation, got %v", rq)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults:    []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k2)}, Cursor: []byte("c2")}},
								MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
				)

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_GqlQuery{
						GqlQuery: &fixturepb.GqlQuerySpec{
							QueryString:   "SELECT * FROM Task WHERE tag = @tag OFFSET @1",
							AllowLiterals: false,
							NamedBindings: map[string]*fixturepb.GqlBindingSpec{
								"tag": {BindingType: &fixturepb.GqlBindingSpec_Value{Value: strVal("ops")}},
							},
							PositionalBindings: []*fixturepb.GqlBindingSpec{
								{BindingType: &fixturepb.GqlBindingSpec_Cursor{Cursor: []byte("start1")}},
							},
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "GQL continuation"); err != nil {
					return err
				}
				if len(resp.Entities) != 2 {
					return fmt.Errorf("expected 2 entities across GQL batches, got %d", len(resp.Entities))
				}
				return nil
			},
		},
		{
			Name:     "QueryAggregationsCountSumAvg",
			Category: "queries",
			SpecRef:  "Spec §10.1, §10.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-agg", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				readTS := time.Unix(1700000000, 0).UTC()
				env.Server.Expect(
					Expectation{
						Method: "RunAggregationQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							ar := req.(*pb.RunAggregationQueryRequest)
							if err := AssertRoutingHeaders(md, "proj-agg", "", tr); err != nil {
								return err
							}
							if ar.GetReadOptions().GetReadConsistency() != pb.ReadOptions_EVENTUAL {
								return fmt.Errorf("expected EVENTUAL read consistency, got %v", ar.ReadOptions)
							}
							aggs := ar.GetAggregationQuery().GetAggregations()
							if len(aggs) != 3 ||
								aggs[0].GetCount() == nil || aggs[0].Alias != "total" ||
								aggs[1].GetSum().GetProperty().GetName() != "amount" || aggs[1].Alias != "sum_amt" ||
								aggs[2].GetAvg().GetProperty().GetName() != "amount" || aggs[2].Alias != "avg_amt" {
								return fmt.Errorf("unexpected aggregations on wire: %v", aggs)
							}
							return nil
						},
						Resp: &pb.RunAggregationQueryResponse{
							Batch: &pb.AggregationResultBatch{
								AggregationResults: []*pb.AggregationResult{{
									AggregateProperties: map[string]*pb.Value{
										"total":   {ValueType: &pb.Value_IntegerValue{IntegerValue: 10}},
										"sum_amt": {ValueType: &pb.Value_IntegerValue{IntegerValue: 250}},
										"avg_amt": {ValueType: &pb.Value_DoubleValue{DoubleValue: 25.0}},
									},
								}},
							},
						},
					},
					Expectation{
						Method: "RunAggregationQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							ar := req.(*pb.RunAggregationQueryRequest)
							rt := ar.GetReadOptions().GetReadTime()
							if rt == nil || !rt.AsTime().Equal(readTS) {
								return fmt.Errorf("expected ReadTime=%v, got %v", readTS, rt)
							}
							return nil
						},
						Resp: &pb.RunAggregationQueryResponse{
							Batch: &pb.AggregationResultBatch{
								AggregationResults: []*pb.AggregationResult{{
									AggregateProperties: map[string]*pb.Value{
										"total": {ValueType: &pb.Value_IntegerValue{IntegerValue: 4}},
									},
								}},
							},
						},
					},
				)

				r1, err := env.Fixture.RunAggregationQuery(ctx, &fixturepb.RunAggregationQueryFixtureRequest{
					ClientId: cid,
					BaseQuery: &fixturepb.RunAggregationQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Order"},
					},
					Aggregations: []*fixturepb.AggregationSpec{
						{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "total"},
						{Op: fixturepb.AggregationOp_AGGREGATION_OP_SUM, Alias: "sum_amt", Property: "amount"},
						{Op: fixturepb.AggregationOp_AGGREGATION_OP_AVG, Alias: "avg_amt", Property: "amount"},
					},
					ReadSpec: &fixturepb.ReadSpec{
						ReadConsistency: fixturepb.ReadConsistencySpec_READ_CONSISTENCY_EVENTUAL,
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "RunAggregationQuery multi-agg"); err != nil {
					return err
				}
				if r1.Results["total"].GetIntegerValue() != 10 ||
					r1.Results["sum_amt"].GetIntegerValue() != 250 ||
					r1.Results["avg_amt"].GetDoubleValue() != 25.0 {
					return fmt.Errorf("unexpected aggregation results: %v", r1.Results)
				}

				r2, err := env.Fixture.RunAggregationQuery(ctx, &fixturepb.RunAggregationQueryFixtureRequest{
					ClientId: cid,
					BaseQuery: &fixturepb.RunAggregationQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Order"},
					},
					Aggregations: []*fixturepb.AggregationSpec{
						{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "total"},
					},
					ReadSpec: &fixturepb.ReadSpec{ReadTime: timestamppb.New(readTS)},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r2.Error, "RunAggregationQuery read_time"); err != nil {
					return err
				}
				if r2.Results["total"].GetIntegerValue() != 4 {
					return fmt.Errorf("expected total=4, got %v", r2.Results)
				}
				return nil
			},
		},
		{
			Name:     "QueryAggregationCountUpTo",
			Category: "queries",
			SpecRef:  "Spec §10.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-agg", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				upTo := int64(500)
				env.Server.Expect(Expectation{
					Method: "RunAggregationQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						aggs := req.(*pb.RunAggregationQueryRequest).GetAggregationQuery().GetAggregations()
						if len(aggs) != 1 || aggs[0].GetCount().GetUpTo().GetValue() != 500 {
							return fmt.Errorf("expected Count(up_to=500), got %v", aggs)
						}
						return nil
					},
					Resp: &pb.RunAggregationQueryResponse{
						Batch: &pb.AggregationResultBatch{
							AggregationResults: []*pb.AggregationResult{{
								AggregateProperties: map[string]*pb.Value{
									"capped": {ValueType: &pb.Value_IntegerValue{IntegerValue: 500}},
								},
							}},
						},
					},
				})

				resp, err := env.Fixture.RunAggregationQuery(ctx, &fixturepb.RunAggregationQueryFixtureRequest{
					ClientId: cid,
					BaseQuery: &fixturepb.RunAggregationQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Order"},
					},
					Aggregations: []*fixturepb.AggregationSpec{
						{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "capped", UpTo: &upTo},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "RunAggregationQuery UpTo"); err != nil {
					return err
				}
				if resp.Results["capped"].GetIntegerValue() != 500 {
					return fmt.Errorf("expected capped=500, got %v", resp.Results)
				}
				return nil
			},
		},
	}
}
