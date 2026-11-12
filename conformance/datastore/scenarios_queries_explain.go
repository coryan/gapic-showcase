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
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func queryExplainScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "QueryExplainPlanOnlyAndAnalyze",
			Category: "queries",
			SpecRef:  "Spec §11.1, §11.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-exp", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				idxStruct, err := structpb.NewStruct(map[string]any{"properties": "(prio ASC)"})
				if err != nil {
					return err
				}
				dbgStruct, err := structpb.NewStruct(map[string]any{"documents_scanned": "2"})
				if err != nil {
					return err
				}

				k1 := strKey("Task", "e1")
				k2 := strKey("Task", "e2")

				env.Server.Expect(
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							eo := req.(*pb.RunQueryRequest).GetExplainOptions()
							if eo == nil || eo.Analyze {
								return fmt.Errorf("expected ExplainOptions(analyze=false), got %v", eo)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS},
							ExplainMetrics: &pb.ExplainMetrics{
								PlanSummary: &pb.PlanSummary{IndexesUsed: []*structpb.Struct{idxStruct}},
							},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							eo := req.(*pb.RunQueryRequest).GetExplainOptions()
							if eo == nil || !eo.Analyze {
								return fmt.Errorf("expected ExplainOptions(analyze=true), got %v", eo)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults:    []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k1)}}},
								EndCursor:        []byte("ec1"),
								MoreResults:      pb.QueryResultBatch_NOT_FINISHED,
							},
						},
					},
					Expectation{
						Method: "RunQuery",
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults:    []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k2)}}},
								MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
							},
							ExplainMetrics: &pb.ExplainMetrics{
								PlanSummary: &pb.PlanSummary{IndexesUsed: []*structpb.Struct{idxStruct}},
								ExecutionStats: &pb.ExecutionStats{
									ResultsReturned:   2,
									ExecutionDuration: durationpb.New(15 * time.Millisecond),
									ReadOperations:    2,
									DebugStats:        dbgStruct,
								},
							},
						},
					},
					Expectation{
						Method: "RunAggregationQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							eo := req.(*pb.RunAggregationQueryRequest).GetExplainOptions()
							if eo == nil || !eo.Analyze {
								return fmt.Errorf("expected ExplainOptions(analyze=true) on aggregation, got %v", eo)
							}
							return nil
						},
						Resp: &pb.RunAggregationQueryResponse{
							Batch: &pb.AggregationResultBatch{
								AggregationResults: []*pb.AggregationResult{{
									AggregateProperties: map[string]*pb.Value{
										"cnt": {ValueType: &pb.Value_IntegerValue{IntegerValue: 2}},
									},
								}},
							},
							ExplainMetrics: &pb.ExplainMetrics{
								PlanSummary: &pb.PlanSummary{IndexesUsed: []*structpb.Struct{idxStruct}},
								ExecutionStats: &pb.ExecutionStats{
									ResultsReturned:   1,
									ExecutionDuration: durationpb.New(5 * time.Millisecond),
									ReadOperations:    1,
								},
							},
						},
					},
				)

				// 1. Plan-only entity query.
				r1, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId:       cid,
					QuerySpec:      &fixturepb.RunQueryFixtureRequest_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"}},
					ExplainOptions: &fixturepb.ExplainOptionsSpec{Analyze: false},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "explain plan-only"); err != nil {
					return err
				}
				if len(r1.Entities) != 0 || r1.GetExplainMetrics().GetPlanSummary() == nil || r1.GetExplainMetrics().GetExecutionStats() != nil {
					return fmt.Errorf("expected 0 entities, PlanSummary only: %v", r1)
				}

				// 2. Analyze entity query across 2 batches.
				r2, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId:       cid,
					QuerySpec:      &fixturepb.RunQueryFixtureRequest_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"}},
					ExplainOptions: &fixturepb.ExplainOptionsSpec{Analyze: true},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r2.Error, "explain analyze"); err != nil {
					return err
				}
				if len(r2.Entities) != 2 || r2.GetExplainMetrics().GetExecutionStats().GetResultsReturned() != 2 {
					return fmt.Errorf("expected 2 entities and ExecutionStats, got entities=%d metrics=%v", len(r2.Entities), r2.ExplainMetrics)
				}

				// 3. Analyze aggregation query.
				r3, err := env.Fixture.RunAggregationQuery(ctx, &fixturepb.RunAggregationQueryFixtureRequest{
					ClientId: cid,
					BaseQuery: &fixturepb.RunAggregationQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"},
					},
					Aggregations:   []*fixturepb.AggregationSpec{{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "cnt"}},
					ExplainOptions: &fixturepb.ExplainOptionsSpec{Analyze: true},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r3.Error, "explain aggregation analyze"); err != nil {
					return err
				}
				if r3.GetExplainMetrics().GetExecutionStats().GetReadOperations() != 1 {
					return fmt.Errorf("expected aggregation ExplainMetrics, got %v", r3.ExplainMetrics)
				}
				return nil
			},
		},
		{
			Name:     "QueryExplainBeforeDoneClientStateError",
			Category: "queries",
			SpecRef:  "Spec §11.2, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-exp", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Task", "e1")
				k2 := strKey("Task", "e2")

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_FULL,
							EntityResults: []*pb.EntityResult{
								{Entity: &pb.Entity{Key: KeyToPB(k1)}},
								{Entity: &pb.Entity{Key: KeyToPB(k2)}},
							},
							EndCursor:   []byte("ec1"),
							MoreResults: pb.QueryResultBatch_NOT_FINISHED,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId:               cid,
					QuerySpec:              &fixturepb.RunQueryFixtureRequest_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"}},
					ExplainOptions:         &fixturepb.ExplainOptionsSpec{Analyze: true},
					CheckExplainBeforeDone: true,
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_STATE)
			},
		},
	}
}
