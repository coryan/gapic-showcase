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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func retryScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "RetryIdempotentUnavailableLookupQueryAggregationReserveIds",
			Category: "retries",
			SpecRef:  "Spec §3.4",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-retry", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Item", "r1")
				env.Server.Expect(
					Expectation{Method: "Lookup", Err: status.Error(codes.Unavailable, "transient 1")},
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{Method: "RunQuery", Err: status.Error(codes.Unavailable, "transient 2")},
					Expectation{
						Method: "RunQuery",
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS},
						},
					},
					Expectation{Method: "RunAggregationQuery", Err: status.Error(codes.Unavailable, "transient 3")},
					Expectation{
						Method: "RunAggregationQuery",
						Resp: &pb.RunAggregationQueryResponse{
							Batch: &pb.AggregationResultBatch{
								AggregationResults: []*pb.AggregationResult{{
									AggregateProperties: map[string]*pb.Value{
										"cnt": {ValueType: &pb.Value_IntegerValue{IntegerValue: 0}},
									},
								}},
							},
						},
					},
					Expectation{Method: "ReserveIds", Err: status.Error(codes.Unavailable, "transient 4")},
					Expectation{
						Method: "ReserveIds",
						Resp:   &pb.ReserveIdsResponse{},
					},
				)

				gResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(gResp.Error, "Get UNAVAILABLE retry"); err != nil {
					return err
				}

				qResp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId:  cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Item"}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(qResp.Error, "RunQuery UNAVAILABLE retry"); err != nil {
					return err
				}

				aResp, err := env.Fixture.RunAggregationQuery(ctx, &fixturepb.RunAggregationQueryFixtureRequest{
					ClientId: cid,
					BaseQuery: &fixturepb.RunAggregationQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Item"},
					},
					Aggregations: []*fixturepb.AggregationSpec{{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "cnt"}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(aResp.Error, "RunAggregationQuery UNAVAILABLE retry"); err != nil {
					return err
				}

				rResp, err := env.Fixture.ReserveIds(ctx, &fixturepb.ReserveIdsFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{intKey("Item", 1)},
				})
				if err != nil {
					return err
				}
				return CheckNoError(rResp.Error, "ReserveIds UNAVAILABLE retry")
			},
		},
		{
			Name:     "RetryNoRetryCommitAndAllocateIdsDeadlineExceeded",
			Category: "retries",
			SpecRef:  "Spec §3.4",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-retry", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				env.Server.Expect(
					Expectation{Method: "Commit", Err: status.Error(codes.DeadlineExceeded, "slow commit")},
					Expectation{Method: "AllocateIds", Err: status.Error(codes.DeadlineExceeded, "slow alloc")},
				)

				pResp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key:        strKey("Item", "x"),
						Properties: map[string]*fixturepb.ValueSpec{"a": intVal(1)},
					},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(pResp.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_DEADLINE_EXCEEDED); err != nil {
					return fmt.Errorf("Put Commit DEADLINE_EXCEEDED: %w", err)
				}

				aResp, err := env.Fixture.AllocateIds(ctx, &fixturepb.AllocateIdsFixtureRequest{
					ClientId:       cid,
					IncompleteKeys: []*fixturepb.KeySpec{incompleteKey("Item")},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(aResp.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_DEADLINE_EXCEEDED); err != nil {
					return fmt.Errorf("AllocateIds DEADLINE_EXCEEDED: %w", err)
				}
				if len(env.Server.Calls()) != 2 {
					return fmt.Errorf("expected exactly 2 RPCs (no automatic retries), got %d", len(env.Server.Calls()))
				}
				return nil
			},
		},
		{
			Name:     "RetryNoRetryBeginTransactionDeadlineExceeded",
			Category: "retries",
			SpecRef:  "Spec §3.4",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-retry", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				// BeginTransaction is non-idempotent/stateful: must send only 1 RPC on DEADLINE_EXCEEDED.
				env.Server.Expect(
					Expectation{Method: "BeginTransaction", Err: status.Error(codes.DeadlineExceeded, "slow begin")},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					BeginLater:  false,
					MaxAttempts: 1,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: strKey("Item", "x")}},
					}},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_DEADLINE_EXCEEDED); err != nil {
					return err
				}
				if len(env.Server.Calls()) != 1 {
					return fmt.Errorf("expected 1 BeginTransaction RPC, got %d", len(env.Server.Calls()))
				}
				return nil
			},
		},
	}
}
