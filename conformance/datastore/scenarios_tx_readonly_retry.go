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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func txReadOnlyRetryScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "TxAbortedOnSecondQueryBatchResetsAndRetries",
			Category: "transactions",
			SpecRef:  "Spec §8.1, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Task", "1")
				k2 := strKey("Task", "2")
				tx1 := []byte("tx-q1")
				tx2 := []byte("tx-q2")

				env.Server.Expect(
					// Attempt 1 batch 1 returns 1 entity + tx1.
					Expectation{
						Method: "RunQuery",
						Resp: &pb.RunQueryResponse{
							Transaction: tx1,
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults:    []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k1)}}},
								EndCursor:        []byte("ec1"),
								MoreResults:      pb.QueryResultBatch_NOT_FINISHED,
							},
						},
					},
					// Attempt 1 batch 2 aborts.
					Expectation{
						Method: "RunQuery",
						Err:    status.Error(codes.Aborted, "mid-query contention"),
					},
					Expectation{
						Method: "Rollback",
						Resp:   &pb.RollbackResponse{},
					},
					// Attempt 2 passes previous_transaction = tx1 and succeeds cleanly.
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							prev := req.(*pb.RunQueryRequest).GetReadOptions().GetNewTransaction().GetReadWrite().GetPreviousTransaction()
							if !bytes.Equal(prev, tx1) {
								return fmt.Errorf("expected previous_transaction=%q on retry, got %q", tx1, prev)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Transaction: tx2,
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults: []*pb.EntityResult{
									{Entity: &pb.Entity{Key: KeyToPB(k1)}},
									{Entity: &pb.Entity{Key: KeyToPB(k2)}},
								},
								MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
					Expectation{
						Method: "Commit",
						Resp:   &pb.CommitResponse{},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					BeginLater:  true,
					MaxAttempts: 2,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_RunQuery{RunQuery: &fixturepb.TxRunQueryStep{
							QuerySpec: &fixturepb.TxRunQueryStep_StructuredQuery{
								StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"},
							},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Aborted mid-query tx retry"); err != nil {
					return err
				}
				if resp.CallbackAttempts != 2 || len(resp.StepResults) != 1 || len(resp.StepResults[0].GetRunQueryResult().GetEntities()) != 2 {
					return fmt.Errorf("expected 2 clean entities from retry attempt 2, got attempts=%d results=%v", resp.CallbackAttempts, resp.StepResults)
				}
				return nil
			},
		},
		{
			Name:     "TxReadOnlyWithReadTime",
			Category: "transactions",
			SpecRef:  "Spec §8.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Acct", "ro")
				txID := []byte("tx-ro")
				readTS := time.Unix(1700000000, 0).UTC()

				env.Server.Expect(
					Expectation{
						Method: "BeginTransaction",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							ro := req.(*pb.BeginTransactionRequest).GetTransactionOptions().GetReadOnly()
							if ro == nil || !ro.GetReadTime().AsTime().Equal(readTS) {
								return fmt.Errorf("expected ReadOnly(read_time=%v), got %v", readTS, req)
							}
							return nil
						},
						Resp: &pb.BeginTransactionResponse{Transaction: txID},
					},
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{
						Method: "Commit",
						Resp:   &pb.CommitResponse{},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId: cid,
					ReadOnly: true,
					ReadTime: timestamppb.New(readTS),
					Steps: []*fixturepb.TransactionStep{
						{Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: k}}},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "ReadOnly tx with read_time")
			},
		},
		{
			Name:     "TxReadOnlyRejectsWrite",
			Category: "transactions",
			SpecRef:  "Spec §8.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				// Expect zero Commit RPCs; read-only tx must reject mutation buffering locally.
				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					ReadOnly:    true,
					BeginLater:  true,
					MaxAttempts: 1,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							Mutations: []*fixturepb.MutationSpec{{
								Op:     fixturepb.MutationOp_MUTATION_OP_UPSERT,
								Entity: &fixturepb.EntitySpec{Key: strKey("Task", "nope"), Properties: map[string]*fixturepb.ValueSpec{"x": intVal(1)}},
							}},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if resp.Error == nil {
					return fmt.Errorf("expected client-side error when buffering write in ReadOnly tx, got nil")
				}
				if resp.Error.Kind != fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION &&
					resp.Error.Kind != fixturepb.ErrorKind_ERROR_KIND_CLIENT_STATE {
					return fmt.Errorf("expected CLIENT_VALIDATION or CLIENT_STATE, got %s", FormatVeneerError(resp.Error))
				}
				return nil
			},
		},
	}
}
