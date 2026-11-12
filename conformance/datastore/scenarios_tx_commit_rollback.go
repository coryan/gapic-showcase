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

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func txCommitRollbackScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "TxWriteOnlyAndZeroRPCEarlyAbort",
			Category: "transactions",
			SpecRef:  "Spec §8.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Log", "w1")
				txID := []byte("tx-writeonly")

				// Part 1: write-only lazy transaction begins + commits.
				env.Server.Expect(
					Expectation{
						Method: "BeginTransaction",
						Resp:   &pb.BeginTransactionResponse{Transaction: txID},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpsert() == nil {
								return fmt.Errorf("expected 1 Upsert mutation")
							}
							return nil
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 1}}},
					},
				)

				r1, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:   cid,
					BeginLater: true,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							Mutations: []*fixturepb.MutationSpec{{
								Op:     fixturepb.MutationOp_MUTATION_OP_UPSERT,
								Entity: &fixturepb.EntitySpec{Key: k, Properties: map[string]*fixturepb.ValueSpec{"ok": {ValueType: &fixturepb.ValueSpec_BooleanValue{BooleanValue: true}}}},
							}},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "write-only lazy tx"); err != nil {
					return err
				}
				if err := env.Server.VerifyAllExpectationsMet(); err != nil {
					return err
				}
				env.Server.Reset()

				// Part 2: BeginLater + callback fails before any read -> zero RPCs (local Rollback no-op).
				r2, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:   cid,
					BeginLater: true,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_FailCallback{FailCallback: &fixturepb.TxFailStep{Message: "abort early"}},
					}},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(r2.Error, fixturepb.ErrorKind_ERROR_KIND_CALLBACK); err != nil {
					return err
				}
				if len(env.Server.Calls()) != 0 {
					return fmt.Errorf("expected 0 RPCs on zero-read BeginLater abort, got %d", len(env.Server.Calls()))
				}
				return nil
			},
		},
		{
			Name:     "TxCallbackErrorTriggersRollback",
			Category: "transactions",
			SpecRef:  "Spec §8.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Acct", "a1")
				txID := []byte("tx-rb")

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Transaction: txID,
							Missing:     []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{
						Method: "Rollback",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							rb := req.(*pb.RollbackRequest)
							if !bytes.Equal(rb.Transaction, txID) {
								return fmt.Errorf("expected Rollback for %q, got %q", txID, rb.Transaction)
							}
							return nil
						},
						Resp: &pb.RollbackResponse{},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					BeginLater:  true,
					MaxAttempts: 3,
					Steps: []*fixturepb.TransactionStep{
						{Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: k}}},
						{Step: &fixturepb.TransactionStep_FailCallback{FailCallback: &fixturepb.TxFailStep{Message: "business error"}}},
					},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_CALLBACK); err != nil {
					return err
				}
				if resp.CallbackAttempts != 1 {
					return fmt.Errorf("callback error must not retry; expected 1 attempt, got %d", resp.CallbackAttempts)
				}
				return nil
			},
		},
		{
			Name:     "TxAbortedCommitRetriesWithPreviousTransaction",
			Category: "transactions",
			SpecRef:  "Spec §8.1, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Acct", "hot")
				tx1 := []byte("tx-attempt-1")
				tx2 := []byte("tx-attempt-2")

				// Scenario A: Commit 1 returns ABORTED, attempt 2 passes previous_transaction = tx1 and succeeds.
				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Transaction: tx1,
							Found:       []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{
						Method: "Commit",
						Err:    status.Error(codes.Aborted, "contention"),
					},
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							prev := lr.GetReadOptions().GetNewTransaction().GetReadWrite().GetPreviousTransaction()
							if !bytes.Equal(prev, tx1) {
								return fmt.Errorf("expected ReadWrite.previous_transaction=%q on retry, got %q", tx1, prev)
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Transaction: tx2,
							Found:       []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{
						Method: "Commit",
						Resp:   &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 2}}},
					},
				)

				r1, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					BeginLater:  true,
					MaxAttempts: 3,
					Steps: []*fixturepb.TransactionStep{
						{Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: k}}},
						{Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							Mutations: []*fixturepb.MutationSpec{{
								Op:     fixturepb.MutationOp_MUTATION_OP_UPSERT,
								Entity: &fixturepb.EntitySpec{Key: k, Properties: map[string]*fixturepb.ValueSpec{"n": intVal(1)}},
							}},
						}}},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "Aborted retry"); err != nil {
					return err
				}
				if r1.CallbackAttempts != 2 {
					return fmt.Errorf("expected 2 callback attempts, got %d", r1.CallbackAttempts)
				}
				if err := env.Server.VerifyAllExpectationsMet(); err != nil {
					return err
				}
				env.Server.Reset()

				// Scenario B: Exhausting max_attempts = 2 returns terminal ABORTED.
				env.Server.Expect(
					Expectation{Method: "BeginTransaction", Resp: &pb.BeginTransactionResponse{Transaction: tx1}},
					Expectation{Method: "Commit", Err: status.Error(codes.Aborted, "conflict 1")},
					Expectation{
						Method: "BeginTransaction",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							prev := req.(*pb.BeginTransactionRequest).GetTransactionOptions().GetReadWrite().GetPreviousTransaction()
							if !bytes.Equal(prev, tx1) {
								return fmt.Errorf("expected BeginTransaction previous_transaction=%q, got %q", tx1, prev)
							}
							return nil
						},
						Resp: &pb.BeginTransactionResponse{Transaction: tx2},
					},
					Expectation{Method: "Commit", Err: status.Error(codes.Aborted, "conflict 2")},
				)

				r2, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:    cid,
					MaxAttempts: 2,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							Mutations: []*fixturepb.MutationSpec{{
								Op:     fixturepb.MutationOp_MUTATION_OP_UPSERT,
								Entity: &fixturepb.EntitySpec{Key: k, Properties: map[string]*fixturepb.ValueSpec{"n": intVal(2)}},
							}},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(r2.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_ABORTED); err != nil {
					return err
				}
				if r2.CallbackAttempts != 2 {
					return fmt.Errorf("expected 2 attempts before exhausting max_attempts, got %d", r2.CallbackAttempts)
				}
				return nil
			},
		},
	}
}
