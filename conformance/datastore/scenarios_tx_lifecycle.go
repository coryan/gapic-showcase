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
	"sync/atomic"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func txLifecycleScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "TxLazyBeginOnFirstReadAndCommit",
			Category: "transactions",
			SpecRef:  "Spec §8.1, §8.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Acct", "a1")
				k2 := strKey("Acct", "a2")
				kDel := strKey("Acct", "old")
				txID := []byte("tx-lazy-1")

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if err := AssertRoutingHeaders(md, "proj-tx", "", tr); err != nil {
								return err
							}
							if _, ok := lr.GetReadOptions().GetConsistencyType().(*pb.ReadOptions_NewTransaction); !ok {
								return fmt.Errorf("expected ReadOptions.new_transaction on first read, got %v", lr.ReadOptions)
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Transaction: txID,
							Found: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key:        KeyToPB(k1),
									Properties: map[string]*pb.Value{"bal": {ValueType: &pb.Value_IntegerValue{IntegerValue: 100}}},
								},
								Version: 1,
							}},
						},
					},
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if !bytes.Equal(lr.GetReadOptions().GetTransaction(), txID) {
								return fmt.Errorf("expected second read to use transaction %q, got %v", txID, lr.ReadOptions)
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key:        KeyToPB(k2),
									Properties: map[string]*pb.Value{"bal": {ValueType: &pb.Value_IntegerValue{IntegerValue: 50}}},
								},
								Version: 1,
							}},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							qr := req.(*pb.RunQueryRequest)
							if !bytes.Equal(qr.GetReadOptions().GetTransaction(), txID) {
								return fmt.Errorf("expected tx RunQuery to use transaction %q, got %v", txID, qr.ReadOptions)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
					Expectation{
						Method: "RunAggregationQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							ar := req.(*pb.RunAggregationQueryRequest)
							if !bytes.Equal(ar.GetReadOptions().GetTransaction(), txID) {
								return fmt.Errorf("expected tx RunAggregationQuery to use transaction %q, got %v", txID, ar.ReadOptions)
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
						},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if cr.Mode != pb.CommitRequest_TRANSACTIONAL || !bytes.Equal(cr.GetTransaction(), txID) {
								return fmt.Errorf("expected TRANSACTIONAL Commit with %q, got %v", txID, cr)
							}
							if len(cr.Mutations) != 2 || cr.Mutations[0].GetUpsert() == nil || cr.Mutations[1].GetDelete() == nil {
								return fmt.Errorf("expected Upsert + Delete mutations, got %v", cr.Mutations)
							}
							return nil
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{Version: 2}, {Version: 3}},
						},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:   cid,
					BeginLater: true,
					Steps: []*fixturepb.TransactionStep{
						{Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: k1}}},
						{Step: &fixturepb.TransactionStep_Get{Get: &fixturepb.TxGetStep{Key: k2}}},
						{Step: &fixturepb.TransactionStep_RunQuery{RunQuery: &fixturepb.TxRunQueryStep{
							QuerySpec: &fixturepb.TxRunQueryStep_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Acct"}},
						}}},
						{Step: &fixturepb.TransactionStep_RunAggregationQuery{RunAggregationQuery: &fixturepb.TxRunAggregationQueryStep{
							BaseQuery:    &fixturepb.TxRunAggregationQueryStep_StructuredQuery{StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Acct"}},
							Aggregations: []*fixturepb.AggregationSpec{{Op: fixturepb.AggregationOp_AGGREGATION_OP_COUNT, Alias: "cnt"}},
						}}},
						{Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							Mutations: []*fixturepb.MutationSpec{
								{Op: fixturepb.MutationOp_MUTATION_OP_UPSERT, Entity: &fixturepb.EntitySpec{Key: k1, Properties: map[string]*fixturepb.ValueSpec{"bal": intVal(80)}}},
								{Op: fixturepb.MutationOp_MUTATION_OP_DELETE, DeleteKey: kDel},
							},
						}}},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "RunInTransaction lazy"); err != nil {
					return err
				}
				if resp.CallbackAttempts != 1 {
					return fmt.Errorf("expected 1 callback attempt, got %d", resp.CallbackAttempts)
				}
				return nil
			},
		},
		{
			Name:     "TxBeginLaterParallelReadsSerialized",
			Category: "transactions",
			SpecRef:  "Spec §8.2, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Acct", "p1")
				k2 := strKey("Acct", "p2")
				txID := []byte("tx-serial")
				var firstStarted atomic.Bool

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Handle: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) (proto.Message, error) {
							lr := req.(*pb.LookupRequest)
							if _, ok := lr.GetReadOptions().GetConsistencyType().(*pb.ReadOptions_NewTransaction); !ok {
								return nil, status.Errorf(codes.FailedPrecondition, "first parallel read must send new_transaction, got %v", lr.ReadOptions)
							}
							firstStarted.Store(true)
							// Hold open briefly so an unsynchronized second concurrent read would race here.
							time.Sleep(60 * time.Millisecond)
							return &pb.LookupResponse{
								Transaction: txID,
								Found: []*pb.EntityResult{{
									Entity:  &pb.Entity{Key: lr.Keys[0]},
									Version: 1,
								}},
							}, nil
						},
					},
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if !firstStarted.Load() {
								return fmt.Errorf("second read arrived before first")
							}
							if !bytes.Equal(lr.GetReadOptions().GetTransaction(), txID) {
								return fmt.Errorf("concurrent second read did not wait for serialized tx_id; sent %v", lr.ReadOptions)
							}
							return nil
						},
						Handle: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) (proto.Message, error) {
							lr := req.(*pb.LookupRequest)
							return &pb.LookupResponse{
								Found: []*pb.EntityResult{{
									Entity:  &pb.Entity{Key: lr.Keys[0]},
									Version: 1,
								}},
							}, nil
						},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if !bytes.Equal(cr.GetTransaction(), txID) {
								return fmt.Errorf("expected Commit with %q, got %v", txID, cr.TransactionSelector)
							}
							return nil
						},
						Resp: &pb.CommitResponse{},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId:   cid,
					BeginLater: true,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_ParallelReads{
							ParallelReads: &fixturepb.TxParallelReadsStep{Keys: []*fixturepb.KeySpec{k1, k2}},
						},
					}},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "parallel reads in BeginLater tx")
			},
		},
		{
			Name:     "TxPendingKeyResolution",
			Category: "transactions",
			SpecRef:  "Spec §8.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tx", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				inc := incompleteKey("Task")
				alloc := intKey("Task", 9009)
				txID := []byte("tx-pk")

				env.Server.Expect(
					Expectation{
						Method: "BeginTransaction",
						Resp:   &pb.BeginTransactionResponse{Transaction: txID},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetInsert() == nil {
								return fmt.Errorf("expected 1 Insert mutation")
							}
							return nil
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{
								Key:     KeyToPB(alloc),
								Version: 1,
							}},
						},
					},
				)

				resp, err := env.Fixture.RunInTransaction(ctx, &fixturepb.RunInTransactionFixtureRequest{
					ClientId: cid,
					Steps: []*fixturepb.TransactionStep{{
						Step: &fixturepb.TransactionStep_Mutate{Mutate: &fixturepb.TxMutateStep{
							UseConvenienceMethods: true,
							PendingKeyIds:         []string{"pk1"},
							Mutations: []*fixturepb.MutationSpec{{
								Op: fixturepb.MutationOp_MUTATION_OP_INSERT,
								Entity: &fixturepb.EntitySpec{
									Key:        inc,
									Properties: map[string]*fixturepb.ValueSpec{"k": strVal("v")},
								},
							}},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "PendingKey resolution"); err != nil {
					return err
				}
				resolved, ok := resp.ResolvedPendingKeys["pk1"]
				if !ok {
					return fmt.Errorf("missing resolved pending key \"pk1\"")
				}
				return AssertFixtureKeyEqual(resolved, alloc)
			},
		},
	}
}
