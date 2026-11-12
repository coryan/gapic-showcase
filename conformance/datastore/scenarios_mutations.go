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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func mutationBasicScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "MutationPutUpsertAndIncompleteInsert",
			Category: "mutations",
			SpecRef:  "Spec §7.1, §7.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mut", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				completeK := strKey("Task", "c1")
				incompleteK := incompleteKey("Task")
				allocatedK := intKey("Task", 888)

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if err := AssertRoutingHeaders(md, "proj-mut", "", tr); err != nil {
								return err
							}
							if cr.Mode != pb.CommitRequest_NON_TRANSACTIONAL {
								return fmt.Errorf("expected NON_TRANSACTIONAL Commit, got %v", cr.Mode)
							}
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpsert() == nil {
								return fmt.Errorf("expected 1 Upsert for complete key, got %v", cr.Mutations)
							}
							return nil
						},
						// Omitting MutationResult.Key for complete key; veneer must normalize back to completeK.
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{Version: 1}},
						},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if cr.Mode != pb.CommitRequest_NON_TRANSACTIONAL {
								return fmt.Errorf("expected NON_TRANSACTIONAL Commit")
							}
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetInsert() == nil {
								return fmt.Errorf("expected 1 Insert for incomplete key, got %v", cr.Mutations)
							}
							return nil
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{
								Key:     KeyToPB(allocatedK),
								Version: 2,
							}},
						},
					},
				)

				r1, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key:        completeK,
						Properties: map[string]*fixturepb.ValueSpec{"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "Put complete"); err != nil {
					return err
				}
				if err := AssertFixtureKeyEqual(r1.Key, completeK); err != nil {
					return err
				}

				r2, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key:        incompleteK,
						Properties: map[string]*fixturepb.ValueSpec{"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 2}}},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r2.Error, "Put incomplete"); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(r2.Key, allocatedK)
			},
		},
		{
			Name:     "MutationInsertAlreadyExistsWithErrorInfo",
			Category: "mutations",
			SpecRef:  "Spec §3.5, §7.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mut", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "t1")
				ent := &fixturepb.EntitySpec{
					Key:        k,
					Properties: map[string]*fixturepb.ValueSpec{"a": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}},
				}

				st := status.New(codes.AlreadyExists, "entity already exists")
				stWithDetails, err := st.WithDetails(&errdetails.ErrorInfo{
					Reason:   "ENTITY_EXISTS",
					Domain:   "datastore.googleapis.com",
					Metadata: map[string]string{"key": "t1"},
				})
				if err != nil {
					return err
				}

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetInsert() == nil {
								return fmt.Errorf("expected Insert mutation")
							}
							return nil
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 1}}},
					},
					Expectation{
						Method: "Commit",
						Err:    stWithDetails.Err(),
					},
				)

				r1, err := env.Fixture.Insert(ctx, &fixturepb.InsertFixtureRequest{ClientId: cid, Entity: ent})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "first Insert"); err != nil {
					return err
				}

				r2, err := env.Fixture.Insert(ctx, &fixturepb.InsertFixtureRequest{ClientId: cid, Entity: ent})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(r2.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_ALREADY_EXISTS); err != nil {
					return err
				}
				if r2.Error.ErrorInfo == nil {
					return fmt.Errorf("expected ErrorInfo to be preserved on structured service error")
				}
				ei := r2.Error.ErrorInfo
				if ei.Reason != "ENTITY_EXISTS" || ei.Domain != "datastore.googleapis.com" || ei.Metadata["key"] != "t1" {
					return fmt.Errorf("unexpected ErrorInfo: %v", ei)
				}
				return nil
			},
		},
		{
			Name:     "MutationUpdateNotFound",
			Category: "mutations",
			SpecRef:  "Spec §7.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mut", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "missing")
				env.Server.Expect(Expectation{
					Method: "Commit",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cr := req.(*pb.CommitRequest)
						if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpdate() == nil {
							return fmt.Errorf("expected Update mutation")
						}
						return nil
					},
					Err: status.Error(codes.NotFound, "no entity to update"),
				})

				resp, err := env.Fixture.Update(ctx, &fixturepb.UpdateFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key:        k,
						Properties: map[string]*fixturepb.ValueSpec{"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 9}}},
					},
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_NOT_FOUND)
			},
		},
		{
			Name:     "MutationDeleteAndDeleteMultiDedup",
			Category: "mutations",
			SpecRef:  "Spec §7.1, §7.6",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mut", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Task", "d1")
				k2 := strKey("Task", "d2")

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 1 || cr.Mutations[0].GetDelete() == nil {
								return fmt.Errorf("expected 1 Delete mutation")
							}
							return AssertWireKeyMatches(cr.Mutations[0].GetDelete(), k1, "proj-mut", "")
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 1}}},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							if len(cr.Mutations) != 2 {
								return fmt.Errorf("expected 2 deduplicated Delete mutations, got %d", len(cr.Mutations))
							}
							if err := AssertWireKeyMatches(cr.Mutations[0].GetDelete(), k1, "proj-mut", ""); err != nil {
								return err
							}
							return AssertWireKeyMatches(cr.Mutations[1].GetDelete(), k2, "proj-mut", "")
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 2}, {Version: 3}}},
					},
				)

				d1, err := env.Fixture.Delete(ctx, &fixturepb.DeleteFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{k1},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(d1.Error, "Delete"); err != nil {
					return err
				}

				d2, err := env.Fixture.Delete(ctx, &fixturepb.DeleteFixtureRequest{
					ClientId: cid,
					Batch:    true,
					Keys:     []*fixturepb.KeySpec{k1, k2, k1},
				})
				if err != nil {
					return err
				}
				return CheckNoError(d2.Error, "DeleteMulti")
			},
		},
		{
			Name:     "MutationAtomicBatchMutate",
			Category: "mutations",
			SpecRef:  "Spec §7.1, §7.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mut", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				incK := incompleteKey("Item")
				allocK := intKey("Item", 55)
				k1 := strKey("Item", "k1")
				k2 := strKey("Item", "k2")
				k3 := strKey("Item", "k3")

				env.Server.Expect(Expectation{
					Method: "Commit",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cr := req.(*pb.CommitRequest)
						if cr.Mode != pb.CommitRequest_NON_TRANSACTIONAL || len(cr.Mutations) != 4 {
							return fmt.Errorf("expected 4 NON_TRANSACTIONAL mutations, got %v", cr)
						}
						if cr.Mutations[0].GetInsert() == nil || cr.Mutations[1].GetUpsert() == nil ||
							cr.Mutations[2].GetUpdate() == nil || cr.Mutations[3].GetDelete() == nil {
							return fmt.Errorf("unexpected mutation kinds order: %v", cr.Mutations)
						}
						return nil
					},
					Resp: &pb.CommitResponse{
						MutationResults: []*pb.MutationResult{
							{Key: KeyToPB(allocK), Version: 1},
							{Version: 2},
							{Version: 3},
							{Version: 4},
						},
					},
				})

				resp, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{
						{Op: fixturepb.MutationOp_MUTATION_OP_INSERT, Entity: &fixturepb.EntitySpec{Key: incK, Properties: map[string]*fixturepb.ValueSpec{"v": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 0}}}}},
						{Op: fixturepb.MutationOp_MUTATION_OP_UPSERT, Entity: &fixturepb.EntitySpec{Key: k1, Properties: map[string]*fixturepb.ValueSpec{"v": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}}}},
						{Op: fixturepb.MutationOp_MUTATION_OP_UPDATE, Entity: &fixturepb.EntitySpec{Key: k2, Properties: map[string]*fixturepb.ValueSpec{"v": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 2}}}}},
						{Op: fixturepb.MutationOp_MUTATION_OP_DELETE, DeleteKey: k3},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Mutate"); err != nil {
					return err
				}
				if len(resp.Results) != 4 {
					return fmt.Errorf("expected 4 results, got %d", len(resp.Results))
				}
				return AssertFixtureKeyEqual(resp.Results[0].Key, allocK)
			},
		},
	}
}
