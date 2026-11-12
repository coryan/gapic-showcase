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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mutationAdvancedScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "MutationPartialUpdatePropertyMask",
			Category: "mutations",
			SpecRef:  "Spec §7.5",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-pm", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "p1")
				env.Server.Expect(Expectation{
					Method: "Commit",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cr := req.(*pb.CommitRequest)
						m := cr.Mutations[0]
						if m.GetUpdate() == nil || m.PropertyMask == nil {
							return fmt.Errorf("expected Update with PropertyMask, got %v", m)
						}
						if len(m.PropertyMask.Paths) != 2 || m.PropertyMask.Paths[0] != "status" || m.PropertyMask.Paths[1] != "count" {
							return fmt.Errorf("unexpected PropertyMask paths: %v", m.PropertyMask.Paths)
						}
						return nil
					},
					Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 1}}},
				})

				resp, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{{
						Op: fixturepb.MutationOp_MUTATION_OP_UPDATE,
						Entity: &fixturepb.EntitySpec{
							Key: k,
							Properties: map[string]*fixturepb.ValueSpec{
								"status": {ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "done"}},
								"count":  {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 3}},
							},
						},
						PropertyMask: &fixturepb.PropertyMaskSpec{Paths: []string{"status", "count"}},
					}},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "Mutate PropertyMask")
			},
		},
		{
			Name:     "MutationPropertyTransformsAndEmptyMask",
			Category: "mutations",
			SpecRef:  "Spec §7.4, §7.5",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-tr", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Counter", "c1")
				transforms := []*fixturepb.PropertyTransformSpec{
					{Property: "updated_at", TransformType: &fixturepb.PropertyTransformSpec_SetToServerValue{SetToServerValue: fixturepb.ServerValueSpec_SERVER_VALUE_REQUEST_TIME}},
					{Property: "views", TransformType: &fixturepb.PropertyTransformSpec_Increment{Increment: &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 5}}}},
					{Property: "hi", TransformType: &fixturepb.PropertyTransformSpec_Maximum{Maximum: &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 100}}}},
					{Property: "lo", TransformType: &fixturepb.PropertyTransformSpec_Minimum{Minimum: &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_DoubleValue{DoubleValue: 1.5}}}},
					{Property: "tags", TransformType: &fixturepb.PropertyTransformSpec_AppendMissingElements{AppendMissingElements: &fixturepb.ArraySpec{Values: []*fixturepb.ValueSpec{{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "a"}}}}}},
					{Property: "tags", TransformType: &fixturepb.PropertyTransformSpec_RemoveAllFromArray{RemoveAllFromArray: &fixturepb.ArraySpec{Values: []*fixturepb.ValueSpec{{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "b"}}}}}},
				}

				env.Server.Expect(Expectation{
					Method: "Commit",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cr := req.(*pb.CommitRequest)
						m := cr.Mutations[0]
						if m.GetUpdate() == nil || m.PropertyMask == nil || len(m.PropertyMask.Paths) != 0 {
							return fmt.Errorf("expected Update with initialized empty PropertyMask, got mask=%v", m.PropertyMask)
						}
						pts := m.PropertyTransforms
						if len(pts) != 6 {
							return fmt.Errorf("expected 6 PropertyTransforms, got %d", len(pts))
						}
						if pts[0].GetSetToServerValue() != pb.PropertyTransform_REQUEST_TIME ||
							pts[1].GetIncrement().GetIntegerValue() != 5 ||
							pts[2].GetMaximum().GetIntegerValue() != 100 ||
							pts[3].GetMinimum().GetDoubleValue() != 1.5 ||
							len(pts[4].GetAppendMissingElements().GetValues()) != 1 ||
							len(pts[5].GetRemoveAllFromArray().GetValues()) != 1 {
							return fmt.Errorf("unexpected PropertyTransforms on wire: %v", pts)
						}
						return nil
					},
					Resp: &pb.CommitResponse{
						MutationResults: []*pb.MutationResult{{Version: 8}},
					},
				})

				resp, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{{
						Op:           fixturepb.MutationOp_MUTATION_OP_UPDATE,
						Entity:       &fixturepb.EntitySpec{Key: k},
						PropertyMask: &fixturepb.PropertyMaskSpec{Paths: []string{}},
						Transforms:   transforms,
					}},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "Mutate PropertyTransforms")
			},
		},
		{
			Name:     "MutationExposesMutationResultMetadata",
			Category: "mutations",
			SpecRef:  "Spec §7.2, §7.4",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-mr", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Counter", "c1")
				ut := timestamppb.New(time.Unix(1700000500, 0).UTC())

				env.Server.Expect(Expectation{
					Method: "Commit",
					Resp: &pb.CommitResponse{
						MutationResults: []*pb.MutationResult{{
							Key:        KeyToPB(k),
							Version:    42,
							UpdateTime: ut,
							TransformResults: []*pb.Value{
								{ValueType: &pb.Value_IntegerValue{IntegerValue: 15}},
							},
						}},
					},
				})

				resp, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{{
						Op:           fixturepb.MutationOp_MUTATION_OP_UPSERT,
						Entity:       &fixturepb.EntitySpec{Key: k},
						PropertyMask: &fixturepb.PropertyMaskSpec{},
						Transforms: []*fixturepb.PropertyTransformSpec{{
							Property:      "views",
							TransformType: &fixturepb.PropertyTransformSpec_Increment{Increment: &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 5}}},
						}},
					}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Mutate"); err != nil {
					return err
				}
				if len(resp.Results) != 1 {
					return fmt.Errorf("expected 1 result, got %d", len(resp.Results))
				}
				mr := resp.Results[0]
				if mr.Version != 42 || !mr.GetUpdateTime().AsTime().Equal(ut.AsTime()) {
					return fmt.Errorf("expected version=42 update_time=%v, got version=%d update_time=%v", ut.AsTime(), mr.Version, mr.GetUpdateTime().AsTime())
				}
				if len(mr.TransformResults) != 1 || mr.TransformResults[0].GetIntegerValue() != 15 {
					return fmt.Errorf("expected transform_results=[15], got %v", mr.TransformResults)
				}
				return nil
			},
		},
		{
			Name:     "MutationConflictPreconditions",
			Category: "mutations",
			SpecRef:  "Spec §7.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-pre", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Item", "p1")
				bv := int64(7)
				ut := time.Unix(1700000300, 0).UTC()

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							m := cr.Mutations[0]
							if m.GetBaseVersion() != 7 {
								return fmt.Errorf("expected base_version=7, got %v", m.ConflictDetectionStrategy)
							}
							return nil
						},
						Resp: &pb.CommitResponse{MutationResults: []*pb.MutationResult{{Version: 8}}},
					},
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							m := cr.Mutations[0]
							if got := m.GetUpdateTime(); got == nil || !got.AsTime().Equal(ut) {
								return fmt.Errorf("expected update_time=%v, got %v", ut, m.ConflictDetectionStrategy)
							}
							return nil
						},
						Err: status.Error(codes.Aborted, "precondition update_time mismatch"),
					},
				)

				r1, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{{
						Op:          fixturepb.MutationOp_MUTATION_OP_UPDATE,
						Entity:      &fixturepb.EntitySpec{Key: k, Properties: map[string]*fixturepb.ValueSpec{"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}}},
						BaseVersion: &bv,
					}},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "Mutate base_version"); err != nil {
					return err
				}

				r2, err := env.Fixture.Mutate(ctx, &fixturepb.MutateFixtureRequest{
					ClientId: cid,
					Mutations: []*fixturepb.MutationSpec{{
						Op:         fixturepb.MutationOp_MUTATION_OP_UPDATE,
						Entity:     &fixturepb.EntitySpec{Key: k, Properties: map[string]*fixturepb.ValueSpec{"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 2}}}},
						UpdateTime: timestamppb.New(ut),
					}},
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(r2.Error, fixturepb.ErrorKind_ERROR_KIND_SERVICE, fixturepb.RpcCode_RPC_CODE_ABORTED)
			},
		},
	}
}
