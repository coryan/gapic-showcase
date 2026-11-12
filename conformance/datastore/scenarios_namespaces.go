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

func headerNamespaceScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "HeaderClientDefaultNamespace",
			Category: "headers",
			SpecRef:  "Spec §3.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-ns", "", "tenant-a")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Order", "o1")
				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if len(lr.Keys) != 1 || lr.Keys[0].GetPartitionId().GetNamespaceId() != "tenant-a" {
								return fmt.Errorf("expected key namespace tenant-a, got %v", lr.Keys)
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Missing: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							qr := req.(*pb.RunQueryRequest)
							if qr.GetPartitionId().GetNamespaceId() != "tenant-a" {
								return fmt.Errorf("expected RunQueryRequest.partition_id.namespace_id=tenant-a, got %q", qr.GetPartitionId().GetNamespaceId())
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
				)

				gResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(gResp.Error, "Get"); err != nil {
					return err
				}
				qResp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Order"},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(qResp.Error, "RunQuery")
			},
		},
		{
			Name:     "HeaderPerKeyAndQueryNamespaceOverride",
			Category: "headers",
			SpecRef:  "Spec §3.1, §4.1, §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-ns", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := &fixturepb.KeySpec{
					PartitionId: &fixturepb.PartitionSpec{NamespaceId: "ns-key"},
					Path: []*fixturepb.PathElementSpec{{
						Kind:   "Order",
						IdType: &fixturepb.PathElementSpec_Name{Name: "o2"},
					}},
				}

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if len(lr.Keys) != 1 || lr.Keys[0].GetPartitionId().GetNamespaceId() != "ns-key" {
								return fmt.Errorf("expected key namespace ns-key, got %v", lr.Keys)
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key: &pb.Key{
										PartitionId: &pb.PartitionId{
											ProjectId:   "proj-ns",
											NamespaceId: "ns-key",
										},
										Path: KeyToPB(k).Path,
									},
								},
								Version: 1,
							}},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							qr := req.(*pb.RunQueryRequest)
							if qr.GetPartitionId().GetNamespaceId() != "ns-query" {
								return fmt.Errorf("expected RunQuery partition namespace ns-query, got %q", qr.GetPartitionId().GetNamespaceId())
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
				)

				gResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(gResp.Error, "Get"); err != nil {
					return err
				}
				if err := AssertFixtureKeyEqual(gResp.GetResult().GetEntity().GetKey(), k); err != nil {
					return err
				}

				qResp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId:    cid,
					NamespaceId: "ns-query",
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Order"},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(qResp.Error, "RunQuery")
			},
		},
		{
			Name:     "HeaderKeyClientIndependenceAndEquality",
			Category: "headers",
			SpecRef:  "Spec §4.1, §4.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "p1", "db1", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "t1")
				serverReturnedKey := &pb.Key{
					PartitionId: &pb.PartitionId{
						ProjectId:  "p1",
						DatabaseId: "db1",
					},
					Path: KeyToPB(k).Path,
				}

				env.Server.Expect(Expectation{
					Method: "Lookup",
					Resp: &pb.LookupResponse{
						Found: []*pb.EntityResult{{
							Entity: &pb.Entity{
								Key: serverReturnedKey,
								Properties: map[string]*pb.Value{
									"ok": {ValueType: &pb.Value_BooleanValue{BooleanValue: true}},
								},
							},
							Version: 3,
						}},
					},
				})

				gResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(gResp.Error, "Get"); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(gResp.GetResult().GetEntity().GetKey(), k)
			},
		},
	}
}
