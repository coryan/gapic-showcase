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

func strKey(kind, name string) *fixturepb.KeySpec {
	return &fixturepb.KeySpec{
		Path: []*fixturepb.PathElementSpec{{
			Kind:   kind,
			IdType: &fixturepb.PathElementSpec_Name{Name: name},
		}},
	}
}

func intKey(kind string, id int64) *fixturepb.KeySpec {
	return &fixturepb.KeySpec{
		Path: []*fixturepb.PathElementSpec{{
			Kind:   kind,
			IdType: &fixturepb.PathElementSpec_Id{Id: id},
		}},
	}
}

func incompleteKey(kind string) *fixturepb.KeySpec {
	return &fixturepb.KeySpec{
		Path: []*fixturepb.PathElementSpec{{Kind: kind}},
	}
}

func headerRoutingScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "HeaderDefaultDatabase",
			Category: "headers",
			SpecRef:  "Spec §3.1, §3.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "test-project", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Task", "t1")
				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						if err := AssertRoutingHeaders(md, "test-project", "", tr); err != nil {
							return err
						}
						if lr.ProjectId != "test-project" || lr.DatabaseId != "" {
							return fmt.Errorf("got partition (%q, %q), want (\"test-project\", \"\")", lr.ProjectId, lr.DatabaseId)
						}
						return nil
					},
					Resp: &pb.LookupResponse{
						Missing: []*pb.EntityResult{{
							Entity:  &pb.Entity{Key: KeyToPB(k)},
							Version: 1,
						}},
					},
				})

				resp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId: cid,
					Key:      k,
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Get"); err != nil {
					return err
				}
				if resp.GetResult().GetFound() {
					return fmt.Errorf("expected found=false for missing entity, got true")
				}
				return nil
			},
		},
		{
			Name:     "HeaderNamedDatabaseRoutingAndEscaping",
			Category: "headers",
			SpecRef:  "Spec §3.1, §3.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				const proj = "proj-1"
				const db = "test-db#1"
				cid, cleanup, err := env.NewClient(ctx, proj, db, "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Item", "i1")
				validateHeader := func(methodProj, methodDB string, md metadata.MD, tr fixturepb.Transport) error {
					if err := AssertRoutingHeaders(md, proj, db, tr); err != nil {
						return err
					}
					if methodProj != proj || methodDB != db {
						return fmt.Errorf("wire partition (%q, %q) != (%q, %q)", methodProj, methodDB, proj, db)
					}
					return nil
				}

				env.Server.Expect(
					Expectation{
						Method: "Commit",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							cr := req.(*pb.CommitRequest)
							return validateHeader(cr.ProjectId, cr.DatabaseId, md, tr)
						},
						Resp: &pb.CommitResponse{
							MutationResults: []*pb.MutationResult{{Version: 10}},
						},
					},
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							return validateHeader(lr.ProjectId, lr.DatabaseId, md, tr)
						},
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{
								Entity: &pb.Entity{
									Key: KeyToPB(k),
									Properties: map[string]*pb.Value{
										"name": {ValueType: &pb.Value_StringValue{StringValue: "one"}},
									},
								},
								Version: 10,
							}},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							qr := req.(*pb.RunQueryRequest)
							return validateHeader(qr.ProjectId, qr.DatabaseId, md, tr)
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
					Expectation{
						Method: "AllocateIds",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							ar := req.(*pb.AllocateIdsRequest)
							return validateHeader(ar.ProjectId, ar.DatabaseId, md, tr)
						},
						Resp: &pb.AllocateIdsResponse{
							Keys: []*pb.Key{KeyToPB(intKey("Item", 77))},
						},
					},
				)

				putResp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key: k,
						Properties: map[string]*fixturepb.ValueSpec{
							"name": {ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "one"}},
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(putResp.Error, "Put"); err != nil {
					return err
				}

				getResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(getResp.Error, "Get"); err != nil {
					return err
				}
				if !getResp.GetResult().GetFound() {
					return fmt.Errorf("expected found=true")
				}

				qResp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Item"},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(qResp.Error, "RunQuery"); err != nil {
					return err
				}

				allocResp, err := env.Fixture.AllocateIds(ctx, &fixturepb.AllocateIdsFixtureRequest{
					ClientId:       cid,
					IncompleteKeys: []*fixturepb.KeySpec{incompleteKey("Item")},
				})
				if err != nil {
					return err
				}
				return CheckNoError(allocResp.Error, "AllocateIds")
			},
		},
	}
}
