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

func keyIDScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "KeyCompleteIDAndNameRoundTrip",
			Category: "keys",
			SpecRef:  "Spec §4.1, §4.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-keys", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				kID := intKey("Task", 42)
				kName := strKey("Task", "alpha-key")

				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						if err := AssertRoutingHeaders(md, "proj-keys", "", tr); err != nil {
							return err
						}
						if len(lr.Keys) != 2 {
							return fmt.Errorf("expected 2 keys, got %d", len(lr.Keys))
						}
						if err := AssertWireKeyMatches(lr.Keys[0], kID, "proj-keys", ""); err != nil {
							return err
						}
						return AssertWireKeyMatches(lr.Keys[1], kName, "proj-keys", "")
					},
					Resp: &pb.LookupResponse{
						Found: []*pb.EntityResult{
							{Entity: &pb.Entity{Key: KeyToPB(kID)}, Version: 1},
							{Entity: &pb.Entity{Key: KeyToPB(kName)}, Version: 2},
						},
					},
				})

				resp, err := env.Fixture.GetMulti(ctx, &fixturepb.GetMultiFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{kID, kName},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "GetMulti"); err != nil {
					return err
				}
				if len(resp.Results) != 2 || !resp.Results[0].Found || !resp.Results[1].Found {
					return fmt.Errorf("expected 2 found results, got %v", resp.Results)
				}
				if err := AssertFixtureKeyEqual(resp.Results[0].Entity.Key, kID); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(resp.Results[1].Entity.Key, kName)
			},
		},
		{
			Name:     "KeyMultiLevelAncestorPath",
			Category: "keys",
			SpecRef:  "Spec §4.1, §4.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-keys", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				deepKey := &fixturepb.KeySpec{
					Path: []*fixturepb.PathElementSpec{
						{Kind: "Org", IdType: &fixturepb.PathElementSpec_Name{Name: "acme"}},
						{Kind: "Project", IdType: &fixturepb.PathElementSpec_Id{Id: 100}},
						{Kind: "Task", IdType: &fixturepb.PathElementSpec_Name{Name: "build"}},
					},
				}

				env.Server.Expect(Expectation{
					Method: "Commit",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cr := req.(*pb.CommitRequest)
						if len(cr.Mutations) != 1 || cr.Mutations[0].GetUpsert() == nil {
							return fmt.Errorf("expected 1 Upsert mutation, got %v", cr.Mutations)
						}
						return AssertWireKeyMatches(cr.Mutations[0].GetUpsert().Key, deepKey, "proj-keys", "")
					},
					Resp: &pb.CommitResponse{
						MutationResults: []*pb.MutationResult{{Version: 1}},
					},
				})

				resp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key: deepKey,
						Properties: map[string]*fixturepb.ValueSpec{
							"ok": {ValueType: &fixturepb.ValueSpec_BooleanValue{BooleanValue: true}},
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Put"); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(resp.Key, deepKey)
			},
		},
		{
			Name:     "KeyAllocateIds",
			Category: "keys",
			SpecRef:  "Spec §4.5",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-alloc", "named-db", "")
				if err != nil {
					return err
				}
				defer cleanup()

				incRoot := incompleteKey("Task")
				incChild := &fixturepb.KeySpec{
					Path: []*fixturepb.PathElementSpec{
						{Kind: "Parent", IdType: &fixturepb.PathElementSpec_Name{Name: "p1"}},
						{Kind: "Task"},
					},
				}
				alloc1 := intKey("Task", 1001)
				alloc2 := &fixturepb.KeySpec{
					Path: []*fixturepb.PathElementSpec{
						{Kind: "Parent", IdType: &fixturepb.PathElementSpec_Name{Name: "p1"}},
						{Kind: "Task", IdType: &fixturepb.PathElementSpec_Id{Id: 2002}},
					},
				}

				env.Server.Expect(Expectation{
					Method: "AllocateIds",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						ar := req.(*pb.AllocateIdsRequest)
						if err := AssertRoutingHeaders(md, "proj-alloc", "named-db", tr); err != nil {
							return err
						}
						if len(ar.Keys) != 2 {
							return fmt.Errorf("expected 2 incomplete keys, got %d", len(ar.Keys))
						}
						if err := AssertWireKeyMatches(ar.Keys[0], incRoot, "proj-alloc", "named-db"); err != nil {
							return err
						}
						return AssertWireKeyMatches(ar.Keys[1], incChild, "proj-alloc", "named-db")
					},
					Resp: &pb.AllocateIdsResponse{
						Keys: []*pb.Key{KeyToPB(alloc1), KeyToPB(alloc2)},
					},
				})

				resp, err := env.Fixture.AllocateIds(ctx, &fixturepb.AllocateIdsFixtureRequest{
					ClientId:       cid,
					IncompleteKeys: []*fixturepb.KeySpec{incRoot, incChild},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "AllocateIds"); err != nil {
					return err
				}
				if len(resp.AllocatedKeys) != 2 {
					return fmt.Errorf("expected 2 allocated keys, got %d", len(resp.AllocatedKeys))
				}
				if err := AssertFixtureKeyEqual(resp.AllocatedKeys[0], alloc1); err != nil {
					return err
				}
				return AssertFixtureKeyEqual(resp.AllocatedKeys[1], alloc2)
			},
		},
		{
			Name:     "KeyReserveIds",
			Category: "keys",
			SpecRef:  "Spec §4.5",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-res", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := intKey("Task", 501)
				k2 := intKey("Task", 502)

				env.Server.Expect(Expectation{
					Method: "ReserveIds",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						rr := req.(*pb.ReserveIdsRequest)
						if err := AssertRoutingHeaders(md, "proj-res", "", tr); err != nil {
							return err
						}
						if rr.ProjectId != "proj-res" || len(rr.Keys) != 2 {
							return fmt.Errorf("unexpected ReserveIdsRequest: %v", rr)
						}
						if err := AssertWireKeyMatches(rr.Keys[0], k1, "proj-res", ""); err != nil {
							return err
						}
						return AssertWireKeyMatches(rr.Keys[1], k2, "proj-res", "")
					},
					Resp: &pb.ReserveIdsResponse{},
				})

				resp, err := env.Fixture.ReserveIds(ctx, &fixturepb.ReserveIdsFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{k1, k2},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "ReserveIds")
			},
		},
		{
			Name:     "KeyReserveIdsSequential",
			Category: "keys",
			SpecRef:  "Spec §4.5",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-res-seq", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				start := intKey("Task", 100)
				want := []*fixturepb.KeySpec{
					intKey("Task", 100),
					intKey("Task", 101),
					intKey("Task", 102),
				}

				env.Server.Expect(Expectation{
					Method: "ReserveIds",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						rr := req.(*pb.ReserveIdsRequest)
						if err := AssertRoutingHeaders(md, "proj-res-seq", "", tr); err != nil {
							return err
						}
						if len(rr.Keys) != 3 {
							return fmt.Errorf("expected 3 sequential keys, got %d", len(rr.Keys))
						}
						for i, wk := range want {
							if err := AssertWireKeyMatches(rr.Keys[i], wk, "proj-res-seq", ""); err != nil {
								return fmt.Errorf("key[%d]: %w", i, err)
							}
						}
						return nil
					},
					Resp: &pb.ReserveIdsResponse{},
				})

				resp, err := env.Fixture.ReserveIds(ctx, &fixturepb.ReserveIdsFixtureRequest{
					ClientId:           cid,
					SequentialStartKey: start,
					SequentialCount:    3,
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "ReserveIdsSequential")
			},
		},
		{
			Name:     "KeyRejectIncompleteOnLookupUpdateDelete",
			Category: "keys",
			SpecRef:  "Spec §4.2, §7.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-val", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				inc := incompleteKey("Task")
				// Expect zero RPCs on SimServer.

				getResp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId: cid,
					Key:      inc,
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(getResp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION); err != nil {
					return fmt.Errorf("Get incomplete key: %w", err)
				}

				updResp, err := env.Fixture.Update(ctx, &fixturepb.UpdateFixtureRequest{
					ClientId: cid,
					Entity:   &fixturepb.EntitySpec{Key: inc},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(updResp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION); err != nil {
					return fmt.Errorf("Update incomplete key: %w", err)
				}

				delResp, err := env.Fixture.Delete(ctx, &fixturepb.DeleteFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{inc},
				})
				if err != nil {
					return err
				}
				if err := AssertVeneerError(delResp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION); err != nil {
					return fmt.Errorf("Delete incomplete key: %w", err)
				}
				return nil
			},
		},
		{
			Name:     "KeyRejectIncompleteAncestor",
			Category: "keys",
			SpecRef:  "Spec §4.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-val", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				badChild := &fixturepb.KeySpec{
					Path: []*fixturepb.PathElementSpec{
						{Kind: "Parent"}, // incomplete ancestor
						{Kind: "Child", IdType: &fixturepb.PathElementSpec_Name{Name: "c1"}},
					},
				}

				resp, err := env.Fixture.Put(ctx, &fixturepb.PutFixtureRequest{
					ClientId: cid,
					Entity: &fixturepb.EntitySpec{
						Key: badChild,
						Properties: map[string]*fixturepb.ValueSpec{
							"x": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}},
						},
					},
				})
				if err != nil {
					return err
				}
				return AssertVeneerError(resp.Error, fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION)
			},
		},
	}
}
