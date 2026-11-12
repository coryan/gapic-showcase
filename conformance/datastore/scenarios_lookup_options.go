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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func lookupOptionScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "LookupEventualConsistency",
			Category: "lookup",
			SpecRef:  "Spec §6.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-ro", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Snapshot", "s-ev")
				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						if lr.GetReadOptions().GetReadConsistency() != pb.ReadOptions_EVENTUAL {
							return fmt.Errorf("expected ReadConsistency=EVENTUAL, got %v", lr.ReadOptions)
						}
						return nil
					},
					Resp: &pb.LookupResponse{
						Missing: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 1}},
					},
				})

				r1, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId: cid,
					Key:      k,
					ReadSpec: &fixturepb.ReadSpec{
						ReadConsistency: fixturepb.ReadConsistencySpec_READ_CONSISTENCY_EVENTUAL,
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(r1.Error, "Get eventual")
			},
		},
		{
			Name:     "LookupReadTime",
			Category: "lookup",
			SpecRef:  "Spec §6.2",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-ro", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Snapshot", "s1")
				readTS := time.Unix(1700000000, 0).UTC()

				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						rt := lr.GetReadOptions().GetReadTime()
						if rt == nil || !rt.AsTime().Equal(readTS) {
							return fmt.Errorf("expected ReadTime=%v, got %v", readTS, rt)
						}
						return nil
					},
					Resp: &pb.LookupResponse{
						Missing: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k)}, Version: 2}},
					},
				})

				r2, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId: cid,
					Key:      k,
					ReadSpec: &fixturepb.ReadSpec{
						ReadTime: timestamppb.New(readTS),
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(r2.Error, "Get read_time")
			},
		},
		{
			Name:     "LookupWithPropertyMask",
			Category: "lookup",
			SpecRef:  "Spec §6.3",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-pm", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Article", "a1")
				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						paths := lr.GetPropertyMask().GetPaths()
						if len(paths) != 2 || paths[0] != "title" || paths[1] != "metadata.author" {
							return fmt.Errorf("expected PropertyMask [title, metadata.author], got %v", paths)
						}
						return nil
					},
					Resp: &pb.LookupResponse{
						Found: []*pb.EntityResult{{
							Entity: &pb.Entity{
								Key: KeyToPB(k),
								Properties: map[string]*pb.Value{
									"title": {ValueType: &pb.Value_StringValue{StringValue: "Masked"}},
								},
							},
							Version: 1,
						}},
					},
				})

				resp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{
					ClientId:     cid,
					Key:          k,
					PropertyMask: &fixturepb.PropertyMaskSpec{Paths: []string{"title", "metadata.author"}},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "Get PropertyMask")
			},
		},
	}
}
