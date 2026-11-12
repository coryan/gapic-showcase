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

func lookupCoreScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "LookupSingleFoundAndMissing",
			Category: "lookup",
			SpecRef:  "Spec §6.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-get", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				kFound := strKey("Task", "present")
				kMiss := strKey("Task", "absent")
				wantEntity := &fixturepb.EntitySpec{
					Key: kFound,
					Properties: map[string]*fixturepb.ValueSpec{
						"title": {ValueType: &fixturepb.ValueSpec_StringValue{StringValue: "found-me"}},
					},
				}

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if err := AssertRoutingHeaders(md, "proj-get", "", tr); err != nil {
								return err
							}
							if len(lr.Keys) != 1 {
								return fmt.Errorf("expected 1 key, got %d", len(lr.Keys))
							}
							return AssertWireKeyMatches(lr.Keys[0], kFound, "proj-get", "")
						},
						Resp: &pb.LookupResponse{
							Found: []*pb.EntityResult{{
								Entity:  EntityToPB(wantEntity),
								Version: 5,
							}},
						},
					},
					Expectation{
						Method: "Lookup",
						Resp: &pb.LookupResponse{
							Missing: []*pb.EntityResult{{
								Entity:  &pb.Entity{Key: KeyToPB(kMiss)},
								Version: 6,
							}},
						},
					},
				)

				r1, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: kFound})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "Get found"); err != nil {
					return err
				}
				if !r1.GetResult().GetFound() {
					return fmt.Errorf("expected found=true")
				}
				if err := AssertEntitySpecEqual(r1.GetResult().GetEntity(), wantEntity); err != nil {
					return err
				}

				r2, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: kMiss})
				if err != nil {
					return err
				}
				if err := CheckNoError(r2.Error, "Get missing"); err != nil {
					return err
				}
				if r2.GetResult().GetFound() {
					return fmt.Errorf("expected found=false for missing entity")
				}
				return nil
			},
		},
		{
			Name:     "LookupExposesEntityMetadata",
			Category: "lookup",
			SpecRef:  "Spec §6.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-meta", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k := strKey("Doc", "meta")
				ct := timestamppb.New(time.Unix(1700000100, 0).UTC())
				ut := timestamppb.New(time.Unix(1700000200, 0).UTC())

				env.Server.Expect(Expectation{
					Method: "Lookup",
					Resp: &pb.LookupResponse{
						Found: []*pb.EntityResult{{
							Entity: &pb.Entity{
								Key: KeyToPB(k),
								Properties: map[string]*pb.Value{
									"v": {ValueType: &pb.Value_IntegerValue{IntegerValue: 1}},
								},
							},
							Version:    19,
							CreateTime: ct,
							UpdateTime: ut,
						}},
					},
				})

				resp, err := env.Fixture.Get(ctx, &fixturepb.GetFixtureRequest{ClientId: cid, Key: k})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "Get"); err != nil {
					return err
				}
				meta := resp.GetResult().GetMetadata()
				if meta == nil || meta.Version != 19 {
					return fmt.Errorf("expected metadata version 19, got %v", meta)
				}
				if !meta.GetCreateTime().AsTime().Equal(ct.AsTime()) || !meta.GetUpdateTime().AsTime().Equal(ut.AsTime()) {
					return fmt.Errorf("metadata timestamps mismatch: got (%v, %v), want (%v, %v)",
						meta.GetCreateTime().AsTime(), meta.GetUpdateTime().AsTime(), ct.AsTime(), ut.AsTime())
				}
				return nil
			},
		},
		{
			Name:     "LookupBatchDedupOrderAndMissing",
			Category: "lookup",
			SpecRef:  "Spec §6.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-batch", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Task", "k1")
				k2 := strKey("Task", "k2")
				k3 := strKey("Task", "k3")
				e1 := &fixturepb.EntitySpec{
					Key:        k1,
					Properties: map[string]*fixturepb.ValueSpec{"val": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}},
				}
				e3 := &fixturepb.EntitySpec{
					Key:        k3,
					Properties: map[string]*fixturepb.ValueSpec{"val": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 3}}},
				}

				env.Server.Expect(Expectation{
					Method: "Lookup",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						lr := req.(*pb.LookupRequest)
						if len(lr.Keys) != 3 {
							return fmt.Errorf("expected deduplicated 3 keys in LookupRequest, got %d", len(lr.Keys))
						}
						return nil
					},
					Resp: &pb.LookupResponse{
						// Out-of-order response with missing key k2.
						Found: []*pb.EntityResult{
							{Entity: EntityToPB(e3), Version: 3},
							{Entity: EntityToPB(e1), Version: 1},
						},
						Missing: []*pb.EntityResult{
							{Entity: &pb.Entity{Key: KeyToPB(k2)}, Version: 2},
						},
					},
				})

				resp, err := env.Fixture.GetMulti(ctx, &fixturepb.GetMultiFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{k1, k2, k1, k3},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "GetMulti"); err != nil {
					return err
				}
				if len(resp.Results) != 4 {
					return fmt.Errorf("expected 4 aligned results, got %d", len(resp.Results))
				}
				if !resp.Results[0].Found || resp.Results[1].Found || !resp.Results[2].Found || !resp.Results[3].Found {
					return fmt.Errorf("unexpected found bitmap: [%v, %v, %v, %v]",
						resp.Results[0].Found, resp.Results[1].Found, resp.Results[2].Found, resp.Results[3].Found)
				}
				if err := AssertEntitySpecEqual(resp.Results[0].Entity, e1); err != nil {
					return err
				}
				if err := AssertEntitySpecEqual(resp.Results[2].Entity, e1); err != nil {
					return err
				}
				return AssertEntitySpecEqual(resp.Results[3].Entity, e3)
			},
		},
		{
			Name:     "LookupBatchDeferredMultiRoundLoop",
			Category: "lookup",
			SpecRef:  "Spec §6.1, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-def", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				k1 := strKey("Item", "d1")
				k2 := strKey("Item", "d2")
				k3 := strKey("Item", "d3")
				e1 := &fixturepb.EntitySpec{Key: k1, Properties: map[string]*fixturepb.ValueSpec{"n": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 1}}}}
				e3 := &fixturepb.EntitySpec{Key: k3, Properties: map[string]*fixturepb.ValueSpec{"n": {ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: 3}}}}

				env.Server.Expect(
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if len(lr.Keys) != 3 {
								return fmt.Errorf("round 1 expected 3 keys, got %d", len(lr.Keys))
							}
							return nil
						},
						Resp: &pb.LookupResponse{
							Found:    []*pb.EntityResult{{Entity: EntityToPB(e1), Version: 1}},
							Deferred: []*pb.Key{KeyToPB(k2), KeyToPB(k3)},
						},
					},
					Expectation{
						Method: "Lookup",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							lr := req.(*pb.LookupRequest)
							if len(lr.Keys) != 2 {
								return fmt.Errorf("round 2 expected 2 deferred keys, got %d", len(lr.Keys))
							}
							if err := AssertWireKeyMatches(lr.Keys[0], k2, "proj-def", ""); err != nil {
								return err
							}
							return AssertWireKeyMatches(lr.Keys[1], k3, "proj-def", "")
						},
						Resp: &pb.LookupResponse{
							Found:   []*pb.EntityResult{{Entity: EntityToPB(e3), Version: 3}},
							Missing: []*pb.EntityResult{{Entity: &pb.Entity{Key: KeyToPB(k2)}, Version: 2}},
						},
					},
				)

				resp, err := env.Fixture.GetMulti(ctx, &fixturepb.GetMultiFixtureRequest{
					ClientId: cid,
					Keys:     []*fixturepb.KeySpec{k1, k2, k3},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "GetMulti deferred"); err != nil {
					return err
				}
				if len(resp.Results) != 3 || !resp.Results[0].Found || resp.Results[1].Found || !resp.Results[2].Found {
					return fmt.Errorf("unexpected deferred results alignment: %v", resp.Results)
				}
				if err := AssertEntitySpecEqual(resp.Results[0].Entity, e1); err != nil {
					return err
				}
				return AssertEntitySpecEqual(resp.Results[2].Entity, e3)
			},
		},
	}
}
