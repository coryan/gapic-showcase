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
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func queryPaginationScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "QueryMultiBatchPaginationAndSkippedResults",
			Category: "queries",
			SpecRef:  "Spec §9.3, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-page", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				lim := int32(3)
				off := int32(3)
				k1 := strKey("Task", "e1")
				k2 := strKey("Task", "e2")
				k3 := strKey("Task", "e3")

				env.Server.Expect(
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							q := req.(*pb.RunQueryRequest).GetQuery()
							if q.Offset != 3 || q.GetLimit().GetValue() != 3 || len(q.StartCursor) != 0 {
								return fmt.Errorf("batch 1 unexpected offset=%d limit=%v cursor=%x", q.Offset, q.Limit, q.StartCursor)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								SkippedResults:   2,
								SkippedCursor:    []byte("sc1"),
								EndCursor:        []byte("sc1"),
								MoreResults:      pb.QueryResultBatch_NOT_FINISHED,
							},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							q := req.(*pb.RunQueryRequest).GetQuery()
							if q.Offset != 1 || q.GetLimit().GetValue() != 3 || string(q.StartCursor) != "sc1" {
								return fmt.Errorf("batch 2 expected offset=1 limit=3 cursor=sc1, got offset=%d limit=%v cursor=%q", q.Offset, q.Limit, q.StartCursor)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								SkippedResults:   1,
								SkippedCursor:    []byte("sc2"),
								EntityResults: []*pb.EntityResult{
									{Entity: &pb.Entity{Key: KeyToPB(k1), Properties: map[string]*pb.Value{"n": {ValueType: &pb.Value_IntegerValue{IntegerValue: 1}}}}, Cursor: []byte("c1")},
								},
								EndCursor:   []byte("ec2"),
								MoreResults: pb.QueryResultBatch_NOT_FINISHED,
							},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							q := req.(*pb.RunQueryRequest).GetQuery()
							if q.Offset != 0 || q.GetLimit().GetValue() != 2 || string(q.StartCursor) != "ec2" {
								return fmt.Errorf("batch 3 expected offset=0 limit=2 cursor=ec2, got offset=%d limit=%v cursor=%q", q.Offset, q.Limit, q.StartCursor)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{
								EntityResultType: pb.EntityResult_FULL,
								EntityResults: []*pb.EntityResult{
									{Entity: &pb.Entity{Key: KeyToPB(k2), Properties: map[string]*pb.Value{"n": {ValueType: &pb.Value_IntegerValue{IntegerValue: 2}}}}, Cursor: []byte("c2")},
									{Entity: &pb.Entity{Key: KeyToPB(k3), Properties: map[string]*pb.Value{"n": {ValueType: &pb.Value_IntegerValue{IntegerValue: 3}}}}, Cursor: []byte("c3")},
								},
								EndCursor:   []byte("ec3"),
								MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
							},
						},
					},
				)

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:   "Task",
							Offset: &off,
							Limit:  &lim,
						},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "multi-batch RunQuery"); err != nil {
					return err
				}
				if len(resp.Entities) != 3 {
					return fmt.Errorf("expected 3 entities across batches, got %d", len(resp.Entities))
				}
				return nil
			},
		},
		{
			Name:     "QueryMidBatchStopCursor",
			Category: "queries",
			SpecRef:  "Spec §9.3, §12.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-cur", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_FULL,
							EntityResults: []*pb.EntityResult{
								{Entity: &pb.Entity{Key: KeyToPB(strKey("Task", "1"))}, Cursor: []byte("c1")},
								{Entity: &pb.Entity{Key: KeyToPB(strKey("Task", "2"))}, Cursor: []byte("c2")},
								{Entity: &pb.Entity{Key: KeyToPB(strKey("Task", "3"))}, Cursor: []byte("c3")},
							},
							EndCursor:   []byte("batch_end"),
							MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task"},
					},
					StopAfterItems: 2,
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(resp.Error, "mid-batch cursor"); err != nil {
					return err
				}
				if len(resp.Entities) != 2 {
					return fmt.Errorf("expected 2 entities when stopping early, got %d", len(resp.Entities))
				}
				if !bytes.Equal(resp.Cursor, []byte("c2")) {
					return fmt.Errorf("expected mid-batch cursor \"c2\", got %q", resp.Cursor)
				}
				return nil
			},
		},
		{
			Name:     "QueryReadPropertyMask",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-pm", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						qr := req.(*pb.RunQueryRequest)
						paths := qr.GetPropertyMask().GetPaths()
						if len(paths) != 1 || paths[0] != "title" {
							return fmt.Errorf("expected RunQuery PropertyMask [title], got %v", paths)
						}
						return nil
					},
					Resp: &pb.RunQueryResponse{
						Batch: &pb.QueryResultBatch{
							EntityResultType: pb.EntityResult_FULL,
							MoreResults:      pb.QueryResultBatch_NO_MORE_RESULTS,
						},
					},
				})

				resp, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Article"},
					},
					PropertyMask: &fixturepb.PropertyMaskSpec{Paths: []string{"title"}},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "RunQuery PropertyMask")
			},
		},
	}
}
