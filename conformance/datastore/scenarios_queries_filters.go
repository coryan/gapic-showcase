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

func propFilter(prop string, op fixturepb.PropertyFilterOp, val *fixturepb.ValueSpec) *fixturepb.FilterSpec {
	return &fixturepb.FilterSpec{
		FilterType: &fixturepb.FilterSpec_PropertyFilter{
			PropertyFilter: &fixturepb.PropertyFilterSpec{
				Property: prop,
				Op:       op,
				Value:    val,
			},
		},
	}
}

func andFilter(filters ...*fixturepb.FilterSpec) *fixturepb.FilterSpec {
	return &fixturepb.FilterSpec{
		FilterType: &fixturepb.FilterSpec_CompositeFilter{
			CompositeFilter: &fixturepb.CompositeFilterSpec{
				Op:      fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_AND,
				Filters: filters,
			},
		},
	}
}

func orFilter(filters ...*fixturepb.FilterSpec) *fixturepb.FilterSpec {
	return &fixturepb.FilterSpec{
		FilterType: &fixturepb.FilterSpec_CompositeFilter{
			CompositeFilter: &fixturepb.CompositeFilterSpec{
				Op:      fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_OR,
				Filters: filters,
			},
		},
	}
}

func intVal(n int64) *fixturepb.ValueSpec {
	return &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_IntegerValue{IntegerValue: n}}
}

func strVal(s string) *fixturepb.ValueSpec {
	return &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_StringValue{StringValue: s}}
}

func collectPropertyOps(f *pb.Filter) []pb.PropertyFilter_Operator {
	if f == nil {
		return nil
	}
	if pf := f.GetPropertyFilter(); pf != nil {
		return []pb.PropertyFilter_Operator{pf.Op}
	}
	var out []pb.PropertyFilter_Operator
	if cf := f.GetCompositeFilter(); cf != nil {
		for _, ch := range cf.Filters {
			out = append(out, collectPropertyOps(ch)...)
		}
	}
	return out
}

func queryFilterScenarios() []Scenario {
	return []Scenario{
		{
			Name:     "QueryPropertyFiltersAllOps",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				arrVals := &fixturepb.ValueSpec{
					ValueType: &fixturepb.ValueSpec_ArrayValue{
						ArrayValue: &fixturepb.ArraySpec{Values: []*fixturepb.ValueSpec{intVal(1), intVal(2)}},
					},
				}
				filter := andFilter(
					propFilter("a", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN, intVal(10)),
					propFilter("b", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN_OR_EQUAL, intVal(20)),
					propFilter("c", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN, intVal(1)),
					propFilter("d", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN_OR_EQUAL, intVal(2)),
					propFilter("e", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL, strVal("ok")),
					propFilter("f", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_EQUAL, strVal("bad")),
					propFilter("g", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_IN, arrVals),
					propFilter("h", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_IN, arrVals),
				)

				wantOps := map[pb.PropertyFilter_Operator]bool{
					pb.PropertyFilter_LESS_THAN:             false,
					pb.PropertyFilter_LESS_THAN_OR_EQUAL:    false,
					pb.PropertyFilter_GREATER_THAN:          false,
					pb.PropertyFilter_GREATER_THAN_OR_EQUAL: false,
					pb.PropertyFilter_EQUAL:                 false,
					pb.PropertyFilter_NOT_EQUAL:             false,
					pb.PropertyFilter_IN:                    false,
					pb.PropertyFilter_NOT_IN:                false,
				}

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						qr := req.(*pb.RunQueryRequest)
						if err := AssertRoutingHeaders(md, "proj-q", "", tr); err != nil {
							return err
						}
						ops := collectPropertyOps(qr.GetQuery().GetFilter())
						for _, op := range ops {
							wantOps[op] = true
						}
						for op, seen := range wantOps {
							if !seen {
								return fmt.Errorf("missing PropertyFilter operator %v on wire query %v", op, qr.GetQuery().GetFilter())
							}
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
						StructuredQuery: &fixturepb.StructuredQuerySpec{
							Kind:   "Task",
							Filter: filter,
						},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "RunQuery")
			},
		},
		{
			Name:     "QueryNestedCompositeAndOrFilters",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				nested := andFilter(
					propFilter("active", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL, &fixturepb.ValueSpec{ValueType: &fixturepb.ValueSpec_BooleanValue{BooleanValue: true}}),
					orFilter(
						propFilter("prio", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL, intVal(1)),
						propFilter("tag", fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL, strVal("urgent")),
					),
				)

				env.Server.Expect(Expectation{
					Method: "RunQuery",
					Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
						cf := req.(*pb.RunQueryRequest).GetQuery().GetFilter().GetCompositeFilter()
						if cf == nil || cf.Op != pb.CompositeFilter_AND || len(cf.Filters) != 2 {
							return fmt.Errorf("expected top-level AND(2), got %v", req.(*pb.RunQueryRequest).GetQuery().GetFilter())
						}
						inner := cf.Filters[1].GetCompositeFilter()
						if inner == nil || inner.Op != pb.CompositeFilter_OR || len(inner.Filters) != 2 {
							return fmt.Errorf("expected nested OR(2), got %v", cf.Filters[1])
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
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task", Filter: nested},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(resp.Error, "RunQuery nested composite")
			},
		},
		{
			Name:     "QueryAncestorAndKindless",
			Category: "queries",
			SpecRef:  "Spec §9.1",
			Run: func(ctx context.Context, env *ScenarioEnv) error {
				cid, cleanup, err := env.NewClient(ctx, "proj-q", "", "")
				if err != nil {
					return err
				}
				defer cleanup()

				anc := strKey("List", "root")
				env.Server.Expect(
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							q := req.(*pb.RunQueryRequest).GetQuery()
							if len(q.Kind) != 1 || q.Kind[0].Name != "Task" {
								return fmt.Errorf("expected Kind=Task, got %v", q.Kind)
							}
							ops := collectPropertyOps(q.Filter)
							for _, op := range ops {
								if op == pb.PropertyFilter_HAS_ANCESTOR {
									return nil
								}
							}
							return fmt.Errorf("expected HAS_ANCESTOR filter in %v", q.Filter)
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS},
						},
					},
					Expectation{
						Method: "RunQuery",
						Validate: func(req proto.Message, md metadata.MD, tr fixturepb.Transport) error {
							q := req.(*pb.RunQueryRequest).GetQuery()
							if len(q.Kind) != 0 {
								return fmt.Errorf("expected empty Kind for kindless query, got %v", q.Kind)
							}
							return nil
						},
						Resp: &pb.RunQueryResponse{
							Batch: &pb.QueryResultBatch{MoreResults: pb.QueryResultBatch_NO_MORE_RESULTS},
						},
					},
				)

				r1, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "Task", AncestorKey: anc},
					},
				})
				if err != nil {
					return err
				}
				if err := CheckNoError(r1.Error, "ancestor query"); err != nil {
					return err
				}

				r2, err := env.Fixture.RunQuery(ctx, &fixturepb.RunQueryFixtureRequest{
					ClientId: cid,
					QuerySpec: &fixturepb.RunQueryFixtureRequest_StructuredQuery{
						StructuredQuery: &fixturepb.StructuredQuerySpec{Kind: "", AncestorKey: anc},
					},
				})
				if err != nil {
					return err
				}
				return CheckNoError(r2.Error, "kindless query")
			},
		},
	}
}
