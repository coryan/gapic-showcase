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
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func propertyFilterOpToPB(op fixturepb.PropertyFilterOp) pb.PropertyFilter_Operator {
	switch op {
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN:
		return pb.PropertyFilter_LESS_THAN
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN_OR_EQUAL:
		return pb.PropertyFilter_LESS_THAN_OR_EQUAL
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN:
		return pb.PropertyFilter_GREATER_THAN
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN_OR_EQUAL:
		return pb.PropertyFilter_GREATER_THAN_OR_EQUAL
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL:
		return pb.PropertyFilter_EQUAL
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_IN:
		return pb.PropertyFilter_IN
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_EQUAL:
		return pb.PropertyFilter_NOT_EQUAL
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_HAS_ANCESTOR:
		return pb.PropertyFilter_HAS_ANCESTOR
	case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_IN:
		return pb.PropertyFilter_NOT_IN
	default:
		return pb.PropertyFilter_OPERATOR_UNSPECIFIED
	}
}

func compositeFilterOpToPB(op fixturepb.CompositeFilterOp) pb.CompositeFilter_Operator {
	switch op {
	case fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_AND:
		return pb.CompositeFilter_AND
	case fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_OR:
		return pb.CompositeFilter_OR
	default:
		return pb.CompositeFilter_OPERATOR_UNSPECIFIED
	}
}

func orderDirectionToPB(d fixturepb.OrderDirection) pb.PropertyOrder_Direction {
	switch d {
	case fixturepb.OrderDirection_ORDER_DIRECTION_ASCENDING:
		return pb.PropertyOrder_ASCENDING
	case fixturepb.OrderDirection_ORDER_DIRECTION_DESCENDING:
		return pb.PropertyOrder_DESCENDING
	default:
		return pb.PropertyOrder_DIRECTION_UNSPECIFIED
	}
}

func distanceMeasureToPB(dm fixturepb.DistanceMeasureSpec) pb.FindNearest_DistanceMeasure {
	switch dm {
	case fixturepb.DistanceMeasureSpec_DISTANCE_MEASURE_EUCLIDEAN:
		return pb.FindNearest_EUCLIDEAN
	case fixturepb.DistanceMeasureSpec_DISTANCE_MEASURE_COSINE:
		return pb.FindNearest_COSINE
	case fixturepb.DistanceMeasureSpec_DISTANCE_MEASURE_DOT_PRODUCT:
		return pb.FindNearest_DOT_PRODUCT
	default:
		return pb.FindNearest_DISTANCE_MEASURE_UNSPECIFIED
	}
}

// FilterToPB converts a fixture FilterSpec to a wire datastorepb.Filter.
func FilterToPB(f *fixturepb.FilterSpec) *pb.Filter {
	if f == nil {
		return nil
	}
	switch ft := f.FilterType.(type) {
	case *fixturepb.FilterSpec_PropertyFilter:
		if ft.PropertyFilter == nil {
			return nil
		}
		return &pb.Filter{
			FilterType: &pb.Filter_PropertyFilter{
				PropertyFilter: &pb.PropertyFilter{
					Property: &pb.PropertyReference{Name: ft.PropertyFilter.Property},
					Op:       propertyFilterOpToPB(ft.PropertyFilter.Op),
					Value:    ValueToPB(ft.PropertyFilter.Value),
				},
			},
		}
	case *fixturepb.FilterSpec_CompositeFilter:
		if ft.CompositeFilter == nil {
			return nil
		}
		children := make([]*pb.Filter, 0, len(ft.CompositeFilter.Filters))
		for _, c := range ft.CompositeFilter.Filters {
			children = append(children, FilterToPB(c))
		}
		return &pb.Filter{
			FilterType: &pb.Filter_CompositeFilter{
				CompositeFilter: &pb.CompositeFilter{
					Op:      compositeFilterOpToPB(ft.CompositeFilter.Op),
					Filters: children,
				},
			},
		}
	}
	return nil
}

// StructuredQueryToPB converts a fixture StructuredQuerySpec to a wire datastorepb.Query.
func StructuredQueryToPB(q *fixturepb.StructuredQuerySpec) *pb.Query {
	if q == nil {
		return nil
	}
	out := &pb.Query{
		StartCursor: q.StartCursor,
		EndCursor:   q.EndCursor,
	}
	if q.Kind != "" {
		out.Kind = []*pb.KindExpression{{Name: q.Kind}}
	}
	if q.KeysOnly {
		out.Projection = []*pb.Projection{{Property: &pb.PropertyReference{Name: "__key__"}}}
	} else {
		for _, p := range q.Projections {
			out.Projection = append(out.Projection, &pb.Projection{Property: &pb.PropertyReference{Name: p}})
		}
		if q.Distinct {
			for _, p := range q.Projections {
				out.DistinctOn = append(out.DistinctOn, &pb.PropertyReference{Name: p})
			}
		} else {
			for _, d := range q.DistinctOn {
				out.DistinctOn = append(out.DistinctOn, &pb.PropertyReference{Name: d})
			}
		}
	}

	var filters []*pb.Filter
	if q.Filter != nil {
		filters = append(filters, FilterToPB(q.Filter))
	}
	if q.AncestorKey != nil {
		filters = append(filters, &pb.Filter{
			FilterType: &pb.Filter_PropertyFilter{
				PropertyFilter: &pb.PropertyFilter{
					Property: &pb.PropertyReference{Name: "__key__"},
					Op:       pb.PropertyFilter_HAS_ANCESTOR,
					Value:    &pb.Value{ValueType: &pb.Value_KeyValue{KeyValue: KeyToPB(q.AncestorKey)}},
				},
			},
		})
	}
	if len(filters) == 1 {
		out.Filter = filters[0]
	} else if len(filters) > 1 {
		out.Filter = &pb.Filter{
			FilterType: &pb.Filter_CompositeFilter{
				CompositeFilter: &pb.CompositeFilter{
					Op:      pb.CompositeFilter_AND,
					Filters: filters,
				},
			},
		}
	}

	for _, o := range q.Orders {
		out.Order = append(out.Order, &pb.PropertyOrder{
			Property:  &pb.PropertyReference{Name: o.Property},
			Direction: orderDirectionToPB(o.Direction),
		})
	}
	if q.Limit != nil {
		out.Limit = wrapperspb.Int32(q.GetLimit())
	}
	if q.Offset != nil {
		out.Offset = q.GetOffset()
	}
	if q.FindNearest != nil {
		fn := &pb.FindNearest{
			VectorProperty: &pb.PropertyReference{Name: q.FindNearest.VectorProperty},
			QueryVector: ValueToPB(&fixturepb.ValueSpec{
				ValueType: &fixturepb.ValueSpec_VectorValue{VectorValue: q.FindNearest.QueryVector},
			}),
			DistanceMeasure:        distanceMeasureToPB(q.FindNearest.DistanceMeasure),
			DistanceResultProperty: q.FindNearest.DistanceResultProperty,
		}
		if q.FindNearest.Limit != nil {
			fn.Limit = wrapperspb.Int32(q.FindNearest.GetLimit())
		}
		if q.FindNearest.DistanceThreshold != nil {
			fn.DistanceThreshold = wrapperspb.Double(q.FindNearest.GetDistanceThreshold())
		}
		out.FindNearest = fn
	}
	return out
}

// GqlQueryToPB converts a fixture GqlQuerySpec to a wire datastorepb.GqlQuery.
func GqlQueryToPB(g *fixturepb.GqlQuerySpec) *pb.GqlQuery {
	if g == nil {
		return nil
	}
	out := &pb.GqlQuery{
		QueryString:   g.QueryString,
		AllowLiterals: g.AllowLiterals,
		NamedBindings: make(map[string]*pb.GqlQueryParameter, len(g.NamedBindings)),
	}
	for k, b := range g.NamedBindings {
		out.NamedBindings[k] = gqlBindingToPB(b)
	}
	for _, b := range g.PositionalBindings {
		out.PositionalBindings = append(out.PositionalBindings, gqlBindingToPB(b))
	}
	return out
}

func gqlBindingToPB(b *fixturepb.GqlBindingSpec) *pb.GqlQueryParameter {
	if b == nil {
		return nil
	}
	out := &pb.GqlQueryParameter{}
	switch bt := b.BindingType.(type) {
	case *fixturepb.GqlBindingSpec_Value:
		out.ParameterType = &pb.GqlQueryParameter_Value{Value: ValueToPB(bt.Value)}
	case *fixturepb.GqlBindingSpec_Cursor:
		c := make([]byte, len(bt.Cursor))
		copy(c, bt.Cursor)
		out.ParameterType = &pb.GqlQueryParameter_Cursor{Cursor: c}
	}
	return out
}

// AggregationsToPB converts fixture AggregationSpecs into datastorepb Aggregations.
func AggregationsToPB(aggs []*fixturepb.AggregationSpec) []*pb.AggregationQuery_Aggregation {
	out := make([]*pb.AggregationQuery_Aggregation, 0, len(aggs))
	for _, a := range aggs {
		pba := &pb.AggregationQuery_Aggregation{Alias: a.Alias}
		switch a.Op {
		case fixturepb.AggregationOp_AGGREGATION_OP_COUNT:
			cnt := &pb.AggregationQuery_Aggregation_Count{}
			if a.UpTo != nil {
				cnt.UpTo = wrapperspb.Int64(a.GetUpTo())
			}
			pba.Operator = &pb.AggregationQuery_Aggregation_Count_{Count: cnt}
		case fixturepb.AggregationOp_AGGREGATION_OP_SUM:
			pba.Operator = &pb.AggregationQuery_Aggregation_Sum_{
				Sum: &pb.AggregationQuery_Aggregation_Sum{
					Property: &pb.PropertyReference{Name: a.Property},
				},
			}
		case fixturepb.AggregationOp_AGGREGATION_OP_AVG:
			pba.Operator = &pb.AggregationQuery_Aggregation_Avg_{
				Avg: &pb.AggregationQuery_Aggregation_Avg{
					Property: &pb.PropertyReference{Name: a.Property},
				},
			}
		}
		out = append(out, pba)
	}
	return out
}

// ExplainMetricsFromPB converts a wire ExplainMetrics into ExplainMetricsView.
func ExplainMetricsFromPB(em *pb.ExplainMetrics) *fixturepb.ExplainMetricsView {
	if em == nil {
		return nil
	}
	out := &fixturepb.ExplainMetricsView{}
	if em.PlanSummary != nil {
		out.PlanSummary = &fixturepb.PlanSummaryView{
			IndexesUsed: em.PlanSummary.IndexesUsed,
		}
	}
	if em.ExecutionStats != nil {
		out.ExecutionStats = &fixturepb.ExecutionStatsView{
			ResultsReturned:   em.ExecutionStats.ResultsReturned,
			ExecutionDuration: em.ExecutionStats.ExecutionDuration,
			ReadOperations:    em.ExecutionStats.ReadOperations,
			DebugStats:        em.ExecutionStats.DebugStats,
		}
	}
	return out
}

// ExplainMetricsToPB converts ExplainMetricsView into a wire ExplainMetrics.
func ExplainMetricsToPB(em *fixturepb.ExplainMetricsView) *pb.ExplainMetrics {
	if em == nil {
		return nil
	}
	out := &pb.ExplainMetrics{}
	if em.PlanSummary != nil {
		out.PlanSummary = &pb.PlanSummary{IndexesUsed: em.PlanSummary.IndexesUsed}
	}
	if em.ExecutionStats != nil {
		out.ExecutionStats = &pb.ExecutionStats{
			ResultsReturned:   em.ExecutionStats.ResultsReturned,
			ExecutionDuration: em.ExecutionStats.ExecutionDuration,
			ReadOperations:    em.ExecutionStats.ReadOperations,
			DebugStats:        em.ExecutionStats.DebugStats,
		}
	}
	return out
}
