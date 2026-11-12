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

// Package datastore provides a DatastoreFixture gRPC test proxy wrapping cloud.google.com/go/datastore.
package datastore

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	ds "cloud.google.com/go/datastore"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func queryFromSpec(sq *fixturepb.StructuredQuerySpec, namespace string, rs *fixturepb.ReadSpec, tx *ds.Transaction) (*ds.Query, bool, error) {
	if sq == nil {
		return nil, false, errors.New("missing structured_query")
	}
	if sq.FindNearest != nil {
		return nil, false, fmt.Errorf("%w: cloud.google.com/go/datastore does not support FindNearest vector search", errFeatureNotImplemented)
	}

	q := ds.NewQuery(sq.Kind)
	if namespace != "" {
		q = q.Namespace(namespace)
	}
	if sq.AncestorKey != nil {
		q = q.Ancestor(keyFromSpec(sq.AncestorKey))
	}
	if rs != nil && rs.ReadConsistency == fixturepb.ReadConsistencySpec_READ_CONSISTENCY_EVENTUAL {
		q = q.EventualConsistency()
	}
	if tx != nil {
		q = q.Transaction(tx)
	}
	if sq.Filter != nil {
		ef, err := filterFromSpec(sq.Filter)
		if err != nil {
			return nil, false, err
		}
		q = q.FilterEntity(ef)
	}
	if sq.KeysOnly {
		q = q.KeysOnly()
	} else {
		if len(sq.Projections) > 0 {
			q = q.Project(sq.Projections...)
		}
		if sq.Distinct {
			q = q.Distinct()
		}
		if len(sq.DistinctOn) > 0 {
			q = q.DistinctOn(sq.DistinctOn...)
		}
	}
	for _, o := range sq.Orders {
		if o.Direction == fixturepb.OrderDirection_ORDER_DIRECTION_DESCENDING {
			q = q.Order("-" + o.Property)
		} else {
			q = q.Order(o.Property)
		}
	}
	if sq.Limit != nil {
		q = q.Limit(int(sq.GetLimit()))
	}
	if sq.Offset != nil {
		q = q.Offset(int(sq.GetOffset()))
	}
	if len(sq.StartCursor) > 0 {
		c, err := cursorFromBytes(sq.StartCursor)
		if err != nil {
			return nil, false, err
		}
		q = q.Start(c)
	}
	if len(sq.EndCursor) > 0 {
		c, err := cursorFromBytes(sq.EndCursor)
		if err != nil {
			return nil, false, err
		}
		q = q.End(c)
	}
	return q, sq.KeysOnly, nil
}

func filterFromSpec(f *fixturepb.FilterSpec) (ds.EntityFilter, error) {
	switch ft := f.FilterType.(type) {
	case *fixturepb.FilterSpec_PropertyFilter:
		pf := ft.PropertyFilter
		val, err := valueFromSpec(pf.Value)
		if err != nil {
			return nil, err
		}
		var opStr string
		switch pf.Op {
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN:
			opStr = "<"
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_LESS_THAN_OR_EQUAL:
			opStr = "<="
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN:
			opStr = ">"
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_GREATER_THAN_OR_EQUAL:
			opStr = ">="
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_EQUAL:
			opStr = "="
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_EQUAL:
			opStr = "!="
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_IN:
			opStr = "in"
		case fixturepb.PropertyFilterOp_PROPERTY_FILTER_OP_NOT_IN:
			opStr = "not-in"
		default:
			return nil, fmt.Errorf("unsupported filter op %v", pf.Op)
		}
		return ds.PropertyFilter{
			FieldName: pf.Property,
			Operator:  opStr,
			Value:     val,
		}, nil
	case *fixturepb.FilterSpec_CompositeFilter:
		cf := ft.CompositeFilter
		children := make([]ds.EntityFilter, 0, len(cf.Filters))
		for _, c := range cf.Filters {
			child, err := filterFromSpec(c)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		switch cf.Op {
		case fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_AND:
			return ds.AndFilter{Filters: children}, nil
		case fixturepb.CompositeFilterOp_COMPOSITE_FILTER_OP_OR:
			return ds.OrFilter{Filters: children}, nil
		default:
			return nil, fmt.Errorf("unsupported composite filter op %v", cf.Op)
		}
	default:
		return nil, errors.New("empty FilterSpec")
	}
}

func aggregationQueryFromSpec(base *ds.Query, aggs []*fixturepb.AggregationSpec) (*ds.AggregationQuery, error) {
	aq := base.NewAggregationQuery()
	for _, a := range aggs {
		if a.UpTo != nil {
			return nil, fmt.Errorf("%w: cloud.google.com/go/datastore does not support Count UpTo", errFeatureNotImplemented)
		}
		switch a.Op {
		case fixturepb.AggregationOp_AGGREGATION_OP_COUNT:
			aq = aq.WithCount(a.Alias)
		case fixturepb.AggregationOp_AGGREGATION_OP_SUM:
			aq = aq.WithSum(a.Property, a.Alias)
		case fixturepb.AggregationOp_AGGREGATION_OP_AVG:
			aq = aq.WithAvg(a.Property, a.Alias)
		}
	}
	return aq, nil
}

func explainMetricsToView(em *ds.ExplainMetrics) *fixturepb.ExplainMetricsView {
	if em == nil {
		return nil
	}
	out := &fixturepb.ExplainMetricsView{}
	if em.PlanSummary != nil {
		indexes := make([]*structpb.Struct, 0, len(em.PlanSummary.IndexesUsed))
		for _, m := range em.PlanSummary.IndexesUsed {
			if m == nil {
				continue
			}
			st, err := structpb.NewStruct(*m)
			if err == nil {
				indexes = append(indexes, st)
			}
		}
		out.PlanSummary = &fixturepb.PlanSummaryView{IndexesUsed: indexes}
	}
	if em.ExecutionStats != nil {
		es := &fixturepb.ExecutionStatsView{
			ResultsReturned: em.ExecutionStats.ResultsReturned,
			ReadOperations:  em.ExecutionStats.ReadOperations,
		}
		if em.ExecutionStats.ExecutionDuration != nil {
			es.ExecutionDuration = durationpb.New(*em.ExecutionStats.ExecutionDuration)
		}
		if em.ExecutionStats.DebugStats != nil {
			if st, err := structpb.NewStruct(*em.ExecutionStats.DebugStats); err == nil {
				es.DebugStats = st
			}
		}
		out.ExecutionStats = es
	}
	return out
}

func cursorFromBytes(b []byte) (ds.Cursor, error) {
	if len(b) == 0 {
		return ds.Cursor{}, nil
	}
	enc := strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
	return ds.DecodeCursor(enc)
}

func cursorToBytes(c ds.Cursor) ([]byte, error) {
	s := c.String()
	if s == "" {
		return nil, nil
	}
	if m := len(s) % 4; m != 0 {
		s += strings.Repeat("=", 4-m)
	}
	return base64.URLEncoding.DecodeString(s)
}
