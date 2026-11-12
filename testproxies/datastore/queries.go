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
	"context"

	ds "cloud.google.com/go/datastore"
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/api/iterator"
)

func (s *Server) RunQuery(ctx context.Context, req *fixturepb.RunQueryFixtureRequest) (*fixturepb.RunQueryFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if req.PropertyMask != nil {
		return &fixturepb.RunQueryFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support PropertyMask on RunQuery"),
		}, nil
	}
	if req.CheckExplainBeforeDone {
		return &fixturepb.RunQueryFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore Iterator.ExplainMetrics is a plain field without pre-exhaustion state error validation"),
		}, nil
	}
	if req.GetGqlQuery() != nil {
		return &fixturepb.RunQueryFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support GQL queries"),
		}, nil
	}
	q, keysOnly, err := queryFromSpec(req.GetStructuredQuery(), req.NamespaceId, req.ReadSpec, nil)
	if err != nil {
		return &fixturepb.RunQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}

	c, cleanup, err := clientForReadSpec(ctx, entry, req.ReadSpec, false)
	if err != nil {
		return &fixturepb.RunQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}
	defer cleanup()

	var runOpts []ds.RunOption
	if req.ExplainOptions != nil {
		runOpts = append(runOpts, ds.ExplainOptions{Analyze: req.ExplainOptions.Analyze})
	}

	resp, entities, err := executeQuery(ctx, c, q, keysOnly, req.ExplainOptions, req.StopAfterItems, runOpts...)
	if err != nil {
		return &fixturepb.RunQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}

	if req.RoundTripUpsertResults {
		for _, ent := range entities {
			if _, err := c.Put(ctx, ent.Key, ent); err != nil {
				return &fixturepb.RunQueryFixtureResponse{Error: toVeneerError(err)}, nil
			}
		}
	}
	return resp, nil
}

// RunAggregationQuery executes an aggregation query.
func (s *Server) RunAggregationQuery(ctx context.Context, req *fixturepb.RunAggregationQueryFixtureRequest) (*fixturepb.RunAggregationQueryFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if req.GetGqlQuery() != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support GQL aggregation queries"),
		}, nil
	}

	baseQ, _, err := queryFromSpec(req.GetStructuredQuery(), req.NamespaceId, req.ReadSpec, nil)
	if err != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}
	aq, err := aggregationQueryFromSpec(baseQ, req.Aggregations)
	if err != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}

	c, cleanup, err := clientForReadSpec(ctx, entry, req.ReadSpec, false)
	if err != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}
	defer cleanup()

	var runOpts []ds.RunOption
	if req.ExplainOptions != nil {
		runOpts = append(runOpts, ds.ExplainOptions{Analyze: req.ExplainOptions.Analyze})
	}

	res, err := c.RunAggregationQueryWithOptions(ctx, aq, runOpts...)
	if err != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{Error: toVeneerError(err)}, nil
	}

	out := make(map[string]*fixturepb.ValueSpec, len(res.Result))
	for k, v := range res.Result {
		if pbVal, ok := v.(*pb.Value); ok {
			out[k] = aggPBValueToSpec(pbVal)
		}
	}
	return &fixturepb.RunAggregationQueryFixtureResponse{
		Results:        out,
		ExplainMetrics: explainMetricsToView(res.ExplainMetrics),
	}, nil
}

// RunInTransaction executes declarative transaction steps inside Client.RunInTransaction.

func executeQuery(
	ctx context.Context,
	c *ds.Client,
	q *ds.Query,
	keysOnly bool,
	explain *fixturepb.ExplainOptionsSpec,
	stopAfter int32,
	opts ...ds.RunOption,
) (*fixturepb.RunQueryFixtureResponse, []*dynamicEntity, error) {
	it := c.RunWithOptions(ctx, q, opts...)
	var (
		keys     []*fixturepb.KeySpec
		entities []*fixturepb.EntitySpec
		dynEnts  []*dynamicEntity
	)

	planOnly := explain != nil && !explain.Analyze
	for {
		if stopAfter > 0 && int32(len(keys)+len(entities)) >= stopAfter {
			break
		}
		if keysOnly {
			k, err := it.Next(nil)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, nil, err
			}
			keys = append(keys, keyToSpec(k))
		} else {
			d := &dynamicEntity{}
			k, err := it.Next(d)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, nil, err
			}
			if d.Key == nil {
				d.Key = k
			}
			entities = append(entities, dynamicToEntitySpec(d))
			dynEnts = append(dynEnts, d)
		}
		if planOnly {
			break
		}
	}

	var curBytes []byte
	if stopAfter > 0 {
		cur, err := it.Cursor()
		if err != nil {
			return nil, nil, err
		}
		curBytes, err = cursorToBytes(cur)
		if err != nil {
			return nil, nil, err
		}
	}

	return &fixturepb.RunQueryFixtureResponse{
		Keys:           keys,
		Entities:       entities,
		Cursor:         curBytes,
		ExplainMetrics: explainMetricsToView(it.ExplainMetrics),
	}, dynEnts, nil
}
