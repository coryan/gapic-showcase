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
	"errors"
	"fmt"

	ds "cloud.google.com/go/datastore"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
)

func (s *Server) Get(ctx context.Context, req *fixturepb.GetFixtureRequest) (*fixturepb.GetFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if req.PropertyMask != nil {
		return &fixturepb.GetFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support PropertyMask on Get"),
		}, nil
	}
	c, cleanup, err := clientForReadSpec(ctx, entry, req.ReadSpec, true)
	if err != nil {
		return &fixturepb.GetFixtureResponse{Error: toVeneerError(err)}, nil
	}
	defer cleanup()

	var d dynamicEntity
	err = c.Get(ctx, keyFromSpec(req.Key), &d)
	if errors.Is(err, ds.ErrNoSuchEntity) {
		return &fixturepb.GetFixtureResponse{
			Result: &fixturepb.FoundEntity{Found: false},
		}, nil
	}
	if err != nil {
		return &fixturepb.GetFixtureResponse{Error: toVeneerError(err)}, nil
	}
	return &fixturepb.GetFixtureResponse{
		Result: &fixturepb.FoundEntity{
			Found:  true,
			Entity: dynamicToEntitySpec(&d),
		},
	}, nil
}

// GetMulti retrieves multiple entities by keys.
func (s *Server) GetMulti(ctx context.Context, req *fixturepb.GetMultiFixtureRequest) (*fixturepb.GetMultiFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if req.PropertyMask != nil {
		return &fixturepb.GetMultiFixtureResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support PropertyMask on GetMulti"),
		}, nil
	}
	c, cleanup, err := clientForReadSpec(ctx, entry, req.ReadSpec, true)
	if err != nil {
		return &fixturepb.GetMultiFixtureResponse{Error: toVeneerError(err)}, nil
	}
	defer cleanup()

	keys := make([]*ds.Key, len(req.Keys))
	for i, k := range req.Keys {
		keys[i] = keyFromSpec(k)
	}
	dsts := make([]dynamicEntity, len(keys))
	err = c.GetMulti(ctx, keys, dsts)
	if err == nil {
		results := make([]*fixturepb.FoundEntity, len(dsts))
		for i := range dsts {
			results[i] = &fixturepb.FoundEntity{
				Found:  true,
				Entity: dynamicToEntitySpec(&dsts[i]),
			}
		}
		return &fixturepb.GetMultiFixtureResponse{Results: results}, nil
	}
	if me, ok := err.(ds.MultiError); ok {
		results := make([]*fixturepb.FoundEntity, len(me))
		for i, subErr := range me {
			switch {
			case subErr == nil:
				results[i] = &fixturepb.FoundEntity{
					Found:  true,
					Entity: dynamicToEntitySpec(&dsts[i]),
				}
			case errors.Is(subErr, ds.ErrNoSuchEntity):
				results[i] = &fixturepb.FoundEntity{Found: false}
			default:
				return &fixturepb.GetMultiFixtureResponse{Error: toVeneerError(subErr)}, nil
			}
		}
		return &fixturepb.GetMultiFixtureResponse{Results: results}, nil
	}
	return &fixturepb.GetMultiFixtureResponse{Error: toVeneerError(err)}, nil
}

// Put upserts a complete entity or inserts an incomplete entity.
func (s *Server) Put(ctx context.Context, req *fixturepb.PutFixtureRequest) (*fixturepb.PutFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	k, d, err := entityToDynamic(req.Entity)
	if err != nil {
		return &fixturepb.PutFixtureResponse{Error: toVeneerError(err)}, nil
	}
	var retKey *ds.Key
	if len(req.Transforms) > 0 {
		trs, err := transformsFromSpec(req.Transforms)
		if err != nil {
			return &fixturepb.PutFixtureResponse{Error: toVeneerError(err)}, nil
		}
		retKey, err = entry.client.PutWithOptions(ctx, &ds.PutRequest{
			Key:        k,
			Entity:     d,
			Transforms: trs,
		})
		if err != nil {
			return &fixturepb.PutFixtureResponse{Error: toVeneerError(err)}, nil
		}
	} else {
		retKey, err = entry.client.Put(ctx, k, d)
		if err != nil {
			return &fixturepb.PutFixtureResponse{Error: toVeneerError(err)}, nil
		}
	}
	return &fixturepb.PutFixtureResponse{Key: keyToSpec(retKey)}, nil
}

// Insert inserts a new entity using NewInsert + Mutate.
func (s *Server) Insert(ctx context.Context, req *fixturepb.InsertFixtureRequest) (*fixturepb.InsertFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	k, d, err := entityToDynamic(req.Entity)
	if err != nil {
		return &fixturepb.InsertFixtureResponse{Error: toVeneerError(err)}, nil
	}
	keys, err := entry.client.Mutate(ctx, ds.NewInsert(k, d))
	if err != nil {
		return &fixturepb.InsertFixtureResponse{Error: toVeneerError(err)}, nil
	}
	var outKey *fixturepb.KeySpec
	if len(keys) > 0 {
		outKey = keyToSpec(keys[0])
	}
	return &fixturepb.InsertFixtureResponse{Key: outKey}, nil
}

// Update updates an existing entity using NewUpdate + Mutate.
func (s *Server) Update(ctx context.Context, req *fixturepb.UpdateFixtureRequest) (*fixturepb.UpdateFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	k, d, err := entityToDynamic(req.Entity)
	if err != nil {
		return &fixturepb.UpdateFixtureResponse{Error: toVeneerError(err)}, nil
	}
	if _, err := entry.client.Mutate(ctx, ds.NewUpdate(k, d)); err != nil {
		return &fixturepb.UpdateFixtureResponse{Error: toVeneerError(err)}, nil
	}
	return &fixturepb.UpdateFixtureResponse{}, nil
}

// Delete deletes one or more keys via Delete or DeleteMulti.
func (s *Server) Delete(ctx context.Context, req *fixturepb.DeleteFixtureRequest) (*fixturepb.DeleteFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	keys := make([]*ds.Key, len(req.Keys))
	for i, k := range req.Keys {
		keys[i] = keyFromSpec(k)
	}
	if req.Batch || len(keys) > 1 {
		err = entry.client.DeleteMulti(ctx, keys)
	} else if len(keys) == 1 {
		err = entry.client.Delete(ctx, keys[0])
	} else {
		err = entry.client.DeleteMulti(ctx, nil)
	}
	if err != nil {
		return &fixturepb.DeleteFixtureResponse{Error: toVeneerError(err)}, nil
	}
	return &fixturepb.DeleteFixtureResponse{}, nil
}

// Mutate applies a batch of heterogeneous mutations non-transactionally.
func (s *Server) Mutate(ctx context.Context, req *fixturepb.MutateFixtureRequest) (*fixturepb.MutateFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	muts, err := mutationsFromSpec(req.Mutations)
	if err != nil {
		return &fixturepb.MutateFixtureResponse{Error: toVeneerError(err)}, nil
	}
	keys, err := entry.client.Mutate(ctx, muts...)
	if err != nil {
		return &fixturepb.MutateFixtureResponse{Error: toVeneerError(err)}, nil
	}
	results := make([]*fixturepb.MutationResultSpec, len(keys))
	for i, k := range keys {
		results[i] = &fixturepb.MutationResultSpec{Key: keyToSpec(k)}
	}
	return &fixturepb.MutateFixtureResponse{Results: results}, nil
}

// RunQuery executes a query and collects results/cursor/explain metrics.

func transformsFromSpec(specs []*fixturepb.PropertyTransformSpec) ([]ds.PropertyTransform, error) {
	out := make([]ds.PropertyTransform, 0, len(specs))
	for _, ts := range specs {
		switch tt := ts.TransformType.(type) {
		case *fixturepb.PropertyTransformSpec_SetToServerValue:
			out = append(out, ds.SetToServerTime(ts.Property))
		case *fixturepb.PropertyTransformSpec_Increment:
			v, err := valueFromSpec(tt.Increment)
			if err != nil {
				return nil, err
			}
			out = append(out, ds.Increment(ts.Property, v))
		case *fixturepb.PropertyTransformSpec_Maximum:
			v, err := valueFromSpec(tt.Maximum)
			if err != nil {
				return nil, err
			}
			out = append(out, ds.Maximum(ts.Property, v))
		case *fixturepb.PropertyTransformSpec_Minimum:
			v, err := valueFromSpec(tt.Minimum)
			if err != nil {
				return nil, err
			}
			out = append(out, ds.Minimum(ts.Property, v))
		case *fixturepb.PropertyTransformSpec_AppendMissingElements:
			var elems []any
			if tt.AppendMissingElements != nil {
				for _, ev := range tt.AppendMissingElements.Values {
					gv, err := valueFromSpec(ev)
					if err != nil {
						return nil, err
					}
					elems = append(elems, gv)
				}
			}
			out = append(out, ds.AppendMissingElements(ts.Property, elems...))
		case *fixturepb.PropertyTransformSpec_RemoveAllFromArray:
			var elems []any
			if tt.RemoveAllFromArray != nil {
				for _, ev := range tt.RemoveAllFromArray.Values {
					gv, err := valueFromSpec(ev)
					if err != nil {
						return nil, err
					}
					elems = append(elems, gv)
				}
			}
			out = append(out, ds.RemoveAllFromArray(ts.Property, elems...))
		}
	}
	return out, nil
}

func mutationsFromSpec(specs []*fixturepb.MutationSpec) ([]*ds.Mutation, error) {
	muts := make([]*ds.Mutation, 0, len(specs))
	for _, m := range specs {
		if m.BaseVersion != nil || m.UpdateTime != nil {
			return nil, fmt.Errorf("%w: cloud.google.com/go/datastore does not support mutation conflict preconditions", errFeatureNotImplemented)
		}
		var mut *ds.Mutation
		switch m.Op {
		case fixturepb.MutationOp_MUTATION_OP_INSERT:
			if m.PropertyMask != nil {
				return nil, fmt.Errorf("%w: PropertyMask cannot be applied to Insert", errFeatureNotImplemented)
			}
			k, d, err := entityToDynamic(m.Entity)
			if err != nil {
				return nil, err
			}
			mut = ds.NewInsert(k, d)
		case fixturepb.MutationOp_MUTATION_OP_UPSERT:
			k, d, err := entityToDynamic(m.Entity)
			if err != nil {
				return nil, err
			}
			mut = ds.NewUpsert(k, d)
		case fixturepb.MutationOp_MUTATION_OP_UPDATE:
			k, d, err := entityToDynamic(m.Entity)
			if err != nil {
				return nil, err
			}
			mut = ds.NewUpdate(k, d)
		case fixturepb.MutationOp_MUTATION_OP_DELETE:
			dk := m.DeleteKey
			if dk == nil && m.Entity != nil {
				dk = m.Entity.Key
			}
			mut = ds.NewDelete(keyFromSpec(dk))
		}
		if m.PropertyMask != nil {
			mut = mut.WithPropertyMask(m.PropertyMask.Paths...)
		}
		if len(m.Transforms) > 0 {
			trs, err := transformsFromSpec(m.Transforms)
			if err != nil {
				return nil, err
			}
			mut = mut.WithTransforms(trs...)
		}
		muts = append(muts, mut)
	}
	return muts, nil
}
