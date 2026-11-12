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
	"google.golang.org/protobuf/proto"
)

func (s *referenceFixtureServer) AllocateIds(ctx context.Context, req *fixturepb.AllocateIdsFixtureRequest) (*fixturepb.AllocateIdsFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	pbKeys := make([]*pb.Key, 0, len(req.IncompleteKeys))
	for _, k := range req.IncompleteKeys {
		if err := refValidateKeyAncestors(k); err != nil {
			return &fixturepb.AllocateIdsFixtureResponse{Error: toRefVeneerError(err)}, nil
		}
		pbKeys = append(pbKeys, c.applyPartitionKey(k))
	}
	var resp pb.AllocateIdsResponse
	if err := c.callRPC(ctx, "allocateIds", "AllocateIds", &pb.AllocateIdsRequest{
		ProjectId:  c.projectID,
		DatabaseId: c.databaseID,
		Keys:       pbKeys,
	}, &resp, false); err != nil {
		return &fixturepb.AllocateIdsFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	out := make([]*fixturepb.KeySpec, len(resp.Keys))
	for i, k := range resp.Keys {
		out[i] = KeyFromPB(k)
	}
	return &fixturepb.AllocateIdsFixtureResponse{AllocatedKeys: out}, nil
}

func (s *referenceFixtureServer) ReserveIds(ctx context.Context, req *fixturepb.ReserveIdsFixtureRequest) (*fixturepb.ReserveIdsFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	var keys []*fixturepb.KeySpec
	if req.SequentialCount > 0 {
		if err := refValidateKeyComplete(req.SequentialStartKey); err != nil {
			return &fixturepb.ReserveIdsFixtureResponse{Error: toRefVeneerError(err)}, nil
		}
		last := req.SequentialStartKey.Path[len(req.SequentialStartKey.Path)-1]
		baseID := last.GetId()
		if baseID == 0 {
			return &fixturepb.ReserveIdsFixtureResponse{
				Error: &fixturepb.VeneerError{
					Kind:    fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION,
					Message: "sequential_start_key must have numeric id",
				},
			}, nil
		}
		for i := int64(0); i < req.SequentialCount; i++ {
			cp := proto.Clone(req.SequentialStartKey).(*fixturepb.KeySpec)
			cp.Path[len(cp.Path)-1].IdType = &fixturepb.PathElementSpec_Id{Id: baseID + i}
			keys = append(keys, cp)
		}
	} else {
		for _, k := range req.Keys {
			if err := refValidateKeyComplete(k); err != nil {
				return &fixturepb.ReserveIdsFixtureResponse{Error: toRefVeneerError(err)}, nil
			}
		}
		keys = req.Keys
	}

	pbKeys := make([]*pb.Key, len(keys))
	for i, k := range keys {
		pbKeys[i] = c.applyPartitionKey(k)
	}
	var resp pb.ReserveIdsResponse
	if err := c.callRPC(ctx, "reserveIds", "ReserveIds", &pb.ReserveIdsRequest{
		ProjectId:  c.projectID,
		DatabaseId: c.databaseID,
		Keys:       pbKeys,
	}, &resp, true); err != nil {
		return &fixturepb.ReserveIdsFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.ReserveIdsFixtureResponse{}, nil
}

func foundEntityFromPB(er *pb.EntityResult) *fixturepb.FoundEntity {
	return &fixturepb.FoundEntity{
		Found:  true,
		Entity: entitySpecFromWire(er.Entity),
		Metadata: &fixturepb.EntityMetadata{
			Version:    er.Version,
			CreateTime: er.CreateTime,
			UpdateTime: er.UpdateTime,
		},
	}
}

func (c *refClient) lookupMulti(ctx context.Context, keys []*fixturepb.KeySpec, ro *pb.ReadOptions, pm *pb.PropertyMask) ([]*fixturepb.FoundEntity, error) {
	for _, k := range keys {
		if err := refValidateKeyComplete(k); err != nil {
			return nil, err
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}

	var unique []*pb.Key
	seen := make(map[string]bool, len(keys))
	reqCanon := make([]string, len(keys))
	for i, k := range keys {
		pk := c.applyPartitionKey(k)
		ck := refCanonicalKey(pk)
		reqCanon[i] = ck
		if !seen[ck] {
			seen[ck] = true
			unique = append(unique, pk)
		}
	}

	foundByKey := make(map[string]*fixturepb.FoundEntity, len(unique))
	pending := unique
	for len(pending) > 0 {
		var resp pb.LookupResponse
		if err := c.callRPC(ctx, "lookup", "Lookup", &pb.LookupRequest{
			ProjectId:    c.projectID,
			DatabaseId:   c.databaseID,
			ReadOptions:  ro,
			Keys:         pending,
			PropertyMask: pm,
		}, &resp, true); err != nil {
			return nil, err
		}
		for _, er := range resp.Found {
			if er != nil && er.Entity != nil {
				foundByKey[refCanonicalKey(er.Entity.Key)] = foundEntityFromPB(er)
			}
		}
		for _, er := range resp.Missing {
			if er != nil && er.Entity != nil {
				foundByKey[refCanonicalKey(er.Entity.Key)] = &fixturepb.FoundEntity{Found: false}
			}
		}
		pending = resp.Deferred
	}

	out := make([]*fixturepb.FoundEntity, len(keys))
	for i, ck := range reqCanon {
		if fe, ok := foundByKey[ck]; ok {
			out[i] = fe
		} else {
			out[i] = &fixturepb.FoundEntity{Found: false}
		}
	}
	return out, nil
}

func (s *referenceFixtureServer) Get(ctx context.Context, req *fixturepb.GetFixtureRequest) (*fixturepb.GetFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	res, err := c.lookupMulti(ctx, []*fixturepb.KeySpec{req.Key}, ReadSpecToPB(req.ReadSpec), PropertyMaskToPB(req.PropertyMask))
	if err != nil {
		return &fixturepb.GetFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.GetFixtureResponse{Result: res[0]}, nil
}

func (s *referenceFixtureServer) GetMulti(ctx context.Context, req *fixturepb.GetMultiFixtureRequest) (*fixturepb.GetMultiFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	res, err := c.lookupMulti(ctx, req.Keys, ReadSpecToPB(req.ReadSpec), PropertyMaskToPB(req.PropertyMask))
	if err != nil {
		return &fixturepb.GetMultiFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.GetMultiFixtureResponse{Results: res}, nil
}

func (s *referenceFixtureServer) Put(ctx context.Context, req *fixturepb.PutFixtureRequest) (*fixturepb.PutFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if err := refValidateEntity(req.Entity); err != nil {
		return &fixturepb.PutFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	if err := refValidateKeyAncestors(req.Entity.Key); err != nil {
		return &fixturepb.PutFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	op := fixturepb.MutationOp_MUTATION_OP_INSERT
	if refValidateKeyComplete(req.Entity.Key) == nil {
		op = fixturepb.MutationOp_MUTATION_OP_UPSERT
	}
	m := &fixturepb.MutationSpec{
		Op:         op,
		Entity:     req.Entity,
		Transforms: req.Transforms,
	}
	res, err := c.commitSingleBatch(ctx, []*fixturepb.MutationSpec{m}, []*pb.Mutation{c.buildMutation(m)})
	if err != nil {
		return &fixturepb.PutFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.PutFixtureResponse{Key: res[0].Key}, nil
}

func (s *referenceFixtureServer) Insert(ctx context.Context, req *fixturepb.InsertFixtureRequest) (*fixturepb.InsertFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if err := refValidateEntity(req.Entity); err != nil {
		return &fixturepb.InsertFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	if err := refValidateKeyAncestors(req.Entity.Key); err != nil {
		return &fixturepb.InsertFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	m := &fixturepb.MutationSpec{Op: fixturepb.MutationOp_MUTATION_OP_INSERT, Entity: req.Entity}
	res, err := c.commitSingleBatch(ctx, []*fixturepb.MutationSpec{m}, []*pb.Mutation{c.buildMutation(m)})
	if err != nil {
		return &fixturepb.InsertFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.InsertFixtureResponse{Key: res[0].Key}, nil
}

func (s *referenceFixtureServer) Update(ctx context.Context, req *fixturepb.UpdateFixtureRequest) (*fixturepb.UpdateFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if err := refValidateEntity(req.Entity); err != nil {
		return &fixturepb.UpdateFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	if err := refValidateKeyComplete(req.Entity.Key); err != nil {
		return &fixturepb.UpdateFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	m := &fixturepb.MutationSpec{Op: fixturepb.MutationOp_MUTATION_OP_UPDATE, Entity: req.Entity}
	if _, err := c.commitSingleBatch(ctx, []*fixturepb.MutationSpec{m}, []*pb.Mutation{c.buildMutation(m)}); err != nil {
		return &fixturepb.UpdateFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.UpdateFixtureResponse{}, nil
}

func (s *referenceFixtureServer) Delete(ctx context.Context, req *fixturepb.DeleteFixtureRequest) (*fixturepb.DeleteFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	for _, k := range req.Keys {
		if err := refValidateKeyComplete(k); err != nil {
			return &fixturepb.DeleteFixtureResponse{Error: toRefVeneerError(err)}, nil
		}
	}
	var specs []*fixturepb.MutationSpec
	var muts []*pb.Mutation
	seen := make(map[string]bool, len(req.Keys))
	for _, k := range req.Keys {
		pk := c.applyPartitionKey(k)
		ck := refCanonicalKey(pk)
		if seen[ck] {
			continue
		}
		seen[ck] = true
		specs = append(specs, &fixturepb.MutationSpec{Op: fixturepb.MutationOp_MUTATION_OP_DELETE, DeleteKey: k})
		muts = append(muts, &pb.Mutation{Operation: &pb.Mutation_Delete{Delete: pk}})
	}
	if _, err := c.commitSingleBatch(ctx, specs, muts); err != nil {
		return &fixturepb.DeleteFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.DeleteFixtureResponse{}, nil
}

func (c *refClient) validateMutation(m *fixturepb.MutationSpec) error {
	if m == nil {
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "nil mutation"}
	}
	switch m.Op {
	case fixturepb.MutationOp_MUTATION_OP_DELETE:
		dk := m.DeleteKey
		if dk == nil && m.Entity != nil {
			dk = m.Entity.Key
		}
		return refValidateKeyComplete(dk)
	case fixturepb.MutationOp_MUTATION_OP_UPDATE:
		if err := refValidateEntity(m.Entity); err != nil {
			return err
		}
		return refValidateKeyComplete(m.Entity.Key)
	case fixturepb.MutationOp_MUTATION_OP_INSERT, fixturepb.MutationOp_MUTATION_OP_UPSERT:
		if err := refValidateEntity(m.Entity); err != nil {
			return err
		}
		return refValidateKeyAncestors(m.Entity.Key)
	default:
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: fmt.Sprintf("unknown mutation op %v", m.Op)}
	}
}

func (c *refClient) buildMutation(m *fixturepb.MutationSpec) *pb.Mutation {
	pm := MutationSpecToPB(m)
	switch op := pm.Operation.(type) {
	case *pb.Mutation_Insert:
		op.Insert = c.applyPartitionEntity(m.Entity)
	case *pb.Mutation_Upsert:
		op.Upsert = c.applyPartitionEntity(m.Entity)
	case *pb.Mutation_Update:
		op.Update = c.applyPartitionEntity(m.Entity)
	case *pb.Mutation_Delete:
		dk := m.DeleteKey
		if dk == nil && m.Entity != nil {
			dk = m.Entity.Key
		}
		op.Delete = c.applyPartitionKey(dk)
	}
	return pm
}

func mutationInputKey(m *fixturepb.MutationSpec) *fixturepb.KeySpec {
	if m.Op == fixturepb.MutationOp_MUTATION_OP_DELETE {
		if m.DeleteKey != nil {
			return m.DeleteKey
		}
		if m.Entity != nil {
			return m.Entity.Key
		}
		return nil
	}
	if m.Entity != nil {
		return m.Entity.Key
	}
	return nil
}

func mutationResultsFromPB(specs []*fixturepb.MutationSpec, pbRes []*pb.MutationResult) []*fixturepb.MutationResultSpec {
	out := make([]*fixturepb.MutationResultSpec, len(specs))
	for i, sp := range specs {
		item := &fixturepb.MutationResultSpec{}
		if i < len(pbRes) && pbRes[i] != nil {
			mr := pbRes[i]
			item.Version = mr.Version
			item.CreateTime = mr.CreateTime
			item.UpdateTime = mr.UpdateTime
			if mr.Key != nil {
				item.Key = KeyFromPB(mr.Key)
			} else {
				item.Key = mutationInputKey(sp)
			}
			for _, tr := range mr.TransformResults {
				item.TransformResults = append(item.TransformResults, ValueFromPB(tr))
			}
		} else {
			item.Key = mutationInputKey(sp)
		}
		out[i] = item
	}
	return out
}

func (c *refClient) commitSingleBatch(ctx context.Context, specs []*fixturepb.MutationSpec, muts []*pb.Mutation) ([]*fixturepb.MutationResultSpec, error) {
	var resp pb.CommitResponse
	if err := c.callRPC(ctx, "commit", "Commit", &pb.CommitRequest{
		ProjectId:  c.projectID,
		DatabaseId: c.databaseID,
		Mode:       pb.CommitRequest_NON_TRANSACTIONAL,
		Mutations:  muts,
	}, &resp, false); err != nil {
		return nil, err
	}
	return mutationResultsFromPB(specs, resp.MutationResults), nil
}

func (s *referenceFixtureServer) Mutate(ctx context.Context, req *fixturepb.MutateFixtureRequest) (*fixturepb.MutateFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	muts := make([]*pb.Mutation, len(req.Mutations))
	for i, m := range req.Mutations {
		if err := c.validateMutation(m); err != nil {
			return &fixturepb.MutateFixtureResponse{Error: toRefVeneerError(err)}, nil
		}
		muts[i] = c.buildMutation(m)
	}
	res, err := c.commitSingleBatch(ctx, req.Mutations, muts)
	if err != nil {
		return &fixturepb.MutateFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return &fixturepb.MutateFixtureResponse{Results: res}, nil
}
