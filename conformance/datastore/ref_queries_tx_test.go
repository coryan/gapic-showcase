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
	"sync"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *refClient) runQueryWithRO(
	ctx context.Context,
	namespaceID string,
	sq *fixturepb.StructuredQuerySpec,
	gql *fixturepb.GqlQuerySpec,
	ro *pb.ReadOptions,
	pm *pb.PropertyMask,
	eo *fixturepb.ExplainOptionsSpec,
	stopAfter int32,
	checkExplainBeforeDone bool,
	onResponse func(*pb.RunQueryResponse),
) (*fixturepb.RunQueryFixtureResponse, error) {
	if sq != nil && sq.Offset != nil && sq.GetOffset() < 0 {
		return nil, &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "query offset must be non-negative"}
	}
	if sq != nil && sq.AncestorKey != nil {
		if err := refValidateKeyComplete(sq.AncestorKey); err != nil {
			return nil, err
		}
	}

	var pbEO *pb.ExplainOptions
	if eo != nil {
		pbEO = &pb.ExplainOptions{Analyze: eo.Analyze}
	}

	req := &pb.RunQueryRequest{
		ProjectId:      c.projectID,
		DatabaseId:     c.databaseID,
		PartitionId:    c.effectiveNamespace(namespaceID),
		ReadOptions:    ro,
		PropertyMask:   pm,
		ExplainOptions: pbEO,
	}

	keysOnly := false
	var compiledQuery *pb.Query
	if sq != nil {
		keysOnly = sq.KeysOnly
		compiledQuery = StructuredQueryToPB(sq)
		if sq.AncestorKey != nil && compiledQuery.Filter != nil {
			// Ensure ancestor key has partition info normalized.
			fixAncestorPartition(compiledQuery.Filter, c)
		}
		req.QueryType = &pb.RunQueryRequest_Query{Query: compiledQuery}
	} else if gql != nil {
		req.QueryType = &pb.RunQueryRequest_GqlQuery{GqlQuery: GqlQueryToPB(gql)}
	}

	out := &fixturepb.RunQueryFixtureResponse{}
	var explain *pb.ExplainMetrics
	var lastEndCursor []byte
	emitted := int32(0)

	for {
		var resp pb.RunQueryResponse
		if err := c.callRPC(ctx, "runQuery", "RunQuery", req, &resp, true); err != nil {
			return nil, err
		}
		if onResponse != nil {
			onResponse(&resp)
		}
		if resp.ExplainMetrics != nil {
			explain = resp.ExplainMetrics
		}
		if compiledQuery == nil && resp.Query != nil {
			compiledQuery = proto.Clone(resp.Query).(*pb.Query)
		}

		batch := resp.Batch
		if batch == nil {
			break
		}
		for _, er := range batch.EntityResults {
			if er == nil || er.Entity == nil {
				continue
			}
			emitted++
			if keysOnly {
				out.Keys = append(out.Keys, KeyFromPB(er.Entity.Key))
			} else {
				out.Entities = append(out.Entities, entitySpecFromWire(er.Entity))
			}
			lastEndCursor = er.Cursor
			if stopAfter > 0 && emitted >= stopAfter {
				out.Cursor = lastEndCursor
				out.ExplainMetrics = ExplainMetricsFromPB(explain)
				return out, nil
			}
		}
		if len(batch.EndCursor) > 0 {
			lastEndCursor = batch.EndCursor
		} else if len(batch.SkippedCursor) > 0 {
			lastEndCursor = batch.SkippedCursor
		}

		if checkExplainBeforeDone && batch.MoreResults == pb.QueryResultBatch_NOT_FINISHED {
			return nil, &refKindError{
				kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_STATE,
				msg:  "cannot read explain metrics before query iterator is exhausted",
			}
		}

		if batch.MoreResults != pb.QueryResultBatch_NOT_FINISHED {
			break
		}

		if compiledQuery == nil {
			compiledQuery = &pb.Query{}
		}
		nextQ := proto.Clone(compiledQuery).(*pb.Query)
		if nextQ.Offset > 0 {
			rem := nextQ.Offset - batch.SkippedResults
			if rem < 0 {
				rem = 0
			}
			nextQ.Offset = rem
		}
		if nextQ.Limit != nil {
			remLim := nextQ.Limit.Value - int32(len(batch.EntityResults))
			if remLim < 0 {
				remLim = 0
			}
			nextQ.Limit.Value = remLim
		}
		if len(batch.EndCursor) > 0 {
			nextQ.StartCursor = batch.EndCursor
		} else if len(batch.SkippedCursor) > 0 {
			nextQ.StartCursor = batch.SkippedCursor
		}
		compiledQuery = nextQ
		req.QueryType = &pb.RunQueryRequest_Query{Query: compiledQuery}
	}

	out.Cursor = lastEndCursor
	out.ExplainMetrics = ExplainMetricsFromPB(explain)
	return out, nil
}

func fixAncestorPartition(f *pb.Filter, c *refClient) {
	if f == nil {
		return
	}
	if pf := f.GetPropertyFilter(); pf != nil && pf.Op == pb.PropertyFilter_HAS_ANCESTOR {
		if kv := pf.Value.GetKeyValue(); kv != nil {
			ns := kv.GetPartitionId().GetNamespaceId()
			if ns == "" {
				ns = c.namespaceID
			}
			kv.PartitionId = &pb.PartitionId{
				ProjectId:   c.projectID,
				DatabaseId:  c.databaseID,
				NamespaceId: ns,
			}
		}
		return
	}
	if cf := f.GetCompositeFilter(); cf != nil {
		for _, ch := range cf.Filters {
			fixAncestorPartition(ch, c)
		}
	}
}

func (s *referenceFixtureServer) RunQuery(ctx context.Context, req *fixturepb.RunQueryFixtureRequest) (*fixturepb.RunQueryFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	res, err := c.runQueryWithRO(
		ctx,
		req.NamespaceId,
		req.GetStructuredQuery(),
		req.GetGqlQuery(),
		ReadSpecToPB(req.ReadSpec),
		PropertyMaskToPB(req.PropertyMask),
		req.ExplainOptions,
		req.StopAfterItems,
		req.CheckExplainBeforeDone,
		nil,
	)
	if err != nil {
		return &fixturepb.RunQueryFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	if req.RoundTripUpsertResults {
		for _, es := range res.Entities {
			m := &fixturepb.MutationSpec{Op: fixturepb.MutationOp_MUTATION_OP_UPSERT, Entity: es}
			if _, perr := c.commitSingleBatch(ctx, []*fixturepb.MutationSpec{m}, []*pb.Mutation{c.buildMutation(m)}); perr != nil {
				return &fixturepb.RunQueryFixtureResponse{Error: toRefVeneerError(perr)}, nil
			}
		}
	}
	return res, nil
}

func (c *refClient) runAggWithRO(
	ctx context.Context,
	namespaceID string,
	sq *fixturepb.StructuredQuerySpec,
	gql *fixturepb.GqlQuerySpec,
	aggs []*fixturepb.AggregationSpec,
	ro *pb.ReadOptions,
	eo *fixturepb.ExplainOptionsSpec,
	onResponse func(*pb.RunAggregationQueryResponse),
) (*fixturepb.RunAggregationQueryFixtureResponse, error) {
	if sq != nil && sq.Offset != nil && sq.GetOffset() < 0 {
		return nil, &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "query offset must be non-negative"}
	}
	aq := &pb.AggregationQuery{
		Aggregations: AggregationsToPB(aggs),
	}
	if sq != nil {
		q := StructuredQueryToPB(sq)
		if sq.AncestorKey != nil && q.Filter != nil {
			fixAncestorPartition(q.Filter, c)
		}
		aq.QueryType = &pb.AggregationQuery_NestedQuery{NestedQuery: q}
	}

	req := &pb.RunAggregationQueryRequest{
		ProjectId:   c.projectID,
		DatabaseId:  c.databaseID,
		PartitionId: c.effectiveNamespace(namespaceID),
		ReadOptions: ro,
	}
	if eo != nil {
		req.ExplainOptions = &pb.ExplainOptions{Analyze: eo.Analyze}
	}
	if gql != nil {
		req.QueryType = &pb.RunAggregationQueryRequest_GqlQuery{GqlQuery: GqlQueryToPB(gql)}
	} else {
		req.QueryType = &pb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: aq}
	}

	var resp pb.RunAggregationQueryResponse
	if err := c.callRPC(ctx, "runAggregationQuery", "RunAggregationQuery", req, &resp, true); err != nil {
		return nil, err
	}
	if onResponse != nil {
		onResponse(&resp)
	}
	outMap := make(map[string]*fixturepb.ValueSpec)
	if resp.Batch != nil && len(resp.Batch.AggregationResults) > 0 {
		for alias, val := range resp.Batch.AggregationResults[0].AggregateProperties {
			outMap[alias] = ValueFromPB(val)
		}
	}
	return &fixturepb.RunAggregationQueryFixtureResponse{
		Results:        outMap,
		ExplainMetrics: ExplainMetricsFromPB(resp.ExplainMetrics),
	}, nil
}

func (s *referenceFixtureServer) RunAggregationQuery(ctx context.Context, req *fixturepb.RunAggregationQueryFixtureRequest) (*fixturepb.RunAggregationQueryFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	res, err := c.runAggWithRO(
		ctx,
		req.NamespaceId,
		req.GetStructuredQuery(),
		req.GetGqlQuery(),
		req.Aggregations,
		ReadSpecToPB(req.ReadSpec),
		req.ExplainOptions,
		nil,
	)
	if err != nil {
		return &fixturepb.RunAggregationQueryFixtureResponse{Error: toRefVeneerError(err)}, nil
	}
	return res, nil
}

type refTxState struct {
	client   *refClient
	readOnly bool
	readTime *timestamppb.Timestamp
	prevTxID []byte

	initMu sync.Mutex
	mu     sync.Mutex
	txID   []byte
}

func (t *refTxState) txOptions() *pb.TransactionOptions {
	if t.readOnly {
		return &pb.TransactionOptions{
			Mode: &pb.TransactionOptions_ReadOnly_{
				ReadOnly: &pb.TransactionOptions_ReadOnly{ReadTime: t.readTime},
			},
		}
	}
	return &pb.TransactionOptions{
		Mode: &pb.TransactionOptions_ReadWrite_{
			ReadWrite: &pb.TransactionOptions_ReadWrite{PreviousTransaction: t.prevTxID},
		},
	}
}

// withReadOptions serializes lazy transaction start so only the first read sends new_transaction
// and concurrent initial reads wait until txID is obtained before issuing ReadOptions.transaction.
func (t *refTxState) withReadOptions(fn func(ro *pb.ReadOptions, recordTx func([]byte)) error) error {
	t.mu.Lock()
	id := t.txID
	t.mu.Unlock()
	if len(id) > 0 {
		return fn(&pb.ReadOptions{ConsistencyType: &pb.ReadOptions_Transaction{Transaction: id}}, nil)
	}

	t.initMu.Lock()
	defer t.initMu.Unlock()

	t.mu.Lock()
	id = t.txID
	t.mu.Unlock()
	if len(id) > 0 {
		return fn(&pb.ReadOptions{ConsistencyType: &pb.ReadOptions_Transaction{Transaction: id}}, nil)
	}

	ro := &pb.ReadOptions{
		ConsistencyType: &pb.ReadOptions_NewTransaction{NewTransaction: t.txOptions()},
	}
	return fn(ro, func(newID []byte) {
		if len(newID) > 0 {
			t.mu.Lock()
			if len(t.txID) == 0 {
				t.txID = append([]byte(nil), newID...)
			}
			t.mu.Unlock()
		}
	})
}

func (c *refClient) lookupMultiWithTxRecorder(ctx context.Context, keys []*fixturepb.KeySpec, ro *pb.ReadOptions, pm *pb.PropertyMask, recordTx func([]byte)) ([]*fixturepb.FoundEntity, error) {
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
	curRO := ro
	for len(pending) > 0 {
		var resp pb.LookupResponse
		if err := c.callRPC(ctx, "lookup", "Lookup", &pb.LookupRequest{
			ProjectId:    c.projectID,
			DatabaseId:   c.databaseID,
			ReadOptions:  curRO,
			Keys:         pending,
			PropertyMask: pm,
		}, &resp, true); err != nil {
			return nil, err
		}
		if recordTx != nil && len(resp.Transaction) > 0 {
			recordTx(resp.Transaction)
			curRO = &pb.ReadOptions{ConsistencyType: &pb.ReadOptions_Transaction{Transaction: resp.Transaction}}
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

func (s *referenceFixtureServer) RunInTransaction(ctx context.Context, req *fixturepb.RunInTransactionFixtureRequest) (*fixturepb.RunInTransactionFixtureResponse, error) {
	c, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	maxAttempts := int(req.MaxAttempts)
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	var prevTxID []byte
	attempts := int32(0)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		attempts++
		tx := &refTxState{
			client:   c,
			readOnly: req.ReadOnly,
			readTime: req.ReadTime,
			prevTxID: prevTxID,
		}

		if !req.BeginLater {
			var beginResp pb.BeginTransactionResponse
			if err := c.callRPC(ctx, "beginTransaction", "BeginTransaction", &pb.BeginTransactionRequest{
				ProjectId:          c.projectID,
				DatabaseId:         c.databaseID,
				TransactionOptions: tx.txOptions(),
			}, &beginResp, false); err != nil {
				return &fixturepb.RunInTransactionFixtureResponse{
					CallbackAttempts: attempts,
					Error:            toRefVeneerError(err),
				}, nil
			}
			tx.txID = beginResp.Transaction
		}

		type pendingMut struct {
			spec         *fixturepb.MutationSpec
			pbMut        *pb.Mutation
			pendingKeyID string
		}
		var pending []pendingMut
		var stepResults []*fixturepb.TransactionStepResult
		var stepErr error

		for _, st := range req.Steps {
			if stepErr != nil {
				break
			}
			switch s := st.Step.(type) {
			case *fixturepb.TransactionStep_Get:
				var found []*fixturepb.FoundEntity
				stepErr = tx.withReadOptions(func(ro *pb.ReadOptions, rec func([]byte)) error {
					var lerr error
					found, lerr = c.lookupMultiWithTxRecorder(ctx, []*fixturepb.KeySpec{s.Get.Key}, ro, PropertyMaskToPB(s.Get.PropertyMask), rec)
					return lerr
				})
				if stepErr == nil {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_GetResult{GetResult: found[0]},
					})
				}

			case *fixturepb.TransactionStep_GetMulti:
				var found []*fixturepb.FoundEntity
				stepErr = tx.withReadOptions(func(ro *pb.ReadOptions, rec func([]byte)) error {
					var lerr error
					found, lerr = c.lookupMultiWithTxRecorder(ctx, s.GetMulti.Keys, ro, PropertyMaskToPB(s.GetMulti.PropertyMask), rec)
					return lerr
				})
				if stepErr == nil {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_GetMultiResult{
							GetMultiResult: &fixturepb.GetMultiFixtureResponse{Results: found},
						},
					})
				}

			case *fixturepb.TransactionStep_RunQuery:
				var qres *fixturepb.RunQueryFixtureResponse
				stepErr = tx.withReadOptions(func(ro *pb.ReadOptions, rec func([]byte)) error {
					var qerr error
					qres, qerr = c.runQueryWithRO(
						ctx,
						s.RunQuery.NamespaceId,
						s.RunQuery.GetStructuredQuery(),
						s.RunQuery.GetGqlQuery(),
						ro,
						PropertyMaskToPB(s.RunQuery.PropertyMask),
						s.RunQuery.ExplainOptions,
						s.RunQuery.StopAfterItems,
						false,
						func(r *pb.RunQueryResponse) {
							if rec != nil && len(r.Transaction) > 0 {
								rec(r.Transaction)
							}
						},
					)
					return qerr
				})
				if stepErr == nil {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_RunQueryResult{RunQueryResult: qres},
					})
				}

			case *fixturepb.TransactionStep_RunAggregationQuery:
				var ares *fixturepb.RunAggregationQueryFixtureResponse
				stepErr = tx.withReadOptions(func(ro *pb.ReadOptions, rec func([]byte)) error {
					var aerr error
					ares, aerr = c.runAggWithRO(
						ctx,
						s.RunAggregationQuery.NamespaceId,
						s.RunAggregationQuery.GetStructuredQuery(),
						s.RunAggregationQuery.GetGqlQuery(),
						s.RunAggregationQuery.Aggregations,
						ro,
						s.RunAggregationQuery.ExplainOptions,
						func(r *pb.RunAggregationQueryResponse) {
							if rec != nil && len(r.Transaction) > 0 {
								rec(r.Transaction)
							}
						},
					)
					return aerr
				})
				if stepErr == nil {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_RunAggregationQueryResult{RunAggregationQueryResult: ares},
					})
				}

			case *fixturepb.TransactionStep_Mutate:
				if tx.readOnly {
					stepErr = &refKindError{
						kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_STATE,
						msg:  "cannot mutate in a read-only transaction",
					}
					break
				}
				for i, m := range s.Mutate.Mutations {
					if verr := c.validateMutation(m); verr != nil {
						stepErr = verr
						break
					}
					var pkID string
					if i < len(s.Mutate.PendingKeyIds) {
						pkID = s.Mutate.PendingKeyIds[i]
					}
					pending = append(pending, pendingMut{
						spec:         m,
						pbMut:        c.buildMutation(m),
						pendingKeyID: pkID,
					})
				}

			case *fixturepb.TransactionStep_ParallelReads:
				keys := s.ParallelReads.Keys
				results := make([]*fixturepb.FoundEntity, len(keys))
				errs := make([]error, len(keys))
				var wg sync.WaitGroup
				for i, k := range keys {
					wg.Add(1)
					go func(idx int, key *fixturepb.KeySpec) {
						defer wg.Done()
						errs[idx] = tx.withReadOptions(func(ro *pb.ReadOptions, rec func([]byte)) error {
							found, lerr := c.lookupMultiWithTxRecorder(ctx, []*fixturepb.KeySpec{key}, ro, nil, rec)
							if lerr != nil {
								return lerr
							}
							results[idx] = found[0]
							return nil
						})
					}(i, k)
				}
				wg.Wait()
				for _, e := range errs {
					if e != nil {
						stepErr = e
						break
					}
				}
				if stepErr == nil {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_ParallelReadsResult{
							ParallelReadsResult: &fixturepb.TxParallelReadsResult{Results: results},
						},
					})
				}

			case *fixturepb.TransactionStep_FailCallback:
				stepErr = &refKindError{
					kind: fixturepb.ErrorKind_ERROR_KIND_CALLBACK,
					msg:  s.FailCallback.Message,
				}
			}
		}

		if stepErr != nil {
			if len(tx.txID) > 0 {
				var rbResp pb.RollbackResponse
				_ = c.callRPC(ctx, "rollback", "Rollback", &pb.RollbackRequest{
					ProjectId:   c.projectID,
					DatabaseId:  c.databaseID,
					Transaction: tx.txID,
				}, &rbResp, false)
			}
			if isAbortedErr(stepErr) && attempt+1 < maxAttempts {
				prevTxID = tx.txID
				continue
			}
			return &fixturepb.RunInTransactionFixtureResponse{
				CallbackAttempts: attempts,
				Error:            toRefVeneerError(stepErr),
			}, nil
		}

		if len(tx.txID) == 0 {
			var beginResp pb.BeginTransactionResponse
			if err := c.callRPC(ctx, "beginTransaction", "BeginTransaction", &pb.BeginTransactionRequest{
				ProjectId:          c.projectID,
				DatabaseId:         c.databaseID,
				TransactionOptions: tx.txOptions(),
			}, &beginResp, false); err != nil {
				return &fixturepb.RunInTransactionFixtureResponse{
					CallbackAttempts: attempts,
					Error:            toRefVeneerError(err),
				}, nil
			}
			tx.txID = beginResp.Transaction
		}

		pbMuts := make([]*pb.Mutation, len(pending))
		for i, p := range pending {
			pbMuts[i] = p.pbMut
		}
		var commitResp pb.CommitResponse
		commitErr := c.callRPC(ctx, "commit", "Commit", &pb.CommitRequest{
			ProjectId:           c.projectID,
			DatabaseId:          c.databaseID,
			Mode:                pb.CommitRequest_TRANSACTIONAL,
			TransactionSelector: &pb.CommitRequest_Transaction{Transaction: tx.txID},
			Mutations:           pbMuts,
		}, &commitResp, false)
		if commitErr != nil {
			if isAbortedErr(commitErr) && attempt+1 < maxAttempts {
				prevTxID = tx.txID
				continue
			}
			return &fixturepb.RunInTransactionFixtureResponse{
				CallbackAttempts: attempts,
				Error:            toRefVeneerError(commitErr),
			}, nil
		}

		resolved := make(map[string]*fixturepb.KeySpec)
		for i, p := range pending {
			if p.pendingKeyID == "" {
				continue
			}
			if i < len(commitResp.MutationResults) && commitResp.MutationResults[i].Key != nil {
				resolved[p.pendingKeyID] = KeyFromPB(commitResp.MutationResults[i].Key)
			} else {
				resolved[p.pendingKeyID] = mutationInputKey(p.spec)
			}
		}
		return &fixturepb.RunInTransactionFixtureResponse{
			CallbackAttempts:    attempts,
			StepResults:         stepResults,
			ResolvedPendingKeys: resolved,
		}, nil
	}

	return &fixturepb.RunInTransactionFixtureResponse{
		CallbackAttempts: attempts,
		Error: &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_SERVICE,
			RpcCode: RpcCodePtr(fixturepb.RpcCode_RPC_CODE_ABORTED),
			Message: "exhausted transaction attempts",
		},
	}, nil
}
