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
	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"golang.org/x/sync/errgroup"
)

func (s *Server) RunInTransaction(ctx context.Context, req *fixturepb.RunInTransactionFixtureRequest) (*fixturepb.RunInTransactionFixtureResponse, error) {
	entry, err := s.getClient(req.ClientId)
	if err != nil {
		return nil, err
	}
	if err := validateTransactionStepsSupport(req.Steps); err != nil {
		return &fixturepb.RunInTransactionFixtureResponse{Error: toVeneerError(err)}, nil
	}

	var txOpts []ds.TransactionOption
	if req.ReadOnly {
		txOpts = append(txOpts, ds.ReadOnly)
	}
	if req.ReadTime != nil {
		txOpts = append(txOpts, ds.WithReadTime(req.ReadTime.AsTime()))
	}
	if req.MaxAttempts > 0 {
		txOpts = append(txOpts, ds.MaxAttempts(int(req.MaxAttempts)))
	}
	if req.BeginLater {
		txOpts = append(txOpts, ds.BeginLater)
	}

	var (
		attempts    int32
		stepResults []*fixturepb.TransactionStepResult
		pendingKeys map[string]*ds.PendingKey
	)

	cmt, err := entry.client.RunInTransaction(ctx, func(tx *ds.Transaction) error {
		attempts++
		stepResults = make([]*fixturepb.TransactionStepResult, 0, len(req.Steps))
		pendingKeys = make(map[string]*ds.PendingKey)

		for _, step := range req.Steps {
			switch st := step.Step.(type) {
			case *fixturepb.TransactionStep_Get:
				var d dynamicEntity
				err := tx.Get(keyFromSpec(st.Get.Key), &d)
				if errors.Is(err, ds.ErrNoSuchEntity) {
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_GetResult{
							GetResult: &fixturepb.FoundEntity{Found: false},
						},
					})
					continue
				}
				if err != nil {
					return err
				}
				stepResults = append(stepResults, &fixturepb.TransactionStepResult{
					Result: &fixturepb.TransactionStepResult_GetResult{
						GetResult: &fixturepb.FoundEntity{
							Found:  true,
							Entity: dynamicToEntitySpec(&d),
						},
					},
				})

			case *fixturepb.TransactionStep_GetMulti:
				keys := make([]*ds.Key, len(st.GetMulti.Keys))
				for i, k := range st.GetMulti.Keys {
					keys[i] = keyFromSpec(k)
				}
				dsts := make([]dynamicEntity, len(keys))
				err := tx.GetMulti(keys, dsts)
				if me, ok := err.(ds.MultiError); ok {
					found := make([]*fixturepb.FoundEntity, len(me))
					for i, sub := range me {
						switch {
						case sub == nil:
							found[i] = &fixturepb.FoundEntity{Found: true, Entity: dynamicToEntitySpec(&dsts[i])}
						case errors.Is(sub, ds.ErrNoSuchEntity):
							found[i] = &fixturepb.FoundEntity{Found: false}
						default:
							return sub
						}
					}
					stepResults = append(stepResults, &fixturepb.TransactionStepResult{
						Result: &fixturepb.TransactionStepResult_GetMultiResult{
							GetMultiResult: &fixturepb.GetMultiFixtureResponse{Results: found},
						},
					})
					continue
				}
				if err != nil {
					return err
				}
				found := make([]*fixturepb.FoundEntity, len(dsts))
				for i := range dsts {
					found[i] = &fixturepb.FoundEntity{Found: true, Entity: dynamicToEntitySpec(&dsts[i])}
				}
				stepResults = append(stepResults, &fixturepb.TransactionStepResult{
					Result: &fixturepb.TransactionStepResult_GetMultiResult{
						GetMultiResult: &fixturepb.GetMultiFixtureResponse{Results: found},
					},
				})

			case *fixturepb.TransactionStep_RunQuery:
				q, keysOnly, err := queryFromSpec(st.RunQuery.GetStructuredQuery(), st.RunQuery.NamespaceId, nil, tx)
				if err != nil {
					return err
				}
				var runOpts []ds.RunOption
				if st.RunQuery.ExplainOptions != nil {
					runOpts = append(runOpts, ds.ExplainOptions{Analyze: st.RunQuery.ExplainOptions.Analyze})
				}
				qResp, _, err := executeQuery(ctx, entry.client, q, keysOnly, st.RunQuery.ExplainOptions, st.RunQuery.StopAfterItems, runOpts...)
				if err != nil {
					return err
				}
				stepResults = append(stepResults, &fixturepb.TransactionStepResult{
					Result: &fixturepb.TransactionStepResult_RunQueryResult{RunQueryResult: qResp},
				})

			case *fixturepb.TransactionStep_RunAggregationQuery:
				baseQ, _, err := queryFromSpec(st.RunAggregationQuery.GetStructuredQuery(), st.RunAggregationQuery.NamespaceId, nil, tx)
				if err != nil {
					return err
				}
				aq, err := aggregationQueryFromSpec(baseQ, st.RunAggregationQuery.Aggregations)
				if err != nil {
					return err
				}
				var runOpts []ds.RunOption
				if st.RunAggregationQuery.ExplainOptions != nil {
					runOpts = append(runOpts, ds.ExplainOptions{Analyze: st.RunAggregationQuery.ExplainOptions.Analyze})
				}
				res, err := entry.client.RunAggregationQueryWithOptions(ctx, aq, runOpts...)
				if err != nil {
					return err
				}
				out := make(map[string]*fixturepb.ValueSpec, len(res.Result))
				for k, v := range res.Result {
					if pbVal, ok := v.(*pb.Value); ok {
						out[k] = aggPBValueToSpec(pbVal)
					}
				}
				stepResults = append(stepResults, &fixturepb.TransactionStepResult{
					Result: &fixturepb.TransactionStepResult_RunAggregationQueryResult{
						RunAggregationQueryResult: &fixturepb.RunAggregationQueryFixtureResponse{
							Results:        out,
							ExplainMetrics: explainMetricsToView(res.ExplainMetrics),
						},
					},
				})

			case *fixturepb.TransactionStep_Mutate:
				if err := applyTxMutations(tx, st.Mutate, pendingKeys); err != nil {
					return err
				}

			case *fixturepb.TransactionStep_ParallelReads:
				results := make([]*fixturepb.FoundEntity, len(st.ParallelReads.Keys))
				var g errgroup.Group
				for i, ks := range st.ParallelReads.Keys {
					i, ks := i, ks
					g.Go(func() error {
						var d dynamicEntity
						err := tx.Get(keyFromSpec(ks), &d)
						if errors.Is(err, ds.ErrNoSuchEntity) {
							results[i] = &fixturepb.FoundEntity{Found: false}
							return nil
						}
						if err != nil {
							return err
						}
						results[i] = &fixturepb.FoundEntity{Found: true, Entity: dynamicToEntitySpec(&d)}
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return err
				}
				stepResults = append(stepResults, &fixturepb.TransactionStepResult{
					Result: &fixturepb.TransactionStepResult_ParallelReadsResult{
						ParallelReadsResult: &fixturepb.TxParallelReadsResult{Results: results},
					},
				})

			case *fixturepb.TransactionStep_FailCallback:
				return &callbackError{msg: st.FailCallback.Message}
			}
		}
		return nil
	}, txOpts...)

	if err != nil {
		return &fixturepb.RunInTransactionFixtureResponse{
			CallbackAttempts: attempts,
			Error:            toVeneerError(err),
		}, nil
	}

	resolved := make(map[string]*fixturepb.KeySpec, len(pendingKeys))
	for id, pk := range pendingKeys {
		resolved[id] = keyToSpec(cmt.Key(pk))
	}
	return &fixturepb.RunInTransactionFixtureResponse{
		CallbackAttempts:    attempts,
		StepResults:         stepResults,
		ResolvedPendingKeys: resolved,
	}, nil
}

func validateTransactionStepsSupport(steps []*fixturepb.TransactionStep) error {
	for _, step := range steps {
		switch st := step.Step.(type) {
		case *fixturepb.TransactionStep_Get:
			if st.Get.PropertyMask != nil {
				return fmt.Errorf("%w: cloud.google.com/go/datastore does not support PropertyMask on Transaction.Get", errFeatureNotImplemented)
			}
		case *fixturepb.TransactionStep_GetMulti:
			if st.GetMulti.PropertyMask != nil {
				return fmt.Errorf("%w: cloud.google.com/go/datastore does not support PropertyMask on Transaction.GetMulti", errFeatureNotImplemented)
			}
		case *fixturepb.TransactionStep_RunQuery:
			if st.RunQuery.PropertyMask != nil || st.RunQuery.GetGqlQuery() != nil || st.RunQuery.GetStructuredQuery().GetFindNearest() != nil {
				return fmt.Errorf("%w: unsupported RunQuery option in transaction", errFeatureNotImplemented)
			}
		case *fixturepb.TransactionStep_RunAggregationQuery:
			if st.RunAggregationQuery.GetGqlQuery() != nil {
				return fmt.Errorf("%w: GQL aggregation queries not supported", errFeatureNotImplemented)
			}
			for _, a := range st.RunAggregationQuery.Aggregations {
				if a.UpTo != nil {
					return fmt.Errorf("%w: Count UpTo not supported in Go client", errFeatureNotImplemented)
				}
			}
		case *fixturepb.TransactionStep_Mutate:
			for _, m := range st.Mutate.Mutations {
				if m.BaseVersion != nil || m.UpdateTime != nil {
					return fmt.Errorf("%w: conflict preconditions not supported in Go client", errFeatureNotImplemented)
				}
				if _, err := mutationsFromSpec([]*fixturepb.MutationSpec{m}); errors.Is(err, errFeatureNotImplemented) {
					return err
				}
			}
		}
	}
	return nil
}

func applyTxMutations(tx *ds.Transaction, step *fixturepb.TxMutateStep, pendingKeys map[string]*ds.PendingKey) error {
	if step.UseConvenienceMethods {
		for i, m := range step.Mutations {
			var pkID string
			if i < len(step.PendingKeyIds) {
				pkID = step.PendingKeyIds[i]
			}
			switch m.Op {
			case fixturepb.MutationOp_MUTATION_OP_DELETE:
				dk := m.DeleteKey
				if dk == nil && m.Entity != nil {
					dk = m.Entity.Key
				}
				if err := tx.Delete(keyFromSpec(dk)); err != nil {
					return err
				}
			case fixturepb.MutationOp_MUTATION_OP_UPSERT:
				if m.PropertyMask == nil {
					k, d, err := entityToDynamic(m.Entity)
					if err != nil {
						return err
					}
					var pk *ds.PendingKey
					if len(m.Transforms) > 0 {
						trs, err := transformsFromSpec(m.Transforms)
						if err != nil {
							return err
						}
						pk, err = tx.PutWithOptions(&ds.PutRequest{Key: k, Entity: d, Transforms: trs})
						if err != nil {
							return err
						}
					} else {
						pk, err = tx.Put(k, d)
						if err != nil {
							return err
						}
					}
					if pkID != "" && pk != nil {
						pendingKeys[pkID] = pk
					}
					continue
				}
				fallthrough
			default:
				muts, err := mutationsFromSpec([]*fixturepb.MutationSpec{m})
				if err != nil {
					return err
				}
				pks, err := tx.Mutate(muts...)
				if err != nil {
					return err
				}
				if pkID != "" && len(pks) > 0 && pks[0] != nil {
					pendingKeys[pkID] = pks[0]
				}
			}
		}
		return nil
	}

	muts, err := mutationsFromSpec(step.Mutations)
	if err != nil {
		return err
	}
	pks, err := tx.Mutate(muts...)
	if err != nil {
		return err
	}
	for i, pk := range pks {
		if i < len(step.PendingKeyIds) && step.PendingKeyIds[i] != "" && pk != nil {
			pendingKeys[step.PendingKeyIds[i]] = pk
		}
	}
	return nil
}
