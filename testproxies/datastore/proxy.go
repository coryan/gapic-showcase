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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	ds "cloud.google.com/go/datastore"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var errFeatureNotImplemented = errors.New("feature not implemented")

type callbackError struct {
	msg string
}

func (e *callbackError) Error() string {
	return e.msg
}

type clientEntry struct {
	client     *ds.Client
	projectID  string
	databaseID string
	endpoint   string
}

// Server implements fixturepb.DatastoreFixtureServer backed by cloud.google.com/go/datastore.
type Server struct {
	fixturepb.UnimplementedDatastoreFixtureServer

	mu      sync.Mutex
	clients map[string]*clientEntry
	nextID  atomic.Uint64
}

// NewServer constructs a new Go Datastore fixture server.
func NewServer() *Server {
	return &Server{
		clients: make(map[string]*clientEntry),
	}
}

// Close closes all remaining managed Datastore clients.
func (s *Server) Close() {
	s.mu.Lock()
	entries := make([]*clientEntry, 0, len(s.clients))
	for _, e := range s.clients {
		entries = append(entries, e)
	}
	s.clients = make(map[string]*clientEntry)
	s.mu.Unlock()

	for _, e := range entries {
		_ = e.client.Close()
	}
}

func dialDatastoreClient(ctx context.Context, projectID, databaseID, endpoint string) (*ds.Client, error) {
	target := endpoint
	if target != "" && !strings.Contains(target, "://") {
		target = "passthrough:///" + target
	}
	opts := []option.ClientOption{
		option.WithEndpoint(target),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	return ds.NewClientWithDatabase(ctx, projectID, databaseID, opts...)
}

func (s *Server) getClient(id string) (*clientEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.clients[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown client_id %q", id)
	}
	return e, nil
}

func clientForReadSpec(ctx context.Context, entry *clientEntry, rs *fixturepb.ReadSpec, isPointRead bool) (*ds.Client, func(), error) {
	if rs == nil {
		return entry.client, func() {}, nil
	}
	if isPointRead && rs.ReadConsistency == fixturepb.ReadConsistencySpec_READ_CONSISTENCY_EVENTUAL {
		return nil, nil, fmt.Errorf("%w: cloud.google.com/go/datastore does not support EventualConsistency on point lookups", errFeatureNotImplemented)
	}
	if rs.ReadTime != nil {
		c, err := dialDatastoreClient(ctx, entry.projectID, entry.databaseID, entry.endpoint)
		if err != nil {
			return nil, nil, err
		}
		c = c.WithReadOptions(ds.ReadTime(rs.ReadTime.AsTime()))
		return c, func() { _ = c.Close() }, nil
	}
	return entry.client, func() {}, nil
}

// CreateClient creates a cloud.google.com/go/datastore client pointing at the harness server.
func (s *Server) CreateClient(ctx context.Context, req *fixturepb.CreateClientRequest) (*fixturepb.CreateClientResponse, error) {
	if req.Transport == fixturepb.Transport_TRANSPORT_HTTP_JSON {
		return &fixturepb.CreateClientResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore only supports gRPC transport"),
		}, nil
	}
	if req.NamespaceId != "" {
		return &fixturepb.CreateClientResponse{
			Error: featureNotImplemented("cloud.google.com/go/datastore does not support a client-level default namespace"),
		}, nil
	}
	c, err := dialDatastoreClient(ctx, req.ProjectId, req.DatabaseId, req.Endpoint)
	if err != nil {
		return &fixturepb.CreateClientResponse{Error: toVeneerError(err)}, nil
	}

	id := strconv.FormatUint(s.nextID.Add(1), 10)
	s.mu.Lock()
	s.clients[id] = &clientEntry{
		client:     c,
		projectID:  req.ProjectId,
		databaseID: req.DatabaseId,
		endpoint:   req.Endpoint,
	}
	s.mu.Unlock()

	return &fixturepb.CreateClientResponse{ClientId: id}, nil
}

// CloseClient closes and removes a managed client.
func (s *Server) CloseClient(ctx context.Context, req *fixturepb.CloseClientRequest) (*fixturepb.CloseClientResponse, error) {
	s.mu.Lock()
	e, ok := s.clients[req.ClientId]
	if ok {
		delete(s.clients, req.ClientId)
	}
	s.mu.Unlock()

	if ok {
		_ = e.client.Close()
	}
	return &fixturepb.CloseClientResponse{}, nil
}

func featureNotImplemented(msg string) *fixturepb.VeneerError {
	return &fixturepb.VeneerError{
		Kind:    fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED,
		Message: msg,
	}
}

func toVeneerError(err error) *fixturepb.VeneerError {
	if err == nil {
		return nil
	}
	if me, ok := err.(ds.MultiError); ok {
		for _, sub := range me {
			if sub != nil {
				err = sub
				break
			}
		}
	}
	if errors.Is(err, errFeatureNotImplemented) {
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED,
			Message: err.Error(),
		}
	}
	var cbErr *callbackError
	if errors.As(err, &cbErr) {
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_CALLBACK,
			Message: cbErr.Error(),
		}
	}
	if errors.Is(err, ds.ErrConcurrentTransaction) {
		rc := fixturepb.RpcCode_RPC_CODE_ABORTED
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_SERVICE,
			RpcCode: &rc,
			Message: err.Error(),
		}
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		rc := fixturepb.RpcCode(st.Code())
		ve := &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_SERVICE,
			RpcCode: &rc,
			Message: st.Message(),
		}
		for _, d := range st.Details() {
			if ei, ok := d.(*errdetails.ErrorInfo); ok {
				md := make(map[string]string, len(ei.Metadata))
				for k, v := range ei.Metadata {
					md[k] = v
				}
				ve.ErrorInfo = &fixturepb.ErrorInfoSpec{
					Reason:   ei.Reason,
					Domain:   ei.Domain,
					Metadata: md,
				}
				break
			}
		}
		return ve
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_TIMEOUT,
			Message: err.Error(),
		}
	}
	if errors.Is(err, ds.ErrInvalidKey) ||
		errors.Is(err, ds.ErrInvalidEntityType) ||
		errors.Is(err, ds.ErrDifferentKeyAndDstLength) ||
		errors.Is(err, ds.ErrDifferentKeyAndTransformsLength) ||
		strings.HasPrefix(err.Error(), "datastore:") {
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION,
			Message: err.Error(),
		}
	}
	return &fixturepb.VeneerError{
		Kind:    fixturepb.ErrorKind_ERROR_KIND_UNSPECIFIED,
		Message: err.Error(),
	}
}
