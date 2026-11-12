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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	genproto "github.com/googleapis/gapic-showcase/server/genproto"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// --- 100%-Compliant Dual-Transport Reference Fixture (Self-Test of Harness Wire Protocols) ---

type refKindError struct {
	kind       fixturepb.ErrorKind
	rpcCode    *fixturepb.RpcCode
	httpStatus *int32
	errorInfo  *fixturepb.ErrorInfoSpec
	msg        string
}

func (e *refKindError) Error() string {
	return e.msg
}

func toRefVeneerError(err error) *fixturepb.VeneerError {
	if err == nil {
		return nil
	}
	var rk *refKindError
	if errors.As(err, &rk) {
		return &fixturepb.VeneerError{
			Kind:       rk.kind,
			RpcCode:    rk.rpcCode,
			HttpStatus: rk.httpStatus,
			ErrorInfo:  rk.errorInfo,
			Message:    rk.msg,
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &fixturepb.VeneerError{
			Kind:    fixturepb.ErrorKind_ERROR_KIND_TIMEOUT,
			Message: err.Error(),
		}
	}
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded {
			// Server-returned DEADLINE_EXCEEDED stays ERROR_KIND_SERVICE when coming from backend;
			// local context deadlines are handled above or via HTTP/gRPC status check.
		}
		return &fixturepb.VeneerError{
			Kind:      fixturepb.ErrorKind_ERROR_KIND_SERVICE,
			RpcCode:   RpcCodePtr(GRPCCodeToRpcCode(st.Code())),
			ErrorInfo: ExtractErrorInfo(err),
			Message:   st.Message(),
		}
	}
	return &fixturepb.VeneerError{
		Kind:    fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION,
		Message: err.Error(),
	}
}

type refClient struct {
	id          string
	projectID   string
	databaseID  string
	namespaceID string
	endpoint    string
	transport   fixturepb.Transport

	grpcConn   *grpc.ClientConn
	grpcClient pb.DatastoreClient
	httpClient *http.Client
}

type referenceFixtureServer struct {
	fixturepb.UnimplementedDatastoreFixtureServer

	mu      sync.Mutex
	clients map[string]*refClient
	nextID  atomic.Uint64
}

func newReferenceFixtureServer() *referenceFixtureServer {
	return &referenceFixtureServer{
		clients: make(map[string]*refClient),
	}
}

func (s *referenceFixtureServer) Close() {
	s.mu.Lock()
	list := make([]*refClient, 0, len(s.clients))
	for _, c := range s.clients {
		list = append(list, c)
	}
	s.clients = make(map[string]*refClient)
	s.mu.Unlock()

	for _, c := range list {
		if c.grpcConn != nil {
			_ = c.grpcConn.Close()
		}
	}
}

func (s *referenceFixtureServer) getClient(id string) (*refClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown client %q", id)
	}
	return c, nil
}

func (s *referenceFixtureServer) CreateClient(ctx context.Context, req *fixturepb.CreateClientRequest) (*fixturepb.CreateClientResponse, error) {
	tr := req.Transport
	if tr == fixturepb.Transport_TRANSPORT_UNSPECIFIED {
		tr = fixturepb.Transport_TRANSPORT_GRPC
	}
	c := &refClient{
		id:          strconv.FormatUint(s.nextID.Add(1), 10),
		projectID:   req.ProjectId,
		databaseID:  req.DatabaseId,
		namespaceID: req.NamespaceId,
		endpoint:    req.Endpoint,
		transport:   tr,
	}
	switch tr {
	case fixturepb.Transport_TRANSPORT_GRPC:
		conn, err := grpc.NewClient(req.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &fixturepb.CreateClientResponse{Error: toRefVeneerError(err)}, nil
		}
		c.grpcConn = conn
		c.grpcClient = pb.NewDatastoreClient(conn)
	case fixturepb.Transport_TRANSPORT_HTTP_JSON:
		c.httpClient = &http.Client{}
	default:
		return &fixturepb.CreateClientResponse{
			Error: &fixturepb.VeneerError{
				Kind:    fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED,
				Message: fmt.Sprintf("unsupported transport %v", tr),
			},
		}, nil
	}

	s.mu.Lock()
	s.clients[c.id] = c
	s.mu.Unlock()
	return &fixturepb.CreateClientResponse{ClientId: c.id}, nil
}

func (s *referenceFixtureServer) CloseClient(ctx context.Context, req *fixturepb.CloseClientRequest) (*fixturepb.CloseClientResponse, error) {
	s.mu.Lock()
	c, ok := s.clients[req.ClientId]
	if ok {
		delete(s.clients, req.ClientId)
	}
	s.mu.Unlock()

	if ok && c.grpcConn != nil {
		_ = c.grpcConn.Close()
	}
	return &fixturepb.CloseClientResponse{}, nil
}

func (c *refClient) routingHeaders() map[string]string {
	reqParams := "project_id=" + url.QueryEscape(c.projectID)
	prefix := "projects/" + url.QueryEscape(c.projectID)
	if c.databaseID != "" {
		reqParams += "&database_id=" + url.QueryEscape(c.databaseID)
		prefix += "/databases/" + url.QueryEscape(c.databaseID)
	}
	apiClient := "gccl/ref-0.1.0 "
	if c.transport == fixturepb.Transport_TRANSPORT_HTTP_JSON {
		apiClient += "rest/1.0"
	} else {
		apiClient += "grpc/1.0"
	}
	return map[string]string{
		"x-goog-request-params":        reqParams,
		"google-cloud-resource-prefix": prefix,
		"x-goog-api-client":            apiClient,
	}
}

func (c *refClient) callRPC(ctx context.Context, verb, grpcMethod string, req proto.Message, resp proto.Message, idempotent bool) error {
	maxAttempts := 1
	if idempotent {
		maxAttempts = 3
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
		proto.Reset(resp)
		lastErr = c.callOnce(ctx, verb, grpcMethod, req, resp)
		if lastErr == nil {
			return nil
		}
		if !idempotent || !isUnavailableErr(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func isUnavailableErr(err error) bool {
	var rk *refKindError
	if errors.As(err, &rk) {
		return rk.rpcCode != nil && *rk.rpcCode == fixturepb.RpcCode_RPC_CODE_UNAVAILABLE
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

func isAbortedErr(err error) bool {
	var rk *refKindError
	if errors.As(err, &rk) {
		return rk.rpcCode != nil && *rk.rpcCode == fixturepb.RpcCode_RPC_CODE_ABORTED
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Aborted
}

func (c *refClient) callOnce(ctx context.Context, verb, grpcMethod string, req proto.Message, resp proto.Message) error {
	headers := c.routingHeaders()
	if c.transport == fixturepb.Transport_TRANSPORT_HTTP_JSON {
		endpoint := strings.TrimPrefix(strings.TrimPrefix(c.endpoint, "http://"), "https://")
		rawURL := fmt.Sprintf("http://%s/v1/projects/%s:%s", endpoint, url.PathEscape(c.projectID), verb)
		body, err := (protojson.MarshalOptions{UseProtoNames: false}).Marshal(req)
		if err != nil {
			return err
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			httpReq.Header.Set(k, v)
		}
		httpResp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return err
		}
		defer httpResp.Body.Close()
		respBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return err
		}
		if httpResp.StatusCode != http.StatusOK {
			var restErr genproto.RestError
			if uerr := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(respBytes, &restErr); uerr == nil && restErr.Error != nil {
				rpcCode := fixturepb.RpcCode(restErr.Error.Status)
				httpCode := int32(httpResp.StatusCode)
				var eiSpec *fixturepb.ErrorInfoSpec
				for _, d := range restErr.Error.Details {
					var ei errdetails.ErrorInfo
					if d.UnmarshalTo(&ei) == nil {
						md := make(map[string]string, len(ei.Metadata))
						for k, v := range ei.Metadata {
							md[k] = v
						}
						eiSpec = &fixturepb.ErrorInfoSpec{
							Reason:   ei.Reason,
							Domain:   ei.Domain,
							Metadata: md,
						}
						break
					}
				}
				return &refKindError{
					kind:       fixturepb.ErrorKind_ERROR_KIND_SERVICE,
					rpcCode:    RpcCodePtr(rpcCode),
					httpStatus: &httpCode,
					errorInfo:  eiSpec,
					msg:        restErr.Error.Message,
				}
			}
			httpCode := int32(httpResp.StatusCode)
			return &refKindError{
				kind:       fixturepb.ErrorKind_ERROR_KIND_SERVICE,
				httpStatus: &httpCode,
				msg:        string(respBytes),
			}
		}
		return (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(respBytes, resp)
	}

	md := metadata.New(headers)
	outCtx := metadata.NewOutgoingContext(ctx, md)
	switch grpcMethod {
	case "Lookup":
		r, err := c.grpcClient.Lookup(outCtx, req.(*pb.LookupRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "RunQuery":
		r, err := c.grpcClient.RunQuery(outCtx, req.(*pb.RunQueryRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "RunAggregationQuery":
		r, err := c.grpcClient.RunAggregationQuery(outCtx, req.(*pb.RunAggregationQueryRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "BeginTransaction":
		r, err := c.grpcClient.BeginTransaction(outCtx, req.(*pb.BeginTransactionRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "Commit":
		r, err := c.grpcClient.Commit(outCtx, req.(*pb.CommitRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "Rollback":
		r, err := c.grpcClient.Rollback(outCtx, req.(*pb.RollbackRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "AllocateIds":
		r, err := c.grpcClient.AllocateIds(outCtx, req.(*pb.AllocateIdsRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	case "ReserveIds":
		r, err := c.grpcClient.ReserveIds(outCtx, req.(*pb.ReserveIdsRequest))
		if err != nil {
			return err
		}
		proto.Merge(resp, r)
	default:
		return fmt.Errorf("unknown method %q", grpcMethod)
	}
	return nil
}

func (c *refClient) applyPartitionKey(ks *fixturepb.KeySpec) *pb.Key {
	if ks == nil {
		return nil
	}
	pk := KeyToPB(ks)
	ns := ks.GetPartitionId().GetNamespaceId()
	if ns == "" {
		ns = c.namespaceID
	}
	pk.PartitionId = &pb.PartitionId{
		ProjectId:   c.projectID,
		DatabaseId:  c.databaseID,
		NamespaceId: ns,
	}
	return pk
}

func (c *refClient) applyPartitionEntity(es *fixturepb.EntitySpec) *pb.Entity {
	if es == nil {
		return nil
	}
	pe := EntityToPB(es)
	if es.Key != nil {
		pe.Key = c.applyPartitionKey(es.Key)
	}
	return pe
}

func (c *refClient) effectiveNamespace(override string) *pb.PartitionId {
	ns := override
	if ns == "" {
		ns = c.namespaceID
	}
	if ns == "" && c.projectID == "" && c.databaseID == "" {
		return nil
	}
	return &pb.PartitionId{
		ProjectId:   c.projectID,
		DatabaseId:  c.databaseID,
		NamespaceId: ns,
	}
}

func stripMeaning18FromValue(v *fixturepb.ValueSpec) {
	if v == nil {
		return
	}
	if v.Meaning == 18 {
		v.Meaning = 0
	}
	if ev := v.GetEntityValue(); ev != nil {
		for _, child := range ev.Properties {
			stripMeaning18FromValue(child)
		}
	}
	if av := v.GetArrayValue(); av != nil {
		for _, child := range av.Values {
			stripMeaning18FromValue(child)
		}
	}
}

func entitySpecFromWire(e *pb.Entity) *fixturepb.EntitySpec {
	es := EntityFromPB(e)
	if es == nil {
		return nil
	}
	for _, v := range es.Properties {
		stripMeaning18FromValue(v)
	}
	return es
}

func refCanonicalKey(k *pb.Key) string {
	if k == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(k.GetPartitionId().GetNamespaceId())
	for _, p := range k.Path {
		b.WriteByte('/')
		b.WriteString(p.Kind)
		b.WriteByte(':')
		if p.GetName() != "" {
			b.WriteString("s:" + p.GetName())
		} else {
			b.WriteString("i:" + strconv.FormatInt(p.GetId(), 10))
		}
	}
	return b.String()
}

func refValidateKeyComplete(k *fixturepb.KeySpec) error {
	if k == nil || len(k.Path) == 0 {
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "key path must not be empty"}
	}
	for i, el := range k.Path {
		if el.Kind == "" || el.IdType == nil {
			return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: fmt.Sprintf("incomplete key element at path[%d]", i)}
		}
	}
	return nil
}

func refValidateKeyAncestors(k *fixturepb.KeySpec) error {
	if k == nil || len(k.Path) == 0 {
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "key path must not be empty"}
	}
	for i := 0; i < len(k.Path)-1; i++ {
		if k.Path[i].Kind == "" || k.Path[i].IdType == nil {
			return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: fmt.Sprintf("incomplete ancestor at path[%d]", i)}
		}
	}
	if k.Path[len(k.Path)-1].Kind == "" {
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "missing kind on leaf key element"}
	}
	return nil
}

func refValidateValue(v *fixturepb.ValueSpec) error {
	if v == nil {
		return nil
	}
	if av := v.GetArrayValue(); av != nil {
		for _, el := range av.Values {
			if el != nil && el.GetArrayValue() != nil {
				return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "arrays cannot directly contain arrays"}
			}
			if err := refValidateValue(el); err != nil {
				return err
			}
		}
	}
	if ev := v.GetEntityValue(); ev != nil {
		for _, child := range ev.Properties {
			if err := refValidateValue(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func refValidateEntity(e *fixturepb.EntitySpec) error {
	if e == nil {
		return &refKindError{kind: fixturepb.ErrorKind_ERROR_KIND_CLIENT_VALIDATION, msg: "entity must not be nil"}
	}
	for _, v := range e.Properties {
		if err := refValidateValue(v); err != nil {
			return err
		}
	}
	return nil
}
