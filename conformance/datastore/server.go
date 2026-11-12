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
	"net"
	"net/http"
	"sync"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Expectation scripts a single expected Datastore data-plane RPC on SimServer.
type Expectation struct {
	Method   string
	Validate func(req proto.Message, md metadata.MD, transport fixturepb.Transport) error
	Handle   func(req proto.Message, md metadata.MD, transport fixturepb.Transport) (proto.Message, error)
	Resp     proto.Message
	Err      error
}

// RecordedCall captures an observed Datastore data-plane RPC and its request details.
type RecordedCall struct {
	Method        string
	Transport     fixturepb.Transport
	Metadata      metadata.MD
	Req           proto.Message
	ValidationErr error
}

// SimServer is a dual-transport (gRPC + HTTP/JSON) fake google.datastore.v1.Datastore
// backend used by conformance scenarios to validate requests and simulate responses.
type SimServer struct {
	pb.UnimplementedDatastoreServer

	lis        net.Listener
	mux        cmux.CMux
	grpcServer *grpc.Server
	httpServer *http.Server
	wg         sync.WaitGroup

	mu                              sync.Mutex
	expectations                    []Expectation
	calls                           []RecordedCall
	implicitRollbacks               []RecordedCall
	unexpectedErrors                []error
	allowImplicitRollbackAfterAbort bool
	lastCommitAborted               bool
	implicitRollbackValidator       func(req *pb.RollbackRequest, md metadata.MD, transport fixturepb.Transport) error
}

// NewSimServer creates and starts a dual-transport simulated Datastore server on 127.0.0.1:0.
func NewSimServer() (*SimServer, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	s := &SimServer{
		lis:                             lis,
		allowImplicitRollbackAfterAbort: true,
	}

	s.mux = cmux.New(lis)
	httpLis := s.mux.Match(cmux.HTTP1())
	grpcLis := s.mux.Match(cmux.HTTP2(), cmux.Any())

	s.grpcServer = grpc.NewServer()
	pb.RegisterDatastoreServer(s.grpcServer, s)

	s.httpServer = &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
	}

	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		_ = s.grpcServer.Serve(grpcLis)
	}()
	go func() {
		defer s.wg.Done()
		_ = s.httpServer.Serve(httpLis)
	}()
	go func() {
		defer s.wg.Done()
		_ = s.mux.Serve()
	}()

	return s, nil
}

// Addr returns the listener address in host:port form.
func (s *SimServer) Addr() string {
	return s.lis.Addr().String()
}

// Close shuts down the simulated server and waits for serving goroutines to exit.
func (s *SimServer) Close() error {
	s.grpcServer.Stop()
	_ = s.httpServer.Close()
	s.mux.Close()
	err := s.lis.Close()
	s.wg.Wait()
	return err
}

// Reset clears queued expectations and recorded calls.
func (s *SimServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectations = nil
	s.calls = nil
	s.implicitRollbacks = nil
	s.unexpectedErrors = nil
	s.lastCommitAborted = false
	s.allowImplicitRollbackAfterAbort = true
	s.implicitRollbackValidator = nil
}

// SetImplicitRollbackConfig configures whether an implicit Rollback sent after an
// ABORTED Commit (as Go's RunInTransaction performs) is accepted without consuming
// the next retry expectation.
func (s *SimServer) SetImplicitRollbackConfig(allow bool, validator func(*pb.RollbackRequest, metadata.MD, fixturepb.Transport) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowImplicitRollbackAfterAbort = allow
	s.implicitRollbackValidator = validator
}

// Expect appends one or more scripted expectations in FIFO order.
func (s *SimServer) Expect(exps ...Expectation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectations = append(s.expectations, exps...)
}

// Calls returns a copy of all matched recorded RPCs.
func (s *SimServer) Calls() []RecordedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RecordedCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// ImplicitRollbacks returns a copy of benign implicit Rollback calls absorbed after ABORTED Commits.
func (s *SimServer) ImplicitRollbacks() []RecordedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RecordedCall, len(s.implicitRollbacks))
	copy(out, s.implicitRollbacks)
	return out
}

// VerifyAllExpectationsMet checks that every expected RPC was invoked and passed validation.
func (s *SimServer) VerifyAllExpectationsMet() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.unexpectedErrors) > 0 {
		return s.unexpectedErrors[0]
	}
	for i, c := range s.calls {
		if c.ValidationErr != nil {
			return fmt.Errorf("call %d (%s) validation failed: %w", i, c.Method, c.ValidationErr)
		}
	}
	for i, rb := range s.implicitRollbacks {
		if rb.ValidationErr != nil {
			return fmt.Errorf("implicit rollback %d validation failed: %w", i, rb.ValidationErr)
		}
	}
	if len(s.expectations) > 0 {
		remaining := make([]string, 0, len(s.expectations))
		for _, e := range s.expectations {
			remaining = append(remaining, e.Method)
		}
		return fmt.Errorf("unconsumed expectations remaining: %v", remaining)
	}
	return nil
}

func (s *SimServer) dispatch(method string, transport fixturepb.Transport, md metadata.MD, req proto.Message) (proto.Message, error) {
	reqCopy := proto.Clone(req)
	mdCopy := md.Copy()

	s.mu.Lock()
	// Absorb implicit Rollback right after an ABORTED Commit when the next expectation is not Rollback.
	if method == "Rollback" && s.lastCommitAborted && s.allowImplicitRollbackAfterAbort {
		nextIsRollback := len(s.expectations) > 0 && s.expectations[0].Method == "Rollback"
		if !nextIsRollback {
			s.lastCommitAborted = false
			var valErr error
			if s.implicitRollbackValidator != nil {
				if rbReq, ok := reqCopy.(*pb.RollbackRequest); ok {
					valErr = s.implicitRollbackValidator(rbReq, mdCopy, transport)
				}
			}
			s.implicitRollbacks = append(s.implicitRollbacks, RecordedCall{
				Method:        method,
				Transport:     transport,
				Metadata:      mdCopy,
				Req:           reqCopy,
				ValidationErr: valErr,
			})
			s.mu.Unlock()
			return &pb.RollbackResponse{}, nil
		}
	}

	if len(s.expectations) == 0 {
		err := fmt.Errorf("unexpected RPC %s (no remaining scripted expectations)", method)
		s.unexpectedErrors = append(s.unexpectedErrors, err)
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	exp := s.expectations[0]
	if exp.Method != method {
		err := fmt.Errorf("expected RPC %s, got %s", exp.Method, method)
		s.unexpectedErrors = append(s.unexpectedErrors, err)
		s.expectations = s.expectations[1:]
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.expectations = s.expectations[1:]
	s.mu.Unlock()

	var valErr error
	if exp.Validate != nil {
		valErr = exp.Validate(reqCopy, mdCopy, transport)
	}

	var resp proto.Message
	var rpcErr error
	if exp.Handle != nil {
		resp, rpcErr = exp.Handle(reqCopy, mdCopy, transport)
	} else {
		resp, rpcErr = exp.Resp, exp.Err
	}

	s.mu.Lock()
	s.calls = append(s.calls, RecordedCall{
		Method:        method,
		Transport:     transport,
		Metadata:      mdCopy,
		Req:           reqCopy,
		ValidationErr: valErr,
	})
	if method == "Commit" && status.Code(rpcErr) == codes.Aborted {
		s.lastCommitAborted = true
	} else if method != "Rollback" {
		s.lastCommitAborted = false
	}
	s.mu.Unlock()

	return resp, rpcErr
}

func (s *SimServer) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("Lookup", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LookupResponse), nil
}

func (s *SimServer) RunQuery(ctx context.Context, req *pb.RunQueryRequest) (*pb.RunQueryResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("RunQuery", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RunQueryResponse), nil
}

func (s *SimServer) RunAggregationQuery(ctx context.Context, req *pb.RunAggregationQueryRequest) (*pb.RunAggregationQueryResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("RunAggregationQuery", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RunAggregationQueryResponse), nil
}

func (s *SimServer) BeginTransaction(ctx context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("BeginTransaction", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.BeginTransactionResponse), nil
}

func (s *SimServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("Commit", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.CommitResponse), nil
}

func (s *SimServer) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("Rollback", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RollbackResponse), nil
}

func (s *SimServer) AllocateIds(ctx context.Context, req *pb.AllocateIdsRequest) (*pb.AllocateIdsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("AllocateIds", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AllocateIdsResponse), nil
}

func (s *SimServer) ReserveIds(ctx context.Context, req *pb.ReserveIdsRequest) (*pb.ReserveIdsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	resp, err := s.dispatch("ReserveIds", fixturepb.Transport_TRANSPORT_GRPC, md, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ReserveIdsResponse), nil
}
