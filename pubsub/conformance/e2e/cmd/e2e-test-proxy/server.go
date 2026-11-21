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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProxyServer implements all 13 RPCs of proxypb.PubSubProxyServer.
type ProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer

	cfg        *ProfileConfig
	grpcServer *grpc.Server
	traces     *TraceStore

	pubMu      sync.Mutex
	publishers map[string]*PublisherInstance
	pubSeq     atomic.Int64

	shutdownOnce sync.Once
	stopCh       chan struct{}
}

// NewProxyServer creates a ProxyServer instance.
func NewProxyServer(cfg *ProfileConfig, grpcServer *grpc.Server) *ProxyServer {
	return &ProxyServer{
		cfg:        cfg,
		grpcServer: grpcServer,
		traces:     NewTraceStore(cfg),
		publishers: make(map[string]*PublisherInstance),
		stopCh:     make(chan struct{}),
	}
}

// Shutdown gracefully stops active publishers and the gRPC server within graceMs.
func (s *ProxyServer) Shutdown(graceMs int64) {
	s.shutdownOnce.Do(func() {
		close(s.stopCh)

		s.pubMu.Lock()
		pubs := make([]*PublisherInstance, 0, len(s.publishers))
		for _, p := range s.publishers {
			pubs = append(pubs, p)
		}
		s.publishers = make(map[string]*PublisherInstance)
		s.pubMu.Unlock()

		grace := 250 * time.Millisecond
		if graceMs > 0 {
			grace = time.Duration(graceMs) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		for _, p := range pubs {
			_ = p.Close(ctx)
		}

		if s.grpcServer != nil {
			done := make(chan struct{})
			go func() {
				s.grpcServer.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(grace):
				s.grpcServer.Stop()
			}
		}
	})
}

func (s *ProxyServer) getPublisher(id string) (*PublisherInstance, error) {
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	p, ok := s.publishers[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publisher_id %q", id)
	}
	return p, nil
}

func (s *ProxyServer) blockIfFault(faultName string) error {
	if !s.cfg.HasFault(faultName) {
		return nil
	}
	if p := os.Getenv("E2E_PROXY_FAULT_ENTERED_FILE"); p != "" {
		if f, err := os.CreateTemp(filepath.Dir(p), "."+filepath.Base(p)+".tmp.*"); err == nil {
			tmpName := f.Name()
			_, werr := f.WriteString(faultName + "\n")
			cerr := f.Close()
			if werr != nil || cerr != nil || os.Rename(tmpName, p) != nil {
				_ = os.Remove(tmpName)
			}
		}
	}
	<-s.stopCh
	return status.Error(codes.Unavailable, "proxy shutting down")
}

func (s *ProxyServer) GetCapabilities(ctx context.Context, _ *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return s.cfg.BuildCapabilities(), nil
}

func (s *ProxyServer) ResetTraces(ctx context.Context, _ *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	s.traces.Reset()
	return &proxypb.ResetTracesResponse{}, nil
}

func (s *ProxyServer) DrainTraces(ctx context.Context, _ *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	if err := s.blockIfFault("hang-drain"); err != nil {
		return nil, err
	}
	return &proxypb.DrainTracesResponse{
		Spans: s.traces.Drain(),
	}, nil
}

func (s *ProxyServer) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	s.cfg.TrackScenarioRPC()
	if err := s.blockIfFault("hang-create"); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("pub-%d", s.pubSeq.Add(1))
	pub, err := NewPublisherInstance(id, req, s.cfg, s.traces)
	if err != nil {
		return nil, err
	}
	s.pubMu.Lock()
	s.publishers[id] = pub
	s.pubMu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func (s *ProxyServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	s.cfg.TrackScenarioRPC()
	if err := s.blockIfFault("hang-publish"); err != nil {
		return nil, err
	}
	pub, err := s.getPublisher(req.PublisherId)
	if err != nil {
		return nil, err
	}
	return pub.Publish(ctx, req)
}

func (s *ProxyServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	if err := s.blockIfFault("hang-publish"); err != nil {
		return nil, err
	}
	pub, err := s.getPublisher(req.PublisherId)
	if err != nil {
		return nil, err
	}
	return pub.AwaitPublish(ctx, req.PublishToken)
}

func (s *ProxyServer) FlushPublisher(ctx context.Context, req *proxypb.FlushPublisherRequest) (*proxypb.FlushPublisherResponse, error) {
	if err := s.blockIfFault("hang-publish"); err != nil {
		return nil, err
	}
	pub, err := s.getPublisher(req.PublisherId)
	if err != nil {
		return nil, err
	}
	if err := pub.Flush(ctx); err != nil {
		return nil, err
	}
	return &proxypb.FlushPublisherResponse{}, nil
}

func (s *ProxyServer) StopPublisher(ctx context.Context, req *proxypb.StopPublisherRequest) (*proxypb.StopPublisherResponse, error) {
	pub, err := s.getPublisher(req.PublisherId)
	if err != nil {
		return nil, err
	}
	if err := pub.Stop(ctx); err != nil {
		return nil, err
	}
	return &proxypb.StopPublisherResponse{}, nil
}

func (s *ProxyServer) ResumePublish(ctx context.Context, req *proxypb.ResumePublishRequest) (*proxypb.ResumePublishResponse, error) {
	if !s.cfg.SupportsResumePublish {
		return nil, status.Error(codes.Unimplemented, "resume publish not supported")
	}
	pub, err := s.getPublisher(req.PublisherId)
	if err != nil {
		return nil, err
	}
	pub.ResumePublish(req.OrderingKey)
	return &proxypb.ResumePublishResponse{}, nil
}

func (s *ProxyServer) ClosePublisher(ctx context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	s.pubMu.Lock()
	pub, ok := s.publishers[req.PublisherId]
	if ok {
		delete(s.publishers, req.PublisherId)
	}
	s.pubMu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publisher_id %q", req.PublisherId)
	}
	if err := pub.Close(ctx); err != nil {
		return nil, err
	}
	return &proxypb.ClosePublisherResponse{}, nil
}

func (s *ProxyServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	s.cfg.TrackScenarioRPC()
	return runSubscriberSession(stream, s)
}

func (s *ProxyServer) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	s.cfg.TrackScenarioRPC()
	return handleUnaryPull(ctx, req, s)
}

func (s *ProxyServer) ShutdownProxy(ctx context.Context, req *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	if s.cfg.HasFault("ignore-shutdown") {
		return &proxypb.ShutdownProxyResponse{}, nil
	}
	graceMs := req.GracePeriodMs
	go func() {
		time.Sleep(15 * time.Millisecond)
		s.Shutdown(graceMs)
	}()
	return &proxypb.ShutdownProxyResponse{}, nil
}
