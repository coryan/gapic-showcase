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

package scenarios_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLeaseScenarios_Unit(t *testing.T) {
	m3cAssertUnitMeta(t, scenarios.ExportLeaseScenarios(), []harness.Scenario{
		{ID: "LEASE-01", Category: "lease"},
		{ID: "LEASE-02", Category: "lease", Timeout: 60 * time.Second},
		{ID: "LEASE-03", Category: "lease", Timeout: 45 * time.Second},
		{ID: "LEASE-04", Category: "lease"},
		{ID: "LEASE-05", Category: "lease"},
	})
}
func TestLeaseScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportLeaseScenarios()
	m3cRunProfileTests(t, list, 5, 5*time.Second, map[string]int{"php": 5})
	m3cRunFaultCases(t, list, []m3cFaultCase{
		{prof: "cpp", fault: "lease03-sub10-tail", sc: "LEASE-03", timeout: 2 * time.Second, want: harness.StatusPass},
		{prof: "java", fault: "lease03-sub10-tail", sc: "LEASE-03", timeout: 2 * time.Second, want: harness.StatusPass},
		{prof: "java", fault: "lease05-modack-1", sc: "LEASE-05", timeout: 2 * time.Second, want: harness.StatusPass},
		{prof: "go", fault: "lease02-async-preack-race", sc: "LEASE-02", timeout: 2 * time.Second, want: harness.StatusPass},
	})
}
func TestLeaseScenarios_FaultDetection(t *testing.T) {
	m3cRunFaultCases(t, scenarios.ExportLeaseScenarios(), []m3cFaultCase{
		{prof: "go", fault: "runtime-unimpl-sub", sc: "LEASE-01", want: harness.StatusUnsupportedByClient},
		{prof: "go", fault: "lease01-drops-ack", sc: "LEASE-01", sub: "acked ack_ids set mismatch", timeout: 3 * time.Second},
		{prof: "go", fault: "lease01-drops-phase-a-nack", sc: "LEASE-01", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "lease01-acks-instead-of-nack", sc: "LEASE-01", sub: "ack_ids set mismatch", timeout: 3 * time.Second},
		{prof: "go", fault: "lease01-no-split-ack-1000", sc: "LEASE-01", sub: "outside [1, 1000]", timeout: 3 * time.Second},
		{prof: "go", fault: "lease01-no-split-nack-1000", sc: "LEASE-01", sub: "outside [1, 1000]", timeout: 3 * time.Second},
		{prof: "go", fault: "lease01-exceeds-512kb", sc: "LEASE-01", sub: "exceeds 524288 bytes", timeout: 3 * time.Second},
		{prof: "go", fault: "lease01-empty-ack-req", sc: "LEASE-01", sub: "ack_ids count 0 outside [1, 1000]", timeout: 3 * time.Second},
		{prof: "go", fault: "lease02-no-periodic-extend", sc: "LEASE-02", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "lease02-deadline-below-10", sc: "LEASE-02", sub: "outside 0 or [10, 600]"},
		{prof: "go", fault: "lease02-deadline-above-600", sc: "LEASE-02", sub: "outside 0 or [10, 600]"},
		{prof: "go", fault: "lease02-modack-above-600", sc: "LEASE-02", sub: "outside 0 or [10, 600]"},
		{prof: "go", fault: "lease02-post-ack-modack", sc: "LEASE-02", sub: "post-Ack ModifyAckDeadline"},
		{prof: "go", fault: "lease02-phaseb-post-ack-modack", sc: "LEASE-02", sub: "post-Ack ModifyAckDeadline"},
		{prof: "go", fault: "lease02-nacks-while-extending", sc: "LEASE-02", sub: "nacked ack_ids"},
		{prof: "go", fault: "lease03-extends-forever", sc: "LEASE-03", sub: "unexpected ModifyAckDeadline"},
		{prof: "go", fault: "lease03-delayed-modack-after-fc-release", sc: "LEASE-03", sub: "unexpected ModifyAckDeadline"},
		{prof: "rust", fault: "lease03-delayed-modack-after-fc-release", sc: "LEASE-03", sub: "unexpected ModifyAckDeadline"},
		{prof: "go", fault: "lease03-delayed-modack-after-fc-release", sc: "LEASE-03", sub: "unexpected ModifyAckDeadline", timeout: 15 * time.Second, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.Defaults.SubscriberMaxExtensionMs = 3_600_000 }},
		{prof: "go", fault: "lease03-spurious-nack-on-expiry", sc: "LEASE-03", sub: "nacked ack_ids set mismatch"},
		{prof: "rust", fault: "lease03-rust-early-drop-nack", sc: "LEASE-03", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "lease03-leaks-fc-deadlock", sc: "LEASE-03", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "rust", fault: "lease03-rust-leaks-handler-deadlock", sc: "LEASE-03", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "lease03-no-pre-expiry-modack", sc: "LEASE-03", sub: "pre-expiry ModifyAckDeadline"},
		{prof: "cpp", fault: "short-max-ext", sc: "LEASE-03", sub: "pre-expiry ModifyAckDeadline", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.Defaults.SubscriberMaxExtensionMs = 15 }},
		{prof: "rust", fault: "lease03-no-pre-expiry-modack", sc: "LEASE-03", sub: "pre-expiry ModifyAckDeadline"},
		{prof: "go", fault: "lease03-negative-dl", sc: "LEASE-03", sub: "outside [10, 600]"},
		{prof: "cpp", fault: "lease03-negative-dl", sc: "LEASE-03", sub: "outside [1, 600]"},
		{prof: "go", fault: "lease03-sub10-tail", sc: "LEASE-03", sub: "outside [10, 600]"},
		{prof: "go", fault: "lease04-crashes-on-ack-err", sc: "LEASE-04"},
		{prof: "go", fault: "lease04-crashes-on-modack-err", sc: "LEASE-04"},
		{prof: "go", fault: "lease04-reopens-stream", sc: "LEASE-04", sub: "stream survival"},
		{prof: "go", fault: "lease04-closes-before-stop", sc: "LEASE-04", sub: "1 open stream before stop"},
		{prof: "cpp", fault: "lease05-missing-auto-nack", sc: "LEASE-05", sub: "nacked ack_ids set mismatch"},
		{prof: "rust", fault: "lease05-acks-instead-of-nack", sc: "LEASE-05", sub: "ack_ids set mismatch"},
		{prof: "go", fault: "lease05-spurious-nack-when-disabled", sc: "LEASE-05", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "lease05-modack-1", sc: "LEASE-05", sub: "outside [10, 600]"},
		{prof: "java", fault: "lease05-modack-601", sc: "LEASE-05", sub: "outside [1, 600]"},
		{prof: "go", fault: "lease05-sp-bad-mod-dl", sc: "LEASE-05", sub: "outside [10, 600]"},
		{prof: "go", fault: "pull02-empty-clientid", sc: "LEASE-05", sub: "empty client_id"},
		{prof: "node", fault: "lease05-node-never-evicts", sc: "LEASE-05", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "wrong-ack-sub", sc: "LEASE-01", sub: "subscription mismatch"},
		{prof: "go", fault: "missing-ack-req-params", sc: "LEASE-04", sub: "x-goog-request-params"},
		{prof: "go", fault: "emit-unexpected-span", sc: "LEASE-02", sub: "expected zero spans"},
		{prof: "go", fault: "spurious-nack-after-ack", sc: "LEASE-04", sub: "nacked ack_ids set mismatch"},
	})
}
func TestLeaseScenarios_GoStopWaitsMaxExtZeroNack(t *testing.T) {
	sr := m3cRunScenarios(t, m3cMockConfig{
		profile: "go", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.Defaults.SubscriberMaxExtensionMs = 120 },
	}, scenarios.ExportLeaseScenarios(), "^LEASE-05$", 2*time.Second).Scenarios[0]
	if sr.Status != harness.StatusPass || sr.DurationMs < 100 {
		t.Fatalf("want PASS waiting >=100ms for MaxExtensionMs expiry with 0 Nacks, got %+v", sr)
	}
}
func TestLeaseScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3cRunNoLeakFaults(t, scenarios.ExportLeaseScenarios(), 650*time.Millisecond,
		[3]string{"go", "lease02-no-periodic-extend", "^LEASE-02$"},
		[3]string{"go", "lease03-leaks-fc-deadlock", "^LEASE-03$"},
		[3]string{"rust", "lease03-rust-leaks-handler-deadlock", "^LEASE-03$"},
	)
}

type leaseBoundsMITM struct {
	pubsubpb.UnimplementedSubscriberServer
	srv       *grpc.Server
	addr      string
	upstream  *grpc.ClientConn
	subClient pubsubpb.SubscriberClient
	wg        sync.WaitGroup
}

func startLeaseBoundsMITM(targetAddr string) (*leaseBoundsMITM, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	m := &leaseBoundsMITM{
		srv:       grpc.NewServer(),
		addr:      lis.Addr().String(),
		upstream:  conn,
		subClient: pubsubpb.NewSubscriberClient(conn),
	}
	pubsubpb.RegisterSubscriberServer(m.srv, m)
	m.wg.Go(func() { _ = m.srv.Serve(lis) })
	return m, nil
}

func (s *leaseBoundsMITM) close() {
	s.srv.Stop()
	s.wg.Wait()
	_ = s.upstream.Close()
}

func (s *leaseBoundsMITM) fwdCtx(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, md.Copy())
	}
	return ctx
}

func (s *leaseBoundsMITM) StreamingPull(srv grpc.BidiStreamingServer[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	upCtx, cancel := context.WithCancel(s.fwdCtx(srv.Context()))
	defer cancel()
	cli, err := s.subClient.StreamingPull(upCtx)
	if err != nil {
		return err
	}
	s.wg.Go(func() {
		injected := false
		for {
			req, rerr := srv.Recv()
			if rerr != nil {
				_ = cli.CloseSend()
				return
			}
			if !injected {
				injected = true
				cloned := proto.Clone(req).(*pubsubpb.StreamingPullRequest)
				for i := range 1001 {
					cloned.ModifyDeadlineAckIds = append(cloned.ModifyDeadlineAckIds, fmt.Sprintf("extra-ext-%04d", i))
					cloned.ModifyDeadlineSeconds = append(cloned.ModifyDeadlineSeconds, 10)
				}
				req = cloned
			}
			if serr := cli.Send(req); serr != nil {
				return
			}
		}
	})
	for {
		resp, rerr := cli.Recv()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return rerr
		}
		if serr := srv.Send(resp); serr != nil {
			return serr
		}
	}
}

func (s *leaseBoundsMITM) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	return s.subClient.Acknowledge(s.fwdCtx(ctx), req)
}

func (s *leaseBoundsMITM) ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	return s.subClient.ModifyAckDeadline(s.fwdCtx(ctx), req)
}

type leaseBoundsHandle struct {
	harness.ProxyHandle
	client proxypb.PubSubProxyClient
	mu     *sync.Mutex
	mitms  *[]*leaseBoundsMITM
}

func (h *leaseBoundsHandle) Client() proxypb.PubSubProxyClient { return h.client }

func (h *leaseBoundsHandle) Close(ctx context.Context) error {
	err := h.ProxyHandle.Close(ctx)
	h.mu.Lock()
	mitms := *h.mitms
	*h.mitms = nil
	h.mu.Unlock()
	for _, m := range mitms {
		m.close()
	}
	return err
}

type leaseBoundsClient struct {
	proxypb.PubSubProxyClient
	mu    *sync.Mutex
	mitms *[]*leaseBoundsMITM
}

func (c *leaseBoundsClient) SubscriberSession(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent], error) {
	stream, err := c.PubSubProxyClient.SubscriberSession(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &leaseBoundsSubSession{BidiStreamingClient: stream, mu: c.mu, mitms: c.mitms}, nil
}

type leaseBoundsSubSession struct {
	grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
	mu    *sync.Mutex
	mitms *[]*leaseBoundsMITM
}

func (s *leaseBoundsSubSession) Send(cmd *proxypb.SubscriberCommand) error {
	if start := cmd.GetStart(); start != nil && start.GetEndpoint() != "" {
		mitm, err := startLeaseBoundsMITM(start.GetEndpoint())
		if err != nil {
			return err
		}
		s.mu.Lock()
		*s.mitms = append(*s.mitms, mitm)
		s.mu.Unlock()
		cloned := proto.Clone(cmd).(*proxypb.SubscriberCommand)
		cloned.GetStart().Endpoint = mitm.addr
		return s.BidiStreamingClient.Send(cloned)
	}
	return s.BidiStreamingClient.Send(cmd)
}

func TestLeaseScenarios_StreamingPullBoundsExceeded(t *testing.T) {
	baseConnect := m3cMockConnect(m3cMockConfig{profile: "go"})
	target := harness.TargetSpec{
		Name: "lease05-streaming-pull-bounds-exceeded",
		Connect: func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
			h, err := baseConnect(ctx, ts)
			if err != nil {
				return nil, err
			}
			var mu sync.Mutex
			var mitms []*leaseBoundsMITM
			return &leaseBoundsHandle{
				ProxyHandle: h,
				mu:          &mu,
				mitms:       &mitms,
				client:      &leaseBoundsClient{PubSubProxyClient: h.Client(), mu: &mu, mitms: &mitms},
			}, nil
		},
	}
	runner, err := harness.NewRunner(harness.RunConfig{
		Targets:         []harness.TargetSpec{target},
		Scenarios:       scenarios.ExportLeaseScenarios(),
		Filter:          "^LEASE-05$",
		ScenarioTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget: %v", err)
	}
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "StreamingPull bounds exceeded") {
		t.Fatalf("expected StatusFail containing 'StreamingPull bounds exceeded', got scenarios=%+v", rep.Scenarios)
	}
}
