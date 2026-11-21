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
	"io"
	"net"
	"slices"
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

func TestTracingExtraScenarios_Metadata(t *testing.T) {
	if scenarios.Len() != 65 {
		t.Fatalf("DefaultRegistry len = %d, want 65", scenarios.Len())
	}
	tr := scenarios.ExportTracingExtraScenarios()
	if len(tr) != 3 {
		t.Fatalf("len(ExportTracingExtraScenarios()) = %d, want 3", len(tr))
	}
	wantIDs := []string{"TRAC-01b", "TRAC-02", "TRAC-04"}
	for i, sc := range tr {
		wantTO := map[string]time.Duration{"TRAC-04": 60 * time.Second}[sc.ID]
		if sc.ID != wantIDs[i] || sc.Category != "traces" || sc.Feature == "" || sc.Description == "" || !sc.TraceOnly || sc.Timeout != wantTO {
			t.Fatalf("scenario[%d] unexpected metadata: %+v", i, sc)
		}
		if strings.Contains(sc.ID+sc.Name+sc.Category, "PUB-01") || strings.Contains(sc.ID+sc.Name+sc.Category, "NOMATCH") {
			t.Fatalf("scenario %s contains forbidden filter token", sc.ID)
		}
	}
	if !tr[0].IsolateProxy || !tr[1].IsolateProxy || !slices.Contains(tr[1].RequiredCapabilities, harness.CapPublishHedging) {
		t.Fatalf("TRAC-01b/TRAC-02 isolation or capability wrong: %+v %+v", tr[0], tr[1])
	}
	if out, err := harness.FilterScenarios(tr, "^traces$"); err != nil || len(out) != 3 {
		t.Fatalf("^traces$ matched %d scenarios (err=%v)", len(out), err)
	}
	if out, err := harness.FilterScenarios(tr, "^NOMATCH$"); err != nil || len(out) != 0 {
		t.Fatalf("^NOMATCH$ matched %d scenarios (err=%v)", len(out), err)
	}
}

func TestTracingExtraScenarios_Profiles(t *testing.T) {
	tr := scenarios.ExportTracingExtraScenarios()
	for _, tc := range []struct {
		name           string
		cfg            m3eProfileCfg
		pass, un, skip int
	}{
		{"go_full", m3eProfileCfg{Lang: "go", Monitoring: true, Hedging: true, EOD: true}, 3, 0, 0},
		{"cpp_partial_caps", m3eProfileCfg{Lang: "cpp", Monitoring: true, CppSpans: true}, 2, 1, 0},
		{"cpp_eod", m3eProfileCfg{Lang: "cpp", Monitoring: true, CppSpans: true, EOD: true}, 2, 1, 0},
		{"rust_hedging_only", m3eProfileCfg{Lang: "rust", Monitoring: true, Hedging: true}, 3, 0, 0},
		{"supports_monitoring_false", m3eProfileCfg{Lang: "go", Monitoring: false, Hedging: true, EOD: true}, 0, 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := m3eRunSuite(t, tr, tc.cfg, 0)
			if rep.TargetError != "" || rep.Summary.Fail != 0 || rep.Summary.Pass != tc.pass || rep.Summary.UnsupportedByClient != tc.un || rep.Summary.SkippedNoMonitoring != tc.skip {
				t.Fatalf("%s unexpected: err=%q summary=%+v results=%+v", tc.name, rep.TargetError, rep.Summary, rep.Scenarios)
			}
		})
	}
}

func TestTracingExtraScenarios_FaultDetection(t *testing.T) {
	tr := scenarios.ExportTracingExtraScenarios()
	m3eRunFaults(t, tr, "TRAC-02", m3eProfileCfg{Lang: "go", Monitoring: true, Hedging: true}, 650*time.Millisecond, harness.StatusTimeout, "attempt0-wins")
	m3eRunFaults(t, tr, "TRAC-01b", m3eProfileCfg{Lang: "go", Monitoring: true}, 0, "",
		"inject-wire-trace-disabled", "inject-unexpected-span", "cb-trace-disabled", "mutate-caller-attrs", "wrong-sub-msg-id", "wrong-sub-data", "wrong-sub-key", "wrong-sub-attr", "wrong-pub-msg-id")
	m3eRunFaults(t, tr, "TRAC-02", m3eProfileCfg{Lang: "go", Monitoring: true, Hedging: true}, 0, "",
		"trac02-wrong-win-id", "trac02-skip-prefill", "trac02-bad-prefill-batch", "trac02-single-attempt", "attempt0-not-canceled", "trac02-drop-prefill-tp", "unsampled-wire-trace", "trac02-drop-target-wire-trace", "omit-hedged-events", "inject-unexpected-span")
	m3eRunFaults(t, tr, "TRAC-04", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}, 0, "",
		"drop-cb-trace", "drop-cb-trace-b", "process-before-ack", "process-too-short", "omit-process-span", "omit-process-ack-called", "only-receipt-modack", "omit-periodic-modack-span", "omit-eod-span-error", "swallow-eod-settlement", "eod-skip-wire-ack")
}

func TestTracingExtraScenarios_NoGoroutineOrFDLeak_REMSPLIT650(t *testing.T) {
	m3eRunNoLeakFaults(t, scenarios.ExportTracingExtraScenarios(), 650*time.Millisecond, []m3eFaultCase{
		{"TRAC-01b block publish", "TRAC-01b", m3eProfileCfg{Lang: "go", Monitoring: true, Fault: "block-pub-ctx"}, 0, ""},
		{"TRAC-02 attempt0 gate hang", "TRAC-02", m3eProfileCfg{Lang: "go", Monitoring: true, Hedging: true, Fault: "attempt0-wins"}, 0, ""},
		{"TRAC-04 block subscribe", "TRAC-04", m3eProfileCfg{Lang: "go", Monitoring: true, Fault: "block-sub-ctx"}, 0, ""},
	})
}

type trac01bSubMITM struct {
	pubsubpb.UnimplementedSubscriberServer
	srv           *grpc.Server
	addr          string
	upstream      *grpc.ClientConn
	subClient     pubsubpb.SubscriberClient
	mutateAckSub  bool
	addExtraAckID bool
	wg            sync.WaitGroup
}

func startTrac01bSubMITM(targetAddr string, mutateAckSub, addExtraAckID bool) (*trac01bSubMITM, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	m := &trac01bSubMITM{
		srv:           grpc.NewServer(),
		addr:          lis.Addr().String(),
		upstream:      conn,
		subClient:     pubsubpb.NewSubscriberClient(conn),
		mutateAckSub:  mutateAckSub,
		addExtraAckID: addExtraAckID,
	}
	pubsubpb.RegisterSubscriberServer(m.srv, m)
	m.wg.Go(func() { _ = m.srv.Serve(lis) })
	return m, nil
}

func (s *trac01bSubMITM) close() {
	s.srv.Stop()
	s.wg.Wait()
	_ = s.upstream.Close()
}

func (s *trac01bSubMITM) fwdCtx(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, md.Copy())
	}
	return ctx
}

func (s *trac01bSubMITM) StreamingPull(srv grpc.BidiStreamingServer[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]) error {
	upCtx, cancel := context.WithCancel(s.fwdCtx(srv.Context()))
	defer cancel()
	cli, err := s.subClient.StreamingPull(upCtx)
	if err != nil {
		return err
	}
	s.wg.Go(func() {
		for {
			req, rerr := srv.Recv()
			if rerr != nil {
				_ = cli.CloseSend()
				return
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

func (s *trac01bSubMITM) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	cloned := proto.Clone(req).(*pubsubpb.AcknowledgeRequest)
	if s.mutateAckSub {
		cloned.Subscription = cloned.GetSubscription() + "-wrong"
	}
	if s.addExtraAckID {
		cloned.AckIds = append(cloned.AckIds, "extra-bogus-ack-id")
	}
	return s.subClient.Acknowledge(s.fwdCtx(ctx), cloned)
}

func (s *trac01bSubMITM) ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	return s.subClient.ModifyAckDeadline(s.fwdCtx(ctx), req)
}

type trac01bWrapHandle struct {
	harness.ProxyHandle
	client proxypb.PubSubProxyClient
	mitms  *[]*trac01bSubMITM
	mu     *sync.Mutex
}

func (h *trac01bWrapHandle) Client() proxypb.PubSubProxyClient { return h.client }

func (h *trac01bWrapHandle) Close(ctx context.Context) error {
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

type trac01bWrapClient struct {
	proxypb.PubSubProxyClient
	mutateAckSub         bool
	addExtraAckID        bool
	injectUnexpectedPull bool
	mitms                *[]*trac01bSubMITM
	mu                   *sync.Mutex
}

func (c *trac01bWrapClient) CreatePublisher(ctx context.Context, in *proxypb.CreatePublisherRequest, opts ...grpc.CallOption) (*proxypb.CreatePublisherResponse, error) {
	if c.injectUnexpectedPull && in.GetEndpoint() != "" {
		conn, err := grpc.NewClient("passthrough:///"+in.GetEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		_, _ = pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{
			Subscription:      "projects/m3e-proj/subscriptions/unexpected",
			MaxMessages:       1,
			ReturnImmediately: true,
		})
		_ = conn.Close()
	}
	return c.PubSubProxyClient.CreatePublisher(ctx, in, opts...)
}

type trac01bSubSessionWrap struct {
	grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
	mutateAckSub  bool
	addExtraAckID bool
	mitms         *[]*trac01bSubMITM
	mu            *sync.Mutex
}

func (s *trac01bSubSessionWrap) Send(cmd *proxypb.SubscriberCommand) error {
	if start := cmd.GetStart(); start != nil && (s.mutateAckSub || s.addExtraAckID) {
		mitm, err := startTrac01bSubMITM(start.GetEndpoint(), s.mutateAckSub, s.addExtraAckID)
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

func (c *trac01bWrapClient) SubscriberSession(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent], error) {
	stream, err := c.PubSubProxyClient.SubscriberSession(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &trac01bSubSessionWrap{
		BidiStreamingClient: stream,
		mutateAckSub:        c.mutateAckSub,
		addExtraAckID:       c.addExtraAckID,
		mitms:               c.mitms,
		mu:                  c.mu,
	}, nil
}

func TestTRAC01b_AckAndUnexpectedRPCFaults(t *testing.T) {
	tr := scenarios.ExportTracingExtraScenarios()
	baseSpec := m3eTargetSpec("mock-go-trac01b-faults", m3eProfileCfg{Lang: "go", Monitoring: true}, false)

	cases := []struct {
		name                 string
		mutateAckSub         bool
		addExtraAckID        bool
		injectUnexpectedPull bool
		wantSub              string
		wantAltSub           string
	}{
		{
			name:         "WrongAckSubscription",
			mutateAckSub: true,
			wantSub:      "subscription mismatch",
		},
		{
			name:          "ExtraAckID",
			addExtraAckID: true,
			wantSub:       "ack_ids",
		},
		{
			name:                 "UnexpectedPullRPC",
			injectUnexpectedPull: true,
			wantSub:              "Subscriber/Pull",
			wantAltSub:           "unexpected RPC",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := harness.TargetSpec{
				Name: tc.name,
				Connect: func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
					h, err := baseSpec.Connect(ctx, ts)
					if err != nil {
						return nil, err
					}
					var mu sync.Mutex
					var mitms []*trac01bSubMITM
					return &trac01bWrapHandle{
						ProxyHandle: h,
						mitms:       &mitms,
						mu:          &mu,
						client: &trac01bWrapClient{
							PubSubProxyClient:    h.Client(),
							mutateAckSub:         tc.mutateAckSub,
							addExtraAckID:        tc.addExtraAckID,
							injectUnexpectedPull: tc.injectUnexpectedPull,
							mitms:                &mitms,
							mu:                   &mu,
						},
					}, nil
				},
			}
			runner, err := harness.NewRunner(harness.RunConfig{
				Targets:         []harness.TargetSpec{target},
				Scenarios:       tr,
				Filter:          "^TRAC-01b$",
				ScenarioTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			rep, err := runner.RunTarget(context.Background(), target)
			if err != nil {
				t.Fatalf("RunTarget: %v", err)
			}
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail {
				t.Fatalf("%s: expected StatusFail, got scenarios=%+v", tc.name, rep.Scenarios)
			}
			gotErr := rep.Scenarios[0].Error
			if !strings.Contains(gotErr, tc.wantSub) && (tc.wantAltSub == "" || !strings.Contains(gotErr, tc.wantAltSub)) {
				t.Fatalf("%s: expected error containing %q (or %q), got %q", tc.name, tc.wantSub, tc.wantAltSub, gotErr)
			}
		})
	}
}

type tracUnexpectedSubSession struct {
	grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent]
	ctx          context.Context
	injectPhaseA bool
	injectPhaseB bool
	client       *tracUnexpectedClient
}

func (s *tracUnexpectedSubSession) Send(cmd *proxypb.SubscriberCommand) error {
	if start := cmd.GetStart(); start != nil && start.GetEndpoint() != "" {
		if !start.GetUseExactlyOnce() && s.client != nil {
			s.client.mu.Lock()
			s.client.phaseAEndpoint = start.GetEndpoint()
			s.client.mu.Unlock()
		}
		shouldInject := (!start.GetUseExactlyOnce() && s.injectPhaseA) || (start.GetUseExactlyOnce() && s.injectPhaseB)
		if shouldInject {
			conn, err := grpc.NewClient("passthrough:///"+start.GetEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			_, _ = pubsubpb.NewPublisherClient(conn).Publish(s.ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/m3e-proj/topics/unexpected",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("unexpected-publish")}},
			})
			_ = conn.Close()
		}
	}
	return s.BidiStreamingClient.Send(cmd)
}

type tracUnexpectedClient struct {
	proxypb.PubSubProxyClient
	injectPullOnCreatePub   bool
	injectPhaseA            bool
	injectPhaseB            bool
	injectPhaseTransition   bool
	mu                      sync.Mutex
	phaseAEndpoint          string
	transitionResetInjected bool
}

func (c *tracUnexpectedClient) ResetTraces(ctx context.Context, in *proxypb.ResetTracesRequest, opts ...grpc.CallOption) (*proxypb.ResetTracesResponse, error) {
	if c.injectPhaseTransition {
		c.mu.Lock()
		ep := c.phaseAEndpoint
		already := c.transitionResetInjected
		if ep != "" && !already {
			c.transitionResetInjected = true
		}
		c.mu.Unlock()
		if ep != "" && !already {
			conn, err := grpc.NewClient("passthrough:///"+ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			_, _ = pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/m3e-proj/topics/unexpected-transition",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("unexpected-transition-publish")}},
			})
			_ = conn.Close()
		}
	}
	return c.PubSubProxyClient.ResetTraces(ctx, in, opts...)
}

func (c *tracUnexpectedClient) CreatePublisher(ctx context.Context, in *proxypb.CreatePublisherRequest, opts ...grpc.CallOption) (*proxypb.CreatePublisherResponse, error) {
	if c.injectPullOnCreatePub && in.GetEndpoint() != "" {
		conn, err := grpc.NewClient("passthrough:///"+in.GetEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		_, _ = pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{
			Subscription:      "projects/m3e-proj/subscriptions/unexpected",
			MaxMessages:       1,
			ReturnImmediately: true,
		})
		_ = conn.Close()
	}
	return c.PubSubProxyClient.CreatePublisher(ctx, in, opts...)
}

func (c *tracUnexpectedClient) SubscriberSession(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proxypb.SubscriberCommand, proxypb.SubscriberEvent], error) {
	stream, err := c.PubSubProxyClient.SubscriberSession(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &tracUnexpectedSubSession{
		BidiStreamingClient: stream,
		ctx:                 ctx,
		injectPhaseA:        c.injectPhaseA,
		injectPhaseB:        c.injectPhaseB,
		client:              c,
	}, nil
}

type tracUnexpectedHandle struct {
	harness.ProxyHandle
	client proxypb.PubSubProxyClient
}

func (h *tracUnexpectedHandle) Client() proxypb.PubSubProxyClient { return h.client }

func runTracUnexpectedRPCTest(t *testing.T, filter string, cfg m3eProfileCfg, wrapFn func(proxypb.PubSubProxyClient) proxypb.PubSubProxyClient, wantSub, wantAlt string) {
	t.Helper()
	tr := scenarios.ExportTracingExtraScenarios()
	baseSpec := m3eTargetSpec("mock-go-unexpected-rpc", cfg, false)
	target := harness.TargetSpec{
		Name: filter,
		Connect: func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
			h, err := baseSpec.Connect(ctx, ts)
			if err != nil {
				return nil, err
			}
			return &tracUnexpectedHandle{ProxyHandle: h, client: wrapFn(h.Client())}, nil
		},
	}
	runner, err := harness.NewRunner(harness.RunConfig{
		Targets:         []harness.TargetSpec{target},
		Scenarios:       tr,
		Filter:          filter,
		ScenarioTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget: %v", err)
	}
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail {
		t.Fatalf("%s: expected StatusFail, got scenarios=%+v", filter, rep.Scenarios)
	}
	gotErr := rep.Scenarios[0].Error
	if !strings.Contains(gotErr, wantSub) && (wantAlt == "" || !strings.Contains(gotErr, wantAlt)) {
		t.Fatalf("%s: expected error containing %q (or %q), got %q", filter, wantSub, wantAlt, gotErr)
	}
}

func TestTRAC02_UnexpectedRPC(t *testing.T) {
	runTracUnexpectedRPCTest(
		t,
		"^TRAC-02$",
		m3eProfileCfg{Lang: "go", Monitoring: true, Hedging: true},
		func(c proxypb.PubSubProxyClient) proxypb.PubSubProxyClient {
			return &tracUnexpectedClient{PubSubProxyClient: c, injectPullOnCreatePub: true}
		},
		"Subscriber/Pull",
		"disallowed RPC",
	)
}

func TestTRAC04_UnexpectedRPC(t *testing.T) {
	t.Run("PhaseA_UnexpectedPublishRPC", func(t *testing.T) {
		runTracUnexpectedRPCTest(
			t,
			"^TRAC-04$",
			m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true},
			func(c proxypb.PubSubProxyClient) proxypb.PubSubProxyClient {
				return &tracUnexpectedClient{PubSubProxyClient: c, injectPhaseA: true}
			},
			"Publisher/Publish",
			"disallowed RPC",
		)
	})
	t.Run("PhaseB_UnexpectedPublishRPC", func(t *testing.T) {
		runTracUnexpectedRPCTest(
			t,
			"^TRAC-04$",
			m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true},
			func(c proxypb.PubSubProxyClient) proxypb.PubSubProxyClient {
				return &tracUnexpectedClient{PubSubProxyClient: c, injectPhaseB: true}
			},
			"Publisher/Publish",
			"disallowed RPC",
		)
	})
	t.Run("PhaseTransition_UnexpectedPublishDuringResetTraces", func(t *testing.T) {
		runTracUnexpectedRPCTest(
			t,
			"^TRAC-04$",
			m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true},
			func(c proxypb.PubSubProxyClient) proxypb.PubSubProxyClient {
				return &tracUnexpectedClient{PubSubProxyClient: c, injectPhaseTransition: true}
			},
			"Publisher/Publish",
			"disallowed RPC",
		)
	})
}

func TestTRAC01bAndTRAC04_ModAckAndAckNackBounds(t *testing.T) {
	tr := scenarios.ExportTracingExtraScenarios()
	byID := func(id string) harness.Scenario {
		idx := slices.IndexFunc(tr, func(sc harness.Scenario) bool { return sc.ID == id })
		if idx < 0 {
			t.Fatalf("%s missing from ExportTracingExtraScenarios", id)
		}
		return tr[idx]
	}

	for _, tc := range []struct {
		name, scID, fault, wantSub string
		cfg                        m3eProfileCfg
	}{
		{"TRAC01b_SpuriousNack", "TRAC-01b", "trac01b-spurious-nack", "nacked ack_ids set mismatch", m3eProfileCfg{Lang: "go", Monitoring: true}},
		{"TRAC01b_NegModAck", "TRAC-01b", "trac01b-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go", Monitoring: true}},
		{"TRAC01b_OOBModAck", "TRAC-01b", "trac01b-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go", Monitoring: true}},
		{"TRAC04_NegModAck", "TRAC-04", "trac04-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_OOBModAck", "TRAC-04", "trac04-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_PhaseASpuriousNack", "TRAC-04", "trac04-phasea-spurious-nack", "nacked ack_ids set mismatch", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_PhaseANackAlsoAck", "TRAC-04", "trac04-phasea-nack-also-ack", "acked ack_ids set mismatch", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_PhaseANegModAck", "TRAC-04", "trac04-phasea-neg-modack", "outside [1, 600]", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_PhaseAOOBModAck601", "TRAC-04", "trac04-phasea-oob-modack-601", "outside [1, 600]", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
		{"TRAC04_PhaseAStreamingPullBadModDL", "TRAC-04", "trac04-phasea-sp-bad-mod-dl", "outside 0 or [1, 600]", m3eProfileCfg{Lang: "go", Monitoring: true, EOD: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Fault = tc.fault
			rep := m3eRunSuite(t, []harness.Scenario{byID(tc.scID)}, cfg, 0)
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("%s: expected StatusFail containing %q, got scenarios=%+v", tc.name, tc.wantSub, rep.Scenarios)
			}
		})
	}
}
