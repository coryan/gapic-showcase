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
	"fmt"
	"net"
	"net/url"
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
)

func TestLifecycleScenarios_Unit(t *testing.T) {
	m3dAssertUnitMeta(t, scenarios.ExportLifecycleScenarios(), []harness.Scenario{
		{ID: "LIFE-01", Category: "lifecycle", RequiredCapabilities: []harness.Capability{harness.CapShutdownBehavior}}, {ID: "LIFE-02", Category: "lifecycle", RequiredCapabilities: []harness.Capability{harness.CapShutdownBehavior}}, {ID: "LIFE-03", Category: "lifecycle"},
	})
}
func TestLifecycleScenarios_Profiles(t *testing.T) {
	m3dRunProfileTests(t, scenarios.ExportLifecycleScenarios(), 3, 5*time.Second, map[string]int{"cpp": 2, "python": 2, "php": 3})
}
func TestLifecycleScenarios_FaultDetection(t *testing.T) {
	m3dRunFaultCases(t, scenarios.ExportLifecycleScenarios(), []m3dFaultCase{
		{prof: "go", fault: "runtime-unimpl-sub", sc: "LIFE-03", want: harness.StatusUnsupportedByClient},
		m3dGoF("LIFE-01", "life01-ignores-wait", "stopped prematurely"), m3dGoF("LIFE-01", "life01-deliv-on-stop", "while stopping"), m3dGoF("LIFE-01", "life01-bad-nack", "unexpected nacked ack_id"), m3dGoF("LIFE-01", "eod-premature-ack", "premature Ack"), m3dGoF("LIFE-01", "eod-auto-settled-interactive", "auto_settled=true"), {prof: "go", fault: "life01-hangs-finite-timeout", sc: "LIFE-01", sub: "TIMEOUT:", timeout: 1100 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("LIFE-02", "life02-skips-active-nack", "nacked ack_ids set mismatch"), m3dGoF("LIFE-02", "life02-skips-queued-nack", "nacked ack_ids set mismatch"), m3dGoF("LIFE-02", "life02-post-stop-modack", "post-stop lease extension"), m3dGoF("LIFE-02", "eod-premature-ack", "premature Ack"), m3dGoF("LIFE-02", "eod-auto-settled-interactive", "auto_settled=true"), m3dGoF("LIFE-02", "sub-send-eof-with-stopped", "stopped-before-settle"),
		m3dGoF("LIFE-03", "life03-stop-fails-inband", "StoppedEvent failed"), m3dGoF("LIFE-03", "life03-nonzero-stopped", "StoppedEvent failed"), m3dGoF("LIFE-03", "life03-spurious-ack", "expected 0 Ack/ModAck RPCs"), m3dGoF("LIFE-03", "life03-delivers-before-stop", "unexpected MessageDeliveredEvent"),
		m3dGoF("LIFE-03", "sub-empty-tok", "empty delivery_token"), m3dGoF("LIFE-03", "sub-unexpected-ev", "unexpected SubscriberEvent"), m3dGoF("LIFE-03", "life03-bad-total", "total_delivered mismatch"), m3dGoF("LIFE-03", "life03-extra-stream", "expected 1 active stream before first response"), m3dGoF("LIFE-01", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("LIFE-02", "sub-stop-inband-unimpl", "UNIMPLEMENTED"),
		{prof: "go", fault: "no-shutdown-to", sc: "LIFE-02", timeout: 2 * time.Second, want: harness.StatusPass, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsShutdownTimeout = false }},
	})
}
func TestLifecycleScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3dRunNoLeakFaults(t, scenarios.ExportLifecycleScenarios(), 1100*time.Millisecond, [3]string{"go", "life01-hangs-finite-timeout", "^LIFE-01$"})
}

type fastLife01Proxy struct {
	proxypb.UnimplementedPubSubProxyServer
	skipAck bool
	caps    *proxypb.GetCapabilitiesResponse
}

func (p *fastLife01Proxy) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return p.caps, nil
}

func (p *fastLife01Proxy) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}

func (p *fastLife01Proxy) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	first, err := stream.Recv()
	if err != nil || first.GetStart() == nil {
		return err
	}
	start := first.GetStart()
	sub := start.GetSubscription()
	conn, err := grpc.NewClient("passthrough:///"+start.GetEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	sc := pubsubpb.NewSubscriberClient(conn)
	outCtx := metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+url.QueryEscape(sub))
	sp, err := sc.StreamingPull(outCtx)
	if err != nil {
		return err
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: sub, StreamAckDeadlineSeconds: 60, ClientId: "fast-life01-cid"}); err != nil {
		return err
	}
	if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: "fast-life01-cid"}}}); err != nil {
		return err
	}
	resp, err := sp.Recv()
	if err != nil || len(resp.GetReceivedMessages()) == 0 {
		return err
	}
	rm := resp.GetReceivedMessages()[0]
	if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{
		MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: "tok-life01-a", MessageId: rm.GetMessage().GetMessageId(), Data: rm.GetMessage().GetData()},
	}}); err != nil {
		return err
	}
	stopSeen := false
	for {
		cmd, err := stream.Recv()
		if err != nil {
			_ = sp.CloseSend()
			return nil
		}
		if cmd.GetStop() != nil {
			stopSeen = true
		} else if cmd.GetSettle() != nil {
			if !p.skipAck {
				if _, aErr := sc.Acknowledge(outCtx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{rm.GetAckId()}}); aErr != nil {
					return aErr
				}
			}
			_ = sp.CloseSend()
			if stopSeen {
				return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
			}
			return nil
		}
	}
}

type fastLifeHandle struct {
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	caps   *proxypb.GetCapabilitiesResponse
	exited chan error
	once   sync.Once
}

func (h *fastLifeHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *fastLifeHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.caps }
func (h *fastLifeHandle) Exited() <-chan error                           { return h.exited }
func (h *fastLifeHandle) Stderr() string                                 { return "" }
func (h *fastLifeHandle) PID() int                                       { return 0 }
func (h *fastLifeHandle) Close(context.Context) error {
	h.once.Do(func() { _ = h.conn.Close(); h.srv.Stop(); <-h.exited })
	return nil
}

func TestLIFE01A_ZeroGapComplianceAndPrematureStopOracle(t *testing.T) {
	runOnce := func(skipAck bool) (*harness.TargetReport, error) {
		caps := &proxypb.GetCapabilitiesResponse{
			ClientLanguage: "go", ClientLibrary: "fast-life", ClientVersion: "1.0.0",
			SupportsShutdownBehavior: true, SupportsShutdownTimeout: false,
			Defaults: &proxypb.ClientDefaults{SubscriberMinAckExtensionMs: 35, SubscriberMaxAckExtensionMs: 600_000, SubscriberMaxExtensionMs: 3_600_000},
		}
		target := harness.TargetSpec{
			Name: "fast-life",
			Connect: func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
				lis, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					return nil, err
				}
				srv := grpc.NewServer()
				proxypb.RegisterPubSubProxyServer(srv, &fastLife01Proxy{skipAck: skipAck, caps: caps})
				exited := make(chan error, 1)
				go func() { exited <- srv.Serve(lis) }()
				conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					srv.Stop()
					<-exited
					return nil, err
				}
				return &fastLifeHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), caps: caps, exited: exited}, nil
			},
		}
		runner, err := harness.NewRunner(harness.RunConfig{
			Targets: []harness.TargetSpec{target}, Scenarios: scenarios.ExportLifecycleScenarios(), Filter: "^LIFE-01$", ScenarioTimeout: 3 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		return runner.RunTarget(context.Background(), target)
	}

	t.Run("ZeroGap_20xParallel", func(t *testing.T) {
		const workers = 20
		errs := make([]error, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				rep, err := runOnce(false)
				if err != nil {
					errs[idx] = fmt.Errorf("run %d: %w", idx, err)
					return
				}
				if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusPass {
					errs[idx] = fmt.Errorf("run %d want PASS, got %+v", idx, rep.Scenarios)
				}
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Negative_PrematureStopWithoutAck", func(t *testing.T) {
		rep, err := runOnce(true)
		if err != nil {
			t.Fatalf("RunTarget: %v", err)
		}
		if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "StoppedEvent arrived before Acknowledge") {
			t.Fatalf("expected premature stop FAIL, got %+v", rep.Scenarios)
		}
	})
}
