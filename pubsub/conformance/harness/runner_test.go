// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type mockProxy struct {
	proxypb.UnimplementedPubSubProxyServer
	mu                                  sync.Mutex
	caps                                *proxypb.GetCapabilitiesResponse
	spans, emitOnCreatePub              []*proxypb.TraceSpan
	resetCounter                        *atomic.Int64
	resetErr                            error
	unimplPublish, inBandUnimpl, toFake bool
	hangPublish, hangReset              bool
	onClose                             func()
	onGetCaps                           func() (*proxypb.GetCapabilitiesResponse, error)
	exitCh                              chan error
	conn                                *grpc.ClientConn
	srv                                 *grpc.Server
	client                              proxypb.PubSubProxyClient
}

func startMockProxy(t *testing.T, caps *proxypb.GetCapabilitiesResponse, cfg func(*mockProxy)) TargetSpec {
	t.Helper()
	return TargetSpec{Name: caps.GetClientLanguage(), Connect: func(context.Context, TargetSpec) (ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		mp := &mockProxy{caps: caps, exitCh: make(chan error, 1), srv: grpc.NewServer()}
		if cfg != nil {
			cfg(mp)
		}
		proxypb.RegisterPubSubProxyServer(mp.srv, mp)
		go func() { _ = mp.srv.Serve(lis) }()
		conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			mp.srv.Stop()
			return nil, err
		}
		mp.conn, mp.client = conn, proxypb.NewPubSubProxyClient(conn)
		return mp, nil
	}}
}

func (m *mockProxy) Client() proxypb.PubSubProxyClient              { return m.client }
func (m *mockProxy) Capabilities() *proxypb.GetCapabilitiesResponse { return m.caps }
func (m *mockProxy) Exited() <-chan error                           { return m.exitCh }
func (m *mockProxy) Stderr() string                                 { return "" }
func (m *mockProxy) PID() int                                       { return 0 }
func (m *mockProxy) Close(context.Context) error {
	if m.onClose != nil {
		m.onClose()
	}
	_ = m.conn.Close()
	m.srv.Stop()
	return nil
}
func (m *mockProxy) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	if m.onGetCaps != nil {
		return m.onGetCaps()
	}
	return m.caps, nil
}
func (m *mockProxy) waitCancel(ctx context.Context) error {
	<-ctx.Done()
	return status.FromContextError(ctx.Err()).Err()
}
func (m *mockProxy) ResetTraces(ctx context.Context, _ *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	if m.hangReset {
		return nil, m.waitCancel(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resetCounter != nil {
		m.resetCounter.Add(1)
	}
	m.spans = nil
	return &proxypb.ResetTracesResponse{}, m.resetErr
}
func (m *mockProxy) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &proxypb.DrainTracesResponse{Spans: slices.Clone(m.spans)}, nil
}
func (m *mockProxy) CreatePublisher(context.Context, *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	m.mu.Lock()
	m.spans = append(m.spans, m.emitOnCreatePub...)
	m.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: "p1"}, nil
}
func (m *mockProxy) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	switch {
	case m.hangPublish:
		return nil, m.waitCancel(ctx)
	case m.unimplPublish:
		return nil, status.Error(codes.Unimplemented, "rpc not implemented")
	case m.inBandUnimpl:
		return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "in-band"}, nil
	case m.toFake && req.GetPublisherId() != "":
		c, err := grpc.NewClient("passthrough:///"+req.GetPublisherId(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer c.Close()
		if _, err = pubsubpb.NewPublisherClient(c).Publish(ctx, &pubsubpb.PublishRequest{Topic: "projects/p/topics/t", Messages: []*pubsubpb.PubsubMessage{{Data: req.GetData()}}}); err != nil {
			return &proxypb.PublishResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
		}
	}
	return &proxypb.PublishResponse{MessageId: "id-1"}, nil
}
func strAttr(s string) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: s}}
}
func cppUnaryPullErrorSpans(sub string) []*proxypb.TraceSpan {
	return []*proxypb.TraceSpan{{
		TraceId: "0123456789abcdef0123456789abcdef", SpanId: "0123456789abcdef", Name: sub + " receive", Kind: proxypb.SpanKind_SPAN_KIND_CONSUMER,
		InstrumentationScopeName: "gl-cpp", StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, StatusMessage: "pull error",
		Attributes: map[string]*proxypb.AttributeValue{"messaging.system": strAttr("gcp_pubsub"), "messaging.destination.name": strAttr(sub), "gcp.project_id": strAttr("conformance-project"), "messaging.operation.type": strAttr("receive"), "code.function.name": strAttr("pubsub::SubscriberConnection::Pull")},
	}}
}
func mustRunTarget(t *testing.T, cfg RunConfig, spec TargetSpec) *TargetReport {
	t.Helper()
	r, err := NewRunner(cfg)
	must(t, err)
	rep, err := r.RunTarget(context.Background(), spec)
	must(t, err)
	return rep
}
func assertStatuses(t *testing.T, rep *TargetReport, want map[string]Status) {
	t.Helper()
	for _, sr := range rep.Scenarios {
		assert(t, sr.Status == want[sr.ID], "%s status = %s, want %s (err=%q)", sr.ID, sr.Status, want[sr.ID], sr.Error)
	}
}
func testVerifyNoTrace(ctx context.Context, env *ScenarioEnv) error {
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}
func afterPub(fn func(context.Context, *ScenarioEnv) error) func(context.Context, *ScenarioEnv) error {
	return func(ctx context.Context, env *ScenarioEnv) error {
		_ = testCreatePub(ctx, env)
		return fn(ctx, env)
	}
}
func seqRun(fns ...func(context.Context, *ScenarioEnv) error) func(context.Context, *ScenarioEnv) error {
	return func(ctx context.Context, env *ScenarioEnv) error {
		var err error
		for _, fn := range fns {
			err = fn(ctx, env)
		}
		return err
	}
}
func failPub(ctx context.Context, env *ScenarioEnv) error {
	if resp, err := env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: env.Endpoint(), Data: []byte("hi")}); err != nil {
		return env.CheckProxyErr(err)
	} else if IsInBandUnimplemented(resp.GetErrorCode()) {
		return Unsupportedf("in-band")
	} else {
		return env.Failf("err %d", resp.ErrorCode)
	}
}
func TestRunner_R2MonitoringUnsupportedAndFiltering(t *testing.T) {
	base := []Scenario{{ID: "PUB-01", Category: "pub"}, {ID: "TRAC-01", Category: "trace"}}
	_, badFilter := FilterScenarios(base, "[")
	r, err := NewRunner(RunConfig{Scenarios: base, Filter: "^TRAC"})
	_, emptyErr := NewRunner(RunConfig{Scenarios: base, Filter: "NOPE", FailOnEmptyFilter: true})
	assert(t, badFilter != nil && err == nil && len(r.FilteredScenarios()) == 1 && r.FilteredScenarios()[0].ID == "TRAC-01" && emptyErr != nil &&
		StatusPass.Valid() && !Status("X").Valid() && StatusSkippedNoMonitoring.IsPassing() && !StatusFail.IsPassing() && StatusUnsupportedByClient.IsFailure(true) && !StatusUnsupportedByClient.IsFailure(false), "filter/status checks failed")
	noTraceExp, unimplErr := traces.ExpectNoTraces(), status.Error(codes.Unimplemented, "opt")
	goMonCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	var noMonResets, cppResets, goResets atomic.Int64
	noMonSpec := startMockProxy(t, &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 55}}, func(mp *mockProxy) { mp.toFake, mp.resetCounter = true, &noMonResets })
	rep1 := mustRunTarget(t, RunConfig{Scenarios: []Scenario{
		{ID: "UNSUP-TRACE", TraceOnly: true, RequiredCapabilities: []Capability{CapUnaryPull}, Run: testNoop},
		{ID: "CHECK-CAP-UNSUP", CheckCapabilities: func(*proxypb.GetCapabilitiesResponse) (bool, string) { return false, "custom" }, Run: testNoop},
		{ID: "REQ-CAP", Run: func(_ context.Context, env *ScenarioEnv) error { return env.RequireCapability(CapUnaryPull) }},
		{ID: "TRACE-ONLY", TraceOnly: true, Run: testVerifyNoTrace},
		{ID: "FUNC-FAIL-NOMON", Run: seqRun(testVerifyNoTrace, func(_ context.Context, env *ScenarioEnv) error { return env.Failf("mismatch") })},
		{ID: "FAIL-THEN-UNIMPL", Run: seqRun(func(_ context.Context, env *ScenarioEnv) error { return env.Failf("prior functional failure") }, func(_ context.Context, env *ScenarioEnv) error { return env.CheckProxyErr(unimplErr) })},
		{ID: "FAKE-FAULT", Run: func(ctx context.Context, env *ScenarioEnv) error {
			env.Fake.AddPublishRule(fake.PublishRule{Times: 1, Err: status.Error(codes.Unimplemented, "fault")})
			resp, err := env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: env.Endpoint(), Data: []byte("hi")})
			assert(t, err == nil && resp.GetErrorCode() == int32(codes.Unimplemented), "fake fault resp=%v err=%v", resp, err)
			return env.Failf("err %d", resp.ErrorCode)
		}},
		{ID: "ENV-HELPERS", CheckCapabilities: func(*proxypb.GetCapabilitiesResponse) (bool, string) { return true, "" }, FakeServerOptions: []fake.Option{fake.WithMaxPublishRequestBytes(64)}, Run: func(ctx context.Context, env *ScenarioEnv) error {
			spans, drained, derr := env.DrainTraces(ctx)
			env.Diagnosticf("diag=%s", env.Language())
			assert(t, !drained && derr == nil && !env.VerifySpans(spans, noTraceExp).Failed() && env.Defaults().GetPublishBatchCountThreshold() == 55 && !env.HasCapability(CapUnaryPull) && env.NewVerifier() != nil && strings.Contains(env.SubscriptionPath("s"), "/subscriptions/s") && env.TopicPath("t") != "" && env.UniqueName("u") != "", "env helper mismatch")
			return nil
		}},
	}}, noMonSpec)
	assertStatuses(t, rep1, map[string]Status{"UNSUP-TRACE": StatusUnsupportedByClient, "CHECK-CAP-UNSUP": StatusUnsupportedByClient, "REQ-CAP": StatusUnsupportedByClient, "TRACE-ONLY": StatusSkippedNoMonitoring, "FUNC-FAIL-NOMON": StatusFail, "FAIL-THEN-UNIMPL": StatusFail, "FAKE-FAULT": StatusFail, "ENV-HELPERS": StatusPass})
	assert(t, strings.Contains(rep1.Scenarios[5].Error, "prior functional failure") && noMonResets.Load() == 0, "FAIL-THEN-UNIMPL err=%q resets=%d", rep1.Scenarios[5].Error, noMonResets.Load())
	cppSpec := startMockProxy(t, &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}, func(mp *mockProxy) {
		mp.spans, mp.emitOnCreatePub, mp.inBandUnimpl, mp.resetCounter = cppUnaryPullErrorSpans("stale"), cppUnaryPullErrorSpans("s1"), true, &cppResets
	})
	rep2 := mustRunTarget(t, RunConfig{Scenarios: []Scenario{
		{ID: "CPP-DEV", Run: afterPub(func(ctx context.Context, env *ScenarioEnv) error {
			return env.MustVerifyTraces(ctx, &traces.TraceExpectation{Subscription: "projects/conformance-project/subscriptions/s1", UnaryPull: &traces.UnaryPullExpectation{ExpectPullError: true}})
		})},
		{ID: "INBAND-UNIMPL", Run: failPub},
		{ID: "RESET-CLEARS-STALE", Run: testVerifyNoTrace},
		{ID: "TRACE-FAIL-THEN-UNIMPL", Run: afterPub(seqRun(func(ctx context.Context, env *ScenarioEnv) error { return env.VerifyTraces(ctx, noTraceExp).Err() }, func(_ context.Context, env *ScenarioEnv) error { return env.CheckProxyErr(unimplErr) }))},
		{ID: "MULTI-TRACE-FIRST-FAIL", Run: afterPub(func(ctx context.Context, env *ScenarioEnv) error {
			assert(t, env.VerifyTraces(ctx, noTraceExp).Failed() && !env.VerifySpans(nil, noTraceExp).Failed(), "multi-trace")
			return nil
		})},
	}}, cppSpec)
	assertStatuses(t, rep2, map[string]Status{"CPP-DEV": StatusPass, "INBAND-UNIMPL": StatusUnsupportedByClient, "RESET-CLEARS-STALE": StatusPass, "TRACE-FAIL-THEN-UNIMPL": StatusFail, "MULTI-TRACE-FIRST-FAIL": StatusFail})
	assert(t, len(rep2.Scenarios[0].Deviations) > 0 && strings.Contains(rep2.Scenarios[4].Error, "Status=FAIL") && cppResets.Load() == 5, "cpp/reset unexpected (resets=%d)", cppResets.Load())
	goSpec := startMockProxy(t, goMonCaps, func(mp *mockProxy) {
		mp.emitOnCreatePub, mp.unimplPublish, mp.resetCounter = cppUnaryPullErrorSpans("s1"), true, &goResets
	})
	rep3 := mustRunTarget(t, RunConfig{Scenarios: []Scenario{
		{ID: "BAD-TRACE", TraceOnly: true, Run: afterPub(testVerifyNoTrace)},
		{ID: "RPC-UNIMPL", Run: failPub},
		{ID: "PANIC", Run: func(context.Context, *ScenarioEnv) error { panic("boom") }},
		{ID: "AFTER-PANIC", TraceOnly: true, Run: testVerifyNoTrace},
	}}, goSpec)
	assertStatuses(t, rep3, map[string]Status{"BAD-TRACE": StatusFail, "RPC-UNIMPL": StatusUnsupportedByClient, "PANIC": StatusError, "AFTER-PANIC": StatusPass})
	assert(t, strings.Contains(rep3.Scenarios[2].Error, "scenario panic: boom") && rep3.Scenarios[3].ProxyRestarted && goResets.Load() == 4, "go/panic unexpected")
	resetFailSpec := startMockProxy(t, goMonCaps, func(mp *mockProxy) { mp.resetErr = status.Error(codes.Internal, "reset broke") })
	repErr := mustRunTarget(t, RunConfig{Scenarios: []Scenario{{ID: "R1", Run: testNoop}}}, resetFailSpec)
	assert(t, repErr.Scenarios[0].Status == StatusError && strings.Contains(repErr.Scenarios[0].Error, "reset broke"), "ResetTraces err unexpected")
}
func TestRunner_RunAllAndCompareMarkdown(t *testing.T) {
	goSpec := startMockProxy(t, &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true, SupportsUnaryPull: true}, nil)
	rustSpec := startMockProxy(t, &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 99}}, nil)
	var progress bytes.Buffer
	runner, err := NewRunner(RunConfig{Targets: []TargetSpec{goSpec, rustSpec}, ProgressWriter: &progress, Scenarios: []Scenario{{ID: "PUB-01", Category: "pub", Run: testVerifyNoTrace}, {ID: "PULL-OPT", Category: "pull", RequiredCapabilities: []Capability{CapUnaryPull}, Run: testNoop}}})
	must(t, err)
	rep, err := runner.RunAll(context.Background())
	md, mdErr := FormatMarkdownReport(rep)
	assert(t, err == nil && mdErr == nil && rep.Mode == "compare" && len(rep.Targets) == 2 && ExitCode(rep, false) == 0 && ExitCode(rep, true) == 1 && strings.Contains(progress.String(), "[go] PUB-01: PASS"), "RunAll unexpected")
	for _, want := range []string{"# Pub/Sub Cross-Client Conformance Comparison Report", "## Summary", "## Capabilities & Defaults", "## Scenario Matrix", "## Behavioral Differences"} {
		assert(t, strings.Contains(md, want), "compare Markdown missing %q", want)
	}
	nilSpec := TargetSpec{Name: "bad", Connect: func(context.Context, TargetSpec) (ProxyHandle, error) { return nil, nil }}
	rErr, _ := NewRunner(RunConfig{Targets: []TargetSpec{nilSpec, goSpec}, Scenarios: []Scenario{{ID: "S1", Run: testNoop}}})
	repStartup, err := rErr.RunAll(context.Background())
	mdStartup, _ := FormatMarkdownReport(repStartup)
	rTO, _ := NewRunner(RunConfig{Targets: []TargetSpec{goSpec, rustSpec}, SuiteTimeout: 80 * time.Millisecond, Scenarios: []Scenario{{ID: "SLOW", Run: func(context.Context, *ScenarioEnv) error { time.Sleep(150 * time.Millisecond); return nil }}}})
	repTO, toErr := rTO.RunAll(context.Background())
	_, zeroErr := (&Runner{}).RunAll(context.Background())
	assert(t, err == nil && ExitCode(repStartup, false) == 1 && repStartup.Targets[0].TargetError != "" && len(repStartup.Targets[1].Scenarios) == 1 && strings.Contains(mdStartup, "## Diagnostics") && strings.Contains(mdStartup, "Target bad Error") &&
		toErr == nil && repTO.TimedOut && ExitCode(repTO, false) == 1 && repTO.Targets[1].TimedOut && strings.Contains(repTO.Targets[1].TargetError, "suite timed out") && zeroErr != nil, "RunAll TargetError/SuiteTimeout/zero failed")
}

type skewDeadlineCtx struct {
	context.Context
	deadline time.Time
}

func (s skewDeadlineCtx) Deadline() (time.Time, bool) { return s.deadline, true }
func (s skewDeadlineCtx) Done() <-chan struct{}       { return nil }
func (s skewDeadlineCtx) Err() error                  { return nil }
func skewCtx(d time.Duration) skewDeadlineCtx {
	return skewDeadlineCtx{Context: context.Background(), deadline: time.Now().Add(d)}
}
func TestRunner_SuiteTimeoutTimerSkewDeterministic(t *testing.T) {
	pubHang := func(ctx context.Context, env *ScenarioEnv) error {
		_, err := env.Client.Publish(ctx, &proxypb.PublishRequest{})
		return err
	}
	sleepAfter := func(dl time.Time) func(*mockProxy) {
		return func(mp *mockProxy) { mp.onClose = func() { time.Sleep(time.Until(dl) + 15*time.Millisecond) } }
	}
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	for _, tc := range []struct {
		name, wantSub                    string
		skew, suiteTO, scenTO            time.Duration
		hangPub, hangReset, wantTimedOut bool
		wantCount                        int
		scens                            []Scenario
	}{
		{"PublishSkew", "TIMEOUT: suite deadline exceeded after", 100 * time.Millisecond, 0, 10 * time.Second, true, false, true, 1, []Scenario{{ID: "S1", Run: pubHang}, {ID: "S2", Run: testNoop}}},
		{"ResetTracesSkew", "TIMEOUT: suite deadline exceeded after", 100 * time.Millisecond, 0, 10 * time.Second, false, true, true, 1, []Scenario{{ID: "S1", Run: testNoop}, {ID: "S2", Run: testNoop}}},
		{"ScenarioTimeoutPreserved/NoSuite", "TIMEOUT: scenario exceeded timeout (50ms)", 0, 0, 50 * time.Millisecond, true, false, false, 2, []Scenario{{ID: "S1", Run: pubHang}, {ID: "S2", Run: testNoop}}},
		{"ScenarioTimeoutPreserved/LargeSuite", "TIMEOUT: scenario exceeded timeout (50ms)", 0, 5 * time.Second, 50 * time.Millisecond, true, false, false, 2, []Scenario{{ID: "S1", Run: pubHang}, {ID: "S2", Run: testNoop}}},
		{"RealSuiteTimeoutMultiScenarioStop", "TIMEOUT: suite deadline exceeded after", 0, 100 * time.Millisecond, 10 * time.Second, true, false, true, 1, []Scenario{{ID: "S1", Run: pubHang}, {ID: "S2", Run: testNoop}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := startMockProxy(t, goCaps, func(mp *mockProxy) { mp.hangPublish, mp.hangReset = tc.hangPub, tc.hangReset })
			ctx := context.Background()
			if tc.skew > 0 {
				ctx = skewCtx(tc.skew)
			}
			r, _ := NewRunner(RunConfig{SuiteTimeout: tc.suiteTO, ScenarioTimeout: tc.scenTO, Scenarios: tc.scens})
			rep, err := r.RunTarget(ctx, spec)
			assert(t, err == nil && rep.TimedOut == tc.wantTimedOut && len(rep.Scenarios) == tc.wantCount && rep.Scenarios[0].Status == StatusTimeout && strings.Contains(rep.Scenarios[0].Error, tc.wantSub) && (!tc.hangPub || tc.skew == 0 || rep.TargetError != "") && (tc.wantTimedOut || (rep.Scenarios[1].Status == StatusPass && rep.Scenarios[1].ProxyRestarted)), "%s err=%v rep=%+v", tc.name, err, rep)
		})
	}
	countConn := func(c *atomic.Int64, next func(context.Context, TargetSpec) (ProxyHandle, error), retErr error) func(context.Context, TargetSpec) (ProxyHandle, error) {
		return func(ctx context.Context, ts TargetSpec) (ProxyHandle, error) {
			c.Add(1)
			if next == nil {
				return nil, retErr
			}
			return next(ctx, ts)
		}
	}
	t.Run("RunAllBetweenTargetSkew", func(t *testing.T) {
		ctx := skewCtx(80 * time.Millisecond)
		t1 := startMockProxy(t, goCaps, sleepAfter(ctx.deadline))
		t1.Name = "t1"
		var t2Conn atomic.Int64
		r, _ := NewRunner(RunConfig{Targets: []TargetSpec{t1, {Name: "t2", Connect: countConn(&t2Conn, nil, errors.New("unexpected"))}}, ScenarioTimeout: 10 * time.Second, Scenarios: []Scenario{{ID: "S1", Run: testNoop}}})
		rep, err := r.RunAll(ctx)
		assert(t, err == nil && t2Conn.Load() == 0 && rep.TimedOut && !rep.Targets[0].TimedOut && rep.Targets[0].Scenarios[0].Status == StatusPass && rep.Targets[1].TimedOut && strings.Contains(rep.Targets[1].TargetError, "suite timed out before starting target"), "RunAllBetweenTargetSkew err=%v rep=%+v", err, rep)
	})
	t.Run("AcquireAndEntrySkew", func(t *testing.T) {
		var conns, isoConns atomic.Int64
		r, _ := NewRunner(RunConfig{ScenarioTimeout: 10 * time.Second, Scenarios: []Scenario{{ID: "S1", Run: testNoop}, {ID: "S2", IsolateProxy: true, Run: testNoop}}})
		rep, err := r.RunTarget(skewCtx(-time.Millisecond), TargetSpec{Name: "exp", Connect: countConn(&conns, nil, nil)})
		assert(t, err != nil && conns.Load() == 0 && rep.TimedOut && strings.Contains(rep.TargetError, "suite timed out before starting target"), "ExpiredEntry err=%v conns=%d rep=%+v", err, conns.Load(), rep)
		ctxInit := skewCtx(20 * time.Millisecond)
		repInit, errInit := r.RunTarget(ctxInit, TargetSpec{Name: "init", Connect: func(context.Context, TargetSpec) (ProxyHandle, error) {
			time.Sleep(time.Until(ctxInit.deadline) + 15*time.Millisecond)
			return nil, context.DeadlineExceeded
		}})
		assert(t, errInit != nil && repInit.TimedOut && repInit.TargetError != "", "InitSkew err=%v rep=%+v", errInit, repInit)
		ctxIso := skewCtx(80 * time.Millisecond)
		base := startMockProxy(t, goCaps, sleepAfter(ctxIso.deadline))
		repIso, errIso := r.RunTarget(ctxIso, TargetSpec{Name: "iso", Connect: countConn(&isoConns, base.Connect, nil)})
		assert(t, errIso == nil && isoConns.Load() == 1 && repIso.TimedOut && len(repIso.Scenarios) == 2 && repIso.Scenarios[0].Status == StatusPass && repIso.Scenarios[1].Status == StatusTimeout, "IsolatePreStartSkew err=%v conns=%d rep=%+v", errIso, isoConns.Load(), repIso)
	})
}

func TestRunner_CrashStderrTruncationAndTraceOnlySkip(t *testing.T) {
	longErr := strings.Repeat("X", 1200) + "FINAL_MARKER"
	msg := formatCrashMessage(errors.New("exit 2"), longErr, errors.New("rpc fail"))
	assert(t, strings.Contains(msg, `stderr="...`) && strings.Contains(msg, "FINAL_MARKER") && !strings.Contains(msg, strings.Repeat("X", 1100)), "expected >1024B stderr truncation, got %q", msg)

	var connNum atomic.Int64
	monCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	noMonCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: false}
	flipSpec := TargetSpec{
		Name: "flip",
		Connect: func(ctx context.Context, ts TargetSpec) (ProxyHandle, error) {
			caps := monCaps
			if connNum.Add(1) > 1 {
				caps = noMonCaps
			}
			inner := startMockProxy(t, caps, nil)
			return inner.Connect(ctx, ts)
		},
	}
	rep := mustRunTarget(t, RunConfig{
		Scenarios: []Scenario{{ID: "TRACE-FLIP", TraceOnly: true, IsolateProxy: true, Run: testVerifyNoTrace}},
	}, flipSpec)
	assert(t, len(rep.Scenarios) == 1 && rep.Scenarios[0].Status == StatusSkippedNoMonitoring && rep.Scenarios[0].TraceStatus == traces.StatusSkippedNoMonitoring, "expected TraceOnly post-run skip on flipped caps, got %+v", rep.Scenarios)
}

type nilCapsHandle struct {
	ProxyHandle
}

func (nilCapsHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return nil }

func TestRunner_Tier7AdversarialCoverage(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("GAP-A7-H1_ObserveExitWait_AsyncCrashAndCacheAndExitInfo", func(t *testing.T) {
		var exitOnce sync.Once
		spec := startMockProxy(t, goCaps, func(mp *mockProxy) {
			mp.onGetCaps = func() (*proxypb.GetCapabilitiesResponse, error) {
				exitOnce.Do(func() {
					time.AfterFunc(80*time.Millisecond, func() {
						mp.exitCh <- errors.New("exit status 42")
						close(mp.exitCh)
					})
				})
				return nil, status.Error(codes.Unavailable, "shutting down")
			}
		})
		rep := mustRunTarget(t, RunConfig{
			Scenarios: []Scenario{
				{ID: "CRASH-ASYNC-EXIT", Run: func(context.Context, *ScenarioEnv) error { return errors.New("mid-scenario rpc error") }},
				{ID: "RECOVERED", Run: testNoop},
			},
		}, spec)
		assert(t, len(rep.Scenarios) == 2 &&
			rep.Scenarios[0].Status == StatusError &&
			strings.Contains(rep.Scenarios[0].Error, "exit=exit status 42") &&
			!strings.Contains(rep.Scenarios[0].Error, "exit=rpc error:") &&
			rep.Scenarios[1].Status == StatusPass &&
			rep.Scenarios[1].ProxyRestarted, "ObserveExitWait rep=%+v", rep.Scenarios)

		// Direct activeProxy.observeExit verification for timer expiry (<-t.C) and exited cache.
		ch := make(chan error, 1)
		ap := &activeProxy{handle: &exitOnlyHandle{exitCh: ch}}
		exited, err := ap.observeExit(10 * time.Millisecond)
		assert(t, !exited && err == nil, "expected timeout (false, nil), got (%v, %v)", exited, err)

		wantExitErr := errors.New("exit status 7")
		ch <- wantExitErr
		exited, err = ap.observeExit(time.Second)
		assert(t, exited && errors.Is(err, wantExitErr), "first observeExit = (%v, %v)", exited, err)
		// Second call must return cached (true, wantExitErr) even if Exited() is unclosed/empty.
		exited2, err2 := ap.observeExit(0)
		assert(t, exited2 && errors.Is(err2, wantExitErr), "cached observeExit = (%v, %v)", exited2, err2)

		// ProxyProcess fallback to ExitInfo when Exited() channel was already drained/closed.
		p, startErr := StartProxy(context.Background(), helperSpec(t, "pp-exitinfo", "crash-mid-scenario"), ProxyOptions{})
		must(t, startErr)
		_, _ = p.Client().CreatePublisher(context.Background(), &proxypb.CreatePublisherRequest{Topic: "t"})
		<-p.WaitDone()
		<-p.Exited() // Drain the single buffered item so subsequent reads yield !ok on closed exitCh.
		apProc := &activeProxy{handle: p}
		procExited, procErr := apProc.observeExit(0)
		assert(t, procExited && procErr != nil && strings.Contains(procErr.Error(), "exit status 2"), "ProxyProcess ExitInfo fallback = (%v, %v)", procExited, procErr)
		procExited2, procErr2 := apProc.observeExit(0)
		assert(t, procExited2 && procErr2 != nil && procErr2.Error() == procErr.Error(), "ProxyProcess cached observeExit = (%v, %v)", procExited2, procErr2)
		must(t, p.Stop())
	})

	t.Run("GAP-A7-H2_DirectErrProxyInfraReturn", func(t *testing.T) {
		spec := startMockProxy(t, goCaps, nil)
		rep := mustRunTarget(t, RunConfig{
			Scenarios: []Scenario{
				{ID: "ERR-PROXY-INFRA", Run: func(context.Context, *ScenarioEnv) error {
					return fmt.Errorf("wrapped transport failure: %w", ErrProxyInfra)
				}},
				{ID: "AFTER-INFRA", Run: testNoop},
			},
		}, spec)
		assert(t, len(rep.Scenarios) == 2 &&
			rep.Scenarios[0].Status == StatusError &&
			strings.Contains(rep.Scenarios[0].Error, "wrapped transport failure") &&
			rep.Scenarios[1].Status == StatusPass &&
			rep.Scenarios[1].ProxyRestarted, "ErrProxyInfra rep=%+v", rep.Scenarios)

		stopRep := mustRunTarget(t, RunConfig{
			StopOnProxyCrash: true,
			Scenarios: []Scenario{
				{ID: "STOP-PROXY-INFRA", Run: func(context.Context, *ScenarioEnv) error { return ErrProxyInfra }},
				{ID: "SKIPPED", Run: testNoop},
			},
		}, spec)
		assert(t, len(stopRep.Scenarios) == 1 && stopRep.Scenarios[0].Status == StatusError && stopRep.TargetError != "", "StopOnProxyCrash ErrProxyInfra rep=%+v", stopRep)
	})

	t.Run("GAP-A7-H3_SuiteContextCanceledMessage", func(t *testing.T) {
		for _, suiteTO := range []time.Duration{0, 10 * time.Second} {
			spec := startMockProxy(t, goCaps, nil)
			started := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				<-started
				cancel()
			}()
			r, err := NewRunner(RunConfig{
				SuiteTimeout:    suiteTO,
				ScenarioTimeout: 10 * time.Second,
				Scenarios: []Scenario{
					{ID: "WAIT-CANCEL", Run: func(scCtx context.Context, _ *ScenarioEnv) error {
						close(started)
						<-scCtx.Done()
						return scCtx.Err()
					}},
					{ID: "NEVER-RUN", Run: testNoop},
				},
			})
			must(t, err)
			rep, runErr := r.RunTarget(ctx, spec)
			assert(t, runErr == nil &&
				rep.TimedOut &&
				len(rep.Scenarios) == 1 &&
				rep.Scenarios[0].Status == StatusTimeout &&
				strings.Contains(rep.Scenarios[0].Error, "TIMEOUT: suite canceled after") &&
				!strings.Contains(rep.Scenarios[0].Error, "deadline exceeded"),
				"suiteTO=%v rep=%+v err=%v", suiteTO, rep, runErr)
		}
	})

	t.Run("GAP-A7-H4_ActiveCapsNilFallbackToCachedCaps", func(t *testing.T) {
		var connNum atomic.Int64
		spec := TargetSpec{
			Name: "nil-caps-iso",
			Connect: func(ctx context.Context, ts TargetSpec) (ProxyHandle, error) {
				n := connNum.Add(1)
				base := startMockProxy(t, goCaps, func(mp *mockProxy) {
					if n == 3 {
						mp.emitOnCreatePub = cppUnaryPullErrorSpans("s1")
					}
				})
				h, err := base.Connect(ctx, ts)
				if err != nil {
					return nil, err
				}
				if n > 1 {
					return nilCapsHandle{ProxyHandle: h}, nil
				}
				return h, nil
			},
		}
		rep := mustRunTarget(t, RunConfig{
			Scenarios: []Scenario{
				{ID: "NILCAPS-PASS", TraceOnly: true, IsolateProxy: true, Run: testVerifyNoTrace},
				{ID: "NILCAPS-FAIL", TraceOnly: true, IsolateProxy: true, Run: afterPub(testVerifyNoTrace)},
			},
		}, spec)
		assert(t, len(rep.Scenarios) == 2 &&
			rep.Scenarios[0].Status == StatusPass && rep.Scenarios[0].TraceStatus == traces.StatusPass &&
			rep.Scenarios[1].Status == StatusFail && rep.Scenarios[1].TraceStatus == traces.StatusFail,
			"nil activeCaps fallback rep=%+v", rep.Scenarios)
	})
}

type exitOnlyHandle struct {
	ProxyHandle
	exitCh chan error
}

func (h *exitOnlyHandle) Exited() <-chan error { return h.exitCh }

func TestRunner_PreScenarioExpiredSuiteCtx(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	ctx, cancel := context.WithCancel(context.Background())
	var runCount atomic.Int64
	spec := TargetSpec{
		Name: "cancel-after-connect",
		Connect: func(cCtx context.Context, ts TargetSpec) (ProxyHandle, error) {
			h, err := startMockProxy(t, goCaps, nil).Connect(cCtx, ts)
			if err != nil {
				return nil, err
			}
			cancel()
			return h, nil
		},
	}
	r, err := NewRunner(RunConfig{
		Scenarios: []Scenario{
			{ID: "NEVER-RUN", Run: func(context.Context, *ScenarioEnv) error {
				runCount.Add(1)
				return nil
			}},
		},
	})
	must(t, err)
	rep, runErr := r.RunTarget(ctx, spec)
	assert(t, runErr == nil &&
		rep.TimedOut &&
		len(rep.Scenarios) == 0 &&
		runCount.Load() == 0 &&
		strings.Contains(rep.TargetError, "suite timed out"),
		"expected pre-scenario suite timeout without running scenario 1, got err=%v rep=%+v runCount=%d",
		runErr, rep, runCount.Load())
}

func TestRunner_EphemeralFakeStartupError(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	spec := startMockProxy(t, goCaps, nil)
	var runCalled atomic.Bool
	rep := mustRunTarget(t, RunConfig{
		Scenarios: []Scenario{
			{
				ID:                "BAD-EPHEMERAL-FAKE",
				FakeServerOptions: []fake.Option{fake.WithListenAddr("127.0.0.1:-1")},
				Run: func(context.Context, *ScenarioEnv) error {
					runCalled.Store(true)
					return nil
				},
			},
		},
	}, spec)
	assert(t, len(rep.Scenarios) == 1 &&
		rep.Scenarios[0].Status == StatusError &&
		strings.Contains(rep.Scenarios[0].Error, "starting fake server") &&
		!runCalled.Load(),
		"expected StatusError containing 'starting fake server', got rep=%+v runCalled=%v",
		rep.Scenarios, runCalled.Load())
}

func TestRunner_SharedFakeStartupError(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	var connectCalled atomic.Bool
	baseSpec := startMockProxy(t, goCaps, nil)
	spec := TargetSpec{
		Name: "shared-fake-bad-addr",
		Connect: func(ctx context.Context, ts TargetSpec) (ProxyHandle, error) {
			connectCalled.Store(true)
			return baseSpec.Connect(ctx, ts)
		},
	}
	var runCalled atomic.Bool
	r, err := NewRunner(RunConfig{
		FakeServerOptions: []fake.Option{fake.WithListenAddr("invalid-listen-addr-without-port")},
		Scenarios: []Scenario{
			{
				ID: "NEVER-RUN",
				Run: func(context.Context, *ScenarioEnv) error {
					runCalled.Store(true)
					return nil
				},
			},
		},
	})
	must(t, err)
	rep, runErr := r.RunTarget(context.Background(), spec)
	assert(t, runErr != nil &&
		rep != nil &&
		!rep.TimedOut &&
		strings.Contains(rep.TargetError, "failed to start fake Pub/Sub server") &&
		len(rep.Scenarios) == 0 &&
		!connectCalled.Load() &&
		!runCalled.Load(),
		"expected RunTarget error with TargetError containing 'failed to start fake Pub/Sub server', got rep=%+v err=%v connectCalled=%v runCalled=%v",
		rep, runErr, connectCalled.Load(), runCalled.Load())
}

func TestRunner_SharedProxyRestartFailureAfterIsolate(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	baseSpec := startMockProxy(t, goCaps, nil)
	var connAttempts atomic.Int64
	var secondScenarioRan atomic.Bool

	spec := TargetSpec{
		Name: "restart-fail-after-isolate",
		Connect: func(ctx context.Context, ts TargetSpec) (ProxyHandle, error) {
			if connAttempts.Add(1) == 3 {
				return nil, errors.New("shared proxy restart boom")
			}
			return baseSpec.Connect(ctx, ts)
		},
	}

	rep := mustRunTarget(t, RunConfig{
		Scenarios: []Scenario{
			{ID: "ISO-PASS", IsolateProxy: true, Run: testNoop},
			{ID: "SHARED-RESTART-FAIL", Run: func(context.Context, *ScenarioEnv) error {
				secondScenarioRan.Store(true)
				return nil
			}},
		},
	}, spec)

	assert(t, len(rep.Scenarios) == 2 &&
		connAttempts.Load() == 3 &&
		rep.Scenarios[0].Status == StatusPass &&
		rep.Scenarios[1].Status == StatusError &&
		strings.Contains(rep.Scenarios[1].Error, "starting proxy") &&
		strings.Contains(rep.Scenarios[1].Error, "shared proxy restart boom") &&
		!secondScenarioRan.Load(),
		"expected S1 PASS and S2 StatusError containing 'starting proxy', got rep=%+v attempts=%d s2Ran=%v",
		rep.Scenarios, connAttempts.Load(), secondScenarioRan.Load())
}

func TestRunner_SubprocessStartProxyError(t *testing.T) {
	var runCalled atomic.Bool
	r, err := NewRunner(RunConfig{
		Scenarios: []Scenario{
			{
				ID: "NEVER-RUN",
				Run: func(context.Context, *ScenarioEnv) error {
					runCalled.Store(true)
					return nil
				},
			},
		},
	})
	must(t, err)

	rep, runErr := r.RunTarget(context.Background(), helperSpec(t, "bad-subproc", "crash-startup"))
	assert(t, runErr != nil &&
		rep != nil &&
		!rep.TimedOut &&
		strings.Contains(rep.TargetError, "startup failed") &&
		len(rep.Scenarios) == 0 &&
		!runCalled.Load(),
		"expected RunTarget subprocess startup error containing 'startup failed', got rep=%+v err=%v runCalled=%v",
		rep, runErr, runCalled.Load())
}

func TestRunner_EnvFailedCrashPingAndDrainTracesSkipped(t *testing.T) {
	t.Run("GAP-A11-H1_EnvFailedTriggersLivenessPing", func(t *testing.T) {
		goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
		var pingCount atomic.Int64
		spec := startMockProxy(t, goCaps, func(mp *mockProxy) {
			mp.onGetCaps = func() (*proxypb.GetCapabilitiesResponse, error) {
				pingCount.Add(1)
				return nil, status.Error(codes.Unavailable, "liveness ping unavailable")
			}
		})
		rep := mustRunTarget(t, RunConfig{
			Scenarios: []Scenario{
				{
					ID: "ENV-FAILED",
					Run: func(_ context.Context, env *ScenarioEnv) error {
						_ = env.Failf("env assertion failed without returned error")
						return nil
					},
				},
				{ID: "RECOVERED", Run: testNoop},
			},
		}, spec)
		assert(t, len(rep.Scenarios) == 2 &&
			rep.Scenarios[0].Status == StatusError &&
			rep.Scenarios[1].Status == StatusPass &&
			rep.Scenarios[1].ProxyRestarted &&
			pingCount.Load() == 1,
			"expected S1 StatusError, S2 StatusPass with ProxyRestarted=true, and 1 GetCapabilities ping, got scenarios=%+v pings=%d",
			rep.Scenarios, pingCount.Load())
	})

	t.Run("GAP-A11-H2_DrainTracesSetsSkippedStatusWithoutVerify", func(t *testing.T) {
		noMonSpec := startMockProxy(t, &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", SupportsMonitoring: false}, nil)
		rep := mustRunTarget(t, RunConfig{
			Scenarios: []Scenario{
				{
					ID:        "DRAIN-ONLY",
					TraceOnly: false,
					Run: func(ctx context.Context, env *ScenarioEnv) error {
						spans, drained, err := env.DrainTraces(ctx)
						if drained || err != nil || len(spans) != 0 {
							return fmt.Errorf("DrainTraces = (%d spans, %v, %v)", len(spans), drained, err)
						}
						return nil
					},
				},
			},
		}, noMonSpec)
		assert(t, len(rep.Scenarios) == 1 &&
			rep.Scenarios[0].Status == StatusPass &&
			rep.Scenarios[0].TraceStatus == traces.StatusSkippedNoMonitoring,
			"expected StatusPass with TraceStatus=%q, got %+v", traces.StatusSkippedNoMonitoring, rep.Scenarios)
	})
}
