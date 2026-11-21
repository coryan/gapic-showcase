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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const m3fSmokeFilter = "^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03)$"

func TestRegistry_UnitAndDefaults(t *testing.T) {
	noop := func(context.Context, *harness.ScenarioEnv) error { return nil }
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic: %s", name)
			}
		}()
		fn()
	}
	r := scenarios.NewRegistry()
	var nilReg *scenarios.Registry
	_, okR := r.ByID("PUB-01")
	_, okN := nilReg.ByID("X")
	if okR || okN || r.Len() != 0 || len(r.All()) != 0 || nilReg.Len() != 0 || nilReg.All() != nil || nilReg.Register(harness.Scenario{ID: "X", Run: noop}) == nil {
		t.Fatal("unexpected empty or nil Registry behavior")
	}
	for _, bad := range []harness.Scenario{{ID: "", Run: noop}, {ID: "   ", Run: noop}, {ID: " PUB-01", Run: noop}, {ID: "PUB 01", Run: noop}, {ID: "NO-RUN"}, {ID: "NEG", Run: noop, Timeout: -time.Millisecond}} {
		if err := r.Register(bad); err == nil {
			t.Fatalf("expected Register error for %+v", bad)
		}
	}
	caps, opts := []harness.Capability{harness.CapUnaryPull}, []fake.Option{fake.WithSuppressPong(true)}
	for _, sc := range []harness.Scenario{{ID: "Z-01", Name: "Zeta", Category: "publish", RequiredCapabilities: caps, FakeServerOptions: opts, Run: noop}, {ID: "A-01", Name: "Alpha", Category: "pull", Run: noop}, {ID: "M-01", Name: "Mu", Category: "traces", Run: noop}} {
		r.MustRegister(sc)
	}
	caps[0], opts[0] = harness.CapKeepalive, nil
	gotAll := r.All()
	if r.Len() != 3 || cmp.Diff([]string{"Z-01", "A-01", "M-01"}, []string{gotAll[0].ID, gotAll[1].ID, gotAll[2].ID}) != "" || gotAll[0].RequiredCapabilities[0] != harness.CapUnaryPull || gotAll[0].FakeServerOptions[0] == nil {
		t.Fatalf("registry mismatch: len=%d got=%+v", r.Len(), gotAll)
	}
	gotAll[0].RequiredCapabilities[0], gotAll[0].FakeServerOptions[0] = harness.CapPublishHedging, nil
	again, ok := r.ByID("Z-01")
	againOk := ok && again.RequiredCapabilities[0] == harness.CapUnaryPull && again.FakeServerOptions[0] != nil
	again.RequiredCapabilities[0], again.FakeServerOptions[0] = harness.CapPublishHedging, nil
	if again2, ok2 := r.ByID("Z-01"); !againOk || !ok2 || again2.RequiredCapabilities[0] != harness.CapUnaryPull || again2.FakeServerOptions[0] == nil {
		t.Fatalf("missing clone on read: %+v ok=%v ok2=%v", again2, ok, ok2)
	}
	if err := r.Register(harness.Scenario{ID: "A-01", Run: noop}); err == nil {
		t.Fatal("expected duplicate ID error")
	}
	mustPanic("r.MustRegister dup", func() { r.MustRegister(harness.Scenario{ID: "A-01", Run: noop}) })
	if _, err := r.Filter("[invalid"); err == nil {
		t.Fatal("expected invalid regex error")
	}
	if f, err := r.Filter("^pull$"); err != nil || len(f) != 1 || f[0].ID != "A-01" {
		t.Fatalf("unexpected filter: %+v err=%v", f, err)
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = r.Register(harness.Scenario{ID: fmt.Sprintf("CONC-%d", idx), Category: "conc", Run: noop})
			_, _, _, _ = r.All(), r.Len(), func() []harness.Scenario { s, _ := r.Filter("Z-"); return s }(), func() bool { _, ok := r.ByID("Z-01"); return ok }()
		}(i)
	}
	wg.Wait()

	var defIDs []string
	cats := map[string]struct{}{}
	catRE := regexp.MustCompile(`^[a-z]+(_[a-z]+)*$`)
	wantTimeouts := map[string]time.Duration{"PULL-03": 120 * time.Second, "PULL-03b": 120 * time.Second, "LEASE-02": 60 * time.Second, "LEASE-03": 45 * time.Second, "EOD-01": 75 * time.Second, "TRAC-04": 60 * time.Second}
	for i, sc := range scenarios.All() {
		defIDs = append(defIDs, sc.ID)
		cats[sc.Category] = struct{}{}
		if sc.Timeout != wantTimeouts[sc.ID] || (i < 6 && sc.Timeout != 0) || sc.Run == nil || sc.Name == "" || !catRE.MatchString(sc.Category) || sc.Feature == "" || sc.Description == "" {
			t.Errorf("invalid default scenario[%d] %+v", i, sc)
		}
	}
	t1, _ := scenarios.ByID("TRAC-01")
	t3, _ := scenarios.ByID("TRAC-03")
	p4, _ := scenarios.ByID("PULL-04")
	tf, err := scenarios.Filter("^traces$")
	pf, pErr := scenarios.Filter("PUB-01")
	nf, nErr := scenarios.Filter("^NOMATCH$")
	if scenarios.Len() != 65 || len(cats) != 13 || !slices.Equal(defIDs[:6], []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}) || !t1.TraceOnly || t3.TraceOnly || !slices.Equal(p4.RequiredCapabilities, []harness.Capability{harness.CapUnaryPull}) || err != nil || len(tf) != 5 || pErr != nil || len(pf) != 1 || pf[0].ID != "PUB-01" || nErr != nil || len(nf) != 0 {
		t.Fatalf("default registry mismatch: len=%d cats=%d ids[:6]=%v t1=%v t3=%v p4=%v tf=%d err=%v pf=%v nf=%d", scenarios.Len(), len(cats), defIDs[:min(6, len(defIDs))], t1.TraceOnly, t3.TraceOnly, p4.RequiredCapabilities, len(tf), err, pf, len(nf))
	}
	for _, sc := range tf {
		if strings.Contains(sc.ID+sc.Name+sc.Category, "PUB-01") {
			t.Fatalf("traces scenario %+v matched PUB-01", sc)
		}
	}
	if err := scenarios.RegisterDefaults(nil); err == nil {
		t.Fatal("expected RegisterDefaults(nil) error")
	}
	fresh := scenarios.NewRegistry()
	if err := scenarios.RegisterDefaults(fresh); err != nil || fresh.Len() != 65 || scenarios.RegisterDefaults(fresh) == nil {
		t.Fatalf("RegisterDefaults on fresh registry failed: len=%d err=%v", fresh.Len(), err)
	}
	var zeroReg scenarios.Registry
	zeroReg.MustRegister(harness.Scenario{ID: "Z-00", Name: "Zero", Category: "test", Run: noop})
	if zeroReg.Len() != 1 || scenarios.Register(harness.Scenario{ID: "PUB-01", Run: noop}) == nil {
		t.Fatalf("zero/pkg Register failed: zeroLen=%d", zeroReg.Len())
	}
	mustPanic("scenarios.MustRegister dup", func() { scenarios.MustRegister(harness.Scenario{ID: "PUB-01", Run: noop}) })
}

type mockConfig struct{ profile, fault string }
type asyncItem struct {
	data, key, parentTP string
	attrs               map[string]string
	resCh               chan *proxypb.PublishResponse
}
type mockPub struct {
	topic                     string
	enableMon, enableOrdering bool
	conn                      *grpc.ClientConn
	client                    pubsubpb.PublisherClient
	mu                        sync.Mutex
	closed                    bool
	keyBuf                    map[string][]*asyncItem
	keyCh                     map[string]chan []*asyncItem
	tokens                    map[string]chan *proxypb.PublishResponse
	wg                        sync.WaitGroup
}
type mockProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg             mockConfig
	caps            *proxypb.GetCapabilitiesResponse
	mu              sync.Mutex
	pubs            map[string]*mockPub
	spans           []*proxypb.TraceSpan
	subWG           sync.WaitGroup
	leaked          []struct{ ep, sub, aid string }
	lastTID         string
	lastEndpoint    string
	drainInjected   bool
	seq             atomic.Int64
	cppMode         bool
	m3fPullMax2Seen map[string]bool
}
type inProcHandle struct {
	srv     *grpc.Server
	conn    *grpc.ClientConn
	client  proxypb.PubSubProxyClient
	caps    *proxypb.GetCapabilitiesResponse
	exited  chan error
	onClose func()
	once    sync.Once
}

func (h *inProcHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *inProcHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.caps }
func (h *inProcHandle) Exited() <-chan error                           { return h.exited }
func (h *inProcHandle) Stderr() string                                 { return "" }
func (h *inProcHandle) PID() int                                       { return 0 }
func (h *inProcHandle) Close(context.Context) error {
	h.once.Do(func() {
		_ = h.conn.Close()
		h.srv.Stop()
		if h.onClose != nil {
			h.onClose()
		}
	})
	return nil
}

func (m *mockProxyServer) closeAll() {
	m.mu.Lock()
	pubs := slices.Collect(maps.Values(m.pubs))
	clear(m.pubs)
	m.mu.Unlock()
	for _, p := range pubs {
		p.mu.Lock()
		p.closed = true
		for _, ch := range p.keyCh {
			close(ch)
		}
		p.mu.Unlock()
		p.wg.Wait()
		_ = p.conn.Close()
	}
}

func newMockCapabilities(cfg mockConfig) *proxypb.GetCapabilitiesResponse {
	fc := proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE
	if cfg.profile == "cpp" {
		fc = proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK
	}
	defs := &proxypb.ClientDefaults{PublishFlowControlBehavior: fc}
	switch cfg.profile {
	case "rust":
		return &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", ClientLibrary: "google-cloud-pubsub", ClientVersion: "1.0.0", Defaults: defs}
	case "java":
		return &proxypb.GetCapabilitiesResponse{ClientLanguage: "java", ClientLibrary: "google-cloud-pubsub", ClientVersion: "1.0.0", SupportsKeepalive: true, Defaults: defs}
	case "cpp", "cpp_unconfigured_fc":
		return &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", ClientLibrary: "google-cloud-cpp/pubsub", ClientVersion: "2.0.0", SupportsMonitoring: true, SupportsUnaryPull: true, SupportsOrderingOptIn: true, Defaults: defs}
	default:
		return &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "2.0.0", SupportsMonitoring: true, SupportsUnaryPull: true, SupportsOrderingOptIn: true, SupportsKeepalive: true, SupportsShutdownBehavior: cfg.profile != "go_timeout_only" && cfg.profile != "go_basic_stop", SupportsShutdownTimeout: cfg.profile != "go_basic_stop", Defaults: defs}
	}
}

func connectMockProxy(cfg mockConfig) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srv, caps := grpc.NewServer(), newMockCapabilities(cfg)
		mock := &mockProxyServer{cfg: cfg, caps: caps, pubs: map[string]*mockPub{}, cppMode: strings.EqualFold(caps.GetClientLanguage(), "cpp")}
		proxypb.RegisterPubSubProxyServer(srv, mock)
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve(lis) }()
		conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			srv.Stop()
			return nil, err
		}
		return &inProcHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), caps: caps, exited: exited, onClose: mock.closeAll}, nil
	}
}

func pick[T any](cond bool, t, f T) T {
	if cond {
		return t
	}
	return f
}

func (m *mockProxyServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return m.caps, nil
}
func (m *mockProxyServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *mockProxyServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	m.mu.Lock()
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *mockProxyServer) DrainTraces(ctx context.Context, _ *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	m.mu.Lock()
	out := slices.Clone(m.spans)
	m.spans = nil
	ep := m.lastEndpoint
	inject := ""
	if (m.cfg.fault == "trac03-drain-unexpected-rpc" || m.cfg.fault == "trac03-drain-extra-pub") && len(out) > 0 && ep != "" && !m.drainInjected {
		m.drainInjected = true
		inject = m.cfg.fault
	}
	m.mu.Unlock()
	if inject != "" {
		if conn, err := dialFake(ep); err == nil {
			switch inject {
			case "trac03-drain-unexpected-rpc":
				_ = conn.Invoke(ctx, "/google.pubsub.v1.Publisher/GetTopic", &pubsubpb.GetTopicRequest{Topic: "projects/p/topics/t"}, &pubsubpb.Topic{})
			case "trac03-drain-extra-pub":
				_, _ = pubsubpb.NewPublisherClient(conn).Publish(m.outCtx(ctx, "topic", "projects/p/topics/extra"), &pubsubpb.PublishRequest{
					Topic:    "projects/p/topics/extra",
					Messages: []*pubsubpb.PubsubMessage{{Data: []byte("extra-transition")}},
				})
			}
			_ = conn.Close()
		}
	}
	if m.cfg.fault == "missing-traces" {
		out = nil
	}
	return &proxypb.DrainTracesResponse{Spans: out}, nil
}
func (m *mockProxyServer) outCtx(ctx context.Context, key, resource string) context.Context {
	if m.cfg.fault == "missing-routing-header" {
		return ctx
	}
	if m.cfg.profile != "rust" && m.cfg.fault != "unescaped-routing-on-go" {
		resource = url.QueryEscape(resource)
	}
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", key+"="+resource)
}
func (m *mockProxyServer) addSpan(spans ...*proxypb.TraceSpan) {
	if m.cfg.fault == "missing-traces" {
		return
	}
	m.mu.Lock()
	for _, sp := range spans {
		if sp != nil {
			if m.cfg.fault == "invalid-traces" {
				sp.Name, sp.Kind, sp.Links = "broken.span", proxypb.SpanKind_SPAN_KIND_CLIENT, nil
			}
			if m.cfg.fault == "unsampled-traces" || (m.cfg.fault == "unsampled-wire-tp-only" && strings.HasSuffix(sp.GetName(), " create")) || ((m.cfg.fault == "unsampled-cb-tp-only" || (m.cfg.fault == "trac03-fb-unsampled-cb" && strings.Contains(sp.GetName(), "trac03-sub-empty"))) && strings.HasSuffix(sp.GetName(), " process")) {
				sp.TraceFlags = 0
			}
			m.spans = append(m.spans, sp)
		}
	}
	m.mu.Unlock()
}
func (m *mockProxyServer) getPub(id string, del bool) *mockPub {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pubs[id]
	if del {
		delete(m.pubs, id)
	}
	return p
}
func (m *mockProxyServer) span(tid, sid, psid, name string, kind proxypb.SpanKind, sc proxypb.SpanStatusCode, attrs map[string]*proxypb.AttributeValue, evs []*proxypb.SpanEvent, links []*proxypb.SpanLink) *proxypb.TraceSpan {
	now, scope := time.Now(), traces.GoPubSubScopeName
	if m.cppMode {
		scope = traces.CppScopeName
	}
	return &proxypb.TraceSpan{TraceId: tid, SpanId: sid, ParentSpanId: psid, TraceFlags: 1, Name: name, Kind: kind, StatusCode: sc, StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Millisecond)), InstrumentationScopeName: scope, Attributes: attrs, Events: evs, Links: links}
}
func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	b[0] |= 1
	return hex.EncodeToString(b)
}
func parseTPOrNew(tp string) (string, string) {
	if ptp, err := traces.ParseTraceparent(tp); err == nil {
		return ptp.TraceID, ptp.SpanID
	}
	return randHex(16), ""
}
func mkAttrs(kv ...any) map[string]*proxypb.AttributeValue {
	m := make(map[string]*proxypb.AttributeValue, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		switch v := kv[i+1].(type) {
		case string:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: v}}
		case int64:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
		case bool:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
		}
	}
	return m
}
func shortRes(full string) (string, string) {
	if p := strings.Split(full, "/"); len(p) >= 4 {
		return p[1], p[3]
	}
	return "conformance-project", full
}
func dialFake(ep string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
func (m *mockProxyServer) corruptDelivery(msg *pubsubpb.PubsubMessage) ([]byte, map[string]string, string) {
	data, attrs, key := slices.Clone(msg.GetData()), maps.Clone(msg.GetAttributes()), msg.GetOrderingKey()
	switch m.cfg.fault {
	case "wrong-payload":
		data = append(data, []byte("-bad")...)
	case "wrong-attrs":
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["__corrupt__"] = "1"
	case "corrupt-ordering-key":
		key = "unexpected-key"
	}
	return data, attrs, key
}

func (m *mockProxyServer) flushLeaked(ctx context.Context) {
	m.subWG.Wait()
	m.mu.Lock()
	leaked := m.leaked
	m.leaked = nil
	m.mu.Unlock()
	for _, lk := range leaked {
		if c, err := dialFake(lk.ep); err == nil {
			_, _ = pubsubpb.NewSubscriberClient(c).ModifyAckDeadline(m.outCtx(ctx, "subscription", lk.sub), &pubsubpb.ModifyAckDeadlineRequest{Subscription: lk.sub, AckIds: []string{lk.aid}, AckDeadlineSeconds: 0})
			_ = c.Close()
		}
	}
}

func (m *mockProxyServer) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	m.flushLeaked(ctx)
	if m.cfg.fault == "runtime-unimpl-create-pub" {
		return nil, status.Error(codes.Unimplemented, "create publisher unsupported")
	}
	conn, err := dialFake(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("pub-%d", m.seq.Add(1))
	m.mu.Lock()
	m.lastEndpoint = req.GetEndpoint()
	m.pubs[id] = &mockPub{topic: req.GetTopic(), enableMon: req.GetEnableMonitoring(), enableOrdering: req.GetEnableMessageOrdering(), conn: conn, client: pubsubpb.NewPublisherClient(conn), keyBuf: map[string][]*asyncItem{}, keyCh: map[string]chan []*asyncItem{}, tokens: map[string]chan *proxypb.PublishResponse{}}
	m.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: pick(m.cfg.fault == "empty-pub-id", "", id)}, nil
}

func (m *mockProxyServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	if m.cfg.fault == "hang-publish" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	p := m.getPub(req.GetPublisherId(), false)
	if p == nil {
		return nil, status.Error(codes.NotFound, "unknown publisher")
	}
	switch {
	case m.cfg.fault == "runtime-unimpl-pub" || (m.cfg.fault == "unimpl-k1-b2" && string(req.GetData()) == "k1-b2-1") || (m.cfg.fault == "unimpl-k2" && req.GetOrderingKey() == "k2"):
		return nil, status.Error(codes.Unimplemented, "publish unsupported")
	case m.cfg.fault == "inband-unimpl-pub":
		return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "in-band unimplemented"}, nil
	}
	if req.GetOrderingKey() != "" && m.caps.GetSupportsOrderingOptIn() && !p.enableOrdering {
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "ordering not enabled"}, nil
	}
	attrs, data := maps.Clone(req.GetAttributes()), slices.Clone(req.GetData())
	if attrs == nil {
		attrs = map[string]string{}
	}
	switch m.cfg.fault {
	case "wrong-payload":
		data = append(data, []byte("-corrupt")...)
	case "wrong-attrs":
		delete(attrs, "k1")
		attrs["unexpected"] = "1"
	case "pord02-corrupt-unord":
		if req.GetOrderingKey() == "" {
			data = append(data, []byte("-bad")...)
		}
	}
	it := &asyncItem{data: string(data), key: req.GetOrderingKey(), attrs: attrs, parentTP: req.GetParentTraceparent(), resCh: make(chan *proxypb.PublishResponse, 1)}
	if req.GetAsyncFire() {
		tok := fmt.Sprintf("tok-%d", m.seq.Add(1))
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "publisher closed")
		}
		p.tokens[tok] = it.resCh
		if m.cfg.fault == "out-of-order-publish" {
			p.mu.Unlock()
			p.wg.Add(1)
			go func() { defer p.wg.Done(); m.sendBatch(p, []*asyncItem{it}) }()
			return &proxypb.PublishResponse{PublishToken: tok}, nil
		}
		bufKey := it.key
		q := append(p.keyBuf[bufKey], it)
		p.keyBuf[bufKey] = q
		if len(q) >= 2 || (it.key == "" && m.cfg.fault != "pord02-mix-unordered") {
			p.keyBuf[bufKey] = nil
			m.enqueueKeyBatchLocked(p, bufKey, q)
		}
		p.mu.Unlock()
		return &proxypb.PublishResponse{PublishToken: pick(m.cfg.fault == "empty-pub-token", "", tok)}, nil
	}
	m.sendBatch(p, []*asyncItem{it})
	resp := <-it.resCh
	if req.GetVerifyAttributesImmutability() && m.cfg.fault == "caller-attrs-mutated" {
		resp.CallerAttributesMutated = true
	}
	return resp, nil
}

func (m *mockProxyServer) enqueueKeyBatchLocked(p *mockPub, key string, batch []*asyncItem) {
	if p.closed {
		return
	}
	if (m.cfg.fault == "k1-concurrent-in-order" && key == "k1") || (m.cfg.fault == "k2-concurrent-batches" && key == "k2") {
		p.wg.Add(1)
		go func() { defer p.wg.Done(); m.sendBatch(p, batch) }()
		return
	}
	ch := p.keyCh[key]
	if ch == nil {
		ch = make(chan []*asyncItem, 64)
		p.keyCh[key] = ch
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for b := range ch {
				m.sendBatch(p, b)
			}
		}()
	}
	ch <- batch
}

func baseSpanAttrs(proj, topicOrSub string, kv ...any) map[string]*proxypb.AttributeValue {
	m := mkAttrs("gcp.project_id", proj, "messaging.system", "gcp_pubsub", "messaging.destination.name", topicOrSub)
	maps.Copy(m, mkAttrs(kv...))
	return m
}

func (m *mockProxyServer) sendBatch(p *mockPub, batch []*asyncItem) {
	if len(batch) > 1 && (m.cfg.fault == "out-of-order-publish" || (m.cfg.fault == "k1-reversed-only" && batch[0].key == "k1") || (m.cfg.fault == "reversed-batch-only" && batch[0].key == "k2")) {
		slices.Reverse(batch)
	}
	if m.cfg.fault == "missing-rpc" {
		for _, it := range batch {
			it.resCh <- &proxypb.PublishResponse{MessageId: fmt.Sprintf("fake-local-%d", m.seq.Add(1))}
		}
		return
	}
	proj, topicID := shortRes(p.topic)
	spanTopic, spanTopicID := pick(m.cfg.fault == "wrong-span-topic", "projects/"+proj+"/topics/wrong-topic", p.topic), pick(m.cfg.fault == "wrong-span-topic", "wrong-topic", topicID)
	emitMon := p.enableMon || m.cfg.fault == "emit-span-when-disabled"
	var pubSpan *proxypb.TraceSpan
	states := make([]struct{ create, fc, b *proxypb.TraceSpan }, len(batch))
	if emitMon {
		pa := baseSpanAttrs(proj, spanTopicID, "messaging.batch.message_count", int64(len(batch)))
		kind, sc := proxypb.SpanKind_SPAN_KIND_INTERNAL, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		if m.cppMode {
			maps.Copy(pa, mkAttrs("code.function.name", "BatchSink::AsyncPublish", "messaging.operation.type", "publish", "gl-cpp.status_code", "OK"))
			kind, sc = proxypb.SpanKind_SPAN_KIND_CLIENT, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		} else {
			maps.Copy(pa, mkAttrs("gcp.resource.name", spanTopic, "code.function", "publishMessageBundle"))
		}
		pubSpan = m.span(randHex(16), randHex(8), "", spanTopicID+" publish", kind, sc, pa, nil, nil)
	}
	pbMsgs := make([]*pubsubpb.PubsubMessage, len(batch))
	for i, it := range batch {
		attrs := maps.Clone(it.attrs)
		if emitMon {
			ptp, tpErr := traces.ParseTraceparent(it.parentTP)
			unsampled := it.parentTP != "" && (tpErr != nil || !ptp.IsSampled())
			tid, psid := parseTPOrNew(it.parentTP)
			csid := randHex(8)
			if !unsampled {
				if m.cppMode {
					var fcSpan *proxypb.TraceSpan
					if m.cfg.profile != "cpp_unconfigured_fc" && m.cfg.fault != "cpp-block-omit-fc" {
						fcSpan = m.span(tid, randHex(8), csid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, mkAttrs("code.function.name", "pubsub::FlowControlledPublisherConnection::Publish"), nil, nil)
					}
					states[i] = struct{ create, fc, b *proxypb.TraceSpan }{m.span(tid, csid, psid, spanTopicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, baseSpanAttrs(proj, spanTopicID, "code.function.name", "pubsub::PublisherConnection::Publish", "messaging.operation.type", "create", "messaging.message.envelope.size", int64(20+len(it.data)+len(it.key)), "gl-cpp.status_code", "OK"), []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}}, []*proxypb.SpanLink{{TraceId: pubSpan.TraceId, SpanId: pubSpan.SpanId}}), fcSpan, m.span(tid, randHex(8), csid, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, mkAttrs("code.function.name", "pubsub::BatchingPublisherConnection::Publish"), nil, nil)}
				} else {
					states[i] = struct{ create, fc, b *proxypb.TraceSpan }{m.span(tid, csid, psid, spanTopicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0, baseSpanAttrs(proj, spanTopicID, "gcp.resource.name", spanTopic, "code.function", "Publish", "messaging.message.body.size", pick(m.cfg.fault == "wrong-body-size", int64(999), int64(len(it.data))), "messaging.gcp_pubsub.message.ordering_key", it.key), []*proxypb.SpanEvent{{Name: "publish start", Attributes: mkAttrs("messaging.batch.message_count", int64(len(batch)))}, {Name: "publish end"}}, []*proxypb.SpanLink{{TraceId: pubSpan.TraceId, SpanId: pubSpan.SpanId, Attributes: mkAttrs("messaging.operation.name", "publish")}}), m.span(tid, randHex(8), csid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil), m.span(tid, randHex(8), csid, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil)}
				}
				pubSpan.Links = append(pubSpan.Links, &proxypb.SpanLink{TraceId: tid, SpanId: csid})
			}
			if p.enableMon && m.cfg.fault != "invalid-traces" {
				wireTID, flag := map[bool]string{false: tid, true: randHex(16)}[m.cfg.fault == "wrong-trace-id"], map[bool]string{false: "01", true: "00"}[unsampled || m.cfg.fault == "unsampled-traces" || m.cfg.fault == "unsampled-wire-tp-only"]
				attrs[fake.TraceparentAttributeKey] = fmt.Sprintf("00-%s-%s-%s", wireTID, pick(m.cfg.fault == "wrong-wire-span-id", randHex(8), csid), flag)
			}
		}
		if m.cfg.fault == "pub01-unexpected-traceparent" && !p.enableMon {
			attrs[fake.TraceparentAttributeKey] = fmt.Sprintf("00-%s-%s-01", randHex(16), randHex(8))
		}
		ordKey := it.key
		if m.cfg.fault == "pord02-mix-unordered" && it.data == "unord-1" {
			ordKey = "k1"
		}
		pbMsgs[i] = &pubsubpb.PubsubMessage{Data: []byte(it.data), OrderingKey: ordKey, Attributes: attrs, MessageId: pick(m.cfg.fault == "pub01-preset-msgid", "preset-id", "")}
	}
	if m.cfg.fault == "extra-pull-rpc" {
		_, _ = pubsubpb.NewSubscriberClient(p.conn).Pull(m.outCtx(context.Background(), "subscription", "projects/"+proj+"/subscriptions/extra"), &pubsubpb.PullRequest{Subscription: "projects/" + proj + "/subscriptions/extra", MaxMessages: 1, ReturnImmediately: true})
	}
	resp, err := p.client.Publish(m.outCtx(context.Background(), "topic", p.topic), &pubsubpb.PublishRequest{Topic: pick(m.cfg.fault == "wrong-topic", p.topic+"-wrong", p.topic), Messages: pbMsgs})
	if err != nil {
		for _, it := range batch {
			it.resCh <- &proxypb.PublishResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}
		}
		return
	}
	if emitMon {
		m.addSpan(pubSpan)
		if m.cfg.fault == "extra-pub-batch-span" && len(states) > 0 && states[0].create != nil {
			pub2 := m.span(randHex(16), randHex(8), "", spanTopicID+" publish", pubSpan.Kind, pubSpan.StatusCode, pubSpan.Attributes, nil, []*proxypb.SpanLink{{TraceId: states[0].create.TraceId, SpanId: states[0].create.SpanId}})
			states[0].create.Links = append(states[0].create.Links, &proxypb.SpanLink{TraceId: pub2.TraceId, SpanId: pub2.SpanId, Attributes: mkAttrs("messaging.operation.name", "publish")})
			m.addSpan(pub2)
		}
		if m.cppMode {
			m.addSpan(m.span(randHex(16), randHex(8), "", "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 0, nil, nil, nil))
		}
	}
	for i, it := range batch {
		msgID := resp.GetMessageIds()[i]
		if states[i].create != nil {
			states[i].create.Attributes["messaging.message.id"] = mkAttrs("k", pick(m.cfg.fault == "wrong-pub-span-msg-id", msgID+"-bad", msgID))["k"]
			m.addSpan(states[i].fc, states[i].b, states[i].create)
		}
		if m.cfg.fault == "mismatched-message-id" {
			msgID = "wrong-" + msgID
		} else if m.cfg.fault == "pord02-dup-msgid" {
			msgID = "dup-msg-id"
		}
		it.resCh <- &proxypb.PublishResponse{MessageId: msgID}
	}
}

func (m *mockProxyServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	p := m.getPub(req.GetPublisherId(), false)
	p.mu.Lock()
	ch := p.tokens[req.GetPublishToken()]
	delete(p.tokens, req.GetPublishToken())
	p.mu.Unlock()
	select {
	case resp := <-ch:
		if m.cfg.fault == "runtime-unimpl-await" {
			return nil, status.Error(codes.Unimplemented, "await unsupported")
		}
		return pick(m.cfg.fault == "inband-unimpl-await", &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "in-band unimplemented"}, resp), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockProxyServer) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	if p := m.getPub(req.GetPublisherId(), true); p != nil {
		p.mu.Lock()
		for k, rem := range p.keyBuf {
			if len(rem) > 0 {
				p.keyBuf[k] = nil
				m.enqueueKeyBatchLocked(p, k, rem)
			}
		}
		p.closed = true
		for _, ch := range p.keyCh {
			close(ch)
		}
		p.mu.Unlock()
		p.wg.Wait()
		_ = p.conn.Close()
	}
	return &proxypb.ClosePublisherResponse{}, pick(m.cfg.fault == "close-pub-err", status.Error(codes.Internal, "close boom"), nil)
}

func (m *mockProxyServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	sendStop := func(code int32, msg string, tot int64) error {
		return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: code, ErrorMessage: msg, TotalDelivered: tot}}})
	}
	switch m.cfg.fault {
	case "runtime-unimpl-sub":
		return status.Error(codes.Unimplemented, "subscriber unsupported")
	case "stopped-with-err":
		return sendStop(int32(codes.Internal), "boom", 0)
	case "stopped-unexpected":
		return sendStop(0, "", 0)
	}
	m.subWG.Add(1)
	defer m.subWG.Done()
	start, cid := first.GetStart(), fmt.Sprintf("cid-%d", m.seq.Add(1))
	if err := stream.Send(pick(m.cfg.fault == "bad-start-event", &proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: "x"}}}, &proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: pick(m.cfg.fault == "pull01-empty-then-cid", "", cid)}}})); err != nil {
		return err
	}
	if m.cfg.fault == "hang-subscribe" || (m.cfg.fault == "hang-trac03-fallback" && strings.Contains(start.GetSubscription(), "trac03-sub-empty")) {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if m.cfg.fault == "missing-rpc" {
		for cmd, rerr := stream.Recv(); rerr == nil; cmd, rerr = stream.Recv() {
			if cmd.GetStop() != nil {
				return sendStop(0, "", 1)
			}
		}
		return nil
	}
	conn, err := dialFake(start.GetEndpoint())
	if err != nil {
		return err
	}
	ackByTok, spanByTok := map[string]string{}, map[string]*proxypb.TraceSpan{}
	var stopped bool
	subClient, sub := pubsubpb.NewSubscriberClient(conn), start.GetSubscription()
	fb := strings.Contains(sub, "trac03-sub-empty")
	out := func() context.Context { return m.outCtx(stream.Context(), "subscription", sub) }
	ack := func(id string) {
		ackCtx := out()
		if m.cfg.fault == "bad-ack-routing" {
			ackCtx = stream.Context()
		} else if m.cfg.fault == "unescaped-ack-routing" {
			ackCtx = metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+sub)
		}
		_, _ = subClient.Acknowledge(ackCtx, &pubsubpb.AcknowledgeRequest{Subscription: pick(m.cfg.fault == "bad-ack-sub" || (m.cfg.fault == "trac03-fb-bad-ack-sub" && fb), sub+"-wrong", sub), AckIds: []string{pick(m.cfg.fault == "wrong-ack-id", id+"-wrong", id)}})
	}
	nack := func(id string) {
		_, _ = subClient.ModifyAckDeadline(m.outCtx(context.Background(), "subscription", sub), &pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{id}, AckDeadlineSeconds: 0})
	}
	var asyncAckWG sync.WaitGroup
	defer func() {
		asyncAckWG.Wait()
		_ = conn.Close()
		if !stopped && len(ackByTok) > 0 {
			m.mu.Lock()
			for _, aid := range ackByTok {
				m.leaked = append(m.leaked, struct{ ep, sub, aid string }{start.GetEndpoint(), sub, aid})
			}
			m.mu.Unlock()
		}
	}()
	if m.cfg.fault == "wrong-sub-pull-method" {
		_, _ = subClient.Pull(out(), &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
		return nil
	}
	streamOut := out()
	if (m.cfg.profile == "java" || m.cfg.fault == "unescaped-stream-routing") && m.cfg.fault != "missing-stream-routing" {
		streamOut = metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+sub)
	}
	isPhaseA := !strings.Contains(sub, "trac03-sub-empty") && !strings.Contains(sub, "trac03-sub-badtp")
	pv, dl := pick(m.cfg.fault == "missing-keepalive-pv" || !m.caps.GetSupportsKeepalive(), int64(0), 1), pick(m.cfg.fault == "zero-stream-deadline", int32(0), pick(m.caps.GetSupportsKeepalive(), int32(60), 10))
	streamCID := pick(m.cfg.fault == "wrong-client-id-on-stream" || (isPhaseA && strings.HasSuffix(m.cfg.fault, "-mismatch")), cid+"-wrong", pick(m.cfg.fault == "pull01-empty-then-cid", "", cid))
	cmdCh := make(chan *proxypb.SubscriberCommand, 8)
	go func() {
		defer close(cmdCh)
		for c, e := stream.Recv(); e == nil; c, e = stream.Recv() {
			cmdCh <- c
		}
	}()
	proj, subID := shortRes(sub)
	spanSub, spanSubID := pick(m.cfg.fault == "wrong-span-sub", "projects/"+proj+"/subscriptions/wrong-sub", sub), pick(m.cfg.fault == "wrong-span-sub", "wrong-sub", subID)
	for attempt := 0; ; attempt++ {
		sp, err := subClient.StreamingPull(pick(m.cfg.fault == "missing-stream-routing", stream.Context(), streamOut))
		if err != nil {
			return err
		}
		if attempt == 0 && isPhaseA && strings.HasPrefix(m.cfg.fault, "trac03-preframe0-eof-") {
			_ = sp.CloseSend()
			_, _ = sp.Recv()
			continue
		}
		if err = sp.Send(&pubsubpb.StreamingPullRequest{Subscription: pick(m.cfg.fault == "wrong-stream-sub", sub+"-wrong", sub), StreamAckDeadlineSeconds: dl, ClientId: streamCID, ProtocolVersion: pv}); err != nil {
			_, _ = sp.Recv()
			if attempt == 0 && isPhaseA && strings.HasPrefix(m.cfg.fault, "trac03-preframe0-failrecv-") {
				continue
			}
			return err
		}
		if m.cfg.fault == "subseq-frame-sub" {
			_ = sp.Send(&pubsubpb.StreamingPullRequest{Subscription: sub})
		}
		for {
			resp, err := sp.Recv()
			if err != nil {
				if attempt == 0 && isPhaseA && strings.HasPrefix(m.cfg.fault, "trac03-preframe0-failrecv-") {
					break
				}
				if m.cfg.fault == "subseq-frame-sub" && !errors.Is(err, io.EOF) {
					return err
				}
				return nil
			}
			if len(resp.GetReceivedMessages()) == 0 && m.cfg.fault == "empty-resp-crash" {
				return sendStop(int32(codes.Internal), "empty response crash", 0)
			}
			for _, rm := range resp.GetReceivedMessages() {
				msg, tok, cbTP := rm.GetMessage(), pick(m.cfg.fault == "empty-deliv-token", "", fmt.Sprintf("deliv-%d", m.seq.Add(1))), ""
				if m.cfg.fault == "premature-ack" || m.cfg.fault == "premature-ack-only" || ((m.cfg.fault == "trac03-fallback-premature-ack" || m.cfg.fault == "trac03-fallback-premature-ack-only") && msg.GetMessageId() == "msg-trac03-2") {
					ack(rm.GetAckId())
				} else if m.cfg.fault == "premature-inline-ack" || (m.cfg.fault == "trac03-fb-premature-inline-ack" && msg.GetMessageId() == "msg-trac03-2") {
					_ = sp.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{rm.GetAckId()}})
					_ = sp.Send(&pubsubpb.StreamingPullRequest{})
					_, _ = sp.Recv()
				}
				var subSpan *proxypb.TraceSpan
				if start.GetEnableMonitoring() {
					wtp, _ := traces.ExtractTraceparent(msg.GetAttributes())
					tid, psid := parseTPOrNew(wtp)
					if _, perr := traces.ParseTraceparent(wtp); perr == nil {
						m.mu.Lock()
						m.lastTID = tid
						m.mu.Unlock()
					} else {
						if m.cfg.fault == "trac03-fallback-reuse-tid" {
							m.mu.Lock()
							if m.lastTID != "" {
								tid = m.lastTID
							}
							m.mu.Unlock()
						}
						if m.cfg.fault == "trac03-fallback-parent" {
							psid = randHex(8)
						}
					}
					ssid, procSID := randHex(8), randHex(8)
					sa := baseSpanAttrs(proj, spanSubID, "messaging.message.id", pick(m.cfg.fault == "wrong-sub-span-msg-id", msg.GetMessageId()+"-bad", msg.GetMessageId()), "messaging.gcp_pubsub.message.ack_id", pick(m.cfg.fault == "wrong-sub-span-ack-id", rm.GetAckId()+"-bad", rm.GetAckId()))
					if m.cppMode {
						maps.Copy(sa, mkAttrs("messaging.operation.type", "subscribe", "messaging.message.envelope.size", int64(20+len(msg.GetData())+len(msg.GetMessageId())), "messaging.gcp_pubsub.subscription.exactly_once_delivery", false, "messaging.gcp_pubsub.result", "ack"))
						subSpan = m.span(tid, ssid, psid, spanSubID+" subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0, sa, []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.message_ack"}, {Name: "gl-cpp.ack_end"}}, nil)
						for _, n := range []struct{ id, nm string }{{randHex(8), "subscriber concurrency control"}, {randHex(8), "subscriber scheduler"}, {procSID, spanSubID + " process"}} {
							m.addSpan(m.span(tid, n.id, ssid, n.nm, proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, mkAttrs("messaging.system", "gcp_pubsub"), nil, nil))
						}
					} else {
						maps.Copy(sa, mkAttrs("gcp.resource.name", spanSub, "messaging.message.body.size", int64(len(msg.GetData())), "messaging.gcp_pubsub.exactly_once_delivery", false, "messaging.gcp_pubsub.message.ordering_key", "", "messaging.batch.message_count", int64(1), "code.function", "receive", "messaging.gcp_pubsub.result", "acked"))
						subSpan = m.span(tid, ssid, psid, spanSubID+" subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, sa, nil, nil)
						m.addSpan(m.span(tid, randHex(8), ssid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil), m.span(tid, randHex(8), ssid, "subscribe scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil), m.span(tid, procSID, ssid, spanSubID+" process", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0, baseSpanAttrs(proj, spanSubID, "gcp.resource.name", spanSub, "messaging.message.id", msg.GetMessageId(), "messaging.message.body.size", int64(len(msg.GetData())), "messaging.gcp_pubsub.message.ordering_key", "", "messaging.operation.type", "process"), []*proxypb.SpanEvent{{Name: "ack called"}}, nil))
					}
					cbSID := pick(m.cfg.fault == "wrong-cb-span-id", pick(procSID[15] == 'a', procSID[:15]+"b", procSID[:15]+"a"), procSID)
					cbTP = fmt.Sprintf("00-%s-%s-%s", tid, cbSID, map[bool]string{false: "01", true: "00"}[m.cfg.fault == "unsampled-traces" || m.cfg.fault == "unsampled-cb-tp-only" || (m.cfg.fault == "trac03-fb-unsampled-cb" && fb)])
					if m.cfg.fault == "bad-callback-traceparent" {
						cbTP = "not-a-traceparent"
					} else if m.cfg.fault == "empty-cb-tp" && !strings.HasPrefix(msg.GetMessageId(), "msg-trac03-") {
						cbTP = ""
					}
				}
				ackByTok[tok], spanByTok[tok] = rm.GetAckId(), subSpan
				delivData, delivAttrs, delivKey := m.corruptDelivery(msg)
				if m.cfg.fault == "trac03-fb-corrupt-deliv" && fb {
					delivData = append(delivData, []byte("-bad")...)
				}
				if m.cfg.fault == "emit-span-when-disabled" {
					m.addSpan(m.span(randHex(16), randHex(8), "", spanSubID+" subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil))
				}
				switch m.cfg.fault {
				case "runtime-unimpl-deliv":
					return status.Error(codes.Unimplemented, "delivery unsupported")
				case "inband-unimpl-deliv":
					return sendStop(int32(codes.Unimplemented), "in-band unimplemented", 0)
				}
				if err := stream.Send(pick(m.cfg.fault == "unexpected-deliv-ev", &proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: cid}}}, &proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: tok, MessageId: msg.GetMessageId(), Data: delivData, Attributes: delivAttrs, OrderingKey: delivKey, CallbackTraceparent: cbTP, AutoSettled: m.cfg.fault == "auto-settled-flag" || (m.cfg.fault == "trac03-fallback-auto-settled" && msg.GetMessageId() == "msg-trac03-2")}}})); err != nil {
					return err
				}
				if m.cfg.fault == "async-premature-ack" {
					asyncAckWG.Add(1)
					go func(id string) {
						defer asyncAckWG.Done()
						t := time.NewTimer(2 * time.Millisecond)
						defer t.Stop()
						select {
						case <-t.C:
							ack(id)
						case <-stream.Context().Done():
						}
					}(rm.GetAckId())
				}
				for cmd := range cmdCh {
					switch {
					case cmd.GetSettle() != nil:
						dt := cmd.GetSettle().GetDeliveryToken()
						ackID, subSp := ackByTok[dt], spanByTok[dt]
						delete(ackByTok, dt)
						if cmd.GetSettle().GetAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK {
							nack(ackID)
							continue
						}
						if cmd.GetSettle().GetAction() != proxypb.SettleMessageCommand_SETTLE_ACTION_ACK {
							return status.Error(codes.InvalidArgument, "unexpected settle action")
						}
						switch m.cfg.fault {
						case "premature-ack-only", "premature-inline-ack":
						case "trac03-fallback-premature-ack-only", "trac03-fb-premature-inline-ack":
							if msg.GetMessageId() != "msg-trac03-2" {
								ack(ackID)
							}
						case "async-premature-ack":
							asyncAckWG.Wait()
						case "inline-stream-ack":
							_ = sp.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{ackID}})
						case "modack-lease-ext", "bad-modack", "wrong-modack-sub", "out-of-bounds-modack", "unescaped-modack-routing":
							maCtx := pick(m.cfg.fault == "bad-modack", context.Background(), pick(m.cfg.fault == "unescaped-modack-routing", metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+sub), out()))
							_, _ = subClient.ModifyAckDeadline(maCtx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: pick(m.cfg.fault == "wrong-modack-sub", sub+"-wrong", sub), AckIds: []string{ackID}, AckDeadlineSeconds: pick(m.cfg.fault == "out-of-bounds-modack", int32(5), 30)})
							fallthrough
						default:
							ack(ackID)
						}
						switch m.cfg.fault {
						case "duplicate-ack":
							ack(ackID)
						case "unexpected-nack":
							nack(ackID)
						case "pull01-inconsistent-clientids", "trac03-p1-two-streams", "pull01-empty-then-cid":
							if m.cfg.fault != "trac03-p1-two-streams" || !strings.HasPrefix(msg.GetMessageId(), "msg-trac03-") {
								cid2 := pick(m.cfg.fault != "trac03-p1-two-streams", cid+"-2", cid)
								if sp2, e2 := subClient.StreamingPull(out()); e2 == nil {
									_ = sp2.Send(&pubsubpb.StreamingPullRequest{Subscription: sub, StreamAckDeadlineSeconds: dl, ClientId: cid2, ProtocolVersion: pv})
									_ = sp2.CloseSend()
									_, _ = sp2.Recv()
								}
							}
						case "extra-publish-rpc", "trac03-p1-extra-pub":
							if m.cfg.fault == "extra-publish-rpc" || !strings.HasPrefix(msg.GetMessageId(), "msg-trac03-") {
								_, _ = pubsubpb.NewPublisherClient(conn).Publish(m.outCtx(stream.Context(), "topic", "projects/"+proj+"/topics/extra"), &pubsubpb.PublishRequest{Topic: "projects/" + proj + "/topics/extra", Messages: []*pubsubpb.PubsubMessage{{Data: []byte("extra")}}})
							}
						case "settle-error-result":
							_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: dt, Status: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, GrpcErrorCode: int32(codes.PermissionDenied), ErrorMessage: "denied"}}})
						case "settle-code-only":
							_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: dt, Status: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, GrpcErrorCode: int32(codes.PermissionDenied), ErrorMessage: "denied"}}})
						case "settle-status-only":
							_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: dt, Status: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, ErrorMessage: "denied"}}})
						}
						if subSp != nil {
							if m.cppMode {
								m.addSpan(m.span(randHex(16), randHex(8), "", spanSubID+" ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, baseSpanAttrs(proj, spanSubID, "code.function.name", "pubsub::AckHandler::ack", "messaging.gcp_pubsub.message.ack_id", ackID, "messaging.gcp_pubsub.subscription.template", spanSub, "messaging.operation.type", "settle", "messaging.gcp_pubsub.message.delivery_attempt", int64(1), "gl-cpp.status_code", "OK"), nil, []*proxypb.SpanLink{{TraceId: subSp.TraceId, SpanId: subSp.SpanId}}))
							} else {
								subSp.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: mkAttrs("messaging.batch.message_count", int64(1))}, {Name: "ack end"}}
								if m.cfg.fault != "missing-sub-ack-span" {
									ackSpan := m.span(randHex(16), randHex(8), "", spanSubID+" ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, baseSpanAttrs(proj, spanSubID, "gcp.resource.name", spanSub, "messaging.batch.message_count", int64(1), "code.function", "sendAck"), nil, []*proxypb.SpanLink{{TraceId: subSp.TraceId, SpanId: subSp.SpanId}})
									subSp.Links = []*proxypb.SpanLink{{TraceId: ackSpan.TraceId, SpanId: ackSpan.SpanId, Attributes: mkAttrs("messaging.operation.name", "ack")}}
									m.addSpan(ackSpan)
								}
							}
							m.addSpan(subSp)
						}
					case cmd.GetStop() != nil:
						if st := cmd.GetStop(); len(ackByTok) > 0 {
							if (!m.caps.GetSupportsShutdownBehavior() || st.GetShutdownBehavior() != proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY) && (m.caps.GetSupportsShutdownBehavior() || !m.caps.GetSupportsShutdownTimeout() || st.ShutdownTimeoutMs == nil || st.GetShutdownTimeoutMs() != 0) {
								continue
							}
							for _, aid := range ackByTok {
								nack(aid)
							}
						}
						stopped = true
						_ = sp.CloseSend()
						for rerr := error(nil); m.cfg.fault == "subseq-frame-sub" && !errors.Is(rerr, io.EOF); _, rerr = sp.Recv() {
							if rerr != nil {
								return rerr
							}
						}
						switch m.cfg.fault {
						case "runtime-unimpl-stop":
							return status.Error(codes.Unimplemented, "stop unsupported")
						case "unexpected-stop-ev":
							_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: cid}}})
						}
						return sendStop(pick(m.cfg.fault == "stop-with-err", int32(codes.Internal), 0), pick(m.cfg.fault == "stop-with-err", "stop-boom", ""), pick(m.cfg.fault == "wrong-total-delivered", int64(2), 1))
					}
				}
				_ = sp.CloseSend()
				return nil
			}
		}
	}
}

func (m *mockProxyServer) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	m.flushLeaked(ctx)
	if !m.caps.GetSupportsUnaryPull() || m.cfg.fault == "runtime-unimpl-unary-pull" {
		return nil, status.Error(codes.Unimplemented, "unary pull unsupported")
	}
	if m.cfg.fault == "hang-pull" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if m.cfg.fault == "inband-unimpl-unary-pull" {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "in-band unimplemented"}, nil
	}
	if m.cfg.fault == "missing-rpc" || (m.cfg.fault == "pull04-p4-no-pull" && req.MaxAttempts != nil) {
		return &proxypb.UnaryPullResponse{}, nil
	}
	p1, p2, p3, p4 := req.GetMaxMessages() == 1 && req.MaxAttempts == nil, false, false, req.MaxAttempts != nil
	if req.GetMaxMessages() == 2 && !p4 {
		m.mu.Lock()
		if m.m3fPullMax2Seen == nil {
			m.m3fPullMax2Seen = map[string]bool{}
		}
		p3 = m.m3fPullMax2Seen[req.GetSubscription()]
		m.m3fPullMax2Seen[req.GetSubscription()] = true
		m.mu.Unlock()
		p2 = !p3
		if m.cfg.fault == "pull04-p3-skip-pull" && p3 {
			return &proxypb.UnaryPullResponse{}, nil
		}
	}
	conn, err := dialFake(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sc, sub := pubsubpb.NewSubscriberClient(conn), req.GetSubscription()
	uCtx := func(miss bool) context.Context { return pick(miss, ctx, m.outCtx(ctx, "subscription", sub)) }
	pullSub := pick(m.cfg.fault == "wrong-pull-sub" || (m.cfg.fault == "pull04-p1-wrong-sub" && p1) || (m.cfg.fault == "pull04-p2-wrong-sub" && p2) || (m.cfg.fault == "pull04-p3-wrong-sub" && p3) || (m.cfg.fault == "pull04-p4-wrong-sub" && p4), sub+"-wrong", sub)
	pullMax := pick(m.cfg.fault == "wrong-pull-max" || (m.cfg.fault == "pull04-p2-wrong-max" && p2) || (m.cfg.fault == "pull04-p3-wrong-max" && p3), pick(m.cfg.fault == "wrong-pull-max", int32(2), 1), req.GetMaxMessages())
	pr, err := sc.Pull(uCtx(m.cfg.fault == "missing-pull-routing" || (m.cfg.fault == "pull04-p1-miss-routing" && p1) || (m.cfg.fault == "pull04-p2-miss-routing" && p2) || (m.cfg.fault == "pull04-p3-miss-routing" && p3) || (m.cfg.fault == "pull04-p4-miss-routing" && p4)), &pubsubpb.PullRequest{Subscription: pullSub, MaxMessages: pullMax, ReturnImmediately: req.GetReturnImmediately()})
	if err != nil {
		switch m.cfg.fault {
		case "pull04-p4-grpc-deadline":
			return nil, err
		case "pull04-p4-grpc-internal":
			return nil, status.Error(codes.Internal, "wrong grpc err")
		case "pull04-p4-extra-ack":
			_, _ = sc.Acknowledge(uCtx(false), &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{"extra-p4"}})
		case "pull04-p4-empty-ok":
			return &proxypb.UnaryPullResponse{}, nil
		case "pull04-p4-wrong-err":
			return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Internal), ErrorMessage: "wrong err"}, nil
		case "pull04-p4-phantom":
			return &proxypb.UnaryPullResponse{Messages: []*proxypb.UnaryPullResponse_PulledMessage{{MessageId: "phantom"}}}, nil
		case "pull04-p4-err-with-msgs":
			return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.DeadlineExceeded), ErrorMessage: "deadline", Messages: []*proxypb.UnaryPullResponse_PulledMessage{{MessageId: "phantom"}}}, nil
		}
		return &proxypb.UnaryPullResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
	}
	if p2 && m.cfg.fault == "pull04-p2-extra-pull" {
		_, _ = sc.Pull(uCtx(false), &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 2, ReturnImmediately: req.GetReturnImmediately()})
	}
	if (p1 && m.cfg.fault == "pull04-p1-extra-modack") || (p2 && m.cfg.fault == "pull04-p2-extra-modack") {
		_, _ = sc.ModifyAckDeadline(uCtx(false), &pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{pick(p1, "extra-p1", "extra-p2")}, AckDeadlineSeconds: 10})
	}
	var out []*proxypb.UnaryPullResponse_PulledMessage
	for i, rm := range pr.GetReceivedMessages() {
		if m.cfg.fault == "pull04-p2-drop-second" && req.GetMaxMessages() == 2 && i > 0 {
			continue
		}
		if req.GetEnableMonitoring() || m.cfg.fault == "emit-span-when-disabled" {
			m.addSpan(m.span(randHex(16), randHex(8), "", "sub receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0, nil, nil, nil))
		}
		if req.GetSettleAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK && !(m.cfg.fault == "pull04-p2-skip-ack" && req.GetMaxMessages() == 2 && i > 0) {
			ackIDs := []string{pick(m.cfg.fault == "wrong-ack-id", rm.GetAckId()+"-wrong", rm.GetAckId())}
			if p1 && m.cfg.fault == "pull04-p1-extra-ack-id" {
				ackIDs = append(ackIDs, rm.GetAckId()+"-extra")
			}
			if _, err := sc.Acknowledge(uCtx(m.cfg.fault == "bad-ack-routing" || (m.cfg.fault == "pull04-p1-bad-ack-routing" && p1) || (m.cfg.fault == "pull04-p2-bad-ack-routing" && p2)), &pubsubpb.AcknowledgeRequest{Subscription: pick(m.cfg.fault == "bad-ack-sub" || (m.cfg.fault == "pull04-p1-bad-ack-sub" && p1) || (m.cfg.fault == "pull04-p2-bad-ack-sub" && p2), sub+"-wrong", sub), AckIds: ackIDs}); err != nil {
				return nil, err
			}
		}
		data, attrs, key := m.corruptDelivery(rm.GetMessage())
		msgID := rm.GetMessage().GetMessageId()
		if m.cfg.fault == "mismatched-message-id" {
			msgID = "wrong-" + msgID
		}
		st, code, em := proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, int32(0), ""
		switch m.cfg.fault {
		case "settle-error-result":
			st, code, em = proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, int32(codes.PermissionDenied), "denied"
		case "pull04-settle-code-only":
			code, em = int32(codes.PermissionDenied), "denied"
		case "pull04-settle-status-only":
			st, em = proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, "denied"
		}
		out = append(out, &proxypb.UnaryPullResponse_PulledMessage{MessageId: msgID, Data: data, Attributes: attrs, OrderingKey: key, SettleStatus: st, SettleErrorCode: code, SettleErrorMessage: em})
	}
	if len(pr.GetReceivedMessages()) == 0 && req.GetMaxMessages() == 2 && m.cfg.fault == "pull04-p3-phantom" {
		out = append(out, &proxypb.UnaryPullResponse_PulledMessage{MessageId: "phantom"})
	}
	return &proxypb.UnaryPullResponse{Messages: out}, nil
}

func runMockSuite(t *testing.T, cfg mockConfig, filter string, timeout time.Duration) *harness.TargetReport {
	t.Helper()
	target := harness.TargetSpec{Name: cfg.profile, Connect: connectMockProxy(cfg)}
	runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: filter, ScenarioTimeout: timeout})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget(%s): %v", cfg.profile, err)
	}
	return rep
}

func statusMap(rep *harness.TargetReport) map[string]harness.ScenarioResult {
	out := make(map[string]harness.ScenarioResult, len(rep.Scenarios))
	for _, sr := range rep.Scenarios {
		out[sr.ID] = sr
	}
	return out
}

func TestSmokeScenarios_Profiles(t *testing.T) {
	t.Run("Go canonical passes all 6 smoke scenarios", func(t *testing.T) {
		rep := runMockSuite(t, mockConfig{profile: "go"}, m3fSmokeFilter, 5*time.Second)
		byID := statusMap(rep)
		if rep.Summary.Pass != 6 || len(rep.Scenarios) != 6 || byID["TRAC-01"].TraceStatus != traces.StatusPass || byID["TRAC-03"].TraceStatus != traces.StatusPass {
			t.Fatalf("expected 6 PASS, got %+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
	})
	t.Run("Rust non-monitoring skips TRAC-01 and UNSUPPORTED PULL-04 while TRAC-03 passes", func(t *testing.T) {
		byID := statusMap(runMockSuite(t, mockConfig{profile: "rust"}, m3fSmokeFilter, 5*time.Second))
		for _, id := range []string{"PUB-01", "PORD-02", "PULL-01", "TRAC-03"} {
			if byID[id].Status != harness.StatusPass {
				t.Fatalf("%s = %s (%s)", id, byID[id].Status, byID[id].Error)
			}
		}
		if byID["TRAC-03"].TraceStatus != traces.StatusSkippedNoMonitoring || byID["TRAC-01"].Status != harness.StatusSkippedNoMonitoring || byID["PULL-04"].Status != harness.StatusUnsupportedByClient {
			t.Fatalf("unexpected non-mon statuses: %+v", byID)
		}
		if rep := runMockSuite(t, mockConfig{profile: "rust", fault: "trac03-p1-extra-pub"}, "^TRAC-03$", 5*time.Second); rep.Scenarios[0].Status != harness.StatusFail {
			t.Fatalf("rust trac03-p1-extra-pub expected FAIL, got %+v", rep.Scenarios)
		}
	})
	t.Run("Java unencoded StreamingPull vs encoded Ack routing", func(t *testing.T) {
		if rep := runMockSuite(t, mockConfig{profile: "java"}, "^(PULL-01|TRAC-03)$", 5*time.Second); rep.Summary.Pass != 2 {
			t.Fatalf("java expected 2 PASS, got %+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
		for _, flt := range []string{"bad-ack-routing", "unescaped-ack-routing", "unescaped-modack-routing"} {
			if rep := runMockSuite(t, mockConfig{profile: "java", fault: flt}, "^PULL-01$", 5*time.Second); rep.Scenarios[0].Status != harness.StatusFail {
				t.Fatalf("java fault=%s PULL-01 = %s (%s)", flt, rep.Scenarios[0].Status, rep.Scenarios[0].Error)
			}
		}
	})
	t.Run("Cpp profiles pass all 6 with deviations", func(t *testing.T) {
		for _, prof := range []string{"cpp", "cpp_unconfigured_fc"} {
			rep := runMockSuite(t, mockConfig{profile: prof}, m3fSmokeFilter, 5*time.Second)
			byID := statusMap(rep)
			if rep.Summary.Pass != 6 || len(byID["TRAC-01"].Deviations) == 0 || byID["TRAC-03"].TraceStatus != traces.StatusPass || len(byID["TRAC-03"].Deviations) == 0 {
				t.Fatalf("%s mismatch: %+v scenarios=%+v", prof, rep.Summary, rep.Scenarios)
			}
		}
		if byID := statusMap(runMockSuite(t, mockConfig{profile: "cpp", fault: "cpp-block-omit-fc"}, "^(TRAC-01|TRAC-03)$", 5*time.Second)); byID["TRAC-01"].Status != harness.StatusFail || byID["TRAC-03"].Status != harness.StatusFail {
			t.Fatalf("cpp-block-omit-fc expected TRAC-01/TRAC-03 FAIL, got %+v", byID)
		}
		if byID := statusMap(runMockSuite(t, mockConfig{profile: "cpp", fault: "unescaped-stream-routing"}, "^(PULL-01|TRAC-03)$", 5*time.Second)); byID["PULL-01"].Status != harness.StatusFail || byID["TRAC-03"].Status != harness.StatusFail {
			t.Fatalf("cpp unescaped-stream-routing expected PULL-01/TRAC-03 FAIL, got %+v", byID)
		}
	})
	t.Run("PULL-04 DeadlineExceeded gRPC and empty variants pass", func(t *testing.T) {
		for _, flt := range []string{"pull04-p4-grpc-deadline", "pull04-p4-empty-ok"} {
			if rep := runMockSuite(t, mockConfig{profile: "go", fault: flt}, "^PULL-04$", 5*time.Second); rep.Scenarios[0].Status != harness.StatusPass {
				t.Fatalf("fault=%s PULL-04 = %s (%s)", flt, rep.Scenarios[0].Status, rep.Scenarios[0].Error)
			}
		}
	})
	t.Run("Inline stream ack, ModAck lease extension, and PULL-04 unsupported mapping", func(t *testing.T) {
		for _, flt := range []string{"inline-stream-ack", "modack-lease-ext"} {
			if rep := runMockSuite(t, mockConfig{profile: "go", fault: flt}, "^(PULL-01|TRAC-03)$", 5*time.Second); rep.Summary.Pass != 2 {
				t.Fatalf("fault=%s expected 2 PASS, got %+v", flt, rep.Summary)
			}
		}
		for _, tc := range []struct{ flt, sc string }{{"runtime-unimpl-unary-pull", "PULL-04"}, {"inband-unimpl-unary-pull", "PULL-04"}, {"runtime-unimpl-create-pub", "PUB-01"}, {"runtime-unimpl-create-pub", "PORD-02"}, {"unimpl-k1-b2", "PORD-02"}, {"unimpl-k2", "PORD-02"}, {"runtime-unimpl-pub", "PUB-01"}, {"runtime-unimpl-pub", "PORD-02"}, {"inband-unimpl-pub", "PUB-01"}, {"inband-unimpl-pub", "PORD-02"}, {"runtime-unimpl-await", "PORD-02"}, {"inband-unimpl-await", "PORD-02"}, {"runtime-unimpl-sub", "PULL-01"}, {"runtime-unimpl-deliv", "PULL-01"}, {"inband-unimpl-deliv", "PULL-01"}, {"runtime-unimpl-stop", "PULL-01"}} {
			if rep := runMockSuite(t, mockConfig{profile: "go", fault: tc.flt}, "^"+tc.sc+"$", 5*time.Second); rep.Scenarios[0].Status != harness.StatusUnsupportedByClient {
				t.Fatalf("fault=%s %s = %s (%s)", tc.flt, tc.sc, rep.Scenarios[0].Status, rep.Scenarios[0].Error)
			}
		}
		for _, prof := range []string{"go_timeout_only", "go_basic_stop"} {
			if byID := statusMap(runMockSuite(t, mockConfig{profile: prof, fault: "premature-ack-only"}, "^(PULL-01|PULL-04)$", 3*time.Second)); byID["PULL-01"].Status != harness.StatusFail || byID["PULL-04"].Status != harness.StatusPass {
				t.Fatalf("profile=%s: %+v", prof, byID)
			}
		}
	})
}

func TestSmokeScenarios_FaultDetection(t *testing.T) {
	for _, tc := range []struct {
		name, fault        string
		wantFail, wantPass []string
	}{
		{"missing-rpc", "missing-rpc", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, nil}, {"wrong-payload", "wrong-payload", []string{"PUB-01", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, nil},
		{"wrong-attrs", "wrong-attrs", []string{"PUB-01", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, nil}, {"missing-routing-header", "missing-routing-header", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, nil},
		{"unescaped-routing-on-go", "unescaped-routing-on-go", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, nil}, {"out-of-order-publish", "out-of-order-publish", []string{"PORD-02"}, []string{"PUB-01", "PULL-01"}},
		{"k1-concurrent-in-order", "k1-concurrent-in-order", []string{"PORD-02"}, nil}, {"k2-concurrent-batches", "k2-concurrent-batches", []string{"PORD-02"}, nil}, {"k1-reversed-only", "k1-reversed-only", []string{"PORD-02"}, nil},
		{"reversed-batch-only", "reversed-batch-only", []string{"PORD-02"}, []string{"PUB-01"}}, {"premature-ack", "premature-ack", []string{"PULL-01", "TRAC-03"}, []string{"PULL-04", "TRAC-01"}},
		{"premature-ack-only", "premature-ack-only", []string{"PULL-01", "TRAC-03"}, []string{"PULL-04", "TRAC-01"}}, {"async-premature-ack", "async-premature-ack", []string{"PULL-01", "TRAC-03"}, nil},
		{"auto-settled-flag", "auto-settled-flag", []string{"PULL-01", "TRAC-03"}, nil}, {"duplicate-ack", "duplicate-ack", []string{"PULL-01", "TRAC-03"}, nil}, {"wrong-sub-pull-method", "wrong-sub-pull-method", []string{"PULL-01"}, nil},
		{"settle-error-result", "settle-error-result", []string{"PULL-01", "PULL-04", "TRAC-03"}, nil}, {"mismatched-message-id", "mismatched-message-id", []string{"PUB-01", "PORD-02", "PULL-04", "TRAC-01", "TRAC-03"}, nil},
		{"missing-traces", "missing-traces", []string{"TRAC-01", "TRAC-03"}, []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04"}}, {"invalid-traces", "invalid-traces", []string{"TRAC-01", "TRAC-03"}, nil},
		{"unsampled-traces", "unsampled-traces", []string{"TRAC-01", "TRAC-03"}, nil}, {"unsampled-wire-tp-only", "unsampled-wire-tp-only", []string{"TRAC-03"}, nil}, {"unsampled-cb-tp-only", "unsampled-cb-tp-only", []string{"TRAC-03"}, nil},
		{"extra-publish-rpc", "extra-publish-rpc", []string{"PULL-01", "TRAC-03"}, nil}, {"extra-pull-rpc", "extra-pull-rpc", []string{"PORD-02"}, nil}, {"wrong-topic", "wrong-topic", []string{"PUB-01", "PORD-02"}, nil},
		{"missing-keepalive-pv", "missing-keepalive-pv", []string{"PULL-01", "TRAC-03"}, nil}, {"zero-stream-deadline", "zero-stream-deadline", []string{"PULL-01", "TRAC-03"}, nil}, {"wrong-stream-sub", "wrong-stream-sub", []string{"PULL-01"}, nil},
		{"missing-stream-routing", "missing-stream-routing", []string{"PULL-01"}, nil}, {"bad-modack", "bad-modack", []string{"PULL-01"}, nil}, {"wrong-modack-sub", "wrong-modack-sub", []string{"PULL-01"}, nil},
		{"out-of-bounds-modack", "out-of-bounds-modack", []string{"PULL-01"}, nil}, {"unexpected-nack", "unexpected-nack", []string{"PULL-01"}, nil}, {"wrong-ack-id", "wrong-ack-id", []string{"PULL-01", "PULL-04"}, nil},
		{"bad-ack-sub", "bad-ack-sub", []string{"PULL-01", "PULL-04"}, nil}, {"bad-ack-routing", "bad-ack-routing", []string{"PULL-01", "PULL-04"}, nil}, {"wrong-pull-sub", "wrong-pull-sub", []string{"PULL-04"}, nil},
		{"wrong-pull-max", "wrong-pull-max", []string{"PULL-04"}, nil}, {"missing-pull-routing", "missing-pull-routing", []string{"PULL-04"}, nil}, {"wrong-trace-id", "wrong-trace-id", []string{"TRAC-01", "TRAC-03"}, nil},
		{"bad-callback-traceparent", "bad-callback-traceparent", []string{"TRAC-03"}, nil}, {"caller-attrs-mutated", "caller-attrs-mutated", []string{"PUB-01", "TRAC-01", "TRAC-03"}, nil}, {"unexpected-span-when-disabled", "emit-span-when-disabled", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04"}, nil},
		{"pub01-preset-msgid", "pub01-preset-msgid", []string{"PUB-01"}, nil}, {"pub01-unexpected-traceparent", "pub01-unexpected-traceparent", []string{"PUB-01"}, nil}, {"pord02-mix-unordered", "pord02-mix-unordered", []string{"PORD-02"}, nil},
		{"pord02-dup-msgid", "pord02-dup-msgid", []string{"PORD-02"}, nil}, {"pord02-corrupt-unord", "pord02-corrupt-unord", []string{"PORD-02"}, nil}, {"wrong-client-id-on-stream", "wrong-client-id-on-stream", []string{"PULL-01"}, nil},
		{"empty-resp-crash", "empty-resp-crash", []string{"PULL-01"}, nil}, {"pull04-p2-drop-second", "pull04-p2-drop-second", []string{"PULL-04"}, nil}, {"pull04-p2-skip-ack", "pull04-p2-skip-ack", []string{"PULL-04"}, nil},
		{"pull04-p3-phantom", "pull04-p3-phantom", []string{"PULL-04"}, nil}, {"pull04-p4-no-pull", "pull04-p4-no-pull", []string{"PULL-04"}, nil}, {"pull04-p4-wrong-err", "pull04-p4-wrong-err", []string{"PULL-04"}, nil},
		{"pull04-p4-phantom", "pull04-p4-phantom", []string{"PULL-04"}, nil}, {"trac03-fallback-parent", "trac03-fallback-parent", []string{"TRAC-03"}, nil}, {"trac03-fallback-reuse-tid", "trac03-fallback-reuse-tid", []string{"TRAC-03"}, nil},
		{"trac03-fallback-premature-ack", "trac03-fallback-premature-ack", []string{"TRAC-03"}, nil},
		{"corrupt-ordering-key", "corrupt-ordering-key", []string{"PULL-01", "PULL-04", "TRAC-03"}, nil}, {"wrong-total-delivered", "wrong-total-delivered", []string{"PULL-01", "TRAC-03"}, nil}, {"stopped-with-err", "stopped-with-err", []string{"PULL-01", "TRAC-03"}, nil},
		{"stop-with-err", "stop-with-err", []string{"PULL-01", "TRAC-03"}, nil}, {"stopped-unexpected", "stopped-unexpected", []string{"PULL-01", "TRAC-03"}, nil}, {"unexpected-stop-ev", "unexpected-stop-ev", []string{"PULL-01", "TRAC-03"}, nil},
		{"bad-start-event", "bad-start-event", []string{"PULL-01", "TRAC-03"}, nil}, {"wrong-wire-span-id", "wrong-wire-span-id", []string{"TRAC-01", "TRAC-03"}, nil}, {"wrong-body-size", "wrong-body-size", []string{"TRAC-01", "TRAC-03"}, nil},
		{"wrong-span-topic", "wrong-span-topic", []string{"TRAC-01", "TRAC-03"}, nil}, {"wrong-span-sub", "wrong-span-sub", []string{"TRAC-03"}, nil}, {"extra-pub-batch-span", "extra-pub-batch-span", []string{"TRAC-01", "TRAC-03"}, nil},
		{"close-pub-err", "close-pub-err", []string{"PUB-01", "PORD-02"}, nil}, {"empty-deliv-token", "empty-deliv-token", []string{"PULL-01", "TRAC-03"}, nil}, {"empty-pub-id", "empty-pub-id", []string{"PUB-01", "PORD-02"}, nil},
		{"unexpected-deliv-ev", "unexpected-deliv-ev", []string{"PULL-01", "TRAC-03"}, nil}, {"empty-pub-token", "empty-pub-token", []string{"PORD-02"}, nil}, {"missing-sub-ack-span", "missing-sub-ack-span", []string{"TRAC-03"}, nil},
		{"pull01-inconsistent-clientids", "pull01-inconsistent-clientids", []string{"PULL-01"}, nil}, {"pull04-p2-wrong-max", "pull04-p2-wrong-max", []string{"PULL-04"}, nil}, {"pull04-p3-skip-pull", "pull04-p3-skip-pull", []string{"PULL-04"}, nil},
		{"pull04-p4-wrong-sub", "pull04-p4-wrong-sub", []string{"PULL-04"}, nil}, {"pull04-p4-err-with-msgs", "pull04-p4-err-with-msgs", []string{"PULL-04"}, nil}, {"trac03-fallback-premature-ack-only", "trac03-fallback-premature-ack-only", []string{"TRAC-03"}, nil},
		{"trac03-fallback-auto-settled", "trac03-fallback-auto-settled", []string{"TRAC-03"}, nil}, {"trac03-p1-extra-pub", "trac03-p1-extra-pub", []string{"TRAC-03"}, nil},
		{"trac03-fb-bad-ack-sub", "trac03-fb-bad-ack-sub", []string{"TRAC-03"}, nil}, {"trac03-fb-corrupt-deliv", "trac03-fb-corrupt-deliv", []string{"TRAC-03"}, nil}, {"trac03-fb-unsampled-cb", "trac03-fb-unsampled-cb", []string{"TRAC-03"}, nil},
		{"pull04-p2-bad-ack-sub", "pull04-p2-bad-ack-sub", []string{"PULL-04"}, nil}, {"pull04-p2-bad-ack-routing", "pull04-p2-bad-ack-routing", []string{"PULL-04"}, nil}, {"pull04-p2-wrong-sub", "pull04-p2-wrong-sub", []string{"PULL-04"}, nil},
		{"pull04-p2-miss-routing", "pull04-p2-miss-routing", []string{"PULL-04"}, nil}, {"pull04-p2-extra-pull", "pull04-p2-extra-pull", []string{"PULL-04"}, nil}, {"pull04-p2-extra-modack", "pull04-p2-extra-modack", []string{"PULL-04"}, nil},
		{"pull04-p3-wrong-max", "pull04-p3-wrong-max", []string{"PULL-04"}, nil}, {"pull04-p3-wrong-sub", "pull04-p3-wrong-sub", []string{"PULL-04"}, nil}, {"pull04-p3-miss-routing", "pull04-p3-miss-routing", []string{"PULL-04"}, nil},
		{"pull04-p4-miss-routing", "pull04-p4-miss-routing", []string{"PULL-04"}, nil}, {"pull04-p4-grpc-internal", "pull04-p4-grpc-internal", []string{"PULL-04"}, nil}, {"pull04-p4-extra-ack", "pull04-p4-extra-ack", []string{"PULL-04"}, nil},
		{"pull04-settle-code-only", "pull04-settle-code-only", []string{"PULL-04"}, nil}, {"pull04-settle-status-only", "pull04-settle-status-only", []string{"PULL-04"}, nil}, {"unescaped-modack-routing", "unescaped-modack-routing", []string{"PULL-01"}, nil},
		{"pull01-empty-then-cid", "pull01-empty-then-cid", []string{"PULL-01"}, nil}, {"unescaped-stream-routing", "unescaped-stream-routing", []string{"PULL-01", "TRAC-03"}, nil}, {"pull04-p1-wrong-sub", "pull04-p1-wrong-sub", []string{"PULL-04"}, nil},
		{"pull04-p1-miss-routing", "pull04-p1-miss-routing", []string{"PULL-04"}, nil}, {"pull04-p1-bad-ack-sub", "pull04-p1-bad-ack-sub", []string{"PULL-04"}, nil}, {"pull04-p1-bad-ack-routing", "pull04-p1-bad-ack-routing", []string{"PULL-04"}, nil},
		{"pull04-p1-extra-ack-id", "pull04-p1-extra-ack-id", []string{"PULL-04"}, nil}, {"pull04-p1-extra-modack", "pull04-p1-extra-modack", []string{"PULL-04"}, nil},
		{"settle-code-only", "settle-code-only", []string{"PULL-01", "TRAC-03"}, nil}, {"settle-status-only", "settle-status-only", []string{"PULL-01", "TRAC-03"}, nil},
		{"wrong-pub-span-msg-id", "wrong-pub-span-msg-id", []string{"TRAC-01", "TRAC-03"}, nil}, {"wrong-sub-span-msg-id", "wrong-sub-span-msg-id", []string{"TRAC-03"}, nil},
		{"wrong-sub-span-ack-id", "wrong-sub-span-ack-id", []string{"TRAC-03"}, nil}, {"wrong-cb-span-id", "wrong-cb-span-id", []string{"TRAC-03"}, nil},
		{"empty-cb-tp", "empty-cb-tp", []string{"TRAC-03"}, nil}, {"subseq-frame-sub", "subseq-frame-sub", []string{"PULL-01"}, nil},
		{"premature-inline-ack", "premature-inline-ack", []string{"PULL-01", "TRAC-03"}, nil}, {"trac03-fb-premature-inline-ack", "trac03-fb-premature-inline-ack", []string{"TRAC-03"}, nil},
		{"trac03-drain-unexpected-rpc", "trac03-drain-unexpected-rpc", []string{"TRAC-03"}, nil}, {"trac03-drain-extra-pub", "trac03-drain-extra-pub", []string{"TRAC-03"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids := append(slices.Clone(tc.wantFail), tc.wantPass...)
			byID := statusMap(runMockSuite(t, mockConfig{profile: "go", fault: tc.fault}, "^("+strings.Join(ids, "|")+")$", 750*time.Millisecond))
			for _, id := range tc.wantFail {
				wantSt := map[bool]harness.Status{false: harness.StatusFail, true: harness.StatusTimeout}[tc.fault == "wrong-sub-pull-method" || (tc.fault == "missing-rpc" && (id == "PORD-02" || id == "PULL-01"))]
				if sr := byID[id]; sr.Status != wantSt || ((tc.fault == "stopped-with-err" || tc.fault == "stop-with-err") && !strings.Contains(sr.Error, "boom")) || (tc.fault == "empty-pub-id" && !strings.Contains(sr.Error, "empty publisher_id")) || (tc.fault == "unexpected-deliv-ev" && !strings.Contains(sr.Error, "expected MessageDeliveredEvent")) || (tc.fault == "empty-pub-token" && !strings.Contains(sr.Error, "empty publish_token")) {
					t.Errorf("expected %s %s under %s, got %s (%s)", id, wantSt, tc.name, sr.Status, sr.Error)
				}
			}
			for _, id := range tc.wantPass {
				if sr := byID[id]; sr.Status != harness.StatusPass {
					t.Errorf("expected %s PASS under %s, got %s (%s)", id, tc.name, sr.Status, sr.Error)
				}
			}
		})
	}
}

func TestSinglePointInit_AST(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var initFiles []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		isTest := strings.HasSuffix(name, "_test.go")
		ast.Inspect(file, func(n ast.Node) bool {
			if fd, ok := n.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == "init" {
				initFiles = append(initFiles, name)
			}
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok {
					if isTest && (sel.Sel.Name == "Skip" || sel.Sel.Name == "SkipNow" || sel.Sel.Name == "Skipf") {
						t.Errorf("%s calls %s.%s", name, id.Name, sel.Sel.Name)
					}
					if id.Name == "time" && sel.Sel.Name == "Sleep" {
						t.Errorf("%s calls time.Sleep", name)
					}
				}
			}
			return true
		})
	}
	if !slices.Equal(initFiles, []string{"registry.go"}) {
		t.Fatalf("expected single init() in registry.go only, got %v", initFiles)
	}
}

type m3fEOFProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
	mode                    string
	supportsShutdownTimeout bool
}

func (m *m3fEOFProxyServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return &proxypb.GetCapabilitiesResponse{ClientLanguage: "php", ClientLibrary: "m3f-eof", ClientVersion: "1.0.0", SupportsShutdownTimeout: m.supportsShutdownTimeout}, nil
}
func (m *m3fEOFProxyServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *m3fEOFProxyServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	switch m.mode {
	case "unimpl-no-recv":
		return status.Error(codes.Unimplemented, "streaming unimplemented")
	case "stopped-unimpl-no-recv":
		_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "inband unimplemented"}}})
		return nil
	case "start-eof-drain-err":
		_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: "drain"}}})
		return status.Error(codes.Internal, "drain boom")
	case "start-unexpected-event":
		_, _ = stream.Recv()
		_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: "bad-start"}}})
		for c, rerr := stream.Recv(); rerr == nil; c, rerr = stream.Recv() {
			if c.GetStop() != nil {
				return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{}}})
			}
		}
		return nil
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: "eof-cid"}}}); err != nil {
		return err
	}
	conn, err := dialFake(start.GetEndpoint())
	if err != nil {
		return err
	}
	defer conn.Close()
	sc := pubsubpb.NewSubscriberClient(conn)
	openPull := func() (pubsubpb.Subscriber_StreamingPullClient, error) {
		sp, err := sc.StreamingPull(metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+url.QueryEscape(start.GetSubscription())))
		if err != nil {
			return nil, err
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: start.GetSubscription(), StreamAckDeadlineSeconds: 60, ClientId: "eof-cid"}); err != nil {
			return nil, err
		}
		return sp, nil
	}
	sp, err := openPull()
	if err != nil {
		return err
	}
	ackCount := 0
	for {
		r, err := sp.Recv()
		if err != nil {
			if (strings.HasPrefix(m.mode, "stop-") || m.mode == "stop2-close-without-event") && m.mode != "stop-close-without-event" {
				_ = sp.CloseSend()
				reopened := false
				for i := 0; i < 20 && stream.Context().Err() == nil; i++ {
					if conn2, cerr := dialFake(start.GetEndpoint()); cerr == nil {
						sc = pubsubpb.NewSubscriberClient(conn2)
						defer conn2.Close()
						if sp2, err2 := openPull(); err2 == nil {
							sp, reopened = sp2, true
							break
						}
					}
					tm := time.NewTimer(10 * time.Millisecond)
					select {
					case <-tm.C:
					case <-stream.Context().Done():
						tm.Stop()
						return nil
					}
				}
				if reopened {
					continue
				}
			}
			return nil
		}
		for _, rm := range r.GetReceivedMessages() {
			switch m.mode {
			case "recv-empty-token":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{MessageId: rm.GetMessage().GetMessageId()}}})
				return nil
			case "recv-unexpected-event":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: "unexpected"}}})
				return nil
			}
			if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: "t1", MessageId: rm.GetMessage().GetMessageId(), Data: rm.GetMessage().GetData(), Attributes: rm.GetMessage().GetAttributes()}}}); err != nil {
				return err
			}
			switch m.mode {
			case "settle-stopped-internal-eof":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: int32(codes.Internal), ErrorMessage: "settle boom"}}})
				return nil
			case "settle-stopped0-unimpl-eof":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
				return status.Error(codes.Unimplemented, "settle unimplemented")
			case "settle-stopped0-aborted-eof":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
				return status.Error(codes.Aborted, "settle aborted")
			case "settle-eof-no-stopped":
				return nil
			}
			handledAck1 := false
			for c, rerr := stream.Recv(); rerr == nil; c, rerr = stream.Recv() {
				if c.GetSettle() != nil {
					_, _ = sc.Acknowledge(metadata.AppendToOutgoingContext(stream.Context(), "x-goog-request-params", "subscription="+url.QueryEscape(start.GetSubscription())), &pubsubpb.AcknowledgeRequest{Subscription: start.GetSubscription(), AckIds: []string{rm.GetAckId()}})
					if m.mode == "early-stop-eof" {
						_ = sp.CloseSend()
						_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
						return nil
					}
					ackCount++
					if ackCount == 1 && (strings.HasPrefix(m.mode, "stop-") || m.mode == "stop2-close-without-event") && m.mode != "stop-close-without-event" {
						handledAck1 = true
						break
					}
				} else if c.GetStop() != nil {
					_ = sp.CloseSend()
					switch m.mode {
					case "stop-close-without-event", "stop2-close-without-event":
						return nil
					case "stop-abort-err":
						return status.Error(codes.Aborted, "stop aborted")
					case "stop-bad-settle-result":
						_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: "t1", Status: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, GrpcErrorCode: int32(codes.PermissionDenied)}}})
						return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
					case "stop-unexpected-event":
						return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: "unexpected"}}})
					case "stop-inband-internal":
						return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: int32(codes.Internal), ErrorMessage: "stop boom"}}})
					case "stop-bad-total-delivered":
						return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 99}}})
					}
					return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
				}
			}
			if handledAck1 {
				continue
			}
			_ = sp.CloseSend()
			return nil
		}
	}
}

func m3fConnectEOFProxyWithCaps(mode string, supportsShutdownTimeout bool) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srv, caps := grpc.NewServer(), &proxypb.GetCapabilitiesResponse{ClientLanguage: "php", ClientLibrary: "m3f-eof", ClientVersion: "1.0.0", SupportsShutdownTimeout: supportsShutdownTimeout}
		proxypb.RegisterPubSubProxyServer(srv, &m3fEOFProxyServer{mode: mode, supportsShutdownTimeout: supportsShutdownTimeout})
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve(lis) }()
		conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			srv.Stop()
			return nil, err
		}
		return &inProcHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), caps: caps, exited: exited}, nil
	}
}

func m3fConnectEOFProxy(mode string) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return m3fConnectEOFProxyWithCaps(mode, false)
}

func TestHelpers_StartSubscriberEOFDrain_50x(t *testing.T) {
	for _, tc := range []struct {
		mode       string
		iters      int
		wantStatus harness.Status
		wantSub    string
	}{
		{"unimpl-no-recv", 50, harness.StatusUnsupportedByClient, ""},
		{"stopped-unimpl-no-recv", 50, harness.StatusUnsupportedByClient, ""},
		{"settle-stopped-internal-eof", 5, harness.StatusFail, "settle boom"},
		{"settle-stopped0-unimpl-eof", 10, harness.StatusUnsupportedByClient, ""},
		{"early-stop-eof", 50, harness.StatusPass, ""},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			for i := range tc.iters {
				target := harness.TargetSpec{Name: tc.mode, Connect: m3fConnectEOFProxy(tc.mode)}
				runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^PULL-01$", ScenarioTimeout: 2 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				rep, err := runner.RunTarget(context.Background(), target)
				if err != nil || len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != tc.wantStatus || (tc.wantSub != "" && !strings.Contains(rep.Scenarios[0].Error, tc.wantSub)) || (tc.wantStatus == harness.StatusUnsupportedByClient && strings.Contains(rep.Scenarios[0].Reason, "stopped unexpectedly")) {
					t.Fatalf("iter=%d mode=%s err=%v rep=%+v", i, tc.mode, err, rep)
				}
			}
		})
	}
}

type m3fStreamSpec struct {
	mode          string
	startSendErr  error
	settleSendErr error
	stopSendErr   error
	onStart       func(*proxypb.StartSubscriberCommand)
	onStop        func(*proxypb.StopSubscriberCommand)
}
type m3fHandleWrap struct {
	harness.ProxyHandle
	cli proxypb.PubSubProxyClient
}

func (w m3fHandleWrap) Client() proxypb.PubSubProxyClient { return w.cli }

type m3fSubClientWrap struct {
	proxypb.PubSubProxyClient
	spec *m3fStreamSpec
}

func (c *m3fSubClientWrap) SubscriberSession(ctx context.Context, opts ...grpc.CallOption) (proxypb.PubSubProxy_SubscriberSessionClient, error) {
	s, err := c.PubSubProxyClient.SubscriberSession(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &m3fSubStreamWrap{PubSubProxy_SubscriberSessionClient: s, spec: c.spec}, nil
}

type m3fSubStreamWrap struct {
	proxypb.PubSubProxy_SubscriberSessionClient
	spec *m3fStreamSpec
}

func (s *m3fSubStreamWrap) Send(cmd *proxypb.SubscriberCommand) error {
	switch {
	case cmd.GetStart() != nil:
		if s.spec.onStart != nil {
			s.spec.onStart(cmd.GetStart())
		}
		if s.spec.startSendErr != nil {
			return s.spec.startSendErr
		}
	case cmd.GetSettle() != nil && s.spec.settleSendErr != nil:
		return s.spec.settleSendErr
	case cmd.GetStop() != nil:
		if s.spec.onStop != nil {
			s.spec.onStop(cmd.GetStop())
		}
		if s.spec.stopSendErr != nil {
			if !errors.Is(s.spec.stopSendErr, io.EOF) {
				_ = s.PubSubProxy_SubscriberSessionClient.Send(cmd)
			}
			return s.spec.stopSendErr
		}
	}
	return s.PubSubProxy_SubscriberSessionClient.Send(cmd)
}

func m3fWrapConnect(base func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error), spec *m3fStreamSpec) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
		h, err := base(ctx, ts)
		if err != nil {
			return nil, err
		}
		return m3fHandleWrap{ProxyHandle: h, cli: &m3fSubClientWrap{PubSubProxyClient: h.Client(), spec: spec}}, nil
	}
}

func TestHelpers_DeterministicSendPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		spec       m3fStreamSpec
		wantStatus harness.Status
		wantSub    string
	}{
		{"det-start-send-eof-unimpl", m3fStreamSpec{mode: "unimpl-no-recv", startSendErr: io.EOF}, harness.StatusUnsupportedByClient, ""},
		{"det-start-send-non-eof", m3fStreamSpec{mode: "unimpl-no-recv", startSendErr: status.Error(codes.Unavailable, "start boom")}, harness.StatusFail, "start boom"},
		{"det-settle-stopped0-unimpl-eof", m3fStreamSpec{mode: "settle-stopped0-unimpl-eof", settleSendErr: io.EOF}, harness.StatusUnsupportedByClient, ""},
		{"det-settle-stopped-boom-aborted-eof", m3fStreamSpec{mode: "settle-stopped0-aborted-eof", settleSendErr: io.EOF}, harness.StatusFail, "stopped unexpectedly"},
		{"det-settle-eof-no-stopped", m3fStreamSpec{mode: "settle-eof-no-stopped", settleSendErr: io.EOF}, harness.StatusFail, "SettleMessageCommand(ACK)"},
		{"det-early-stop-eof", m3fStreamSpec{mode: "early-stop-eof", stopSendErr: io.EOF}, harness.StatusPass, ""},
		{"det-stop-send-err", m3fStreamSpec{mode: "normal", stopSendErr: status.Error(codes.Unavailable, "stop send boom")}, harness.StatusFail, "stop send boom"},
		{"det-stop-eof-before-stopped", m3fStreamSpec{mode: "stop-close-without-event"}, harness.StatusFail, "closed before StoppedEvent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.spec
			target := harness.TargetSpec{Name: tc.name, Connect: m3fWrapConnect(m3fConnectEOFProxy(spec.mode), &spec)}
			runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^PULL-01$", ScenarioTimeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			rep, err := runner.RunTarget(context.Background(), target)
			if err != nil || len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != tc.wantStatus || (tc.wantSub != "" && !strings.Contains(rep.Scenarios[0].Error, tc.wantSub)) {
				t.Fatalf("%s: err=%v rep=%+v", tc.name, err, rep)
			}
		})
	}
}

func TestTRAC03_NumStreamsAndFallback(t *testing.T) {
	t.Run("TRAC-03 pins NumStreams=1 while PULL-01 keeps nil", func(t *testing.T) {
		var mu sync.Mutex
		var got []*int32
		spec := m3fStreamSpec{onStart: func(cmd *proxypb.StartSubscriberCommand) {
			mu.Lock()
			got = append(got, cmd.NumStreams)
			mu.Unlock()
		}}
		connect := m3fWrapConnect(connectMockProxy(mockConfig{profile: "go"}), &spec)
		run := func(filter string) *harness.TargetReport {
			mu.Lock()
			got = nil
			mu.Unlock()
			target := harness.TargetSpec{Name: "numstreams", Connect: connect}
			runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: filter, ScenarioTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			rep, err := runner.RunTarget(context.Background(), target)
			if err != nil || rep.Summary.Pass != 1 {
				t.Fatalf("%s failed: err=%v rep=%+v", filter, err, rep)
			}
			return rep
		}
		run("^PULL-01$")
		if len(got) != 1 || got[0] != nil {
			t.Fatalf("PULL-01 expected [nil] NumStreams, got %v", got)
		}
		run("^TRAC-03$")
		if len(got) != 3 {
			t.Fatalf("TRAC-03 expected 3 StartSubscriberCommands, got %d (%v)", len(got), got)
		}
		for i, ns := range got {
			if ns == nil || *ns != 1 {
				t.Fatalf("TRAC-03 stream[%d] NumStreams = %v, want 1", i, ns)
			}
		}
	})
	t.Run("TRAC-03 handles 2 streams in Phase A via dynamic readyIdx", func(t *testing.T) {
		if rep := runMockSuite(t, mockConfig{profile: "go", fault: "trac03-p1-two-streams"}, "^TRAC-03$", 5*time.Second); rep.Summary.Pass != 1 {
			t.Fatalf("trac03-p1-two-streams expected PASS, got %+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
	})
}

func TestTRAC03_PreFrame0RetryClientID(t *testing.T) {
	base, ok := scenarios.ByID("TRAC-03")
	if !ok {
		t.Fatal("TRAC-03 missing")
	}
	for _, tc := range []struct {
		name, fault, wantErr string
		injectErr            bool
		wantStatus           harness.Status
	}{
		{"failrecv-match", "trac03-preframe0-failrecv-match", "", true, harness.StatusPass},
		{"failrecv-mismatch", "trac03-preframe0-failrecv-mismatch", "client_id mismatch", true, harness.StatusFail},
		{"eof-match", "trac03-preframe0-eof-match", "", false, harness.StatusPass},
		{"eof-mismatch", "trac03-preframe0-eof-mismatch", "client_id mismatch", false, harness.StatusFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, origRun := base, base.Run
			var snap fake.Snapshot
			sc.Run = func(ctx context.Context, env *harness.ScenarioEnv) error {
				if tc.injectErr {
					env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Times: 1, FailBeforeRecvErr: status.Error(codes.Unavailable, "pre-recv")})
				}
				err := origRun(ctx, env)
				snap = env.Fake.Snapshot()
				return err
			}
			target := harness.TargetSpec{Name: tc.name, Connect: connectMockProxy(mockConfig{profile: "go", fault: tc.fault})}
			runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{sc}, ScenarioTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if rep, rErr := runner.RunTarget(context.Background(), target); rErr != nil || len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != tc.wantStatus || (tc.wantErr != "" && !strings.Contains(rep.Scenarios[0].Error, tc.wantErr)) {
				t.Fatalf("%s: err=%v rep=%+v", tc.name, rErr, rep)
			}
			if len(snap.Streams) < 2 || snap.Streams[0].HasInitial || snap.Streams[0].ClientID != "" || !snap.Streams[1].HasInitial {
				t.Fatalf("%s: bad pre-frame0 stream snapshot: %+v", tc.name, snap.Streams)
			}
		})
	}
}

func TestSmoke_NoGoroutineOrFDLeak(t *testing.T) {
	settled := func(t *testing.T, maxG, maxFD int) (int, int) {
		t.Helper()
		dl, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(15*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for ; ; <-tk.C {
			runtime.GC()
			fd := -1
			if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
				fd = len(entries)
			}
			s, g := string(buf[:runtime.Stack(buf, true)]), runtime.NumGoroutine()
			if !strings.Contains(s, "mockProxyServer") && !strings.Contains(s, "runInteractiveSingleSub") && !strings.Contains(s, "startSubPump") && (maxG < 0 || g <= maxG) && (maxFD < 0 || fd <= maxFD) {
				return g, fd
			}
			if time.Now().After(dl) {
				t.Fatalf("REMSPLIT650 leak: g=%d(max=%d) fd=%d(max=%d)\n%s", g, maxG, fd, maxFD, s)
			}
		}
	}
	for _, tc := range []struct{ name, fault, filter string }{
		{"hang-publish-PUB01", "hang-publish", "^PUB-01$"},
		{"hang-subscribe-PULL01", "hang-subscribe", "^PULL-01$"},
		{"hang-pull-PULL04", "hang-pull", "^PULL-04$"},
		{"hang-fallback-TRAC03", "hang-trac03-fallback", "^TRAC-03$"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coldG, coldFD := 0, 0
			for i := range 4 {
				rep := runMockSuite(t, mockConfig{profile: "go", fault: tc.fault}, tc.filter, 650*time.Millisecond)
				if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusTimeout {
					t.Fatalf("iter %d: want TIMEOUT, got %+v", i, rep.Scenarios)
				}
				if i == 0 {
					coldG, coldFD = settled(t, -1, -1)
				} else if i == 1 || i == 3 {
					settled(t, coldG, coldFD)
				}
			}
		})
	}
}

func TestSubPump_ErrorAndFallbackBranches(t *testing.T) {
	cases := []struct {
		name, mode, wantSub string
		startSendErr        error
		supportsTimeout     bool
		checkStopTOZero     bool
	}{
		{"start-eof-drain-err", "start-eof-drain-err", "drain boom", io.EOF, false, false},
		{"start-unexpected-event-close-fallback", "start-unexpected-event", "expected StartedEvent", nil, true, true},
		{"recv-empty-token", "recv-empty-token", "empty delivery_token", nil, false, false},
		{"recv-unexpected-event", "recv-unexpected-event", "unexpected SubscriberEvent", nil, false, false},
		{"stop2-close-without-event", "stop2-close-without-event", "recv StoppedEvent: EOF", nil, false, false},
		{"stop-abort-err", "stop-abort-err", "stop aborted", nil, false, false},
		{"stop-bad-settle-result", "stop-bad-settle-result", "unexpected settlement failure", nil, false, false},
		{"stop-unexpected-event", "stop-unexpected-event", "waiting StoppedEvent", nil, false, false},
		{"stop-inband-internal", "stop-inband-internal", "stop boom", nil, false, false},
		{"stop-bad-total-delivered", "stop-bad-total-delivered", "total_delivered mismatch", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var stopCmds []*proxypb.StopSubscriberCommand
			spec := m3fStreamSpec{
				mode: tc.mode, startSendErr: tc.startSendErr,
				onStop: func(c *proxypb.StopSubscriberCommand) {
					mu.Lock()
					stopCmds = append(stopCmds, c)
					mu.Unlock()
				},
			}
			target := harness.TargetSpec{Name: tc.name, Connect: m3fWrapConnect(m3fConnectEOFProxyWithCaps(tc.mode, tc.supportsTimeout), &spec)}
			runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^PULL-02b$", ScenarioTimeout: 3 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			rep, rErr := runner.RunTarget(context.Background(), target)
			if rErr != nil || len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("%s: rErr=%v rep=%+v", tc.name, rErr, rep)
			}
			if tc.checkStopTOZero {
				mu.Lock()
				hasZero := len(stopCmds) == 1 && stopCmds[0].ShutdownTimeoutMs != nil && *stopCmds[0].ShutdownTimeoutMs == 0
				mu.Unlock()
				if !hasZero {
					t.Fatalf("%s expected close fallback ShutdownTimeoutMs=0, got %+v", tc.name, stopCmds)
				}
			}
		})
	}
}
