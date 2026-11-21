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
	"encoding/base64"
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const m3bMockMaxBytes = 32 << 20

type m3bBadProtoCodec struct{}

func (m3bBadProtoCodec) Marshal(any) ([]byte, error) { return []byte{0xff, 0xff}, nil }
func (m3bBadProtoCodec) Unmarshal([]byte, any) error { return nil }
func (m3bBadProtoCodec) Name() string                { return "proto" }

var _ encoding.Codec = m3bBadProtoCodec{}

func m3bErrResp(c codes.Code, msg string) *proxypb.PublishResponse {
	return &proxypb.PublishResponse{ErrorCode: int32(c), ErrorMessage: msg}
}

type m3bMockConfig struct {
	profile, fault string
	capsOverride   func(*proxypb.GetCapabilitiesResponse)
}

type m3bMockItem struct {
	pbMsg   *pubsubpb.PubsubMessage
	fcBytes int64
	key     string
	resCh   chan *proxypb.PublishResponse
}

type m3bMockPub struct {
	proxy                                                       *m3bMockServer
	topic, lang, compCodec                                      string
	enableOrdering, compEnabled, hedgeEnabled, closed, wireBusy bool
	fcBehavior                                                  proxypb.FlowControlLimitBehavior
	fcMaxMsgs, fcMaxBytes, fcUsedMsgs, fcUsedBytes, compThresh  int64
	hedgeMaxMilli, hedgeRefill, hedgeMilliTokens                int64
	fcWaiters                                                   []int64
	hedgeDelay, totalTimeout, initRetry, maxRetry               time.Duration
	hedgeLazyErr                                                codes.Code
	retryMult                                                   float64
	retryCodes                                                  map[codes.Code]bool
	pubCtx                                                      context.Context
	pubCancel                                                   context.CancelFunc
	conn                                                        *grpc.ClientConn
	client                                                      pubsubpb.PublisherClient
	mu                                                          sync.Mutex
	fcCond                                                      *sync.Cond
	paused                                                      map[string]bool
	tokens                                                      map[string]chan *proxypb.PublishResponse
	wg                                                          sync.WaitGroup
}

type m3bMockServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg    m3bMockConfig
	caps   *proxypb.GetCapabilitiesResponse
	srvCtx context.Context
	mu     sync.Mutex
	pubs   map[string]*m3bMockPub
	spans  []*proxypb.TraceSpan
	seq    atomic.Int64
}

type m3bMockHandle struct {
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	caps   *proxypb.GetCapabilitiesResponse
	mock   *m3bMockServer
	cancel context.CancelFunc
	exited chan error
	once   sync.Once
}

func (h *m3bMockHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *m3bMockHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.caps }
func (h *m3bMockHandle) Exited() <-chan error                           { return h.exited }
func (h *m3bMockHandle) Stderr() string                                 { return "" }
func (h *m3bMockHandle) PID() int                                       { return 0 }
func (h *m3bMockHandle) Close(context.Context) error {
	h.once.Do(func() { h.cancel(); _ = h.conn.Close(); h.mock.closeAll(); h.srv.Stop() })
	return nil
}

func m3bMockDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(m3bMockMaxBytes), grpc.MaxCallSendMsgSize(m3bMockMaxBytes)))
}

func m3bMockDur(v *int64, def time.Duration) time.Duration {
	if v != nil && *v > 0 {
		return time.Duration(*v) * time.Millisecond
	}
	return def
}

func m3bMockCapabilities(cfg m3bMockConfig) *proxypb.GetCapabilitiesResponse {
	lang := strings.ToLower(cfg.profile)
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage: lang, ClientLibrary: "pubsub-" + lang, ClientVersion: "2.0.0",
		SupportsMonitoring: true, SupportsPublishFlowControl: true, SupportsPublishCompression: true,
		SupportsPublishHedging: true, SupportsOrderingOptIn: true, SupportsCustomRetryCodes: true, SupportsResumePublish: true,
		Defaults: &proxypb.ClientDefaults{PublishTotalTimeoutMs: 60_000, PublishCompressionBytesThreshold: 240, PublishCompressionAlgorithm: proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP},
	}
	specs := map[string]struct {
		lib                         string
		fc, cmp, hdg, ord, mon, res bool
		toMs                        int64
	}{
		"go":     {"cloud.google.com/go/pubsub/v2", true, true, true, true, true, true, 60_000},
		"cpp":    {"google-cloud-cpp/pubsub", true, true, false, true, true, true, 60_000},
		"rust":   {"google-cloud-pubsub", false, false, true, false, false, true, 600_000},
		"java":   {"pubsub-java", true, true, true, true, true, true, 600_000},
		"python": {"pubsub-python", true, false, false, true, true, true, 600_000},
		"ruby":   {"pubsub-ruby", true, true, false, true, true, true, 60_000},
		"php":    {"pubsub-php", false, true, false, true, true, false, 60_000},
		"dotnet": {"pubsub-dotnet", false, true, false, true, true, false, 60_000},
		"node":   {"pubsub-node", false, false, true, true, true, false, 600_000},
	}
	if s, ok := specs[lang]; ok {
		caps.ClientLibrary, caps.SupportsMonitoring, caps.Defaults.PublishTotalTimeoutMs = s.lib, s.mon, s.toMs
		caps.SupportsPublishFlowControl, caps.SupportsPublishCompression, caps.SupportsPublishHedging = s.fc, s.cmp, s.hdg
		caps.SupportsOrderingOptIn, caps.SupportsResumePublish = s.ord, s.res
		if lang == "cpp" {
			caps.Defaults.PublishCompressionBytesThreshold, caps.Defaults.PublishCompressionAlgorithm = 0, proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE
		}
	}
	if cfg.capsOverride != nil {
		cfg.capsOverride(caps)
	}
	return caps
}

func m3bMockConnect(cfg m3bMockConfig) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srv := grpc.NewServer(grpc.MaxRecvMsgSize(m3bMockMaxBytes), grpc.MaxSendMsgSize(m3bMockMaxBytes))
		caps := m3bMockCapabilities(cfg)
		srvCtx, srvCancel := context.WithCancel(context.Background())
		mock := &m3bMockServer{cfg: cfg, caps: caps, srvCtx: srvCtx, pubs: map[string]*m3bMockPub{}}
		proxypb.RegisterPubSubProxyServer(srv, mock)
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve(lis) }()
		conn, err := m3bMockDial(lis.Addr().String())
		if err != nil {
			srvCancel()
			srv.Stop()
			return nil, err
		}
		return &m3bMockHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), caps: caps, mock: mock, cancel: srvCancel, exited: exited}, nil
	}
}

func (p *m3bMockPub) close() {
	p.pubCancel()
	p.mu.Lock()
	p.closed = true
	p.fcCond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
	_ = p.conn.Close()
}

func (p *m3bMockPub) waitCond(ctx context.Context, cond func() bool) bool {
	stopW := context.AfterFunc(ctx, func() { p.mu.Lock(); p.fcCond.Broadcast(); p.mu.Unlock() })
	defer stopW()
	for cond() && !p.closed && ctx.Err() == nil && p.proxy.srvCtx.Err() == nil {
		p.fcCond.Wait()
	}
	return ctx.Err() == nil && !p.closed && p.proxy.srvCtx.Err() == nil
}

func m3bSleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *m3bMockServer) closeAll() {
	m.mu.Lock()
	pubs := make([]*m3bMockPub, 0, len(m.pubs))
	for _, p := range m.pubs {
		pubs = append(pubs, p)
	}
	m.pubs = map[string]*m3bMockPub{}
	m.mu.Unlock()
	for _, p := range pubs {
		p.close()
	}
}

func (m *m3bMockServer) hasFault(f string) bool { return m.cfg.fault == f }

func (m *m3bMockServer) outCtx(ctx context.Context, topic string) context.Context {
	val := url.QueryEscape(topic)
	if strings.EqualFold(m.caps.GetClientLanguage(), "rust") {
		val = topic
	}
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "topic="+val)
}

func (m *m3bMockServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return m.caps, nil
}
func (m *m3bMockServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *m3bMockServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	m.mu.Lock()
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *m3bMockServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	m.mu.Lock()
	out := slices.Clone(m.spans)
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.DrainTracesResponse{Spans: out}, nil
}

func (m *m3bMockServer) requirePub(id string, del bool) (*m3bMockPub, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pubs[id]
	if p == nil {
		return nil, status.Error(codes.NotFound, "unknown publisher_id")
	}
	if del {
		delete(m.pubs, id)
	}
	return p, nil
}

func (m *m3bMockServer) CreatePublisher(_ context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	if m.hasFault("runtime-unimpl-create-pub") || (m.hasFault("runtime-unimpl-fc") && req.GetLimitExceededBehavior() != proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_UNSPECIFIED) {
		return nil, status.Error(codes.Unimplemented, "CreatePublisher unimplemented")
	}
	lang := strings.ToLower(m.caps.GetClientLanguage())
	if lang == "java" && req.TotalTimeoutMs != nil && (*req.TotalTimeoutMs < 10_000 || m.hasFault("pret-timeout-java-clamps-or-fails")) {
		return nil, status.Error(codes.InvalidArgument, "total_timeout below MIN_TOTAL_TIMEOUT")
	}
	var hdgLazy codes.Code
	var hDelay time.Duration
	var hMaxMilli, hRefill int64
	if hdg := req.GetHedging(); hdg != nil && hdg.GetEnabled() {
		hDelay = time.Duration(hdg.GetDelayMs()) * time.Millisecond
		maxTok, ratio := hdg.GetMaxTokens(), hdg.GetRefillRatio()
		badHdg := hDelay < 100*time.Millisecond || hDelay > 10*time.Second || maxTok < 1 || maxTok > 250 || ratio < 0.001 || ratio > 0.2
		conflict := req.GetEnableMessageOrdering() && m.caps.GetSupportsOrderingOptIn() && !m.hasFault("hdg-ord-allows-go")
		lazyOrErr := func(unimplFault, msg string) error {
			if !m.hasFault(unimplFault) && (lang == "java" || lang == "node") {
				return status.Error(codes.InvalidArgument, msg)
			}
			hdgLazy = map[bool]codes.Code{true: codes.Unimplemented, false: codes.InvalidArgument}[m.hasFault(unimplFault)]
			return nil
		}
		switch {
		case conflict:
			if err := lazyOrErr("hdg-ord-inband-unimpl", "hedging and ordering conflict"); err != nil {
				return nil, err
			}
		case badHdg && lang == "rust":
			if !m.hasFault("hdg-wrong-clamp-rust") {
				hDelay = min(max(hDelay, 100*time.Millisecond), 10*time.Second)
			}
			maxTok = min(max(maxTok, 1), 250)
			if !m.hasFault("hdg-wrong-clamp-refill-rust") {
				ratio = min(max(ratio, 0.001), 0.2)
			}
		case badHdg && !m.hasFault("hdg-accepts-invalid-go"):
			if err := lazyOrErr("hdg-inband-unimpl", "invalid hedging settings"); err != nil {
				return nil, err
			}
		}
		if m.hasFault("hdg-ignores-max-tokens") {
			maxTok = max(maxTok, 10)
		}
		if m.hasFault("hdg-refill-too-fast") {
			ratio = 0.25
		}
		hMaxMilli, hRefill = maxTok*1000, int64(math.Round(ratio*1000))
	}
	conn, err := m3bMockDial(req.GetEndpoint())
	if err != nil {
		return nil, err
	}

	compCodec := map[bool]string{true: "deflate", false: "gzip"}[lang == "cpp"]
	if c := map[proxypb.CompressionAlgorithm]string{proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP: "gzip", proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE: "deflate"}[req.GetCompressionAlgorithm()]; c != "" {
		compCodec = c
	}
	if m.hasFault("cmp-wrong-codec") {
		compCodec = map[string]string{"gzip": "deflate", "deflate": "gzip"}[compCodec]
	}
	compThresh := m.caps.GetDefaults().GetPublishCompressionBytesThreshold()
	if req.CompressionBytesThreshold != nil && !m.hasFault("cmp-ignores-threshold") {
		compThresh = req.GetCompressionBytesThreshold()
	}
	compEnabled := m.caps.GetDefaults().GetPublishCompressionEnabledByDefault() || m.hasFault("cmp-default-on")
	if req.EnableCompression != nil {
		compEnabled = req.GetEnableCompression() || m.hasFault("cmp-ignores-disable")
	}

	retrySet := map[codes.Code]bool{codes.Aborted: true, codes.Internal: true, codes.ResourceExhausted: true, codes.Unavailable: true}
	if len(req.GetRetryCodes()) > 0 && !m.hasFault("pret-ignores-custom-code") {
		retrySet = map[codes.Code]bool{codes.Unavailable: m.hasFault("pret-still-retries-unavail")}
		for _, c := range req.GetRetryCodes() {
			retrySet[codes.Code(c)] = true
		}
	} else if m.hasFault("pret-retries-permanent") {
		retrySet[codes.PermissionDenied] = true
	} else if m.hasFault("pret-no-retry-transient") {
		retrySet = map[codes.Code]bool{}
	}

	totTimeout := m3bMockDur(req.TotalTimeoutMs, 60*time.Second)
	if lang == "java" && totTimeout == 10*time.Second && len(req.GetRetryCodes()) == 0 {
		totTimeout = 280 * time.Millisecond
	}
	pubCtx, pubCancel := context.WithCancel(m.srvCtx)
	p := &m3bMockPub{
		proxy: m, topic: req.GetTopic(), lang: lang, enableOrdering: req.GetEnableMessageOrdering(),
		fcBehavior: req.GetLimitExceededBehavior(), fcMaxMsgs: req.GetMaxOutstandingMessages(), fcMaxBytes: req.GetMaxOutstandingBytes(),
		compEnabled: compEnabled, compThresh: compThresh, compCodec: compCodec,
		hedgeEnabled: req.GetHedging() != nil && req.GetHedging().GetEnabled(), hedgeDelay: hDelay, hedgeMaxMilli: hMaxMilli, hedgeRefill: hRefill,
		hedgeMilliTokens: map[bool]int64{true: 1000}[m.hasFault("hdg-init-nonzero")], hedgeLazyErr: hdgLazy, totalTimeout: totTimeout,
		initRetry: m3bMockDur(req.InitialRetryDelayMs, 20*time.Millisecond), maxRetry: m3bMockDur(req.MaxRetryDelayMs, 80*time.Millisecond),
		retryMult: max(req.GetRetryDelayMultiplier(), 1.5), retryCodes: retrySet,
		pubCtx: pubCtx, pubCancel: pubCancel, conn: conn, client: pubsubpb.NewPublisherClient(conn),
		paused: map[string]bool{}, tokens: map[string]chan *proxypb.PublishResponse{},
	}
	p.fcCond = sync.NewCond(&p.mu)
	id := fmt.Sprintf("m3b-pub-%d", m.seq.Add(1))
	m.mu.Lock()
	m.pubs[id] = p
	m.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func (m *m3bMockServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	if m.hasFault("emit-unexpected-span") {
		now := time.Now()
		m.mu.Lock()
		m.spans = append(m.spans, &proxypb.TraceSpan{TraceId: "11111111111111111111111111111111", SpanId: fmt.Sprintf("%016x", m.seq.Add(1)), TraceFlags: 1, Name: "unexpected create", Kind: proxypb.SpanKind_SPAN_KIND_PRODUCER, StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Millisecond))})
		m.mu.Unlock()
	}
	sendWireCopy := func(data []byte) {
		_, _ = p.client.Publish(m.outCtx(ctx, p.topic), &pubsubpb.PublishRequest{Topic: p.topic, Messages: []*pubsubpb.PubsubMessage{{Data: data}}})
	}
	if p.hedgeLazyErr != codes.OK {
		if m.hasFault("hdg-invalid-sends-rpc") {
			sendWireCopy(req.GetData())
		}
		return m3bErrResp(p.hedgeLazyErr, "hedging error"), nil
	}
	pbMsg := &pubsubpb.PubsubMessage{Data: slices.Clone(req.GetData()), Attributes: map[string]string{}, OrderingKey: req.GetOrderingKey()}
	if m.hasFault("pfc-ord-out-of-order") && string(pbMsg.Data) == "k1-4" {
		pbMsg.Data = []byte("k1-wrong-order")
	}
	if m.hasFault("pfc-corrupt-payload") && string(pbMsg.Data) == "pfc01-followup" {
		pbMsg.Data = []byte("corrupt")
	}
	fcSize := int64(proto.Size(pbMsg))
	if p.lang == "cpp" {
		fcSize = int64(20 + len(pbMsg.Data) + len(pbMsg.OrderingKey))
	}

	p.mu.Lock()
	if key := req.GetOrderingKey(); key != "" && p.paused[key] && !m.hasFault("pfc-ord-no-pause") {
		p.mu.Unlock()
		if m.hasFault("pfc-ord-inband-unimpl") {
			return m3bErrResp(codes.Unimplemented, "unimpl"), nil
		}
		return m3bErrResp(codes.FailedPrecondition, "key paused"), nil
	}

	switch p.fcBehavior {
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE:
		if m.hasFault("pfc-ignore-errors") && p.fcUsedMsgs >= p.fcMaxMsgs {
			p.mu.Unlock()
			return m3bErrResp(codes.FailedPrecondition, "flow control reject"), nil
		}
		if m.hasFault("pfc-ignore-blocks") && p.fcUsedMsgs >= p.fcMaxMsgs && !p.waitCond(ctx, func() bool { return p.fcUsedMsgs >= p.fcMaxMsgs }) {
			p.mu.Unlock()
			return m3bErrResp(codes.DeadlineExceeded, "canceled"), nil
		}
		if m.hasFault("pfc-ignore-slow-async") && p.fcUsedMsgs == p.fcMaxMsgs && req.GetAsyncFire() {
			p.mu.Unlock()
			t := time.NewTimer(2100 * time.Millisecond)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done():
				return m3bErrResp(codes.DeadlineExceeded, "canceled"), nil
			case <-p.pubCtx.Done():
				return m3bErrResp(codes.DeadlineExceeded, "closed"), nil
			}
			p.mu.Lock()
			if p.closed || ctx.Err() != nil || m.srvCtx.Err() != nil {
				p.mu.Unlock()
				return m3bErrResp(codes.DeadlineExceeded, "canceled"), nil
			}
		}
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK:
		idleOver := fcSize > p.fcMaxBytes && (p.lang == "java" || p.lang == "python" || p.lang == "ruby")
		if (idleOver && !m.hasFault("pfc-block-allows-idle-py")) || ((p.lang == "go" || p.lang == "cpp") && fcSize > p.fcMaxBytes && m.hasFault("pfc-block-rejects-idle-go")) {
			p.mu.Unlock()
			if m.hasFault("pfc-block-idle-sends-rpc-py") {
				sendWireCopy(pbMsg.Data)
			}
			return m3bErrResp(codes.FailedPrecondition, "oversized"), nil
		}
		acqBytes := fcSize
		if p.lang == "go" || m.hasFault("pfc-block-allows-idle-py") {
			acqBytes = min(fcSize, p.fcMaxBytes)
		}
		var myWaiter int64
		ok := p.waitCond(ctx, func() bool {
			if m.hasFault("pfc-block-no-block") {
				return false
			}
			full := p.fcUsedMsgs+1 > p.fcMaxMsgs || (!m.hasFault("pfc-block-bytes-ignored") && !(p.lang == "cpp" && p.fcUsedMsgs == 0) && p.fcUsedBytes+acqBytes > p.fcMaxBytes)
			fifo := p.lang != "cpp" && p.lang != "python" && !m.hasFault("pfc-block-starves") && len(p.fcWaiters) > 0 && (myWaiter == 0 || p.fcWaiters[0] != myWaiter)
			if (full || fifo) && myWaiter == 0 {
				myWaiter = m.seq.Add(1)
				p.fcWaiters = append(p.fcWaiters, myWaiter)
			}
			return full || fifo
		})
		if myWaiter != 0 {
			p.fcWaiters = slices.DeleteFunc(p.fcWaiters, func(w int64) bool { return w == myWaiter })
			p.fcCond.Broadcast()
		}
		if !ok {
			p.mu.Unlock()
			return m3bErrResp(codes.DeadlineExceeded, "canceled"), nil
		}
		fcSize = acqBytes
	case proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR:
		over := (p.fcUsedMsgs+1 > p.fcMaxMsgs || p.fcUsedBytes+fcSize > p.fcMaxBytes) && !(p.lang == "cpp" && p.fcUsedMsgs == 0)
		if over && !m.hasFault("pfc-sig-succeeds") && !(p.fcUsedMsgs == 0 && m.hasFault("pfc-sig-allows-idle")) {
			if m.hasFault("pfc-sig-blocks") {
				p.waitCond(ctx, func() bool { return true })
				p.mu.Unlock()
				return m3bErrResp(codes.DeadlineExceeded, "blocked"), nil
			}
			if m.hasFault("pfc-sig-leaks-permit") {
				p.fcUsedMsgs++
			}
			if key := req.GetOrderingKey(); key != "" && (((p.lang == "go" || p.lang == "java" || p.lang == "ruby") && !m.hasFault("pfc-ord-no-pause")) || ((p.lang == "cpp" || p.lang == "python") && m.hasFault("pfc-ord-pauses-cpp-py"))) {
				p.paused[key] = true
				if m.hasFault("pfc-ord-leaks-k2") {
					p.paused["k2"] = true
				}
			}
			sendRPC := m.hasFault("pfc-sig-sends-rpc") || (m.hasFault("pfc-sig-idle-sends-rpc") && p.fcUsedMsgs == 0 && fcSize > p.fcMaxBytes)
			p.mu.Unlock()
			if sendRPC {
				sendWireCopy(pbMsg.Data)
			}
			if m.hasFault("pfc-sig-inband-unimpl") {
				return m3bErrResp(codes.Unimplemented, "unimpl"), nil
			}
			return m3bErrResp(codes.FailedPrecondition, "flow control limit exceeded"), nil
		}
	}
	if p.closed {
		p.mu.Unlock()
		return m3bErrResp(codes.DeadlineExceeded, "closed"), nil
	}
	if p.fcBehavior != proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_UNSPECIFIED {
		p.fcUsedMsgs++
		p.fcUsedBytes += fcSize
	}

	it := &m3bMockItem{pbMsg: pbMsg, fcBytes: fcSize, key: req.GetOrderingKey(), resCh: make(chan *proxypb.PublishResponse, 1)}
	var tok string
	if req.GetAsyncFire() {
		tok = fmt.Sprintf("m3b-tok-%d", m.seq.Add(1))
		p.tokens[tok] = it.resCh
	}
	p.wg.Add(1)
	go p.m3bExecBatch(it)
	p.mu.Unlock()
	if req.GetAsyncFire() {
		return &proxypb.PublishResponse{PublishToken: tok}, nil
	}
	select {
	case r := <-it.resCh:
		return r, nil
	case <-ctx.Done():
		return m3bErrResp(codes.DeadlineExceeded, ctx.Err().Error()), nil
	}
}

func (p *m3bMockPub) m3bExecBatch(it *m3bMockItem) {
	defer p.wg.Done()
	m := p.proxy
	batchBytes := int64(proto.Size(it.pbMsg))
	if p.lang == "cpp" {
		batchBytes = int64(20 + len(it.pbMsg.Data))
	}
	compress := p.compEnabled && (batchBytes > p.compThresh || (p.lang != "go" && batchBytes == p.compThresh))
	if m.hasFault("cmp-wrong-boundary") && p.compEnabled {
		compress = (p.lang == "go" && batchBytes >= p.compThresh) || (p.lang != "go" && batchBytes > p.compThresh)
	}
	compress = (compress && !m.hasFault("cmp-no-compress-above")) || m.hasFault("cmp-compresses-below")
	var callOpts []grpc.CallOption
	if compress {
		callOpts = append(callOpts, grpc.UseCompressor(p.compCodec))
	}

	bCtx, bCancel := p.pubCtx, context.CancelFunc(func() {})
	if p.totalTimeout > 0 && !m.hasFault("pret-ignores-timeout") {
		bCtx, bCancel = context.WithTimeout(p.pubCtx, p.totalTimeout)
	}
	defer bCancel()

	pbReq := &pubsubpb.PublishRequest{Topic: p.topic, Messages: []*pubsubpb.PubsubMessage{it.pbMsg}}
	var resp *pubsubpb.PublishResponse
	var err error
	if m.hasFault("pfc-ignore-serializes") {
		p.mu.Lock()
		ok := p.waitCond(p.pubCtx, func() bool { return p.wireBusy })
		if !ok {
			p.mu.Unlock()
			it.resCh <- m3bErrResp(codes.DeadlineExceeded, "closed")
			return
		}
		p.wireBusy = true
		p.mu.Unlock()
		defer func() { p.mu.Lock(); p.wireBusy = false; p.fcCond.Broadcast(); p.mu.Unlock() }()
	}
	if p.hedgeEnabled {
		resp, err = p.m3bRunHedge(bCtx, pbReq, it.key, callOpts)
	} else {
		resp, err = p.m3bRetryLoop(bCtx, pbReq, callOpts)
		if err != nil && m.hasFault("pret-leaks-background-retry") && strings.Contains(string(it.pbMsg.Data), "m-timeout") {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				if m3bSleep(p.pubCtx, 25*time.Millisecond) {
					_, _ = p.client.Publish(m.outCtx(p.pubCtx, p.topic), pbReq, callOpts...)
				}
			}()
		}
	}

	p.mu.Lock()
	if p.fcBehavior != proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_UNSPECIFIED && !m.hasFault("pfc-block-leaks-permit") {
		if !(m.hasFault("pfc02-partial-leak") && string(it.pbMsg.Data) == "pfc02-a-1") {
			p.fcUsedMsgs = max(p.fcUsedMsgs-1, 0)
		}
		p.fcUsedBytes = max(p.fcUsedBytes-it.fcBytes, 0)
		p.fcCond.Broadcast()
	}
	p.mu.Unlock()

	if err != nil {
		msgStr := string(it.pbMsg.Data)
		isTO, isHdgD := strings.Contains(msgStr, "m-timeout"), strings.HasPrefix(msgStr, "hdg02-d")
		if (m.hasFault("pret-timeout-returns-ok") && isTO) || (m.hasFault("pret-swallows-perm") && status.Code(err) == codes.PermissionDenied) || (m.hasFault("hdg-all-fail-returns-ok") && isHdgD) {
			it.resCh <- &proxypb.PublishResponse{MessageId: "bogus-ok"}
			return
		}
		code := status.Code(err)
		if (m.hasFault("pret-inband-unimpl") && code == codes.PermissionDenied) || (m.hasFault("pret-custom-inband-unimpl") && code == codes.Unavailable) || (m.hasFault("pret-timeout-inband-unimpl") && isTO) || (m.hasFault("hdg-all-fail-inband-unimpl") && isHdgD) {
			code = codes.Unimplemented
		}
		it.resCh <- m3bErrResp(code, err.Error())
		return
	}
	msgID := resp.GetMessageIds()[0]
	if m.hasFault("hdg-wrong-msgid") && strings.HasPrefix(string(it.pbMsg.Data), "hdg02-a") {
		msgID = "wrong-id"
	}
	it.resCh <- &proxypb.PublishResponse{MessageId: msgID}
}

func (p *m3bMockPub) telCtx(ctx context.Context, attempt int32, start *timestamppb.Timestamp) context.Context {
	out := p.proxy.outCtx(ctx, p.topic)
	if p.proxy.hasFault("hdg-missing-telemetry") {
		return out
	}
	if p.proxy.hasFault("hdg-wrong-attempt-seq") {
		attempt = 0
	}
	ts := start
	if p.proxy.hasFault("hdg-nil-start-time") {
		ts = nil
	} else if p.proxy.hasFault("hdg-epoch-zero-start-time") {
		ts = &timestamppb.Timestamp{}
	} else if p.proxy.hasFault("hdg-divergent-start-time") {
		ts = timestamppb.New(start.AsTime().Add(time.Duration(attempt+1) * time.Second))
	}
	b, _ := proto.Marshal(&pubsubpb.PubsubClientTelemetry{Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: attempt, PublishStartTime: ts}}})
	return metadata.AppendToOutgoingContext(out, fake.TelemetryHeaderName, base64.StdEncoding.EncodeToString(b))
}

func (p *m3bMockPub) m3bRunHedge(bCtx context.Context, pbReq *pubsubpb.PublishRequest, key string, callOpts []grpc.CallOption) (*pubsubpb.PublishResponse, error) {
	m := p.proxy
	startTS := timestamppb.Now()
	canHedge := (key == "" || m.hasFault("hdg-ord-hedges-keyed-rust")) && !m.hasFault("hdg-ord-no-unkeyed-rust") && p.totalTimeout > p.hedgeDelay && !m.hasFault("hdg-no-hedge")
	refill := func() {
		if !m.hasFault("hdg-never-refills") {
			p.mu.Lock()
			p.hedgeMilliTokens = min(p.hedgeMilliTokens+p.hedgeRefill, p.hedgeMaxMilli)
			p.mu.Unlock()
		}
	}
	if !canHedge {
		att := map[bool]int32{true: 1}[key != "" && m.hasFault("hdg-ord-keyed-nonzero-tel")]
		resp, err := p.m3bRetryLoop(p.telCtx(bCtx, att, startTS), pbReq, callOpts)
		if err == nil {
			refill()
		}
		return resp, err
	}

	hCtx, hCancel := context.WithCancel(bCtx)
	var hwg sync.WaitGroup
	waitHedgeWG := !m.hasFault("hdg-no-cancel-loser")
	defer func() {
		hCancel()
		if waitHedgeWG {
			hwg.Wait()
		}
	}()

	type res struct {
		att  int32
		resp *pubsubpb.PublishResponse
		err  error
	}
	resCh := make(chan res, 8)
	launchAttempt := func(att int32, attemptCtx context.Context, cleanup context.CancelFunc) {
		wg := map[bool]*sync.WaitGroup{true: &p.wg, false: &hwg}[att == 0 && !waitHedgeWG]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cleanup != nil {
				defer cleanup()
			}
			telAtt := att
			if m.hasFault("hdg01-slow6-zero-tel") && telAtt >= 1 && len(pbReq.Messages) > 0 && string(pbReq.Messages[0].Data) == "hdg01-slow6" {
				telAtt = 0
			}
			c := p.telCtx(attemptCtx, telAtt, startTS)
			var r *pubsubpb.PublishResponse
			var e error
			if att == 0 && m.hasFault("hdg-loser-not-canceled") && strings.HasPrefix(string(pbReq.Messages[0].Data), "hdg02-a") {
				_, _ = p.client.Publish(c, pbReq, append(slices.Clone(callOpts), grpc.ForceCodec(m3bBadProtoCodec{}))...)
				<-hCtx.Done()
				e = context.Canceled
			} else if att == 0 {
				r, e = p.m3bRetryLoop(c, pbReq, callOpts)
			} else {
				r, e = p.client.Publish(c, pbReq, callOpts...)
			}
			resCh <- res{att, r, e}
		}()
	}

	att0Ctx := hCtx
	var att0Cleanup context.CancelFunc
	switch {
	case m.hasFault("hdg-no-cancel-loser"):
		att0Ctx, att0Cleanup = context.WithTimeout(p.pubCtx, p.totalTimeout)
	case m.hasFault("hdg-slow-cancel-loser"):
		var slowCancel context.CancelFunc
		att0Ctx, slowCancel = context.WithCancel(bCtx)
		stopSlow := context.AfterFunc(hCtx, func() { _ = m3bSleep(bCtx, 2050*time.Millisecond); slowCancel() })
		att0Cleanup = func() { stopSlow(); slowCancel() }
	}
	launchAttempt(0, att0Ctx, att0Cleanup)

	var nextAtt int32 = 1
	timer := time.NewTimer(p.hedgeDelay)
	defer timer.Stop()

	for {
		select {
		case r := <-resCh:
			if r.err == nil {
				refill()
				if m.hasFault("hdg-no-cancel-fast-primary") && r.att == 0 {
					_, _ = p.client.Publish(p.telCtx(bCtx, 1, startTS), pbReq, callOpts...)
				}
				return r.resp, nil
			}
			if r.att == 0 || m.hasFault("hdg-fails-losing-hedge-err") {
				if m.hasFault("hdg-hang-all") {
					<-bCtx.Done()
					return nil, status.FromContextError(bCtx.Err()).Err()
				}
				return nil, r.err
			}
		case <-timer.C:
			p.mu.Lock()
			okTok := p.hedgeMilliTokens >= 1000
			if okTok {
				p.hedgeMilliTokens -= 1000
			}
			p.mu.Unlock()
			if okTok {
				launchAttempt(nextAtt, hCtx, nil)
				nextAtt++
				timer.Reset(p.hedgeDelay)
			}
		case <-bCtx.Done():
			return nil, status.FromContextError(bCtx.Err()).Err()
		}
	}
}

func (p *m3bMockPub) m3bRetryLoop(ctx context.Context, pbReq *pubsubpb.PublishRequest, callOpts []grpc.CallOption) (*pubsubpb.PublishResponse, error) {
	delay, retries := p.initRetry, 0
	for {
		req := pbReq
		if p.proxy.hasFault("pret-corrupt-retry-payload") && retries == 1 {
			req = proto.CloneOf(pbReq)
			req.Messages[0].Data = []byte("corrupt")
		}
		c := ctx
		if _, hasTel := metadata.FromOutgoingContext(ctx); !hasTel {
			c = p.proxy.outCtx(ctx, p.topic)
		}
		resp, err := p.client.Publish(c, req, callOpts...)
		if err == nil || !p.retryCodes[status.Code(err)] || ctx.Err() != nil || p.proxy.hasFault("pret-no-retry-fast-fail") {
			return resp, err
		}
		if (p.proxy.hasFault("pret-retries-permanent") && retries >= 3) || (p.proxy.hasFault("pret-still-retries-unavail") && status.Code(err) == codes.Unavailable && retries >= 1) {
			return resp, err
		}
		retries++
		if p.proxy.hasFault("pret-zero-backoff") {
			delay = 0
		}
		if delay > 0 && !m3bSleep(ctx, delay) {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		delay = min(time.Duration(float64(delay)*p.retryMult), p.maxRetry)
	}
}

func (m *m3bMockServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	ch := p.tokens[req.GetPublishToken()]
	delete(p.tokens, req.GetPublishToken())
	p.mu.Unlock()
	if ch == nil {
		return nil, status.Error(codes.NotFound, "unknown publish_token")
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (m *m3bMockServer) FlushPublisher(context.Context, *proxypb.FlushPublisherRequest) (*proxypb.FlushPublisherResponse, error) {
	return &proxypb.FlushPublisherResponse{}, nil
}
func (m *m3bMockServer) StopPublisher(context.Context, *proxypb.StopPublisherRequest) (*proxypb.StopPublisherResponse, error) {
	return &proxypb.StopPublisherResponse{}, nil
}
func (m *m3bMockServer) ResumePublish(_ context.Context, req *proxypb.ResumePublishRequest) (*proxypb.ResumePublishResponse, error) {
	if !m.caps.GetSupportsResumePublish() || m.hasFault("runtime-unimpl-resume") {
		return nil, status.Error(codes.Unimplemented, "ResumePublish unimplemented")
	}
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	if !m.hasFault("pfc-ord-resume-noop") {
		p.mu.Lock()
		delete(p.paused, req.GetOrderingKey())
		p.mu.Unlock()
	}
	return &proxypb.ResumePublishResponse{}, nil
}
func (m *m3bMockServer) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	if p, err := m.requirePub(req.GetPublisherId(), true); err == nil {
		p.close()
	}
	return &proxypb.ClosePublisherResponse{}, nil
}

func m3bRunScenarios(t *testing.T, cfg m3bMockConfig, list []harness.Scenario, filter string, timeout time.Duration) *harness.TargetReport {
	t.Helper()
	reg := scenarios.NewRegistry()
	for _, sc := range list {
		reg.MustRegister(sc)
	}
	target := harness.TargetSpec{Name: cfg.profile, Connect: m3bMockConnect(cfg)}
	runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: reg.All(), Filter: filter, ScenarioTimeout: timeout})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget(%s): %v", cfg.profile, err)
	}
	return rep
}

func m3bAssertUnitMetadata(t *testing.T, list, want []harness.Scenario) {
	t.Helper()
	if len(list) != len(want) {
		t.Fatalf("count: want %d, got %d", len(want), len(list))
	}
	badRE, reg := regexp.MustCompile(`^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03|NOMATCH)$`), scenarios.NewRegistry()
	for i, sc := range list {
		w := want[i]
		if sc.ID != w.ID || sc.Category != w.Category || sc.IsolateProxy != w.IsolateProxy || !slices.Equal(sc.RequiredCapabilities, w.RequiredCapabilities) || sc.Timeout != 0 || sc.TraceOnly || sc.Run == nil || sc.Name == "" || sc.Feature == "" || sc.Description == "" {
			t.Errorf("invalid scenario[%d] (%s): got=%+v wantMeta=%+v", i, sc.ID, sc, w)
		}
		if badRE.MatchString(sc.ID) || badRE.MatchString(sc.Name) || badRE.MatchString(sc.Category) || strings.Contains(sc.ID, "PUB-01") || strings.Contains(sc.Name, "PUB-01") {
			t.Errorf("filter safety violation in %+v", sc)
		}
		reg.MustRegister(sc)
	}
	if reg.Len() != len(want) || scenarios.Len() != 65 {
		t.Fatalf("registry isolation violation: reg=%d default=%d", reg.Len(), scenarios.Len())
	}
}

func m3bAssertNoGoroutineLeak(t *testing.T, runTO func()) {
	t.Helper()
	waitSettled := func(maxG int) {
		t.Helper()
		deadline, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(10*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for ; ; <-tk.C {
			s := string(buf[:runtime.Stack(buf, true)])
			if !strings.Contains(s, "m3bExecBatch") && !strings.Contains(s, "m3bRunHedge") && !strings.Contains(s, "m3bRetryLoop") && (maxG < 0 || runtime.NumGoroutine() <= maxG) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("leaked goroutines after timeout: max=%d now=%d\n%s", maxG, runtime.NumGoroutine(), s)
			}
		}
	}
	runTO()
	waitSettled(-1)
	base := runtime.NumGoroutine()
	for range 3 {
		runTO()
	}
	waitSettled(base + 2)
}

func TestPublishFlowControlScenarios_Unit(t *testing.T) {
	reqFC := []harness.Capability{harness.CapPublishFlowControl}
	m3bAssertUnitMetadata(t, scenarios.ExportPublishFlowControlScenarios(), []harness.Scenario{
		{ID: "PFC-01", Category: "flow_control", RequiredCapabilities: reqFC},
		{ID: "PFC-02", Category: "flow_control", RequiredCapabilities: reqFC},
		{ID: "PFC-03", Category: "flow_control", RequiredCapabilities: reqFC},
		{ID: "PFC-03b", Category: "flow_control", RequiredCapabilities: reqFC},
	})
}

func m3bRunProfileTests(t *testing.T, list []harness.Scenario, total int, to time.Duration, passCount map[string]int, unimpl []m3bFaultCase) {
	t.Helper()
	for _, prof := range []string{"go", "cpp", "rust", "java", "python", "ruby", "php", "dotnet", "node"} {
		t.Run("profile_"+prof, func(t *testing.T) {
			rep := m3bRunScenarios(t, m3bMockConfig{profile: prof}, list, "", to)
			wantP := passCount[prof]
			if rep.Summary.Pass != wantP || rep.Summary.UnsupportedByClient != total-wantP {
				t.Fatalf("profile=%s want pass=%d unsup=%d, got %+v scenarios=%+v", prof, wantP, total-wantP, rep.Summary, rep.Scenarios)
			}
		})
	}
	m3bRunFaultCases(t, list, unimpl)
}

func m3bRunNoLeakFaults(t *testing.T, list []harness.Scenario, to time.Duration, cases ...[2]string) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc[0], func(t *testing.T) {
			m3bAssertNoGoroutineLeak(t, func() {
				if sr := m3bRunScenarios(t, m3bMockConfig{profile: "go", fault: tc[0]}, list, tc[1], to).Scenarios[0]; sr.Status != harness.StatusTimeout {
					t.Fatalf("want TIMEOUT, got %s (%s)", sr.Status, sr.Error)
				}
			})
		})
	}
}

func TestPublishFlowControlScenarios_Profiles(t *testing.T) {
	m3bRunProfileTests(t, scenarios.ExportPublishFlowControlScenarios(), 4, 5*time.Second,
		map[string]int{"go": 4, "cpp": 4, "java": 4, "python": 4, "ruby": 4},
		[]m3bFaultCase{
			{prof: "go", fault: "runtime-unimpl-fc", sc: "PFC-01", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient},
			{prof: "go", fault: "runtime-unimpl-resume", sc: "PFC-03b", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient},
			{prof: "go", sc: "PFC-03b", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsResumePublish = false }},
		})
}

type m3bFaultCase struct {
	prof, fault, sc, sub string
	timeout              time.Duration
	want                 harness.Status
	capsOverride         func(*proxypb.GetCapabilitiesResponse)
}

func m3bRunFaultCases(t *testing.T, list []harness.Scenario, cases []m3bFaultCase) {
	t.Helper()
	for _, tc := range cases {
		name := tc.sc + "_" + map[bool]string{true: "override", false: tc.fault}[tc.fault == ""]
		t.Run(name, func(t *testing.T) {
			sr := m3bRunScenarios(t, m3bMockConfig{profile: tc.prof, fault: tc.fault, capsOverride: tc.capsOverride}, list, "^"+tc.sc+"$", tc.timeout).Scenarios[0]
			if sr.Status != tc.want || !strings.Contains(sr.Error, tc.sub) {
				t.Errorf("%s: want %s (%q), got %s (%q)", name, tc.want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}

func TestPublishFlowControlScenarios_FaultDetection(t *testing.T) {
	m3bRunFaultCases(t, scenarios.ExportPublishFlowControlScenarios(), []m3bFaultCase{
		{prof: "go", fault: "pfc-ignore-blocks", sc: "PFC-01", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pfc-ignore-slow-async", sc: "PFC-01", sub: "expected non-blocking", timeout: 5 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ignore-serializes", sc: "PFC-01", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pfc-ignore-errors", sc: "PFC-01", sub: "flow control reject", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-corrupt-payload", sc: "PFC-01", sub: "payloads mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-block-no-block", sc: "PFC-02", sub: "returned prematurely", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-block-leaks-permit", sc: "PFC-02", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pfc02-partial-leak", sc: "PFC-02", sub: "partial 1-of-2 permit leak", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-block-bytes-ignored", sc: "PFC-02", sub: "returned prematurely", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-block-starves", sc: "PFC-02", sub: "returned prematurely", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-block-rejects-idle-go", sc: "PFC-02", sub: "idle oversized BLOCK on go", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "python", fault: "pfc-block-allows-idle-py", sc: "PFC-02", sub: "expected idle oversized BLOCK", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "python", fault: "pfc-block-idle-sends-rpc-py", sc: "PFC-02", sub: "unexpected call after", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-blocks", sc: "PFC-03", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pfc-sig-succeeds", sc: "PFC-03", sub: "expected async publish to fail", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-sends-rpc", sc: "PFC-03", sub: "unexpected call after", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-leaks-permit", sc: "PFC-03", sub: "flow control limit exceeded", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-inband-unimpl", sc: "PFC-03", sub: "UNIMPLEMENTED", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-allows-idle", sc: "PFC-03", sub: "expected async publish to fail", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-sig-idle-sends-rpc", sc: "PFC-03", sub: "unexpected call after", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ord-no-pause", sc: "PFC-03b", sub: "expected async publish to fail", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "cpp", fault: "pfc-ord-pauses-cpp-py", sc: "PFC-03b", sub: "k1-3 post-drain", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ord-leaks-k2", sc: "PFC-03b", sub: "k2-1 unaffected", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ord-resume-noop", sc: "PFC-03b", sub: "k1-4 after ResumePublish", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ord-out-of-order", sc: "PFC-03b", sub: "ordered Publish FIFO mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pfc-ord-inband-unimpl", sc: "PFC-03b", sub: "UNIMPLEMENTED", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "emit-unexpected-span", sc: "PFC-01", sub: "expected zero spans", timeout: 2 * time.Second, want: harness.StatusFail},
	})
}

func TestPublishFlowControlScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3bRunNoLeakFaults(t, scenarios.ExportPublishFlowControlScenarios(), 250*time.Millisecond,
		[2]string{"pfc-ignore-blocks", "^PFC-01$"},
		[2]string{"pfc-ignore-serializes", "^PFC-01$"},
		[2]string{"pfc-ignore-slow-async", "^PFC-01$"},
		[2]string{"pfc-block-leaks-permit", "^PFC-02$"},
		[2]string{"pfc-sig-blocks", "^PFC-03$"},
	)
}

func TestPublishFlowControl_NodeIdleOversizedDiagnostic(t *testing.T) {
	rep := m3bRunScenarios(t, m3bMockConfig{
		profile: "node",
		capsOverride: func(c *proxypb.GetCapabilitiesResponse) {
			c.SupportsPublishFlowControl = true
		},
	}, scenarios.ExportPublishFlowControlScenarios(), "^PFC-02$", 5*time.Second)
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusPass {
		t.Fatalf("PFC-02 node expected PASS, got %+v", rep.Scenarios)
	}
	if !slices.ContainsFunc(rep.Scenarios[0].Diagnostics, func(d string) bool {
		return strings.Contains(d, "PFC-02 idle oversized policy skipped")
	}) {
		t.Fatalf("PFC-02 node expected idle oversized skip diagnostic, got %v", rep.Scenarios[0].Diagnostics)
	}
}
