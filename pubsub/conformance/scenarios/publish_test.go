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
	"maps"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const pubMockMaxGRPCBytes = 32 << 20

type pubMockConfig struct {
	profile, fault string
	capsOverride   func(*proxypb.GetCapabilitiesResponse)
}

type pubMockItem struct {
	pbMsg         *pubsubpb.PubsubMessage
	bytes         int64
	key           string
	block, fcUsed bool
	resCh         chan *proxypb.PublishResponse
	settled       chan struct{}
}

type pubMockQueue struct {
	key      string
	items    []*pubMockItem
	bytes    int64
	timer    *time.Timer
	inFlight bool
	pending  [][]*pubMockItem
}

type pubMockPub struct {
	proxy                                *pubMockServer
	topic                                string
	enableOrdering, fcBlock, preAddSplit bool
	countThresh                          int
	byteThresh, fcMaxMsgs                int64
	delayThresh, totalTimeout            time.Duration
	initRetry, maxRetry                  time.Duration
	retryMult                            float64
	conn                                 *grpc.ClientConn
	client                               pubsubpb.PublisherClient
	mu, globalMu                         sync.Mutex
	fcCond                               *sync.Cond
	fcInUse                              int64
	stopped, closed, broken              bool
	bogusIdx                             int
	paused                               map[string]bool
	queues                               map[string]*pubMockQueue
	pending                              map[*pubMockItem]struct{}
	tokens                               map[string]chan *proxypb.PublishResponse
	wg                                   sync.WaitGroup
}

type pubMockServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg    pubMockConfig
	caps   *proxypb.GetCapabilitiesResponse
	srvCtx context.Context
	mu     sync.Mutex
	pubs   map[string]*pubMockPub
	spans  []*proxypb.TraceSpan
	seq    atomic.Int64
}

type pubMockHandle struct {
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	caps   *proxypb.GetCapabilitiesResponse
	mock   *pubMockServer
	cancel context.CancelFunc
	exited chan error
	once   sync.Once
}

func (h *pubMockHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *pubMockHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.caps }
func (h *pubMockHandle) Exited() <-chan error                           { return h.exited }
func (h *pubMockHandle) Stderr() string                                 { return "" }
func (h *pubMockHandle) PID() int                                       { return 0 }
func (h *pubMockHandle) Close(context.Context) error {
	h.once.Do(func() { h.cancel(); _ = h.conn.Close(); h.mock.closeAll(); h.srv.Stop() })
	return nil
}

func pubMockDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pubMockMaxGRPCBytes), grpc.MaxCallSendMsgSize(pubMockMaxGRPCBytes)))
}

func pubMockDur(v *int64, def time.Duration, allowZero bool) time.Duration {
	if v != nil && (*v > 0 || (allowZero && *v == 0)) {
		return time.Duration(*v) * time.Millisecond
	}
	return def
}

func pubMockCapabilities(cfg pubMockConfig) *proxypb.GetCapabilitiesResponse {
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "2.0.0",
		SupportsMonitoring: true, SupportsPublishFlowControl: true, SupportsOrderingOptIn: true, SupportsResumePublish: true,
		Defaults: &proxypb.ClientDefaults{PublishTotalTimeoutMs: 60_000},
	}
	switch lang := strings.ToLower(cfg.profile); lang {
	case "cpp":
		caps.ClientLanguage, caps.ClientLibrary, caps.SupportsBlockingPublish = "cpp", "google-cloud-cpp/pubsub", true
	case "rust":
		caps.ClientLanguage, caps.ClientLibrary = "rust", "google-cloud-pubsub"
		caps.SupportsMonitoring, caps.SupportsPublishFlowControl, caps.SupportsOrderingOptIn = false, false, false
		caps.Defaults.PublishTotalTimeoutMs = 600_000
	case "java", "python", "php", "node":
		caps.ClientLanguage, caps.ClientLibrary = lang, "pubsub-"+lang
	}
	if cfg.capsOverride != nil {
		cfg.capsOverride(caps)
	}
	return caps
}

func pubMockConnect(cfg pubMockConfig) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srv := grpc.NewServer(grpc.MaxRecvMsgSize(pubMockMaxGRPCBytes), grpc.MaxSendMsgSize(pubMockMaxGRPCBytes))
		caps := pubMockCapabilities(cfg)
		srvCtx, srvCancel := context.WithCancel(context.Background())
		mock := &pubMockServer{cfg: cfg, caps: caps, srvCtx: srvCtx, pubs: map[string]*pubMockPub{}}
		proxypb.RegisterPubSubProxyServer(srv, mock)
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve(lis) }()
		conn, err := pubMockDial(lis.Addr().String())
		if err != nil {
			srvCancel()
			srv.Stop()
			return nil, err
		}
		return &pubMockHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), caps: caps, mock: mock, cancel: srvCancel, exited: exited}, nil
	}
}

func (p *pubMockPub) shutdown(flush bool) {
	p.mu.Lock()
	p.stopped, p.closed = true, true
	p.fcCond.Broadcast()
	for _, q := range p.queues {
		if flush {
			p.flushQueueLocked(q)
		} else if q.timer != nil {
			q.timer.Stop()
			q.timer = nil
		}
	}
	p.mu.Unlock()
	p.wg.Wait()
	_ = p.conn.Close()
}

func (m *pubMockServer) closeAll() {
	m.mu.Lock()
	pubs := make([]*pubMockPub, 0, len(m.pubs))
	for _, p := range m.pubs {
		pubs = append(pubs, p)
	}
	m.pubs = map[string]*pubMockPub{}
	m.mu.Unlock()
	for _, p := range pubs {
		p.shutdown(false)
	}
}

func (m *pubMockServer) hasFault(f string) bool { return m.cfg.fault == f }

func (m *pubMockServer) outCtx(ctx context.Context, topic string) context.Context {
	if m.hasFault("missing-routing-header") {
		return ctx
	}
	val := url.QueryEscape(topic)
	if strings.EqualFold(m.caps.GetClientLanguage(), "rust") {
		val = topic
	}
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "topic="+val)
}

func (m *pubMockServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return m.caps, nil
}
func (m *pubMockServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *pubMockServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	m.mu.Lock()
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *pubMockServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	m.mu.Lock()
	out := slices.Clone(m.spans)
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.DrainTracesResponse{Spans: out}, nil
}

func (m *pubMockServer) getPub(id string, del bool) *pubMockPub {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pubs[id]
	if del {
		delete(m.pubs, id)
	}
	return p
}

func (m *pubMockServer) requirePub(id string, del bool) (*pubMockPub, error) {
	if p := m.getPub(id, del); p != nil {
		return p, nil
	}
	return nil, status.Error(codes.NotFound, "unknown publisher_id")
}

func (m *pubMockServer) CreatePublisher(_ context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	if m.hasFault("runtime-unimpl-create-pub") {
		return nil, status.Error(codes.Unimplemented, "CreatePublisher unimplemented")
	}
	conn, err := pubMockDial(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	countThresh := 100
	if c := int(req.GetCountThreshold()); req.CountThreshold != nil && c > 0 {
		countThresh = c
	}
	if m.hasFault("count-exceeds-1000") && countThresh == 1000 {
		countThresh = 1005
	} else if m.hasFault("count-capped-at-100") && countThresh == 1000 {
		countThresh = 100
	} else {
		countThresh = min(countThresh, 1000)
	}
	byteThresh := int64(1_000_000)
	if req.GetByteThreshold() > 0 {
		byteThresh = req.GetByteThreshold()
	}
	delayThresh := pubMockDur(req.DelayThresholdMs, 10*time.Millisecond, true)
	if (m.hasFault("count-waits-delay") || m.hasFault("large-waits-delay")) && delayThresh > 3200*time.Millisecond {
		delayThresh = 3200 * time.Millisecond
	}
	lang, mult := strings.ToLower(m.caps.GetClientLanguage()), max(req.GetRetryDelayMultiplier(), 1.5)
	p := &pubMockPub{
		proxy: m, topic: req.GetTopic(), enableOrdering: req.GetEnableMessageOrdering(),
		countThresh: countThresh, byteThresh: byteThresh, delayThresh: delayThresh,
		fcMaxMsgs: req.GetMaxOutstandingMessages(), fcBlock: req.GetLimitExceededBehavior() == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK && req.GetMaxOutstandingMessages() > 0,
		preAddSplit:  lang == "cpp" || lang == "rust",
		totalTimeout: pubMockDur(req.TotalTimeoutMs, 60*time.Second, false),
		initRetry:    pubMockDur(req.InitialRetryDelayMs, 20*time.Millisecond, false),
		maxRetry:     pubMockDur(req.MaxRetryDelayMs, 100*time.Millisecond, false),
		retryMult:    mult, conn: conn, client: pubsubpb.NewPublisherClient(conn),
		paused: map[string]bool{}, queues: map[string]*pubMockQueue{},
		pending: map[*pubMockItem]struct{}{}, tokens: map[string]chan *proxypb.PublishResponse{},
	}
	p.fcCond = sync.NewCond(&p.mu)
	id := fmt.Sprintf("pub-%d", m.seq.Add(1))
	m.mu.Lock()
	if m.srvCtx.Err() != nil {
		m.mu.Unlock()
		_ = conn.Close()
		return nil, status.Error(codes.Canceled, "proxy closed")
	}
	m.pubs[id] = p
	m.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func (p *pubMockPub) leakWire(ctx context.Context, n int, msgs ...*pubsubpb.PubsubMessage) {
	for range n {
		_, _ = p.client.Publish(p.proxy.outCtx(ctx, p.topic), &pubsubpb.PublishRequest{Topic: p.topic, Messages: msgs})
	}
}

func (m *pubMockServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	if m.hasFault("runtime-unimpl-pub") {
		return nil, status.Error(codes.Unimplemented, "Publish unimplemented")
	}
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	if req.GetUseBlockingPublisher() && !m.caps.GetSupportsBlockingPublish() {
		return nil, status.Error(codes.Unimplemented, "blocking publish unsupported")
	}
	if req.GetUseBlockingPublisher() && m.hasFault("runtime-unimpl-blocking") {
		return nil, status.Error(codes.Unimplemented, "blocking publish runtime unimpl")
	}
	if m.hasFault("emit-unexpected-span") {
		now := time.Now()
		m.mu.Lock()
		m.spans = append(m.spans, &proxypb.TraceSpan{TraceId: "11111111111111111111111111111111", SpanId: fmt.Sprintf("%016x", m.seq.Add(1)), TraceFlags: 1, Name: "unexpected create", Kind: proxypb.SpanKind_SPAN_KIND_PRODUCER, StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Millisecond))})
		m.mu.Unlock()
	}

	// Validate UTF-8 in attributes / raw_attributes.
	hasInvalidUTF8 := slices.ContainsFunc(req.GetRawAttributes(), func(ra *proxypb.RawAttribute) bool {
		return !utf8.Valid(ra.GetKey()) || !utf8.Valid(ra.GetValue())
	})
	if hasInvalidUTF8 && !m.hasFault("utf8-succeeds") {
		if m.hasFault("utf8-proxy-rpc-err") {
			return nil, status.Error(codes.Internal, "proxy rpc err on raw_attributes")
		}
		if m.hasFault("utf8-inband-unimpl") {
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
		}
		if m.hasFault("utf8-retry-internal") {
			p.leakWire(ctx, 2)
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Internal), ErrorMessage: "retried internal"}, nil
		}
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "invalid UTF-8"}, nil
	}

	if req.GetOrderingKey() != "" && m.caps.GetSupportsOrderingOptIn() && !p.enableOrdering && !m.hasFault("optin-allows-key") {
		if m.hasFault("optin-inband-unimpl") {
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
		}
		if m.hasFault("optin-fails-after-rpc") {
			p.leakWire(ctx, 1, &pubsubpb.PubsubMessage{Data: req.GetData()})
		}
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "ordering not enabled"}, nil
	}

	lang := strings.ToLower(m.caps.GetClientLanguage())
	isEmpty := len(req.GetData()) == 0 && len(req.GetAttributes()) == 0 && len(req.GetRawAttributes()) == 0 && req.GetOrderingKey() == ""
	if isEmpty && (lang == "php" || lang == "node" || lang == "nodejs") && !m.hasFault("php-allows-empty") {
		if m.hasFault("empty-inband-unimpl") {
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
		}
		if m.hasFault("php-rejects-with-wire-rpc") {
			p.leakWire(ctx, 1, &pubsubpb.PubsubMessage{})
		}
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "empty message rejected"}, nil
	}

	attrs := maps.Clone(req.GetAttributes())
	if attrs == nil {
		attrs = map[string]string{}
	}
	if m.hasFault("drop-utf8-attr") {
		maps.DeleteFunc(attrs, func(k, _ string) bool { return k != "ascii" })
	}
	if m.hasFault("add-bogus-attr-on-empty") && isEmpty {
		attrs["bogus"] = "1"
	}
	data := slices.Clone(req.GetData())
	if m.hasFault("corrupt-binary") && len(data) > 0 {
		data[0] ^= 0xff
	}
	if m.hasFault("large-corrupts-byte") && len(data) > 1024 {
		data[len(data)/2] ^= 0xff
	}
	if m.hasFault("blocking-corrupts-payload") && req.GetUseBlockingPublisher() {
		data = append(data, []byte("-bad")...)
	}
	if m.hasFault("drop-attrs-only") && len(data) == 0 && len(attrs) > 0 {
		return &proxypb.PublishResponse{MessageId: "dropped"}, nil
	}
	if m.hasFault("oversized-accepts") && len(data) > 10_000_000 {
		data = []byte("truncated-oversized")
	}

	pbMsg := &pubsubpb.PubsubMessage{Data: data, Attributes: attrs, OrderingKey: req.GetOrderingKey()}
	msgSize := int64(proto.Size(pbMsg))
	if m.hasFault("oversized-wrong-grpc-max") && msgSize > 10_000_000 {
		return nil, status.Error(codes.ResourceExhausted, "message larger than max (4194304)")
	}
	if m.hasFault("oversized-retries") && msgSize > 10_000_000 {
		p.leakWire(ctx, 2, pbMsg)
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "retried oversized"}, nil
	}

	p.mu.Lock()
	if m.srvCtx.Err() != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.Canceled, "proxy closed")
	}
	if (p.stopped || p.closed) && !m.hasFault("allow-publish-after-stop") && !(p.closed && m.hasFault("post-close-succeeds")) {
		p.mu.Unlock()
		if m.hasFault("post-stop-leaks-rpc") {
			p.leakWire(ctx, 1, pbMsg)
		}
		return &proxypb.PublishResponse{ErrorCode: int32(codes.FailedPrecondition), ErrorMessage: "publisher stopped"}, nil
	}
	if p.broken {
		p.mu.Unlock()
		return &proxypb.PublishResponse{ErrorCode: int32(codes.Internal), ErrorMessage: "publisher broken"}, nil
	}
	if req.GetOrderingKey() != "" && p.paused[req.GetOrderingKey()] && !m.hasFault("pord-no-pause-on-err") {
		p.mu.Unlock()
		return &proxypb.PublishResponse{ErrorCode: int32(codes.FailedPrecondition), ErrorMessage: "ordering key paused"}, nil
	}

	fcUsed := false
	if p.fcBlock {
		for p.fcInUse+1 > p.fcMaxMsgs && !p.stopped && !p.closed && ctx.Err() == nil && m.srvCtx.Err() == nil {
			p.fcCond.Wait()
		}
		if ctx.Err() != nil || m.srvCtx.Err() != nil {
			p.mu.Unlock()
			return &proxypb.PublishResponse{ErrorCode: int32(codes.DeadlineExceeded), ErrorMessage: "publish canceled"}, nil
		}
		p.fcInUse++
		fcUsed = true
	}

	if lang == "go" && msgSize > int64(10_000_000-len(p.topic)-5) && !m.hasFault("oversized-accepts") {
		if fcUsed && !m.hasFault("oversized-fc-leak") {
			p.fcInUse--
			p.fcCond.Broadcast()
		}
		p.mu.Unlock()
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "oversized"}, nil
	}
	if m.hasFault("batch1-size1-bogus-ok") {
		if p.bogusIdx++; p.bogusIdx == 2 {
			p.mu.Unlock()
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Internal), ErrorMessage: "local fail m2"}, nil
		}
	}

	it := &pubMockItem{pbMsg: pbMsg, bytes: msgSize, key: req.GetOrderingKey(), block: req.GetUseBlockingPublisher(), fcUsed: fcUsed, resCh: make(chan *proxypb.PublishResponse, 1), settled: make(chan struct{})}
	var tok string
	if req.GetAsyncFire() {
		tok = fmt.Sprintf("tok-%d", m.seq.Add(1))
		p.tokens[tok] = it.resCh
	}
	syncDone := p.enqueueLocked(it)
	p.mu.Unlock()
	m.waitFaultFlush(ctx, syncDone)
	if req.GetAsyncFire() {
		return &proxypb.PublishResponse{PublishToken: tok}, nil
	}
	select {
	case resp := <-it.resCh:
		return resp, nil
	case <-ctx.Done():
		return &proxypb.PublishResponse{ErrorCode: int32(codes.DeadlineExceeded), ErrorMessage: ctx.Err().Error()}, nil
	}
}

func (m *pubMockServer) waitFaultFlush(ctx context.Context, syncDone <-chan struct{}) {
	if syncDone == nil {
		return
	}
	if m.hasFault("count-flushes-at-4") {
		select {
		case <-syncDone:
		case <-ctx.Done():
		}
		return
	}
	t := time.NewTimer(25 * time.Millisecond)
	defer t.Stop()
	select {
	case <-syncDone:
	case <-t.C:
	case <-ctx.Done():
	}
}

func (p *pubMockPub) enqueueLocked(it *pubMockItem) <-chan struct{} {
	p.pending[it] = struct{}{}
	q := p.queues[it.key]
	if q == nil {
		q = &pubMockQueue{key: it.key}
		p.queues[it.key] = q
	}
	if p.preAddSplit && len(q.items) > 0 && (len(q.items) >= p.countThresh || q.bytes+it.bytes > p.byteThresh) {
		p.flushQueueLocked(q)
	}
	q.items = append(q.items, it)
	q.bytes += it.bytes

	m := p.proxy
	shouldFlush := (it.block && !m.hasFault("blocking-batches-instead")) || p.delayThresh == 0 ||
		m.hasFault("flush-premature-before-batch") || m.hasFault("delay-flushes-immediately") ||
		(m.hasFault("batch1-size1-bogus-ok") && p.bogusIdx == 1) ||
		(m.hasFault("count-flushes-at-4") && len(q.items) >= 4) ||
		(len(q.items) >= p.countThresh && !m.hasFault("count-no-auto-flush") && !m.hasFault("count-waits-delay")) ||
		(q.bytes >= p.byteThresh && !m.hasFault("byte-coalesces-all-4") && !m.hasFault("byte-no-auto-flush") && !m.hasFault("large-no-auto-flush") && !m.hasFault("large-waits-delay"))
	if shouldFlush {
		done := p.flushQueueLocked(q)
		if m.hasFault("count-flushes-at-4") || m.hasFault("flush-premature-before-batch") {
			return done
		}
		return nil
	}
	if q.timer == nil && !m.hasFault("delay-no-auto-flush") && !m.hasFault("large-no-auto-flush") {
		var t *time.Timer
		t = time.AfterFunc(p.delayThresh, func() {
			p.mu.Lock()
			if q.timer == t && !p.closed {
				q.timer = nil
				p.flushQueueLocked(q)
			}
			p.mu.Unlock()
		})
		q.timer = t
	}
	return nil
}

func (p *pubMockPub) flushQueueLocked(q *pubMockQueue) <-chan struct{} {
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	if len(q.items) == 0 {
		return nil
	}
	batch := q.items
	q.items, q.bytes = nil, 0
	if q.key != "" && !p.proxy.hasFault("pord-concurrent-same-key") && !p.proxy.hasFault("pord-k2-or-p2-leapfrogs-p1") {
		if p.paused[q.key] && !p.proxy.hasFault("pord-no-pause-on-err") {
			p.failBatchLocked(batch, codes.FailedPrecondition, "key paused")
			return nil
		}
		if q.inFlight {
			q.pending = append(q.pending, batch)
			return nil
		}
		q.inFlight = true
		p.wg.Add(1)
		go p.runOrderedWorker(q, batch)
		return nil
	}
	done := make(chan struct{})
	p.wg.Add(1)
	go func() { defer p.wg.Done(); defer close(done); p.execBatch(batch) }()
	return done
}

func (p *pubMockPub) settleItemLocked(it *pubMockItem, releaseFC bool, resp *proxypb.PublishResponse) {
	delete(p.pending, it)
	if releaseFC && it.fcUsed {
		p.fcInUse--
		p.fcCond.Broadcast()
	}
	it.resCh <- resp
	close(it.settled)
}

func (p *pubMockPub) failBatchLocked(batch []*pubMockItem, code codes.Code, msg string) {
	for _, it := range batch {
		p.settleItemLocked(it, true, &proxypb.PublishResponse{ErrorCode: int32(code), ErrorMessage: msg})
	}
}

func (p *pubMockPub) runOrderedWorker(q *pubMockQueue, batch []*pubMockItem) {
	defer p.wg.Done()
	for {
		ok := p.execBatch(batch)
		p.mu.Lock()
		if !ok && !p.proxy.hasFault("pord-no-pause-on-err") {
			p.paused[q.key] = true
			if p.proxy.hasFault("pord-pause-leaks-k2") {
				p.paused["k2"] = true
			}
			for _, rem := range append(q.pending, q.items) {
				p.failBatchLocked(rem, codes.FailedPrecondition, "key paused")
			}
			q.items, q.bytes, q.pending, q.inFlight = nil, 0, nil, false
			p.mu.Unlock()
			return
		}
		if len(q.pending) > 0 {
			batch = q.pending[0]
			q.pending = q.pending[1:]
			p.mu.Unlock()
			continue
		}
		q.inFlight = false
		p.mu.Unlock()
		return
	}
}

func (p *pubMockPub) execBatch(batch []*pubMockItem) bool {
	m := p.proxy
	if m.hasFault("pord-k2-blocked-while-k1-gated") {
		p.globalMu.Lock()
		defer p.globalMu.Unlock()
	}
	pbMsgs := make([]*pubsubpb.PubsubMessage, len(batch))
	for i, it := range batch {
		pbMsgs[i] = it.pbMsg
	}
	topic := p.topic
	if m.hasFault("wrong-topic") {
		topic += "-wrong"
	}
	pbReq := &pubsubpb.PublishRequest{Topic: topic, Messages: pbMsgs}

	ctx, cancel := context.WithCancel(m.srvCtx)
	lang := strings.ToLower(m.caps.GetClientLanguage())
	extOrdered := len(batch) > 0 && batch[0].key != "" && (lang == "java" || lang == "python") && !m.hasFault("pord-ext-timeout-on-java")
	if p.totalTimeout > 0 && !extOrdered && !m.hasFault("pord-ignore-total-timeout") {
		cancel()
		ctx, cancel = context.WithTimeout(m.srvCtx, p.totalTimeout)
	}
	defer cancel()

	var resp *pubsubpb.PublishResponse
	var err error
	delay := p.initRetry
	for {
		resp, err = p.client.Publish(m.outCtx(ctx, p.topic), pbReq)
		if err == nil {
			break
		}
		code := status.Code(err)
		retryable := code == codes.Unavailable || (m.hasFault("pord-retry-perm-err") && code == codes.InvalidArgument)
		if m.hasFault("pord-no-retry-unavail") {
			retryable = false
		}
		if !retryable || ctx.Err() != nil {
			break
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			err = status.FromContextError(ctx.Err()).Err()
			goto done
		}
		delay = min(time.Duration(float64(delay)*p.retryMult), p.maxRetry)
	}
done:
	if err == nil && len(resp.GetMessageIds()) != len(batch) {
		if m.hasFault("swallow-msgid-mismatch") || m.hasFault("batch1-size1-bogus-ok") {
			ids := make([]string, len(batch))
			for i := range batch {
				ids[i] = fmt.Sprintf("bogus-%d", i)
			}
			resp = &pubsubpb.PublishResponse{MessageIds: ids}
		} else if lang == "rust" {
			p.mu.Lock()
			for i, it := range batch {
				r := &proxypb.PublishResponse{ErrorCode: int32(codes.Unavailable), ErrorMessage: "shutdown"}
				if i < len(resp.GetMessageIds()) {
					r = &proxypb.PublishResponse{MessageId: resp.MessageIds[i]}
				}
				p.settleItemLocked(it, true, r)
			}
			p.mu.Unlock()
			return true
		} else {
			err = status.Error(codes.Internal, "mismatched message_ids count")
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		if m.hasFault("break-publisher-after-mismatch") {
			p.broken = true
		}
		code := status.Code(err)
		relFC := !m.hasFault("oversized-fc-leak")
		for _, it := range batch {
			p.settleItemLocked(it, relFC, &proxypb.PublishResponse{ErrorCode: int32(code), ErrorMessage: err.Error()})
		}
		return false
	}
	for i, it := range batch {
		msgID := resp.MessageIds[i]
		if m.hasFault("mismatched-msgid") {
			msgID = "wrong-" + msgID
		} else if m.hasFault("pub10-wrong-recovery-msgid") && string(it.pbMsg.GetData()) == "pub10-m3" {
			msgID = "bogus-recovery-" + msgID
		}
		p.settleItemLocked(it, true, &proxypb.PublishResponse{MessageId: msgID})
	}
	return true
}

func (m *pubMockServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
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

func (m *pubMockServer) FlushPublisher(ctx context.Context, req *proxypb.FlushPublisherRequest) (*proxypb.FlushPublisherResponse, error) {
	if m.hasFault("runtime-unimpl-flush") {
		return nil, status.Error(codes.Unimplemented, "FlushPublisher unimplemented")
	}
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	for _, q := range p.queues {
		p.flushQueueLocked(q)
	}
	snap := slices.Collect(maps.Keys(p.pending))
	p.mu.Unlock()
	if m.hasFault("flush-returns-early") {
		return &proxypb.FlushPublisherResponse{}, nil
	}
	for _, it := range snap {
		select {
		case <-it.settled:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &proxypb.FlushPublisherResponse{}, nil
}

func (m *pubMockServer) StopPublisher(ctx context.Context, req *proxypb.StopPublisherRequest) (*proxypb.StopPublisherResponse, error) {
	if m.hasFault("runtime-unimpl-stop") {
		return nil, status.Error(codes.Unimplemented, "StopPublisher unimplemented")
	}
	p, err := m.requirePub(req.GetPublisherId(), m.hasFault("post-stop-returns-notfound"))
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.stopped = true
	p.fcCond.Broadcast()
	for _, q := range p.queues {
		if m.hasFault("stop-drops-msgs") && len(q.items) > 1 {
			p.failBatchLocked(q.items[1:], codes.Canceled, "dropped")
			q.items = q.items[:1]
		}
		p.flushQueueLocked(q)
	}
	p.mu.Unlock()
	if m.hasFault("stop-returns-early") {
		return &proxypb.StopPublisherResponse{}, nil
	}
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return &proxypb.StopPublisherResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (m *pubMockServer) ResumePublish(_ context.Context, req *proxypb.ResumePublishRequest) (*proxypb.ResumePublishResponse, error) {
	if !m.caps.GetSupportsResumePublish() || m.hasFault("runtime-unimpl-resume") {
		return nil, status.Error(codes.Unimplemented, "ResumePublish unimplemented")
	}
	p, err := m.requirePub(req.GetPublisherId(), false)
	if err != nil {
		return nil, err
	}
	if m.hasFault("pord-resume-noop") {
		return &proxypb.ResumePublishResponse{}, nil
	}
	p.mu.Lock()
	delete(p.paused, req.GetOrderingKey())
	p.mu.Unlock()
	return &proxypb.ResumePublishResponse{}, nil
}

func (m *pubMockServer) ClosePublisher(ctx context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	if m.hasFault("post-close-succeeds") {
		if p := m.getPub(req.GetPublisherId(), false); p != nil {
			p.mu.Lock()
			p.closed = true
			p.mu.Unlock()
		}
		return &proxypb.ClosePublisherResponse{}, nil
	}
	if p := m.getPub(req.GetPublisherId(), true); p != nil {
		p.shutdown(true)
	}
	return &proxypb.ClosePublisherResponse{}, nil
}

func pubRunScenarios(t *testing.T, cfg pubMockConfig, list []harness.Scenario, filter string, timeout time.Duration) *harness.TargetReport {
	t.Helper()
	reg := scenarios.NewRegistry()
	for _, sc := range list {
		reg.MustRegister(sc)
	}
	target := harness.TargetSpec{Name: cfg.profile, Connect: pubMockConnect(cfg)}
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

func pubStatusMap(rep *harness.TargetReport) map[string]harness.ScenarioResult {
	out := make(map[string]harness.ScenarioResult, len(rep.Scenarios))
	for _, sr := range rep.Scenarios {
		out[sr.ID] = sr
	}
	return out
}

func TestPublishScenarios_UnitAndRegistry(t *testing.T) {
	list := scenarios.ExportPublishScenarios()
	wantIDs := []string{"PUB-02", "PUB-02b", "PUB-03", "PUB-04", "PUB-05", "PUB-06a", "PUB-06b", "PUB-07", "PUB-08", "PUB-09", "PUB-10"}
	if len(list) != len(wantIDs) {
		t.Fatalf("publishScenarios count: want %d, got %d", len(wantIDs), len(list))
	}
	smokeRE, noMatchRE, reg := regexp.MustCompile(`^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03)$`), regexp.MustCompile(`^NOMATCH$`), scenarios.NewRegistry()
	for i, sc := range list {
		if sc.ID != wantIDs[i] || sc.Category != "publish" || sc.Timeout != 0 || sc.TraceOnly || sc.Run == nil || sc.Name == "" || sc.Feature == "" || sc.Description == "" {
			t.Errorf("invalid scenario[%d]: %+v", i, sc)
		}
		if smokeRE.MatchString(sc.ID) || smokeRE.MatchString(sc.Name) || smokeRE.MatchString(sc.Category) || noMatchRE.MatchString(sc.ID) || noMatchRE.MatchString(sc.Name) || noMatchRE.MatchString(sc.Category) {
			t.Errorf("filter safety violation in %+v", sc)
		}
		if (sc.ID == "PUB-10") != sc.IsolateProxy || (sc.ID == "PUB-06b") != (len(sc.FakeServerOptions) > 0) || (sc.ID == "PUB-09") != slices.Equal(sc.RequiredCapabilities, []harness.Capability{harness.CapBlockingPublish}) {
			t.Errorf("scenario metadata mismatch on %s", sc.ID)
		}
		reg.MustRegister(sc)
	}
	if reg.Len() != len(wantIDs) || scenarios.Len() != 65 {
		t.Fatalf("registry isolation violation: reg=%d default=%d", reg.Len(), scenarios.Len())
	}
}

func TestPublishScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportPublishScenarios()
	t.Run("Go and Rust pass 10 and PUB-09 UNSUPPORTED; Cpp passes all 11", func(t *testing.T) {
		for _, tc := range []struct {
			prof     string
			wantPass int
		}{{"go", 10}, {"cpp", 11}, {"rust", 10}} {
			rep := pubRunScenarios(t, pubMockConfig{profile: tc.prof}, list, "", 10*time.Second)
			byID := pubStatusMap(rep)
			if rep.Summary.Pass != tc.wantPass || (tc.wantPass == 10 && byID["PUB-09"].Status != harness.StatusUnsupportedByClient) {
				t.Fatalf("%s mismatch: summary=%+v pub09=%s scenarios=%+v", tc.prof, rep.Summary, byID["PUB-09"].Status, rep.Scenarios)
			}
		}
	})
	t.Run("PUB-02b PHP/Node reject empty vs Python accepts empty", func(t *testing.T) {
		for _, prof := range []string{"php", "node", "python"} {
			if rep := pubRunScenarios(t, pubMockConfig{profile: prof}, list, "^PUB-02b$", 5*time.Second); rep.Summary.Pass != 1 {
				t.Fatalf("profile=%s PUB-02b failed: %+v", prof, rep.Scenarios)
			}
		}
	})
	t.Run("Runtime UNIMPLEMENTED propagates as UNSUPPORTED_BY_CLIENT", func(t *testing.T) {
		for _, tc := range []struct{ prof, flt, sc string }{
			{"go", "runtime-unimpl-create-pub", "PUB-02"}, {"go", "runtime-unimpl-pub", "PUB-02"},
			{"go", "runtime-unimpl-flush", "PUB-07"}, {"go", "runtime-unimpl-stop", "PUB-07"}, {"cpp", "runtime-unimpl-blocking", "PUB-09"},
		} {
			if sr := pubRunScenarios(t, pubMockConfig{profile: tc.prof, fault: tc.flt}, list, "^"+tc.sc+"$", 5*time.Second).Scenarios[0]; sr.Status != harness.StatusUnsupportedByClient {
				t.Errorf("fault=%s %s: want UNSUPPORTED, got %s (%s)", tc.flt, tc.sc, sr.Status, sr.Error)
			}
		}
	})
}

func TestPublishScenarios_FaultDetection(t *testing.T) {
	list := scenarios.ExportPublishScenarios()
	for _, tc := range []struct {
		prof, fault, sc, sub string
		timeout              time.Duration
		want                 harness.Status
	}{
		{"go", "corrupt-binary", "PUB-02", "data mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "drop-utf8-attr", "PUB-02", "attributes mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "utf8-succeeds", "PUB-02", "expected invalid UTF-8", 650 * time.Millisecond, harness.StatusFail},
		{"go", "utf8-retry-internal", "PUB-02", "no retries", 650 * time.Millisecond, harness.StatusFail},
		{"go", "utf8-inband-unimpl", "PUB-02", "UNIMPLEMENTED", 650 * time.Millisecond, harness.StatusFail},
		{"go", "utf8-proxy-rpc-err", "PUB-02", "proxy rpc err", 650 * time.Millisecond, harness.StatusFail},
		{"go", "drop-attrs-only", "PUB-02b", "call count mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "add-bogus-attr-on-empty", "PUB-02b", "attributes mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"php", "php-allows-empty", "PUB-02b", "expected empty message rejection", 650 * time.Millisecond, harness.StatusFail},
		{"php", "php-rejects-with-wire-rpc", "PUB-02b", "unexpected call after seq", 650 * time.Millisecond, harness.StatusFail},
		{"php", "empty-inband-unimpl", "PUB-02b", "UNIMPLEMENTED", 650 * time.Millisecond, harness.StatusFail},
		{"go", "count-flushes-at-4", "PUB-03", "expected 0 Publish RPCs", 650 * time.Millisecond, harness.StatusFail},
		{"go", "count-exceeds-1000", "PUB-03", "outside [1, 1000]", 5 * time.Second, harness.StatusFail},
		{"go", "count-capped-at-100", "PUB-03", "outside [2, 4]", 5 * time.Second, harness.StatusFail},
		{"go", "count-no-auto-flush", "PUB-03", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "count-waits-delay", "PUB-03", ">= 3s", 5 * time.Second, harness.StatusFail},
		{"go", "mismatched-msgid", "PUB-03", "message_ids mismatch", 5 * time.Second, harness.StatusFail},
		{"go", "byte-coalesces-all-4", "PUB-04", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "byte-no-auto-flush", "PUB-04", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "flush-premature-before-batch", "PUB-04", "outside [2, 3]", 650 * time.Millisecond, harness.StatusFail},
		{"go", "delay-no-auto-flush", "PUB-05", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "delay-flushes-immediately", "PUB-05", "flushed too early", 650 * time.Millisecond, harness.StatusFail},
		{"go", "large-no-auto-flush", "PUB-06a", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "large-waits-delay", "PUB-06a", ">= 3s", 5 * time.Second, harness.StatusFail},
		{"go", "large-corrupts-byte", "PUB-06a", "2 MiB payload mismatch", 2 * time.Second, harness.StatusFail},
		{"go", "oversized-accepts", "PUB-06b", "expected oversized", 2 * time.Second, harness.StatusFail},
		{"cpp", "oversized-retries", "PUB-06b", "wire RPCs", 3 * time.Second, harness.StatusFail},
		{"go", "oversized-fc-leak", "PUB-06b", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"go", "oversized-wrong-grpc-max", "PUB-06b", "ResourceExhausted", 2 * time.Second, harness.StatusFail},
		{"go", "flush-premature-before-batch", "PUB-07", "expected 0 Publish RPCs", 650 * time.Millisecond, harness.StatusFail},
		{"go", "flush-returns-early", "PUB-07", "returned prematurely", 650 * time.Millisecond, harness.StatusFail},
		{"go", "stop-returns-early", "PUB-07", "returned prematurely", 650 * time.Millisecond, harness.StatusFail},
		{"go", "stop-drops-msgs", "PUB-07", "dropped", 650 * time.Millisecond, harness.StatusFail},
		{"go", "allow-publish-after-stop", "PUB-08", "expected Publish after StopPublisher", 650 * time.Millisecond, harness.StatusFail},
		{"go", "post-stop-leaks-rpc", "PUB-08", "unexpected call after seq", 650 * time.Millisecond, harness.StatusFail},
		{"go", "post-stop-returns-notfound", "PUB-08", "unknown publisher_id", 650 * time.Millisecond, harness.StatusFail},
		{"go", "post-close-succeeds", "PUB-08", "expected Publish after ClosePublisher", 650 * time.Millisecond, harness.StatusFail},
		{"cpp", "blocking-batches-instead", "PUB-09", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout},
		{"cpp", "blocking-corrupts-payload", "PUB-09", "data mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "swallow-msgid-mismatch", "PUB-10", "expected >= 1 error", 650 * time.Millisecond, harness.StatusFail},
		{"go", "break-publisher-after-mismatch", "PUB-10", "publisher broken", 650 * time.Millisecond, harness.StatusFail},
		{"go", "batch1-size1-bogus-ok", "PUB-10", "expected server to return 1 message_id on batch 1, got 0", 650 * time.Millisecond, harness.StatusFail},
		{"go", "pub10-wrong-recovery-msgid", "PUB-10", "recovery message_ids mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "emit-unexpected-span", "PUB-02", "expected zero spans", 650 * time.Millisecond, harness.StatusFail},
		{"go", "wrong-topic", "PUB-02", "topic mismatch", 650 * time.Millisecond, harness.StatusFail},
		{"go", "missing-routing-header", "PUB-02", "x-goog-request-params", 650 * time.Millisecond, harness.StatusFail},
	} {
		t.Run(tc.sc+"_"+tc.fault, func(t *testing.T) {
			rep := pubRunScenarios(t, pubMockConfig{profile: tc.prof, fault: tc.fault}, list, "^"+tc.sc+"$", tc.timeout)
			sr := rep.Scenarios[0]
			if sr.Status != tc.want || (tc.sub != "" && !strings.Contains(sr.Error, tc.sub)) {
				t.Errorf("%s/%s: want %s (%q), got %s (%q)", tc.sc, tc.fault, tc.want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}

func TestPublishHelpers_AsyncExpectErr(t *testing.T) {
	badAttr := []*proxypb.RawAttribute{{Key: []byte("a\xc5z"), Value: []byte("v")}}
	for _, tc := range []struct {
		name, fault, sub, pubOverride string
		rawAttrs                      []*proxypb.RawAttribute
		rule                          *fake.PublishRule
		callTimeout                   time.Duration
		want                          harness.Status
		wantCode                      int32
	}{
		{name: "immediate-err", rawAttrs: badAttr, want: harness.StatusPass, wantCode: int32(codes.InvalidArgument)},
		{name: "immediate-inband-unimpl", fault: "utf8-inband-unimpl", rawAttrs: badAttr, want: harness.StatusFail, sub: "UNIMPLEMENTED"},
		{name: "await-err", rule: &fake.PublishRule{Name: "h-err", Times: 1, Err: status.Error(codes.InvalidArgument, "wire bad")}, want: harness.StatusPass, wantCode: int32(codes.InvalidArgument)},
		{name: "unexpected-success", want: harness.StatusFail, sub: "expected async publish to fail"},
		{name: "proxy-unimpl", fault: "runtime-unimpl-pub", want: harness.StatusUnsupportedByClient},
		{name: "proxy-notfound", pubOverride: "missing", want: harness.StatusFail, sub: "unknown publisher_id"},
		{name: "await-deadline", rule: &fake.PublishRule{Name: "h-slow", Times: 1, Delay: 300 * time.Millisecond}, callTimeout: 25 * time.Millisecond, want: harness.StatusFail, sub: "AwaitPublish:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := harness.Scenario{ID: "PUB-H", Name: tc.name, Category: "publish", Feature: "PUB-02", Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
				pubID := tc.pubOverride
				if pubID == "" {
					r, err := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: env.Endpoint(), Topic: env.TopicPath(env.UniqueName("h")), ProjectId: env.ProjectID, CountThreshold: proto.Int64(1), DelayThresholdMs: proto.Int64(10)})
					if err != nil {
						return err
					}
					pubID = r.GetPublisherId()
				}
				if tc.rule != nil {
					env.Fake.AddPublishRule(*tc.rule)
				}
				cCtx := ctx
				if tc.callTimeout > 0 {
					var cancel context.CancelFunc
					cCtx, cancel = context.WithTimeout(ctx, tc.callTimeout)
					defer cancel()
				}
				code, _, err := scenarios.ExportPublishAsyncExpectErr(cCtx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("x"), RawAttributes: tc.rawAttrs})
				if err != nil {
					return err
				}
				if tc.wantCode != 0 && code != tc.wantCode {
					return fmt.Errorf("want code %d, got %d", tc.wantCode, code)
				}
				return nil
			}}
			sr := pubRunScenarios(t, pubMockConfig{profile: "go", fault: tc.fault}, []harness.Scenario{sc}, "", 2*time.Second).Scenarios[0]
			if sr.Status != tc.want || (tc.sub != "" && !strings.Contains(sr.Error, tc.sub)) {
				t.Errorf("%s: want %s (%q), got %s (%q)", tc.name, tc.want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}

type pub10WrapHandle struct {
	harness.ProxyHandle
	client proxypb.PubSubProxyClient
}

func (h pub10WrapHandle) Client() proxypb.PubSubProxyClient { return h.client }

type pub10WrapClient struct {
	proxypb.PubSubProxyClient
	flipped *atomic.Bool
}

func (c pub10WrapClient) AwaitPublish(ctx context.Context, in *proxypb.AwaitPublishRequest, opts ...grpc.CallOption) (*proxypb.PublishResponse, error) {
	resp, err := c.PubSubProxyClient.AwaitPublish(ctx, in, opts...)
	if err == nil && resp != nil && resp.GetErrorCode() == 0 && c.flipped.CompareAndSwap(false, true) {
		cloned := proto.Clone(resp).(*proxypb.PublishResponse)
		cloned.MessageId = "bogus-b1-" + cloned.GetMessageId()
		return cloned, nil
	}
	return resp, err
}

func TestPUB10_Batch1PartialOKWrongMessageID(t *testing.T) {
	baseConnect := pubMockConnect(pubMockConfig{profile: "rust"})
	var flipped atomic.Bool
	target := harness.TargetSpec{
		Name: "rust-pub10-bogus-b1-ok",
		Connect: func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
			h, err := baseConnect(ctx, ts)
			if err != nil {
				return nil, err
			}
			return pub10WrapHandle{
				ProxyHandle: h,
				client:      pub10WrapClient{PubSubProxyClient: h.Client(), flipped: &flipped},
			}, nil
		},
	}
	runner, err := harness.NewRunner(harness.RunConfig{
		Targets:         []harness.TargetSpec{target},
		Scenarios:       scenarios.ExportPublishScenarios(),
		Filter:          "^PUB-10$",
		ScenarioTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget: %v", err)
	}
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "PUB-10 batch 1 OK message_id") || !flipped.Load() {
		t.Fatalf("expected StatusFail containing 'PUB-10 batch 1 OK message_id', got scenarios=%+v flipped=%v", rep.Scenarios, flipped.Load())
	}
}
