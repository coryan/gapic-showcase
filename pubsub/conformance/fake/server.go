// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fake

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	defaultListenAddr = "127.0.0.1:0"
	defaultMaxMsgSize = 32 << 20 // 32 MiB
)

// Option configures Server construction.
type Option func(*serverConfig)

type serverTestHooks struct {
	afterTagRPC         func(fullMethod string, ep *epochState)
	onRPCEnd            func(fullMethod string, ep *epochState)
	afterUnknownUnlock  func(ep *epochState)
	beforeInnerAttempt  func(fullMethod string, ep *epochState)
	beforeStreamPreRecv func(ep *epochState)
	onStreamRecvExit    func(ep *epochState)
	beforeStreamScript  func(ep *epochState)
	beforeResetWait     func()
	onResetGraceExpired func()
	onForceCloseExit    func()
}

func withTestHooks(hooks *serverTestHooks) Option {
	return func(c *serverConfig) {
		c.testHooks = hooks
	}
}

type serverConfig struct {
	listenAddr             string
	maxRecvMsgSize         int
	maxSendMsgSize         int
	maxPublishRequestBytes int
	suppressPong           bool
	defaultSubProps        *pubsubpb.StreamingPullResponse_SubscriptionProperties
	strictStreamingPull    bool
	testHooks              *serverTestHooks
}

// WithListenAddr configures the TCP bind address (default "127.0.0.1:0").
func WithListenAddr(addr string) Option {
	return func(c *serverConfig) {
		if addr != "" {
			c.listenAddr = addr
		}
	}
}

// WithMaxRecvMsgSize configures grpc.MaxRecvMsgSize (default 32 MiB).
func WithMaxRecvMsgSize(bytes int) Option {
	return func(c *serverConfig) {
		if bytes > 0 {
			c.maxRecvMsgSize = bytes
		}
	}
}

// WithMaxSendMsgSize configures grpc.MaxSendMsgSize (default 32 MiB).
func WithMaxSendMsgSize(bytes int) Option {
	return func(c *serverConfig) {
		if bytes > 0 {
			c.maxSendMsgSize = bytes
		}
	}
}

// WithMaxPublishRequestBytes sets a server-side proto byte size cap on
// PublishRequest (e.g., 10_000_000 for PUB-06b), returning codes.InvalidArgument
// when exceeded. Values <= 0 disable the cap.
func WithMaxPublishRequestBytes(bytes int) Option {
	return func(c *serverConfig) {
		c.maxPublishRequestBytes = bytes
	}
}

// WithSuppressPong configures whether StreamingPull suppresses empty pong
// responses to client keepalive pings by default.
func WithSuppressPong(suppress bool) Option {
	return func(c *serverConfig) {
		c.suppressPong = suppress
	}
}

// WithDefaultSubscriptionProperties configures initial SubscriptionProperties
// emitted on newly opened StreamingPull streams unless overridden by a script.
func WithDefaultSubscriptionProperties(eod, ordering bool) Option {
	return func(c *serverConfig) {
		c.defaultSubProps = &pubsubpb.StreamingPullResponse_SubscriptionProperties{
			ExactlyOnceDeliveryEnabled: eod,
			MessageOrderingEnabled:     ordering,
		}
	}
}

// WithStrictStreamingPullValidation enables strict protocol validation on
// StreamingPull frames (initial subscription non-empty, initial deadline in
// [10, 600], initial-only client_id/flow-control/protocol_version).
func WithStrictStreamingPullValidation(strict bool) Option {
	return func(c *serverConfig) {
		c.strictStreamingPull = strict
	}
}

type epochState struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	msgIDSeq atomic.Int64
	ackIDSeq atomic.Int64
}

type callRecord struct {
	c CapturedCall
}

func (r *callRecord) toCaptured() CapturedCall {
	return r.c.Clone()
}

type activeCallKey struct{}

type activeCallContext struct {
	ep  *epochState
	rec *callRecord
}

type connAddrPair struct {
	local  string
	remote string
}

type trackedConn struct {
	net.Conn
	addr      connAddrPair
	closeOnce sync.Once
	closeErr  error
	onClose   func(*trackedConn)
}

func (c *trackedConn) Close() error {
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose(c)
		}
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

type connTrackingListener struct {
	net.Listener
	mu      sync.Mutex
	pending map[connAddrPair][]*trackedConn
	active  map[*trackedConn]struct{}
}

func newConnTrackingListener(inner net.Listener) *connTrackingListener {
	return &connTrackingListener{
		Listener: inner,
		pending:  make(map[connAddrPair][]*trackedConn),
		active:   make(map[*trackedConn]struct{}),
	}
}

func (l *connTrackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tc := &trackedConn{
		Conn: conn,
		addr: connAddrPair{
			local:  conn.LocalAddr().String(),
			remote: conn.RemoteAddr().String(),
		},
		onClose: l.removeConn,
	}
	l.mu.Lock()
	l.pending[tc.addr] = append(l.pending[tc.addr], tc)
	l.active[tc] = struct{}{}
	l.mu.Unlock()
	return tc, nil
}

func (l *connTrackingListener) takeConn(info *stats.ConnTagInfo) net.Conn {
	if info == nil || info.LocalAddr == nil || info.RemoteAddr == nil {
		return nil
	}
	key := connAddrPair{
		local:  info.LocalAddr.String(),
		remote: info.RemoteAddr.String(),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.pending[key]
	if len(list) == 0 {
		return nil
	}
	tc := list[0]
	if len(list) == 1 {
		delete(l.pending, key)
	} else {
		l.pending[key] = list[1:]
	}
	return tc
}

func (l *connTrackingListener) removeConn(tc *trackedConn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.active, tc)
	list := l.pending[tc.addr]
	if len(list) > 0 {
		filtered := list[:0]
		for _, item := range list {
			if item != tc {
				filtered = append(filtered, item)
			}
		}
		if len(filtered) == 0 {
			delete(l.pending, tc.addr)
		} else {
			l.pending[tc.addr] = filtered
		}
	}
}

func (l *connTrackingListener) activeConns() []*trackedConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*trackedConn, 0, len(l.active))
	for tc := range l.active {
		out = append(out, tc)
	}
	return out
}

func waitWaitGroupTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Server is a plaintext gRPC fake Pub/Sub data-plane server with wire-level
// capture, epoch-isolated Reset(), programmable fault injection, and dynamic
// StreamingPull control.
type Server struct {
	lis         net.Listener
	connTracker *connTrackingListener
	addr        string
	grpcServer  *grpc.Server

	maxPublishRequestBytes int
	strictStreamingPull    bool
	initialSuppressPong    bool
	initialDefaultSubProps *pubsubpb.StreamingPullResponse_SubscriptionProperties
	testHooks              *serverTestHooks

	lifecycleMu sync.Mutex
	closed      atomic.Bool

	mu       sync.Mutex
	epoch    *epochState
	notifyCh chan struct{}

	calls        []*callRecord
	streams      map[string]*streamSession
	streamOrder  []*streamSession
	readyStreams []*streamSession

	seqCounter      int64
	streamIDCounter int64

	publishAttempts int
	ackAttempts     int
	modAckAttempts  int
	pullAttempts    int
	streamAttempts  int

	activePublishTotal int
	activePublishByKey map[string]int
	activePublishCalls map[*callRecord]struct{}
	maxPublishTotal    int
	maxPublishByKey    map[string]int

	publishRules  []*ruleEntry[PublishRule]
	ackRules      []*ruleEntry[AckRule]
	modAckRules   []*ruleEntry[ModAckRule]
	pullRules     []*ruleEntry[PullRule]
	streamScripts []*ruleEntry[StreamingPullScript]

	suppressPong    bool
	defaultSubProps *pubsubpb.StreamingPullResponse_SubscriptionProperties
}

// NewServer starts a plaintext gRPC fake Pub/Sub data-plane server listening on
// 127.0.0.1:0 (or the configured address).
func NewServer(opts ...Option) (*Server, error) {
	cfg := serverConfig{
		listenAddr:     defaultListenAddr,
		maxRecvMsgSize: defaultMaxMsgSize,
		maxSendMsgSize: defaultMaxMsgSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	rawLis, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", cfg.listenAddr, err)
	}
	lis := newConnTrackingListener(rawLis)

	epCtx, epCancel := context.WithCancel(context.Background())
	s := &Server{
		lis:                    lis,
		connTracker:            lis,
		addr:                   lis.Addr().String(),
		maxPublishRequestBytes: cfg.maxPublishRequestBytes,
		strictStreamingPull:    cfg.strictStreamingPull,
		initialSuppressPong:    cfg.suppressPong,
		initialDefaultSubProps: proto.CloneOf(cfg.defaultSubProps),
		epoch:                  &epochState{ctx: epCtx, cancel: epCancel},
		notifyCh:               make(chan struct{}),
		streams:                make(map[string]*streamSession),
		activePublishByKey:     make(map[string]int),
		activePublishCalls:     make(map[*callRecord]struct{}),
		maxPublishByKey:        make(map[string]int),
		suppressPong:           cfg.suppressPong,
		defaultSubProps:        proto.CloneOf(cfg.defaultSubProps),
		testHooks:              cfg.testHooks,
	}

	sh := &serverStatsHandler{
		tagConn: lis.takeConn,
		tagEpoch: func() any {
			s.mu.Lock()
			ep := s.epoch
			s.mu.Unlock()
			return ep
		},
		onTransportErr: s.recordTransportError,
	}
	if cfg.testHooks != nil {
		if cfg.testHooks.afterTagRPC != nil {
			fn := cfg.testHooks.afterTagRPC
			sh.afterTagRPC = func(fullMethod string, ep any) {
				epState, _ := ep.(*epochState)
				fn(fullMethod, epState)
			}
		}
		if cfg.testHooks.onRPCEnd != nil {
			fn := cfg.testHooks.onRPCEnd
			sh.onRPCEnd = func(fullMethod string, ep any) {
				epState, _ := ep.(*epochState)
				fn(fullMethod, epState)
			}
		}
	}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.maxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.maxSendMsgSize),
		grpc.StatsHandler(sh),
		grpc.UnaryInterceptor(s.unaryInterceptor),
		grpc.StreamInterceptor(s.streamInterceptor),
		grpc.UnknownServiceHandler(s.handleUnknownRPC),
	)

	handler := &dataPlaneHandler{srv: s}
	grpcSrv.RegisterService(&publisherDataPlaneDesc, handler)
	grpcSrv.RegisterService(&subscriberDataPlaneDesc, handler)
	s.grpcServer = grpcSrv

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	return s, nil
}

// Addr returns the listener host:port address (e.g. "127.0.0.1:12345").
func (s *Server) Addr() string {
	return s.addr
}

// Close stops the fake gRPC server, cancels active epoch streams and gates, and
// waits for all in-flight RPC goroutines to exit. Close is idempotent.
func (s *Server) Close() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	if s.closed.Swap(true) {
		return nil
	}

	s.mu.Lock()
	oldEp := s.epoch
	if oldEp != nil {
		oldEp.cancel()
	}
	for _, st := range s.streams {
		st.abort(status.Error(codes.Unavailable, "fake server closed"))
	}
	s.notifyLocked()
	s.mu.Unlock()

	if s.grpcServer != nil {
		stopDone := make(chan struct{})
		go func() {
			s.grpcServer.Stop()
			close(stopDone)
		}()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
	stopLoop:
		for {
			select {
			case <-stopDone:
				break stopLoop
			default:
			}
			select {
			case <-stopDone:
				break stopLoop
			case <-ticker.C:
				if s.connTracker != nil {
					for _, tc := range s.connTracker.activeConns() {
						_ = tc.Close()
					}
				}
			}
		}
	}
	if oldEp != nil {
		oldEp.wg.Wait()
	}
	return nil
}

// Reset cancels any active epoch streams and gates, clears all captured calls,
// rules, scripts, and counters, restores per-scenario defaults configured at
// NewServer construction (WithSuppressPong, WithDefaultSubscriptionProperties),
// and waits until all prior-epoch handler goroutines have exited before
// returning (closing stalled transport connections if a slow client blocks
// HTTP/2 flow control).
func (s *Server) Reset() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	if s.closed.Load() {
		return
	}

	s.mu.Lock()
	oldEp := s.epoch
	oldStreams := slices.Clone(s.streamOrder)
	if oldEp != nil {
		oldEp.cancel()
	}
	for _, st := range s.streams {
		st.abort(status.Error(codes.Unavailable, "fake server reset"))
	}

	epCtx, epCancel := context.WithCancel(context.Background())
	s.epoch = &epochState{ctx: epCtx, cancel: epCancel}
	s.calls = nil
	s.streams = make(map[string]*streamSession)
	s.streamOrder = nil
	s.readyStreams = nil
	s.seqCounter = 0
	s.streamIDCounter = 0
	s.publishAttempts = 0
	s.ackAttempts = 0
	s.modAckAttempts = 0
	s.pullAttempts = 0
	s.streamAttempts = 0
	s.activePublishTotal = 0
	s.activePublishByKey = make(map[string]int)
	s.activePublishCalls = make(map[*callRecord]struct{})
	s.maxPublishTotal = 0
	s.maxPublishByKey = make(map[string]int)
	s.publishRules = nil
	s.ackRules = nil
	s.modAckRules = nil
	s.pullRules = nil
	s.streamScripts = nil
	s.suppressPong = s.initialSuppressPong
	s.defaultSubProps = proto.CloneOf(s.initialDefaultSubProps)
	s.notifyLocked()
	s.mu.Unlock()

	if oldEp != nil {
		if s.testHooks != nil && s.testHooks.beforeResetWait != nil {
			s.testHooks.beforeResetWait()
		}
		if !waitWaitGroupTimeout(&oldEp.wg, 25*time.Millisecond) {
			if s.testHooks != nil && s.testHooks.onResetGraceExpired != nil {
				s.testHooks.onResetGraceExpired()
			}
			for _, st := range oldStreams {
				if !st.isClosed() && st.conn != nil {
					_ = st.conn.Close()
				}
			}
			if !waitWaitGroupTimeout(&oldEp.wg, 25*time.Millisecond) && s.connTracker != nil {
				for _, tc := range s.connTracker.activeConns() {
					_ = tc.Close()
				}
			}
			oldEp.wg.Wait()
		}
	}
}

// SetSuppressPong toggles server-wide suppression of keepalive pong responses.
func (s *Server) SetSuppressPong(suppress bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.suppressPong = suppress
}

// SetDefaultSubscriptionProperties sets default SubscriptionProperties emitted
// on newly opened StreamingPull streams when no script overrides them.
func (s *Server) SetDefaultSubscriptionProperties(eod, ordering bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultSubProps = &pubsubpb.StreamingPullResponse_SubscriptionProperties{
		ExactlyOnceDeliveryEnabled: eod,
		MessageOrderingEnabled:     ordering,
	}
}

// AddPublishRule registers a PublishRule in priority order (first matching
// active rule wins).
func (s *Server) AddPublishRule(rule PublishRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishRules = append(s.publishRules, &ruleEntry[PublishRule]{
		rule: rule,
		rem:  initialRuleRemaining(rule.Times),
	})
}

// AddAckRule registers an AckRule in priority order.
func (s *Server) AddAckRule(rule AckRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackRules = append(s.ackRules, &ruleEntry[AckRule]{
		rule: rule,
		rem:  initialRuleRemaining(rule.Times),
	})
}

// AddModAckRule registers a ModAckRule in priority order.
func (s *Server) AddModAckRule(rule ModAckRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modAckRules = append(s.modAckRules, &ruleEntry[ModAckRule]{
		rule: rule,
		rem:  initialRuleRemaining(rule.Times),
	})
}

// AddPullRule registers a PullRule in priority order.
func (s *Server) AddPullRule(rule PullRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pullRules = append(s.pullRules, &ruleEntry[PullRule]{
		rule: rule,
		rem:  initialRuleRemaining(rule.Times),
	})
}

// AddStreamingPullScript registers a StreamingPullScript in priority order.
func (s *Server) AddStreamingPullScript(script StreamingPullScript) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamScripts = append(s.streamScripts, &ruleEntry[StreamingPullScript]{
		rule: script,
		rem:  initialRuleRemaining(script.Times),
	})
}

func (s *Server) notifyLocked() {
	close(s.notifyCh)
	s.notifyCh = make(chan struct{})
}

func isAllowedUnaryDataPlane(fullMethod string) bool {
	switch fullMethod {
	case MethodPublish, MethodPull, MethodAcknowledge, MethodModifyAckDeadline:
		return true
	default:
		return false
	}
}

func isAllowedDataPlaneMethod(fullMethod string) bool {
	return isAllowedUnaryDataPlane(fullMethod) || fullMethod == MethodStreamingPull
}

func splitFullMethod(fullMethod string) (service, method string) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[:idx], trimmed[idx+1:]
	}
	return "", trimmed
}

func cloneMD(ctx context.Context, meta *rpcMeta) metadata.MD {
	if _, _, md := meta.snapshotHeader(); md != nil {
		return md
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok && md != nil {
		return md.Copy()
	}
	return nil
}

func (s *Server) recordTransportError(ctx context.Context, meta *rpcMeta, end *stats.End) {
	if meta == nil {
		return
	}
	compression, headerWireLen, hdrMD := meta.snapshotHeader()
	if hdrMD == nil {
		if md, ok := metadata.FromIncomingContext(ctx); ok && md != nil {
			hdrMD = md.Copy()
		}
	}
	payload, _ := meta.latestPayload()
	rawTel, tel, telErr := DecodeTelemetryHeader(hdrMD)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	svc, method := splitFullMethod(meta.fullMethod)
	startTime := end.BeginTime
	if startTime.IsZero() {
		startTime = meta.startTime
	}
	endTime := end.EndTime
	if endTime.IsZero() {
		endTime = time.Now()
	}
	errStr := ""
	if end.Error != nil {
		errStr = end.Error.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() || meta.ep != s.epoch {
		return
	}
	s.seqCounter++
	rec := &callRecord{
		c: CapturedCall{
			Seq:                s.seqCounter,
			Timestamp:          startTime,
			EndTime:            endTime,
			Duration:           endTime.Sub(startTime),
			Completed:          true,
			Service:            svc,
			Method:             method,
			FullMethod:         meta.fullMethod,
			CanceledByClient:   ctx.Err() != nil,
			Metadata:           hdrMD,
			RequestParams:      ExtractRequestParams(hdrMD),
			RawTelemetryHeader: rawTel,
			Telemetry:          tel,
			TelemetryDecodeErr: telErrStr,
			Compression:        compression,
			HeaderWireBytes:    headerWireLen,
			UncompressedBytes:  payload.UncompressedBytes,
			CompressedBytes:    payload.CompressedBytes,
			WireBytes:          payload.WireBytes,
			StatusCode:         status.Code(end.Error),
			StatusErr:          errStr,
			Unexpected:         !isAllowedDataPlaneMethod(meta.fullMethod),
			CodecError:         errStr,
		},
	}
	s.calls = append(s.calls, rec)
	s.notifyLocked()
}

func (s *Server) unaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	meta := rpcMetaFromContext(ctx)
	if meta != nil {
		meta.intercepted.Store(true)
	}
	compression, headerWireLen, _ := meta.snapshotHeader()
	payload, _ := meta.latestPayload()
	md := cloneMD(ctx, meta)
	rawTel, tel, telErr := DecodeTelemetryHeader(md)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	svc, method := splitFullMethod(info.FullMethod)
	unexpected := !isAllowedUnaryDataPlane(info.FullMethod)

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "fake server closed")
	}
	if meta != nil && meta.ep != nil && meta.ep != s.epoch {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "fake server reset")
	}
	ep := s.epoch
	ep.wg.Add(1)

	s.seqCounter++
	c := CapturedCall{
		Seq:                s.seqCounter,
		Timestamp:          time.Now(),
		Service:            svc,
		Method:             method,
		FullMethod:         info.FullMethod,
		Metadata:           md,
		RequestParams:      ExtractRequestParams(md),
		RawTelemetryHeader: rawTel,
		Telemetry:          tel,
		TelemetryDecodeErr: telErrStr,
		Compression:        compression,
		HeaderWireBytes:    headerWireLen,
		UncompressedBytes:  payload.UncompressedBytes,
		CompressedBytes:    payload.CompressedBytes,
		WireBytes:          payload.WireBytes,
		Unexpected:         unexpected,
	}

	var publishKeys []string
	switch r := req.(type) {
	case *pubsubpb.PublishRequest:
		c.PublishReq = proto.CloneOf(r)
		s.activePublishTotal++
		c.InFlightAtStart = s.activePublishTotal
		if s.activePublishTotal > s.maxPublishTotal {
			s.maxPublishTotal = s.activePublishTotal
		}
		c.MaxConcurrent = s.activePublishTotal
		publishKeys = uniquePublishOrderingKeys(r.GetMessages())
		for _, k := range publishKeys {
			s.activePublishByKey[k]++
			cur := s.activePublishByKey[k]
			if cur > c.KeyInFlightAtStart {
				c.KeyInFlightAtStart = cur
			}
			if cur > s.maxPublishByKey[k] {
				s.maxPublishByKey[k] = cur
			}
		}
		c.KeyMaxConcurrent = c.KeyInFlightAtStart
		c.ConcurrentKeysAtStart = len(s.activePublishByKey)
	case *pubsubpb.PullRequest:
		c.PullReq = proto.CloneOf(r)
	case *pubsubpb.AcknowledgeRequest:
		c.AckReq = proto.CloneOf(r)
	case *pubsubpb.ModifyAckDeadlineRequest:
		c.ModAckReq = proto.CloneOf(r)
	default:
		if pm, ok := req.(proto.Message); ok && pm != nil {
			c.RawReq = proto.Clone(pm)
		}
	}

	rec := &callRecord{c: c}
	if c.PublishReq != nil {
		for activeRec := range s.activePublishCalls {
			if s.activePublishTotal > activeRec.c.MaxConcurrent {
				activeRec.c.MaxConcurrent = s.activePublishTotal
			}
			for _, k := range publishKeys {
				if cur := s.activePublishByKey[k]; cur > activeRec.c.KeyMaxConcurrent {
					// Check if activeRec actually carries key k.
					if activeRecHasKey(activeRec.c.PublishReq, k) {
						activeRec.c.KeyMaxConcurrent = cur
					}
				}
			}
		}
		s.activePublishCalls[rec] = struct{}{}
	}
	s.calls = append(s.calls, rec)
	s.notifyLocked()
	s.mu.Unlock()

	var resp any
	var handlerErr error
	defer func() {
		endTime := time.Now()
		canceled := ctx.Err() != nil
		var errStr string
		var eiDomain, eiReason string
		var eodFailures map[string]string
		if handlerErr != nil {
			errStr = handlerErr.Error()
			eiDomain, eiReason, eodFailures = extractErrorInfo(handlerErr)
		}

		s.mu.Lock()
		if s.epoch == ep {
			if c.PublishReq != nil {
				s.activePublishTotal--
				if s.activePublishTotal < 0 {
					s.activePublishTotal = 0
				}
				for _, k := range publishKeys {
					s.activePublishByKey[k]--
					if s.activePublishByKey[k] <= 0 {
						delete(s.activePublishByKey, k)
					}
				}
				delete(s.activePublishCalls, rec)
			}
			rec.c.EndTime = endTime
			rec.c.Duration = endTime.Sub(rec.c.Timestamp)
			rec.c.Completed = true
			rec.c.CanceledByClient = canceled
			rec.c.StatusCode = status.Code(handlerErr)
			rec.c.StatusErr = errStr
			rec.c.ResponseErrorInfoDomain = eiDomain
			rec.c.ResponseErrorInfoReason = eiReason
			rec.c.ResponseEODFailures = eodFailures
			if pr, ok := resp.(*pubsubpb.PublishResponse); ok && pr != nil {
				rec.c.ResponseMessageIDs = slices.Clone(pr.GetMessageIds())
			}
			s.notifyLocked()
		}
		s.mu.Unlock()
		ep.wg.Done()
	}()

	if unexpected {
		handlerErr = status.Errorf(codes.Unimplemented, "unexpected RPC %s not implemented", info.FullMethod)
		return nil, handlerErr
	}

	callCtx := context.WithValue(ctx, activeCallKey{}, &activeCallContext{ep: ep, rec: rec})
	resp, handlerErr = handler(callCtx, req)
	return resp, handlerErr
}

func activeRecHasKey(req *pubsubpb.PublishRequest, key string) bool {
	if req == nil || key == "" {
		return false
	}
	for _, m := range req.GetMessages() {
		if m.GetOrderingKey() == key {
			return true
		}
	}
	return false
}

func (s *Server) streamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if meta := rpcMetaFromContext(ss.Context()); meta != nil {
		meta.intercepted.Store(true)
	}
	return handler(srv, ss)
}

func (s *Server) handleUnknownRPC(_ any, stream grpc.ServerStream) error {
	ctx := stream.Context()
	meta := rpcMetaFromContext(ctx)
	if meta != nil {
		meta.intercepted.Store(true)
	}
	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok && meta != nil {
		fullMethod = meta.fullMethod
	}
	compression, headerWireLen, _ := meta.snapshotHeader()
	md := cloneMD(ctx, meta)
	rawTel, tel, telErr := DecodeTelemetryHeader(md)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	svc, method := splitFullMethod(fullMethod)
	err := status.Errorf(codes.Unimplemented, "unexpected RPC %s not implemented", fullMethod)
	now := time.Now()

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server closed")
	}
	if meta != nil && meta.ep != nil && meta.ep != s.epoch {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server reset")
	}
	ep := s.epoch
	ep.wg.Add(1)
	defer ep.wg.Done()

	if s.epoch == ep && !s.closed.Load() {
		s.seqCounter++
		rec := &callRecord{
			c: CapturedCall{
				Seq:                s.seqCounter,
				Timestamp:          now,
				EndTime:            now,
				Completed:          true,
				Service:            svc,
				Method:             method,
				FullMethod:         fullMethod,
				CanceledByClient:   ctx.Err() != nil,
				Metadata:           md,
				RequestParams:      ExtractRequestParams(md),
				RawTelemetryHeader: rawTel,
				Telemetry:          tel,
				TelemetryDecodeErr: telErrStr,
				Compression:        compression,
				HeaderWireBytes:    headerWireLen,
				StatusCode:         codes.Unimplemented,
				StatusErr:          err.Error(),
				Unexpected:         true,
			},
		}
		s.calls = append(s.calls, rec)
		s.notifyLocked()
	}
	s.mu.Unlock()
	if s.testHooks != nil && s.testHooks.afterUnknownUnlock != nil {
		s.testHooks.afterUnknownUnlock(ep)
	}
	return err
}

func extractErrorInfo(err error) (domain, reason string, eodFailures map[string]string) {
	if err == nil {
		return "", "", nil
	}
	st, ok := status.FromError(err)
	if !ok || st == nil {
		return "", "", nil
	}
	for _, d := range st.Details() {
		ei, ok := d.(*errdetails.ErrorInfo)
		if !ok || ei == nil {
			continue
		}
		if domain == "" && reason == "" {
			domain = ei.GetDomain()
			reason = ei.GetReason()
		}
		if ei.GetDomain() == EODDomainPubSub && isEODErrorInfoReason(ei.GetReason()) {
			domain = ei.GetDomain()
			reason = ei.GetReason()
			if eodFailures == nil {
				eodFailures = make(map[string]string, len(ei.GetMetadata()))
			}
			maps.Copy(eodFailures, ei.GetMetadata())
		}
	}
	return domain, reason, eodFailures
}

// dataPlaneHandler embeds Unimplemented*Server only to satisfy grpc.ServiceDesc
// HandlerType checks while registering strictly the 5 data-plane RPCs via custom
// ServiceDesc descriptors.
type dataPlaneHandler struct {
	pubsubpb.UnimplementedPublisherServer
	pubsubpb.UnimplementedSubscriberServer
	srv *Server
}

func (h *dataPlaneHandler) Publish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	return h.srv.handlePublish(ctx, req)
}

func (h *dataPlaneHandler) Pull(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	return h.srv.handlePull(ctx, req)
}

func (h *dataPlaneHandler) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	return h.srv.handleAcknowledge(ctx, req)
}

func (h *dataPlaneHandler) ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	return h.srv.handleModifyAckDeadline(ctx, req)
}

func (h *dataPlaneHandler) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	return h.srv.handleStreamingPull(stream)
}

func _dataPlane_Publish_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(pubsubpb.PublishRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(pubsubpb.PublisherServer).Publish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MethodPublish,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(pubsubpb.PublisherServer).Publish(ctx, req.(*pubsubpb.PublishRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _dataPlane_ModifyAckDeadline_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(pubsubpb.ModifyAckDeadlineRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(pubsubpb.SubscriberServer).ModifyAckDeadline(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MethodModifyAckDeadline,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(pubsubpb.SubscriberServer).ModifyAckDeadline(ctx, req.(*pubsubpb.ModifyAckDeadlineRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _dataPlane_Acknowledge_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(pubsubpb.AcknowledgeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(pubsubpb.SubscriberServer).Acknowledge(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MethodAcknowledge,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(pubsubpb.SubscriberServer).Acknowledge(ctx, req.(*pubsubpb.AcknowledgeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _dataPlane_Pull_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(pubsubpb.PullRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(pubsubpb.SubscriberServer).Pull(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MethodPull,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(pubsubpb.SubscriberServer).Pull(ctx, req.(*pubsubpb.PullRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _dataPlane_StreamingPull_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(pubsubpb.SubscriberServer).StreamingPull(&grpc.GenericServerStream[pubsubpb.StreamingPullRequest, pubsubpb.StreamingPullResponse]{ServerStream: stream})
}

var publisherDataPlaneDesc = grpc.ServiceDesc{
	ServiceName: "google.pubsub.v1.Publisher",
	HandlerType: (*pubsubpb.PublisherServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Publish",
			Handler:    _dataPlane_Publish_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "google/pubsub/v1/pubsub.proto",
}

var subscriberDataPlaneDesc = grpc.ServiceDesc{
	ServiceName: "google.pubsub.v1.Subscriber",
	HandlerType: (*pubsubpb.SubscriberServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ModifyAckDeadline",
			Handler:    _dataPlane_ModifyAckDeadline_Handler,
		},
		{
			MethodName: "Acknowledge",
			Handler:    _dataPlane_Acknowledge_Handler,
		},
		{
			MethodName: "Pull",
			Handler:    _dataPlane_Pull_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamingPull",
			Handler:       _dataPlane_StreamingPull_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "google/pubsub/v1/pubsub.proto",
}

func activeCallEpoch(ctx context.Context, fallback *epochState) *epochState {
	if ac, ok := ctx.Value(activeCallKey{}).(*activeCallContext); ok && ac.ep != nil {
		return ac.ep
	}
	return fallback
}

// checkActiveEpochLocked requires s.mu to be held. If the server is closed or
// s.epoch != ep, it unlocks s.mu and returns a non-nil Unavailable status error.
func (s *Server) checkActiveEpochLocked(ep *epochState) error {
	if s.closed.Load() {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server closed")
	}
	if s.epoch != ep {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server reset")
	}
	return nil
}

func (s *Server) handlePublish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	if s.maxPublishRequestBytes > 0 && proto.Size(req) > s.maxPublishRequestBytes {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"publish request size %d exceeds max_publish_request_bytes %d",
			proto.Size(req),
			s.maxPublishRequestBytes,
		)
	}

	md, _ := metadata.FromIncomingContext(ctx)
	_, tel, _ := DecodeTelemetryHeader(md)
	clonedReq := proto.CloneOf(req)

	if s.testHooks != nil && s.testHooks.beforeInnerAttempt != nil {
		s.testHooks.beforeInnerAttempt(MethodPublish, activeCallEpoch(ctx, nil))
	}

	var matched *PublishRule
	var ep *epochState

	s.mu.Lock()
	ep = activeCallEpoch(ctx, s.epoch)
	if err := s.checkActiveEpochLocked(ep); err != nil {
		return nil, err
	}
	s.publishAttempts++
	mc := &PublishMatchContext{
		Attempt:   s.publishAttempts,
		Request:   clonedReq,
		Metadata:  md.Copy(),
		Telemetry: tel,
	}
	for _, entry := range s.publishRules {
		if !entry.active() {
			continue
		}
		if entry.rule.Match == nil || entry.rule.Match(mc) {
			entry.consume()
			r := entry.rule
			matched = &r
			break
		}
	}
	s.mu.Unlock()

	if matched != nil {
		if matched.Gate != nil {
			if err := matched.Gate.wait(ctx, ep.ctx); err != nil {
				return nil, err
			}
		}
		if err := waitRuleDelay(ctx, ep.ctx, matched.Delay); err != nil {
			return nil, err
		}
		if matched.Err != nil {
			return nil, matched.Err
		}
		if matched.ResponseFunc != nil {
			if ep.ctx.Err() != nil {
				return nil, status.Error(codes.Unavailable, "fake server reset or closed")
			}
			return matched.ResponseFunc(ctx, proto.CloneOf(req))
		}
		if matched.Response != nil {
			return proto.CloneOf(matched.Response), nil
		}
		if matched.MessageIDs != nil {
			return &pubsubpb.PublishResponse{MessageIds: slices.Clone(matched.MessageIDs)}, nil
		}
		if matched.MessageIDCountDelta != 0 {
			count := max(len(req.GetMessages())+matched.MessageIDCountDelta, 0)
			ids := make([]string, count)
			for i := range ids {
				ids[i] = fmt.Sprintf("msg-%d", ep.msgIDSeq.Add(1))
			}
			return &pubsubpb.PublishResponse{MessageIds: ids}, nil
		}
	}

	ids := make([]string, len(req.GetMessages()))
	for i := range ids {
		ids[i] = fmt.Sprintf("msg-%d", ep.msgIDSeq.Add(1))
	}
	return &pubsubpb.PublishResponse{MessageIds: ids}, nil
}

func (s *Server) handleAcknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	clonedReq := proto.CloneOf(req)

	if s.testHooks != nil && s.testHooks.beforeInnerAttempt != nil {
		s.testHooks.beforeInnerAttempt(MethodAcknowledge, activeCallEpoch(ctx, nil))
	}

	var matched *AckRule
	var ep *epochState

	s.mu.Lock()
	ep = activeCallEpoch(ctx, s.epoch)
	if err := s.checkActiveEpochLocked(ep); err != nil {
		return nil, err
	}
	s.ackAttempts++
	mc := &AckMatchContext{
		Attempt:  s.ackAttempts,
		Request:  clonedReq,
		Metadata: md.Copy(),
	}
	for _, entry := range s.ackRules {
		if !entry.active() {
			continue
		}
		if entry.rule.Match == nil || entry.rule.Match(mc) {
			entry.consume()
			r := entry.rule
			matched = &r
			break
		}
	}
	s.mu.Unlock()

	if matched != nil {
		if matched.Gate != nil {
			if err := matched.Gate.wait(ctx, ep.ctx); err != nil {
				return nil, err
			}
		}
		if err := waitRuleDelay(ctx, ep.ctx, matched.Delay); err != nil {
			return nil, err
		}
		if matched.EOD != nil {
			if ep.ctx.Err() != nil {
				return nil, status.Error(codes.Unavailable, "fake server reset or closed")
			}
			if err := matched.EOD.evaluate(req.GetAckIds(), EODReasonAckFailed); err != nil {
				return nil, err
			}
		}
		if matched.Err != nil {
			return nil, matched.Err
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) handleModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	clonedReq := proto.CloneOf(req)

	if s.testHooks != nil && s.testHooks.beforeInnerAttempt != nil {
		s.testHooks.beforeInnerAttempt(MethodModifyAckDeadline, activeCallEpoch(ctx, nil))
	}

	var matched *ModAckRule
	var ep *epochState

	s.mu.Lock()
	ep = activeCallEpoch(ctx, s.epoch)
	if err := s.checkActiveEpochLocked(ep); err != nil {
		return nil, err
	}
	s.modAckAttempts++
	mc := &ModAckMatchContext{
		Attempt:  s.modAckAttempts,
		Request:  clonedReq,
		Metadata: md.Copy(),
	}
	for _, entry := range s.modAckRules {
		if !entry.active() {
			continue
		}
		if entry.rule.Match == nil || entry.rule.Match(mc) {
			entry.consume()
			r := entry.rule
			matched = &r
			break
		}
	}
	s.mu.Unlock()

	if matched != nil {
		if matched.Gate != nil {
			if err := matched.Gate.wait(ctx, ep.ctx); err != nil {
				return nil, err
			}
		}
		if err := waitRuleDelay(ctx, ep.ctx, matched.Delay); err != nil {
			return nil, err
		}
		if matched.EOD != nil {
			if ep.ctx.Err() != nil {
				return nil, status.Error(codes.Unavailable, "fake server reset or closed")
			}
			if err := matched.EOD.evaluate(req.GetAckIds(), EODReasonModAckFailed); err != nil {
				return nil, err
			}
		}
		if matched.Err != nil {
			return nil, matched.Err
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) handlePull(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	clonedReq := proto.CloneOf(req)

	if s.testHooks != nil && s.testHooks.beforeInnerAttempt != nil {
		s.testHooks.beforeInnerAttempt(MethodPull, activeCallEpoch(ctx, nil))
	}

	var matched *PullRule
	var ep *epochState

	s.mu.Lock()
	ep = activeCallEpoch(ctx, s.epoch)
	if err := s.checkActiveEpochLocked(ep); err != nil {
		return nil, err
	}
	s.pullAttempts++
	mc := &PullMatchContext{
		Attempt:  s.pullAttempts,
		Request:  clonedReq,
		Metadata: md.Copy(),
	}
	for _, entry := range s.pullRules {
		if !entry.active() {
			continue
		}
		if entry.rule.Match == nil || entry.rule.Match(mc) {
			entry.consume()
			r := entry.rule
			matched = &r
			break
		}
	}
	s.mu.Unlock()

	if matched != nil {
		if matched.Gate != nil {
			if err := matched.Gate.wait(ctx, ep.ctx); err != nil {
				return nil, err
			}
		}
		if err := waitRuleDelay(ctx, ep.ctx, matched.Delay); err != nil {
			return nil, err
		}
		if matched.Err != nil {
			return nil, matched.Err
		}
		if matched.ResponseFunc != nil {
			if ep.ctx.Err() != nil {
				return nil, status.Error(codes.Unavailable, "fake server reset or closed")
			}
			return matched.ResponseFunc(ctx, proto.CloneOf(req))
		}
		if matched.Response != nil {
			return proto.CloneOf(matched.Response), nil
		}
	}
	return &pubsubpb.PullResponse{}, nil
}
