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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const m3cMockMaxBytes = 32 << 20

type m3cMockConfig struct {
	profile, fault string
	slowKA         bool
	capsOverride   func(*proxypb.GetCapabilitiesResponse)
}
type m3cMockServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg    m3cMockConfig
	caps   *proxypb.GetCapabilitiesResponse
	srvCtx context.Context
	seq    atomic.Int64
	wg     sync.WaitGroup
}
type m3cMockHandle struct {
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	mock   *m3cMockServer
	cancel context.CancelFunc
	exited chan error
	once   sync.Once
}

func (h *m3cMockHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *m3cMockHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.mock.caps }
func (h *m3cMockHandle) Exited() <-chan error                           { return h.exited }
func (h *m3cMockHandle) Stderr() string                                 { return "" }
func (h *m3cMockHandle) PID() int                                       { return 0 }
func (h *m3cMockHandle) Close(context.Context) error {
	h.once.Do(func() { h.cancel(); _ = h.conn.Close(); h.srv.Stop(); h.mock.wg.Wait() })
	return nil
}
func m3cMockDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(m3cMockMaxBytes), grpc.MaxCallSendMsgSize(m3cMockMaxBytes)))
}
func m3cMockCapabilities(cfg m3cMockConfig) *proxypb.GetCapabilitiesResponse {
	lang := strings.ToLower(cfg.profile)
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage: lang, ClientLibrary: "pubsub-" + lang, ClientVersion: "2.0.0",
		SupportsMonitoring: true, SupportsShutdownBehavior: true, SupportsShutdownTimeout: true,
		Defaults: &proxypb.ClientDefaults{
			SubscriberMaxOutstandingMessages: 1000, SubscriberMaxOutstandingBytes: 100 << 20, SubscriberMaxExtensionMs: 40,
			SubscriberMinAckExtensionMs: 10_000, SubscriberMaxAckExtensionMs: 15_000, SubscriberDefaultStreamAckDeadlineSeconds: 60,
			KeepalivePingIntervalMs: 25, KeepalivePongTimeoutMs: 80, RetryInitialDelayMs: 5, RetryMaxDelayMs: 20, RetryDelayMultiplier: 1.5,
		},
	}
	if s, ok := map[string][4]bool{
		"go": {true, false, true, false}, "cpp": {true, true, false, true}, "rust": {false, false, true, true},
		"java": {true, true, true, false}, "python": {true, true, false, false}, "ruby": {true, true, true, false},
		"node": {true, true, true, false}, "dotnet": {true, true, true, true}, "php": {true, true, false, false},
	}[lang]; ok {
		caps.SupportsMonitoring, caps.SupportsUnaryPull, caps.SupportsKeepalive, caps.SupportsAutoNackOnDrop = s[0], s[1], s[2], s[3]
		if !s[2] {
			caps.Defaults.KeepalivePingIntervalMs, caps.Defaults.KeepalivePongTimeoutMs = 0, 0
		} else if cfg.slowKA {
			caps.Defaults.KeepalivePingIntervalMs, caps.Defaults.KeepalivePongTimeoutMs = 250, 1000
		}
	}
	if cfg.capsOverride != nil {
		cfg.capsOverride(caps)
	}
	return caps
}
func m3cMockConnect(cfg m3cMockConfig) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srv := grpc.NewServer(grpc.MaxRecvMsgSize(m3cMockMaxBytes), grpc.MaxSendMsgSize(m3cMockMaxBytes))
		srvCtx, srvCancel := context.WithCancel(context.Background())
		mock := &m3cMockServer{cfg: cfg, caps: m3cMockCapabilities(cfg), srvCtx: srvCtx}
		proxypb.RegisterPubSubProxyServer(srv, mock)
		exited := make(chan error, 1)
		go func() { exited <- srv.Serve(lis) }()
		conn, err := m3cMockDial(lis.Addr().String())
		if err != nil {
			srvCancel()
			srv.Stop()
			return nil, err
		}
		return &m3cMockHandle{srv: srv, conn: conn, client: proxypb.NewPubSubProxyClient(conn), mock: mock, cancel: srvCancel, exited: exited}, nil
	}
}
func (m *m3cMockServer) hasFault(f string) bool { return m.cfg.fault == f }
func (m *m3cMockServer) outCtx(ctx context.Context, sub string) context.Context {
	val := map[bool]string{true: sub, false: url.QueryEscape(sub)}[strings.EqualFold(m.caps.GetClientLanguage(), "rust")]
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "subscription="+val)
}
func (m *m3cMockServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return m.caps, nil
}
func (m *m3cMockServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *m3cMockServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *m3cMockServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	if m.hasFault("emit-unexpected-span") {
		return &proxypb.DrainTracesResponse{Spans: []*proxypb.TraceSpan{{TraceId: "0123456789abcdef0123456789abcdef", SpanId: "0123456789abcdef", TraceFlags: 1, Name: "unexpected", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL, StartTime: timestamppb.Now(), EndTime: timestamppb.Now()}}}, nil
	}
	return &proxypb.DrainTracesResponse{}, nil
}

type m3cMockEntry struct {
	tok, ackID                                         string
	maxExtAt, nextExtAt                                time.Time
	hasMaxExt, underLease, liveHandler, settled, fcRel bool
}
type m3cMockSub struct {
	m                                       *m3cMockServer
	stream                                  proxypb.PubSubProxy_SubscriberSessionServer
	client                                  pubsubpb.SubscriberClient
	sub, fatalM                             string
	start                                   *proxypb.StartSubscriberCommand
	sessCtx, pullCtx, stopCtx               context.Context
	cancel, pullFn, stopFn                  context.CancelFunc
	sendCh                                  chan *proxypb.SubscriberEvent
	flushCh                                 chan struct{}
	leaseRPCMu, mu                          sync.Mutex
	cond                                    *sync.Cond
	leases                                  map[string]*m3cMockEntry
	delayedExt                              map[string]time.Time
	deliverQ                                []*pubsubpb.ReceivedMessage
	pendAck, pendNack                       []string
	fcMax, fcUsed                           int64
	activeLeases, activeHandlers, streamGen int
	nextRetain, nextKA                      time.Time
	stopping                                bool
	fatalC                                  int32
	curStreamCancel                         context.CancelFunc
	curStreamClose                          func()
	curStreamDone                           <-chan struct{}
	stopRequested, reopenAsync              atomic.Bool
	delivCnt                                atomic.Int64
	lastAcked                               atomic.Value
}

func (s *m3cMockSub) emit(ev *proxypb.SubscriberEvent) {
	select {
	case s.sendCh <- ev:
	case <-s.sessCtx.Done():
	}
}
func (s *m3cMockSub) triggerFlush() {
	select {
	case s.flushCh <- struct{}{}:
	default:
	}
}
func (s *m3cMockSub) checkDrainedLocked() {
	if s.stopping && s.activeHandlers == 0 && (!strings.EqualFold(s.m.caps.GetClientLanguage(), "go") || s.activeLeases == 0) {
		s.stopFn()
	}
}
func (s *m3cMockSub) triggerStop(graceful bool) {
	s.stopRequested.Store(true)
	s.pullFn()
	s.mu.Lock()
	s.stopping = true
	lang := s.m.caps.GetClientLanguage()
	shouldWait := graceful && !strings.EqualFold(lang, "python") && (s.activeHandlers > 0 || (strings.EqualFold(lang, "go") && s.activeLeases > 0))
	for _, e := range s.leases {
		if !graceful && e.underLease {
			e.underLease, s.activeLeases = false, s.activeLeases-1
			s.pendNack = append(s.pendNack, e.ackID)
		}
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	if !shouldWait {
		s.stopFn()
	}
}
func (s *m3cMockSub) fatalStop(code int32, msg string) {
	s.mu.Lock()
	if s.fatalC == 0 {
		s.fatalC, s.fatalM = code, msg
	}
	s.mu.Unlock()
	s.triggerStop(false)
}
func (s *m3cMockSub) releaseFCLocked(e *m3cMockEntry) {
	if s.fcMax > 0 && !e.fcRel {
		e.fcRel = true
		s.fcUsed = max(s.fcUsed-1, 0)
		s.cond.Broadcast()
	}
}
func (s *m3cMockSub) acquireFC(ctx context.Context) bool {
	if s.fcMax <= 0 || s.m.hasFault("pull03b-ignores-flow-control") {
		return true
	}
	defer context.AfterFunc(ctx, func() { s.mu.Lock(); s.cond.Broadcast(); s.mu.Unlock() })()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.fcUsed >= s.fcMax && !s.stopping && ctx.Err() == nil {
		s.cond.Wait()
	}
	if s.stopping || ctx.Err() != nil {
		return false
	}
	s.fcUsed++
	return true
}
func (s *m3cMockSub) clampAckSec(forStream bool) int32 {
	if dl, ok := map[string]int32{"lease02-nacks-while-extending": 0, "lease03-sub10-tail": 5, "lease05-modack-1": 1}[s.m.cfg.fault]; !forStream && ok {
		return dl
	}
	switch {
	case s.m.hasFault("lease02-deadline-below-10") && s.start.GetMinDurationPerAckExtensionMs() > 0 && s.start.GetMinDurationPerAckExtensionMs() < 10_000:
		return 5
	case (s.m.hasFault("lease02-deadline-above-600") || (!forStream && s.m.hasFault("lease02-modack-above-600"))) && s.start.GetMaxDurationPerAckExtensionMs() > 600_000:
		return 700
	case s.m.hasFault("lease03-negative-dl"):
		return -1
	}
	dlMs := cmp.Or(max(s.start.GetMaxDurationPerAckExtensionMs(), 0), 60_000)
	if mn := s.start.GetMinDurationPerAckExtensionMs(); mn > 0 && mn < dlMs {
		dlMs = mn
	}
	return min(max(int32(dlMs/1000), 10), 600)
}
func (s *m3cMockSub) isRetryable(err error, hadResp bool) bool {
	if errors.Is(err, io.EOF) {
		return !s.m.hasFault("pull02b-fails-on-abort") || !hadResp
	}
	st, ok := status.FromError(err)
	if !ok || (s.m.hasFault("pull02b-fails-on-abort") && hadResp) {
		return false
	}
	lang := strings.ToLower(s.m.caps.GetClientLanguage())
	switch st.Code() {
	case codes.Unavailable:
		if strings.Contains(st.Message(), "Server shutdownNow invoked") {
			return s.m.hasFault("pull02d-go-java-retries-shutdownnow") || (!s.m.hasFault("pull02d-peer-fails-shutdownnow") && lang != "go" && lang != "java")
		}
		return !s.m.hasFault("pull02-no-retry-transient")
	case codes.Internal, codes.Aborted:
		return !s.m.hasFault("pull02-no-retry-transient")
	case codes.ResourceExhausted:
		return s.m.hasFault("pull02c-go-retries-resexhausted") || (!s.m.hasFault("pull02c-peer-terminates-resexhausted") && !(lang == "go" && hadResp))
	default:
		return s.m.hasFault("pull02-retries-permanent")
	}
}
func m3cChunkAckIDs(sub string, ids []string, isAck bool, dl int32, no1000, no512K bool) (out [][]string) {
	if len(ids) == 0 {
		return nil
	}
	base := proto.Size(&pubsubpb.AcknowledgeRequest{Subscription: sub})
	if !isAck && dl != 0 {
		base = proto.Size(&pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckDeadlineSeconds: dl})
	}
	cur, start := base, 0
	for i, id := range ids {
		entry := 1 + protowire.SizeBytes(len(id))
		if i > start && ((!no1000 && i-start >= fake.MaxAckIDsPerRequest) || (!no512K && cur+entry > fake.MaxAckRequestBytes)) {
			out, cur, start = append(out, ids[start:i]), base, i
		}
		cur += entry
	}
	return append(out, ids[start:])
}
func (m *m3cMockServer) ackMetaCtx(ctx context.Context, sub string, isAck bool) (string, context.Context) {
	callCtx := m.outCtx(ctx, sub)
	if isAck && m.hasFault("wrong-ack-sub") {
		sub += "-wrong"
	}
	if isAck && m.hasFault("missing-ack-req-params") {
		callCtx = ctx
	}
	return sub, callCtx
}
func (s *m3cMockSub) sendBatch(ids []string, isNack bool) {
	if len(ids) == 0 || (isNack && s.m.hasFault("lease01-drops-phase-a-nack") && strings.Contains(s.sub, "lease01-a-")) || (!isNack && s.m.hasFault("lease01-drops-ack")) {
		return
	}
	if isNack && s.m.hasFault("lease01-acks-instead-of-nack") {
		isNack = false
	}
	sub, callCtx := s.m.ackMetaCtx(s.sessCtx, s.sub, !isNack)
	if !isNack && s.m.hasFault("lease01-empty-ack-req") {
		_, _ = s.client.Acknowledge(callCtx, &pubsubpb.AcknowledgeRequest{Subscription: sub})
		return
	}
	no1000 := (isNack && s.m.hasFault("lease01-no-split-nack-1000")) || (!isNack && s.m.hasFault("lease01-no-split-ack-1000"))
	for _, chunk := range m3cChunkAckIDs(sub, ids, !isNack, 0, no1000, !isNack && s.m.hasFault("lease01-exceeds-512kb")) {
		if s.m.hasFault("lease04-closes-before-stop") && !isNack && slices.Contains(chunk, "lease04-m2") {
			s.mu.Lock()
			closeFn, doneCh := s.curStreamClose, s.curStreamDone
			s.mu.Unlock()
			if closeFn != nil {
				closeFn()
				select {
				case <-doneCh:
				case <-s.pullCtx.Done():
				}
			}
		}
		var rpcErr error
		if isNack {
			_, rpcErr = s.client.ModifyAckDeadline(callCtx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: s.sub, AckIds: chunk, AckDeadlineSeconds: 0})
		} else {
			_, rpcErr = s.client.Acknowledge(callCtx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: chunk})
		}
		if rpcErr != nil && !s.start.UseExactlyOnce && !isNack {
			if s.m.hasFault("lease04-crashes-on-ack-err") {
				s.fatalStop(int32(status.Code(rpcErr)), rpcErr.Error())
				return
			}
			if s.m.hasFault("lease04-reopens-stream") {
				s.reopenAsync.Store(true)
				s.mu.Lock()
				wantGen := s.streamGen + 1
				if s.curStreamCancel != nil {
					s.curStreamCancel()
				}
				for s.streamGen < wantGen && !s.stopping && s.pullCtx.Err() == nil && s.sessCtx.Err() == nil {
					s.cond.Wait()
				}
				s.mu.Unlock()
			}
		} else if !isNack && rpcErr == nil {
			s.lastAcked.Store(chunk[len(chunk)-1])
			if s.m.hasFault("spurious-nack-after-ack") {
				_ = s.sendModAckRPC(chunk, 0)
			}
			if s.m.hasFault("lease02-async-preack-race") && slices.Contains(chunk, "ack-lease02-1") {
				wCtx, wCancel := context.WithTimeout(s.stopCtx, 12*time.Millisecond)
				<-wCtx.Done()
				wCancel()
				if s.stopCtx.Err() == nil {
					_ = s.sendModAckRPC(chunk, 15)
				}
			}
			if s.m.hasFault("lease02-phaseb-post-ack-modack") && strings.Contains(s.sub, "lease02-low-") {
				_ = s.sendModAckRPC([]string{"ack-lease02-1"}, 15)
			}
		}
	}
}
func (m *m3cMockServer) mockAttempt(rm *pubsubpb.ReceivedMessage, unary bool) (int32, bool) {
	att := rm.GetDeliveryAttempt()
	if m.hasFault("dlq01-unset-becomes-positive") && att == 0 {
		return 1, true
	}
	if unary && m.hasFault("dlq01-wrong-unary-attempt") && att > 0 {
		return att + 1, true
	}
	if m.hasFault("dlq01-drops-positive") && att > 0 {
		return 0, false
	}
	return att, att > 0
}
func (s *m3cMockSub) noAutoSplit() bool {
	return s.m.hasFault("lease01-no-split-ack-1000") || s.m.hasFault("lease01-no-split-nack-1000") || s.m.hasFault("lease01-exceeds-512kb")
}
func (s *m3cMockSub) sendModAckRPC(ids []string, dl int32) error {
	_, err := s.client.ModifyAckDeadline(s.m.outCtx(s.sessCtx, s.sub), &pubsubpb.ModifyAckDeadlineRequest{Subscription: s.sub, AckIds: ids, AckDeadlineSeconds: dl})
	return err
}
func (s *m3cMockSub) runSettleLoop(done <-chan struct{}) {
	tk := time.NewTicker(map[bool]time.Duration{true: 10 * time.Second, false: 15 * time.Millisecond}[s.noAutoSplit()])
	defer tk.Stop()
	flush := func() {
		s.mu.Lock()
		acks, nacks := s.pendAck, s.pendNack
		s.pendAck, s.pendNack = nil, nil
		s.mu.Unlock()
		if len(acks) > 0 || len(nacks) > 0 {
			s.leaseRPCMu.Lock()
			s.sendBatch(acks, false)
			s.sendBatch(nacks, true)
			s.leaseRPCMu.Unlock()
		}
	}
	for {
		select {
		case <-done:
			flush()
			return
		case <-s.flushCh:
		case <-tk.C:
		}
		flush()
	}
}
func m3cAdvanceTick(now time.Time, next *time.Time, per time.Duration) bool {
	if now.Before(*next) {
		return false
	}
	for !now.Before(*next) {
		*next = next.Add(per)
	}
	return true
}
func (s *m3cMockSub) stepLease(now time.Time, lang string, retPer time.Duration) bool {
	s.leaseRPCMu.Lock()
	defer s.leaseRPCMu.Unlock()
	s.mu.Lock()
	var extIDs, javaTailIDs []string
	var addedNack bool
	extForever, noPreExt := s.m.hasFault("lease03-extends-forever"), s.m.hasFault("lease03-no-pre-expiry-modack")
	delayOff := map[bool]time.Duration{true: 4500 * time.Millisecond, false: 120 * time.Millisecond}[s.start.GetMaxExtensionMs() >= 1500]
	rustDue, kaDue := m3cAdvanceTick(now, &s.nextRetain, retPer), m3cAdvanceTick(now, &s.nextKA, 100*time.Millisecond)
	onExpire := func(e *m3cMockEntry, evict, relFC, dropH bool) {
		if evict {
			e.underLease, s.activeLeases = false, s.activeLeases-1
		}
		if relFC && !s.m.hasFault("lease03-leaks-fc-deadlock") {
			s.releaseFCLocked(e)
		}
		if dropH && e.liveHandler {
			e.liveHandler, s.activeHandlers = false, s.activeHandlers-1
		}
		if s.m.hasFault("lease03-spurious-nack-on-expiry") {
			s.pendNack, addedNack = append(s.pendNack, e.ackID), true
		}
		if s.m.hasFault("lease03-delayed-modack-after-fc-release") && s.delayedExt[e.ackID].IsZero() {
			s.delayedExt[e.ackID] = e.maxExtAt.Add(delayOff)
		}
	}
	for _, e := range s.leases {
		if !e.underLease || (lang == "rust" && !rustDue) {
			continue
		}
		if lang == "cpp" || lang == "node" {
			if now.Before(e.nextExtAt) {
				continue
			}
			e.nextExtAt = now.Add(map[bool]time.Duration{true: 30 * time.Millisecond, false: 20 * time.Millisecond}[lang == "cpp"])
		}
		expired := e.hasMaxExt && now.After(e.maxExtAt) && !extForever
		switch {
		case expired:
			if lang != "go" || kaDue {
				evict := lang != "cpp" && lang != "dotnet" && !(lang == "node" && s.m.hasFault("lease05-node-never-evicts"))
				onExpire(e, evict, lang != "rust" && lang != "cpp" && lang != "dotnet", lang == "python")
			}
		case lang == "java" && e.hasMaxExt && !extForever && e.maxExtAt.Sub(now) <= 10*time.Second:
			javaTailIDs = append(javaTailIDs, e.ackID)
			onExpire(e, true, true, true)
		case lang == "rust" && s.m.hasFault("lease03-rust-early-drop-nack") && e.hasMaxExt:
			e.underLease, s.activeLeases = false, s.activeLeases-1
			s.pendNack, addedNack = append(s.pendNack, e.ackID), true
		case (lang != "cpp" && lang != "node" && now.Before(e.nextExtAt)) || noPreExt:
		default:
			if lang != "cpp" && lang != "node" {
				e.nextExtAt = map[bool]time.Time{true: s.nextKA, false: now.Add(45 * time.Millisecond)}[lang == "go"]
			}
			extIDs = append(extIDs, e.ackID)
		}
	}
	for aid, at := range s.delayedExt {
		if now.After(at) {
			delete(s.delayedExt, aid)
			extIDs = append(extIDs, aid)
		}
	}
	s.checkDrainedLocked()
	s.mu.Unlock()
	if addedNack {
		s.triggerFlush()
	}
	sendChunks := func(ids []string, dl int32, crash bool) bool {
		for _, chunk := range m3cChunkAckIDs(s.sub, ids, false, dl, false, false) {
			if err := s.sendModAckRPC(chunk, dl); err != nil && crash && !s.start.UseExactlyOnce && s.m.hasFault("lease04-crashes-on-modack-err") {
				s.fatalStop(int32(status.Code(err)), err.Error())
				return false
			}
		}
		return true
	}
	sendChunks(javaTailIDs, cmp.Or(map[string]int32{"lease05-modack-601": 601, "lease03-negative-dl": -1, "lease03-sub10-tail": 5}[s.m.cfg.fault], 1), false)
	return s.m.hasFault("lease02-no-periodic-extend") || sendChunks(extIDs, s.clampAckSec(false), true)
}
func (s *m3cMockSub) runLeaseLoop(done <-chan struct{}) {
	lang := strings.ToLower(s.m.caps.GetClientLanguage())
	retStart, retPer := 10*time.Millisecond, 30*time.Millisecond
	if s.m.caps.GetDefaults().GetSubscriberMaxExtensionMs() >= 1500 {
		retStart, retPer = 500*time.Millisecond, 3*time.Second
	}
	now := time.Now()
	s.nextRetain, s.nextKA = now.Add(retStart), now.Add(100*time.Millisecond)
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-done:
			return
		case now := <-tk.C:
			if !s.stepLease(now, lang, retPer) {
				return
			}
		}
	}
}
func (s *m3cMockSub) runDeliverLoop() {
	defer context.AfterFunc(s.pullCtx, func() { s.mu.Lock(); s.cond.Broadcast(); s.mu.Unlock() })()
	lang := strings.ToLower(s.m.caps.GetClientLanguage())
	for {
		s.mu.Lock()
		for len(s.deliverQ) == 0 && !s.stopping && s.pullCtx.Err() == nil {
			s.cond.Wait()
		}
		if s.stopping || s.pullCtx.Err() != nil {
			s.mu.Unlock()
			return
		}
		rm := s.deliverQ[0]
		s.deliverQ[0], s.deliverQ = nil, s.deliverQ[1:]
		s.mu.Unlock()
		if !s.acquireFC(s.pullCtx) {
			return
		}
		tok, now := fmt.Sprintf("tok-%d", s.m.seq.Add(1)), time.Now()
		e := &m3cMockEntry{tok: tok, ackID: rm.GetAckId(), underLease: true, liveHandler: true, nextExtAt: now}
		if s.start.MaxExtensionMs != nil && *s.start.MaxExtensionMs > 0 {
			e.hasMaxExt, e.maxExtAt = true, now.Add(time.Duration(*s.start.MaxExtensionMs)*time.Millisecond)
		} else if s.start.MaxExtensionMs == nil && !s.m.caps.GetSupportsAutoNackOnDrop() {
			e.hasMaxExt, e.maxExtAt = true, now.Add(60*time.Minute)
		}
		if lang == "cpp" {
			e.nextExtAt = now.Add(map[bool]time.Duration{true: 8 * time.Second, false: 18 * time.Millisecond}[s.m.caps.GetDefaults().GetSubscriberMaxExtensionMs() >= 1500])
		} else if lang == "node" {
			e.nextExtAt = now.Add(20 * time.Millisecond)
		}
		s.mu.Lock()
		if s.stopping || s.pullCtx.Err() != nil {
			s.mu.Unlock()
			return
		}
		s.leases[tok] = e
		s.activeLeases++
		s.activeHandlers++
		s.mu.Unlock()
		s.delivCnt.Add(1)
		att, hasAtt := s.m.mockAttempt(rm, false)
		s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{
			DeliveryToken: tok, MessageId: rm.GetMessage().GetMessageId(), Data: rm.GetMessage().GetData(),
			Attributes: rm.GetMessage().GetAttributes(), DeliveryAttempt: att, HasDeliveryAttempt: hasAtt,
		}}})
	}
}
func (s *m3cMockSub) runPullLoop() {
	if s.m.hasFault("pull02-hangs-stream") {
		<-s.pullCtx.Done()
		return
	}
	baseCID := fmt.Sprintf("cid-%d", s.m.seq.Add(1))
	if strings.EqualFold(s.m.caps.GetClientLanguage(), "dotnet") || s.m.hasFault("pull02-empty-clientid") {
		baseCID = ""
	}
	for attempt := 1; s.pullCtx.Err() == nil; attempt++ {
		if attempt > 1 && (s.m.hasFault("pull02b-leaks-old-stream-hung") || s.m.hasFault("lease04-closes-before-stop")) {
			<-s.pullCtx.Done()
			return
		}
		if attempt >= 2 && s.m.hasFault("midstream-unimpl-after-start") {
			s.fatalStop(int32(codes.Unimplemented), "midstream unimplemented")
			return
		}
		cid := baseCID
		if attempt > 1 && (s.m.hasFault("pull02-inconsistent-clientid") || s.m.hasFault("pull02b-inconsistent-clientid")) {
			cid = fmt.Sprintf("%s-s%d", baseCID, attempt)
		}
		hadResp, err := s.runOneStream(attempt, cid)
		if err == nil || s.pullCtx.Err() != nil {
			continue
		}
		if !s.isRetryable(err, hadResp) {
			code := cmp.Or(int32(status.Code(err)), int32(codes.Unknown))
			if c, ok := map[string]int32{
				"pull02-swallows-perm-err": 0, "pull02c-go-swallows-err": 0,
				"sub-inband-unimpl": int32(codes.Unimplemented), "pull02-wrong-fatal-code": int32(codes.Internal),
			}[s.m.cfg.fault]; ok {
				code = c
			}
			s.fatalStop(code, err.Error())
			return
		}
		if !s.m.hasFault("lease04-reopens-stream") {
			tCtx, tCancel := context.WithTimeout(s.pullCtx, 5*time.Millisecond)
			<-tCtx.Done()
			tCancel()
		}
	}
}
func (s *m3cMockSub) runOneStream(attempt int, cid string) (bool, error) {
	stCtx, stCancel := context.WithCancel(s.pullCtx)
	closeCh, streamDone := make(chan struct{}), make(chan struct{})
	var closeOnce sync.Once
	s.mu.Lock()
	s.curStreamCancel, s.curStreamClose, s.curStreamDone = stCancel, func() { closeOnce.Do(func() { close(closeCh) }) }, streamDone
	s.mu.Unlock()
	defer func() {
		stCancel()
		s.mu.Lock()
		s.curStreamCancel, s.curStreamClose, s.curStreamDone = nil, nil, nil
		s.mu.Unlock()
		close(streamDone)
	}()
	streamCtx := s.m.outCtx(stCtx, s.sub)
	if s.m.hasFault("unencoded-streaming-pull") {
		streamCtx = metadata.AppendToOutgoingContext(stCtx, "x-goog-request-params", "subscription="+s.sub)
	}
	if (s.m.hasFault("pull02-bad-initial-req") && strings.Contains(s.sub, "rec-sub")) || (s.m.hasFault("pull02-bad-perm-req") && strings.Contains(s.sub, "perm-sub")) {
		streamCtx = stCtx
	}
	sp, err := s.client.StreamingPull(streamCtx)
	if err != nil {
		return false, err
	}
	pv := map[bool]int64{true: 1}[s.m.caps.GetSupportsKeepalive() && !s.m.hasFault("pull03-missing-protocol-v1")]
	if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: s.sub, StreamAckDeadlineSeconds: s.clampAckSec(true), ClientId: cid, ProtocolVersion: pv}); err != nil {
		return false, err
	}
	if s.m.hasFault("lease05-sp-bad-mod-dl") {
		_ = sp.Send(&pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"ack-lease05-1"}, ModifyDeadlineSeconds: []int32{5}})
	}
	pull03NoPing := attempt == 1 && s.m.hasFault("pull03-closes-without-ping")
	if pull03NoPing {
		_, _ = sp.Header()
		_ = sp.CloseSend()
	}
	if s.m.hasFault("lease04-reopens-stream") && attempt >= 2 {
		_, _ = sp.Header()
		s.mu.Lock()
		s.streamGen++
		s.cond.Broadcast()
		s.mu.Unlock()
	}
	var kaKick atomic.Bool
	var pingMu sync.Mutex
	lastResp, lastPing, pingAt := time.Now(), time.Now(), time.Time{}
	hasKA := !pull03NoPing && s.m.caps.GetSupportsKeepalive() && !(attempt >= 2 && s.m.hasFault("pull03-drops-ping-on-stream2"))
	pingInt := time.Duration(max(s.m.caps.GetDefaults().GetKeepalivePingIntervalMs(), 10)) * time.Millisecond
	pongTO := time.Duration(max(s.m.caps.GetDefaults().GetKeepalivePongTimeoutMs(), 15)) * time.Millisecond
	helperDone := make(chan struct{})
	go func() {
		defer close(helperDone)
		var tkC <-chan time.Time
		if hasKA {
			tk := time.NewTicker(5 * time.Millisecond)
			defer tk.Stop()
			tkC = tk.C
		}
		for {
			select {
			case <-stCtx.Done():
				return
			case <-closeCh:
				_ = sp.CloseSend()
				return
			case now := <-tkC:
				sent := now.Sub(lastPing) >= pingInt
				if sent {
					if sp.Send(&pubsubpb.StreamingPullRequest{}) != nil {
						return
					}
					lastPing = now
				}
				pingMu.Lock()
				if sent && pingAt.IsZero() {
					pingAt = now
				}
				kick := !pingAt.IsZero() && (s.m.hasFault("pull03b-false-pong-timeout") ||
					(s.m.hasFault("pull03-spurious-reconnect-with-pong") && attempt >= 2 && !lastResp.Before(pingAt)) ||
					(lastResp.Before(pingAt) && time.Since(pingAt) >= pongTO && !s.m.hasFault("pull03-no-pong-timeout-hang")))
				if !kick && !pingAt.IsZero() && !lastResp.Before(pingAt) {
					pingAt = time.Time{}
				}
				pingMu.Unlock()
				if kick {
					kaKick.Store(true)
					stCancel()
					return
				}
			}
		}
	}()
	defer func() { stCancel(); <-helperDone }()
	hadResp := false
	for {
		resp, rerr := sp.Recv()
		if rerr != nil {
			if kaKick.Load() || s.reopenAsync.Swap(false) {
				rerr = io.EOF
			}
			return hadResp, rerr
		}
		hadResp = true
		rms := resp.GetReceivedMessages()
		if hasKA && !(attempt >= 2 && s.m.hasFault("pull03-ignores-pong-on-stream2") && len(rms) == 0) {
			pingMu.Lock()
			lastResp = time.Now()
			pingMu.Unlock()
		}
		if len(rms) > 0 {
			s.mu.Lock()
			s.deliverQ = append(s.deliverQ, rms...)
			s.cond.Broadcast()
			s.mu.Unlock()
		}
	}
}
func (m *m3cMockServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	if m.hasFault("runtime-unimpl-sub") || strings.EqualFold(m.caps.GetClientLanguage(), "php") {
		return status.Error(codes.Unimplemented, "SubscriberSession unimplemented")
	}
	first, err := stream.Recv()
	if err != nil || first.GetStart() == nil {
		return cmp.Or(err, status.Error(codes.InvalidArgument, "expected StartSubscriberCommand"))
	}
	start := first.GetStart()
	conn, err := m3cMockDial(start.GetEndpoint())
	if err != nil {
		return err
	}
	defer conn.Close()
	sessCtx, cancel := context.WithCancel(stream.Context())
	defer context.AfterFunc(m.srvCtx, cancel)()
	defer cancel()
	pullCtx, pullFn := context.WithCancel(sessCtx)
	stopCtx, stopFn := context.WithCancel(sessCtx)
	s := &m3cMockSub{
		m: m, stream: stream, client: pubsubpb.NewSubscriberClient(conn), sub: start.GetSubscription(), start: start,
		sessCtx: sessCtx, cancel: cancel, pullCtx: pullCtx, pullFn: pullFn, stopCtx: stopCtx, stopFn: stopFn,
		sendCh: make(chan *proxypb.SubscriberEvent, 128), flushCh: make(chan struct{}, 8),
		leases: map[string]*m3cMockEntry{}, delayedExt: map[string]time.Time{}, fcMax: start.GetMaxOutstandingMessages(),
	}
	s.cond = sync.NewCond(&s.mu)
	defer context.AfterFunc(sessCtx, func() { s.mu.Lock(); s.cond.Broadcast(); s.mu.Unlock() })()
	senderDone := make(chan struct{})
	m.wg.Go(func() {
		defer close(senderDone)
		for ev := range s.sendCh {
			if stream.Send(ev) != nil {
				cancel()
				return
			}
		}
	})
	s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{}}})
	m.wg.Go(func() {
		for {
			cmd, rerr := stream.Recv()
			if rerr != nil {
				if !s.stopRequested.Load() {
					s.triggerStop(false)
				}
				return
			}
			if stopCmd := cmd.GetStop(); stopCmd != nil {
				if m.hasFault("sub-stop-inband-unimpl") {
					s.fatalStop(int32(codes.Unimplemented), "stop unimpl")
					return
				}
				if m.hasFault("lease02-post-ack-modack") {
					s.leaseRPCMu.Lock()
					if aid, _ := s.lastAcked.Load().(string); aid != "" {
						_ = s.sendModAckRPC([]string{aid}, s.clampAckSec(false))
					}
					s.leaseRPCMu.Unlock()
				}
				graceful := (!slices.Contains([]string{"cpp", "node"}, strings.ToLower(m.caps.GetClientLanguage())) || stopCmd.GetShutdownBehavior() == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING) &&
					stopCmd.GetShutdownBehavior() != proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY && (stopCmd.ShutdownTimeoutMs == nil || *stopCmd.ShutdownTimeoutMs < 0)
				s.triggerStop(graceful)
				if !graceful {
					return
				}
			}
			if st := cmd.GetSettle(); st != nil {
				s.handleSettle(st)
			}
		}
	})
	loopDone, loopWG := make(chan struct{}), sync.WaitGroup{}
	for _, fn := range []func(){func() { s.runSettleLoop(loopDone) }, func() { s.runLeaseLoop(loopDone) }, s.runDeliverLoop, s.runPullLoop} {
		loopWG.Go(fn)
	}
	<-s.stopCtx.Done()
	close(loopDone)
	loopWG.Wait()
	s.mu.Lock()
	fc, fm := s.fatalC, s.fatalM
	s.mu.Unlock()
	s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: fc, ErrorMessage: fm, TotalDelivered: s.delivCnt.Load()}}})
	close(s.sendCh)
	<-senderDone
	return nil
}
func (s *m3cMockSub) handleSettle(cmd *proxypb.SettleMessageCommand) {
	s.mu.Lock()
	tok, act := cmd.GetDeliveryToken(), cmd.GetAction()
	e, ok := s.leases[tok]
	if !ok || e.settled || (!e.underLease && act == proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE && s.m.hasFault("lease03-rust-leaks-handler-deadlock")) {
		s.mu.Unlock()
		return
	}
	if e.liveHandler {
		e.liveHandler, s.activeHandlers = false, s.activeHandlers-1
	}
	wasUnderLease, auto := e.underLease, s.m.caps.GetSupportsAutoNackOnDrop()
	ackOnDrop := act == proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE && wasUnderLease && auto && s.m.hasFault("lease05-acks-instead-of-nack")
	nackOnDrop := act == proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE && wasUnderLease && ((auto && !s.m.hasFault("lease05-missing-auto-nack")) || (!auto && s.m.hasFault("lease05-spurious-nack-when-disabled")))
	if act != proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE || !wasUnderLease || ackOnDrop || nackOnDrop || s.m.hasFault("lease05-missing-auto-nack") || strings.EqualFold(s.m.caps.GetClientLanguage(), "ruby") {
		e.settled = true
		if wasUnderLease {
			e.underLease, s.activeLeases = false, s.activeLeases-1
		}
		delete(s.leases, tok)
	}
	if act == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK || ackOnDrop {
		s.pendAck = append(s.pendAck, e.ackID)
	} else if (act == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK && wasUnderLease) || nackOnDrop {
		s.pendNack = append(s.pendNack, e.ackID)
	}
	s.checkDrainedLocked()
	needFlush := !s.noAutoSplit() && (len(s.pendAck) >= fake.MaxAckIDsPerRequest || len(s.pendNack) >= fake.MaxAckIDsPerRequest)
	if !s.m.hasFault("pull03b-deadlocks-on-resume") && !s.m.hasFault("lease03-leaks-fc-deadlock") {
		s.releaseFCLocked(e)
	}
	s.mu.Unlock()
	if needFlush {
		s.triggerFlush()
	}
}
func (m *m3cMockServer) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	if !m.caps.GetSupportsUnaryPull() || m.hasFault("dlq01-unary-grpc-unimpl") {
		return nil, status.Error(codes.Unimplemented, "UnaryPull unimplemented")
	}
	if m.hasFault("dlq01-unary-inband-unimpl") {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unary in-band unimpl"}, nil
	}
	conn, err := m3cMockDial(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sc := pubsubpb.NewSubscriberClient(conn)
	pr, err := sc.Pull(m.outCtx(ctx, req.GetSubscription()), &pubsubpb.PullRequest{Subscription: req.GetSubscription(), MaxMessages: req.GetMaxMessages(), ReturnImmediately: req.GetReturnImmediately()})
	if err != nil {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
	}
	ackSub, ackCtx := m.ackMetaCtx(ctx, req.GetSubscription(), true)
	var out []*proxypb.UnaryPullResponse_PulledMessage
	for _, rm := range pr.GetReceivedMessages() {
		if req.GetSettleAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK {
			if _, err := sc.Acknowledge(ackCtx, &pubsubpb.AcknowledgeRequest{Subscription: ackSub, AckIds: []string{rm.GetAckId()}}); err != nil {
				return nil, err
			}
		}
		att, hasAtt := m.mockAttempt(rm, true)
		msg := rm.GetMessage()
		settleSt, settleCode, settleMsg := proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, int32(0), ""
		if m.hasFault("dlq01-unary-settle-err") {
			settleSt, settleCode, settleMsg = proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, int32(codes.PermissionDenied), "unary settle failed"
		}
		out = append(out, &proxypb.UnaryPullResponse_PulledMessage{
			MessageId: msg.GetMessageId(), Data: msg.GetData(), Attributes: msg.GetAttributes(),
			DeliveryAttempt: att, HasDeliveryAttempt: hasAtt, SettleStatus: settleSt, SettleErrorCode: settleCode, SettleErrorMessage: settleMsg,
		})
	}
	return &proxypb.UnaryPullResponse{Messages: out}, nil
}

type m3cFaultCase struct {
	prof, fault, sc, sub string
	timeout              time.Duration
	want                 harness.Status
	capsOverride         func(*proxypb.GetCapabilitiesResponse)
}

func m3cRunScenarios(t *testing.T, cfg m3cMockConfig, list []harness.Scenario, filter string, timeoutCap time.Duration) *harness.TargetReport {
	t.Helper()
	reg := scenarios.NewRegistry()
	for _, sc := range list {
		if timeoutCap > 0 && (sc.Timeout == 0 || sc.Timeout > timeoutCap) {
			sc.Timeout = timeoutCap
		}
		reg.MustRegister(sc)
	}
	flt, _ := reg.Filter(filter)
	cfg.slowKA = !slices.ContainsFunc(flt, func(sc harness.Scenario) bool { return strings.HasPrefix(sc.ID, "PULL-03") })
	target := harness.TargetSpec{Name: cfg.profile, Connect: m3cMockConnect(cfg)}
	runner, err := harness.NewRunner(harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: reg.All(), Filter: filter, ScenarioTimeout: timeoutCap})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), target)
	if err != nil {
		t.Fatalf("RunTarget(%s): %v", cfg.profile, err)
	}
	return rep
}
func m3cAssertUnitMeta(t *testing.T, list, want []harness.Scenario) {
	t.Helper()
	if len(list) != len(want) {
		t.Fatalf("count mismatch: want %d, got %d", len(want), len(list))
	}
	badRE, reg := regexp.MustCompile(`^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03|NOMATCH)$`), scenarios.NewRegistry()
	for i, sc := range list {
		w := want[i]
		if sc.ID != w.ID || sc.Category != w.Category || sc.IsolateProxy != w.IsolateProxy || sc.Timeout != w.Timeout || !slices.Equal(sc.RequiredCapabilities, w.RequiredCapabilities) || sc.CheckCapabilities != nil || sc.TraceOnly || sc.Run == nil || sc.Name == "" || sc.Feature == "" || sc.Description == "" {
			t.Errorf("invalid scenario[%d] (%s): got=%+v want=%+v", i, sc.ID, sc, w)
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
func m3cRunProfileTests(t *testing.T, list []harness.Scenario, total int, to time.Duration, skipOverrides map[string]int) {
	t.Helper()
	for _, prof := range []string{"go", "cpp", "rust", "java", "python", "ruby", "node", "dotnet", "php"} {
		t.Run("profile_"+prof, func(t *testing.T) {
			wantP := total - skipOverrides[prof]
			rep := m3cRunScenarios(t, m3cMockConfig{profile: prof}, list, "", to)
			if rep.Summary.Pass != wantP || rep.Summary.UnsupportedByClient != total-wantP || rep.Summary.Fail != 0 || rep.Summary.TimeoutCount != 0 || rep.Summary.ErrorCount != 0 {
				t.Fatalf("profile=%s want pass=%d unsup=%d, got %+v scenarios=%+v", prof, wantP, total-wantP, rep.Summary, rep.Scenarios)
			}
		})
	}
}
func m3cRunFaultCases(t *testing.T, list []harness.Scenario, cases []m3cFaultCase) {
	t.Helper()
	for _, tc := range cases {
		to, want := cmp.Or(tc.timeout, 2*time.Second), cmp.Or(tc.want, harness.StatusFail)
		name := tc.sc + "_" + tc.prof + "_" + tc.fault
		t.Run(name, func(t *testing.T) {
			sr := m3cRunScenarios(t, m3cMockConfig{profile: tc.prof, fault: tc.fault, capsOverride: tc.capsOverride}, list, "^"+tc.sc+"$", to).Scenarios[0]
			if sr.Status != want || (tc.sub != "" && !strings.Contains(sr.Error, tc.sub)) {
				t.Errorf("%s: want %s (%q), got %s (%q)", name, want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}
func m3cRunNoLeakFaults(t *testing.T, list []harness.Scenario, to time.Duration, cases ...[3]string) {
	t.Helper()
	settled := func(t *testing.T, maxG int) {
		t.Helper()
		dl, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(10*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for ; ; <-tk.C {
			s := string(buf[:runtime.Stack(buf, true)])
			if !strings.Contains(s, "m3cMock") && !strings.Contains(s, "tcpDropForwarder") && !strings.Contains(s, "startSubPump") && (maxG < 0 || runtime.NumGoroutine() <= maxG) {
				return
			}
			if time.Now().After(dl) {
				t.Fatalf("goroutine leak after timeout: max=%d now=%d\n%s", maxG, runtime.NumGoroutine(), s)
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc[0]+"_"+tc[1], func(t *testing.T) {
			var cold int
			for i := range 4 {
				if sr := m3cRunScenarios(t, m3cMockConfig{profile: tc[0], fault: tc[1]}, list, tc[2], to).Scenarios[0]; sr.Status != harness.StatusTimeout {
					t.Fatalf("want TIMEOUT, got %s (%s)", sr.Status, sr.Error)
				}
				if i == 0 {
					settled(t, -1)
					cold = runtime.NumGoroutine()
				}
			}
			settled(t, cold)
		})
	}
}

func TestPullScenarios_Unit(t *testing.T) {
	m3cAssertUnitMeta(t, scenarios.ExportPullScenarios(), []harness.Scenario{
		{ID: "PULL-02", Category: "pull"}, {ID: "PULL-02b", Category: "pull", IsolateProxy: true},
		{ID: "PULL-02c", Category: "pull"}, {ID: "PULL-02d", Category: "pull"},
		{ID: "PULL-03", Category: "pull", Timeout: 120 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapKeepalive}},
		{ID: "PULL-03b", Category: "pull", Timeout: 120 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapKeepalive}},
	})
}
func TestPullScenarios_Profiles(t *testing.T) {
	m3cRunProfileTests(t, scenarios.ExportPullScenarios(), 6, 3*time.Second, map[string]int{"cpp": 2, "python": 2, "php": 6})
}
func TestPullScenarios_FaultDetection(t *testing.T) {
	m3cRunFaultCases(t, scenarios.ExportPullScenarios(), []m3cFaultCase{
		{prof: "go", fault: "runtime-unimpl-sub", sc: "PULL-02", want: harness.StatusUnsupportedByClient},
		{prof: "go", fault: "sub-inband-unimpl", sc: "PULL-02", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "sub-inband-unimpl", sc: "PULL-02c", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "sub-inband-unimpl", sc: "PULL-02d", sub: "UNIMPLEMENTED"},
		{prof: "java", fault: "sub-inband-unimpl", sc: "PULL-02d", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "midstream-unimpl-after-start", sc: "PULL-02b", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "sub-stop-inband-unimpl", sc: "PULL-02b", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "pull02-no-retry-transient", sc: "PULL-02"},
		{prof: "go", fault: "pull02-inconsistent-clientid", sc: "PULL-02", sub: "inconsistent client_id"},
		{prof: "go", fault: "pull02-empty-clientid", sc: "PULL-02", sub: "empty client_id"},
		{prof: "go", fault: "pull02-bad-initial-req", sc: "PULL-02", sub: "x-goog-request-params"},
		{prof: "go", fault: "pull02-bad-perm-req", sc: "PULL-02", sub: "x-goog-request-params"},
		{prof: "go", fault: "pull02-retries-permanent", sc: "PULL-02", sub: "retried non-retryable"},
		{prof: "go", fault: "pull02-swallows-perm-err", sc: "PULL-02", sub: "expected non-OK"},
		{prof: "go", fault: "pull02-wrong-fatal-code", sc: "PULL-02", sub: "StoppedEvent code mismatch"},
		{prof: "go", fault: "pull02-hangs-stream", sc: "PULL-02", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pull02b-fails-on-abort", sc: "PULL-02b"},
		{prof: "go", fault: "pull02b-inconsistent-clientid", sc: "PULL-02b", sub: "inconsistent client_id"},
		{prof: "go", fault: "pull02b-leaks-old-stream-hung", sc: "PULL-02b", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pull02c-go-retries-resexhausted", sc: "PULL-02c", sub: "retried non-retryable"},
		{prof: "rust", fault: "pull02c-peer-terminates-resexhausted", sc: "PULL-02c"},
		{prof: "go", fault: "pull02c-go-swallows-err", sc: "PULL-02c", sub: "expected non-OK"},
		{prof: "go", fault: "pull02-wrong-fatal-code", sc: "PULL-02c", sub: "StoppedEvent code mismatch"},
		{prof: "go", fault: "pull02d-go-java-retries-shutdownnow", sc: "PULL-02d", sub: "retried non-retryable"},
		{prof: "java", fault: "pull02d-go-java-retries-shutdownnow", sc: "PULL-02d", sub: "retried non-retryable"},
		{prof: "rust", fault: "pull02d-peer-fails-shutdownnow", sc: "PULL-02d"},
		{prof: "go", fault: "pull02-wrong-fatal-code", sc: "PULL-02d", sub: "StoppedEvent code mismatch"},
		{prof: "go", fault: "pull03-missing-protocol-v1", sc: "PULL-03", sub: "protocol_version mismatch"},
		{prof: "go", fault: "pull03-closes-without-ping", sc: "PULL-03", sub: "closed before sending keepalive ping"},
		{prof: "go", fault: "pull03-no-pong-timeout-hang", sc: "PULL-03", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pull03-drops-ping-on-stream2", sc: "PULL-03", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pull03-spurious-reconnect-with-pong", sc: "PULL-03", sub: "expected exactly 2 streams"},
		{prof: "go", fault: "pull03-ignores-pong-on-stream2", sc: "PULL-03", sub: "expected exactly 2 streams"},
		{prof: "go", fault: "pull03b-false-pong-timeout", sc: "PULL-03b"},
		{prof: "go", fault: "pull03b-ignores-flow-control", sc: "PULL-03b", sub: "held under MaxOutstandingMessages"},
		{prof: "go", fault: "pull03b-deadlocks-on-resume", sc: "PULL-03b", sub: "TIMEOUT:", timeout: 800 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "wrong-ack-sub", sc: "PULL-02", sub: "subscription mismatch"},
		{prof: "go", fault: "missing-ack-req-params", sc: "PULL-02", sub: "x-goog-request-params"},
		{prof: "go", fault: "emit-unexpected-span", sc: "PULL-02", sub: "expected zero spans"},
		{prof: "java", fault: "unencoded-streaming-pull", sc: "PULL-02", want: harness.StatusPass},
		{prof: "python", fault: "unencoded-streaming-pull", sc: "PULL-02", want: harness.StatusPass},
		{prof: "node", fault: "unencoded-streaming-pull", sc: "PULL-02", want: harness.StatusPass},
		{prof: "cpp", fault: "unencoded-streaming-pull", sc: "PULL-02", sub: "x-goog-request-params", want: harness.StatusFail},
		{prof: "go", fault: "spurious-nack-after-ack", sc: "PULL-02", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "spurious-nack-after-ack", sc: "PULL-02b", sub: "nacked ack_ids set mismatch"},
		{prof: "rust", fault: "spurious-nack-after-ack", sc: "PULL-02c", sub: "nacked ack_ids set mismatch"},
		{prof: "rust", fault: "spurious-nack-after-ack", sc: "PULL-02d", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "spurious-nack-after-ack", sc: "PULL-03", sub: "nacked ack_ids set mismatch"},
		{prof: "go", fault: "spurious-nack-after-ack", sc: "PULL-03b", sub: "nacked ack_ids set mismatch"},
	})
}
func TestPullScenarios_StartSubUnimpl50x(t *testing.T) {
	list := scenarios.ExportPullScenarios()
	for i := range 50 {
		sr := m3cRunScenarios(t, m3cMockConfig{profile: "php"}, list, "^PULL-02$", 2*time.Second).Scenarios[0]
		if sr.Status != harness.StatusUnsupportedByClient || strings.Contains(sr.Reason, "EOF") || strings.Contains(sr.Error, "EOF") {
			t.Fatalf("iter %d: want UNSUPPORTED_BY_CLIENT without EOF, got %s reason=%q err=%q", i, sr.Status, sr.Reason, sr.Error)
		}
	}
}
func TestPullScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	list := scenarios.ExportPullScenarios()
	m3cRunNoLeakFaults(t, list, 450*time.Millisecond, [3]string{"go", "pull02-hangs-stream", "^PULL-02$"}, [3]string{"go", "pull02b-leaks-old-stream-hung", "^PULL-02b$"}, [3]string{"go", "pull03b-deadlocks-on-resume", "^PULL-03b$"})
	m3cRunNoLeakFaults(t, list, 650*time.Millisecond, [3]string{"go", "pull03-no-pong-timeout-hang", "^PULL-03$"}, [3]string{"go", "pull03-drops-ping-on-stream2", "^PULL-03$"})
}
