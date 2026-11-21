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
	"fmt"
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

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type m3dMockConfig struct {
	profile, fault string
	capsOverride   func(*proxypb.GetCapabilitiesResponse)
}
type m3dMockServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg    m3dMockConfig
	caps   *proxypb.GetCapabilitiesResponse
	srvCtx context.Context
	cancel context.CancelFunc
	srv    *grpc.Server
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	exited chan error
	once   sync.Once
	seq    atomic.Int64
	wg     sync.WaitGroup
}
type m3dEOFClient struct{ proxypb.PubSubProxyClient }
type m3dEOFStream struct {
	proxypb.PubSubProxy_SubscriberSessionClient
	q   []*proxypb.SubscriberEvent
	err error
}

func (c m3dEOFClient) SubscriberSession(ctx context.Context, opts ...grpc.CallOption) (proxypb.PubSubProxy_SubscriberSessionClient, error) {
	st, err := c.PubSubProxyClient.SubscriberSession(ctx, opts...)
	return &m3dEOFStream{PubSubProxy_SubscriberSessionClient: st}, err
}
func (s *m3dEOFStream) Recv() (*proxypb.SubscriberEvent, error) {
	if len(s.q) > 0 {
		ev := s.q[0]
		s.q = s.q[1:]
		return ev, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	ev, err := s.PubSubProxy_SubscriberSessionClient.Recv()
	for err == nil && ev.GetMessageDelivered() != nil && s.err == nil {
		var ev2 *proxypb.SubscriberEvent
		if ev2, s.err = s.PubSubProxy_SubscriberSessionClient.Recv(); s.err == nil {
			s.q = append(s.q, ev2)
		}
	}
	return ev, err
}
func (m *m3dMockServer) Client() proxypb.PubSubProxyClient {
	return map[bool]proxypb.PubSubProxyClient{true: m3dEOFClient{m.client}, false: m.client}[m.hasAny("sub-send-eof-with-stopped", "sub-send-eof-only")]
}
func (m *m3dMockServer) Capabilities() *proxypb.GetCapabilitiesResponse { return m.caps }
func (m *m3dMockServer) Exited() <-chan error                           { return m.exited }
func (m *m3dMockServer) Stderr() string                                 { return "" }
func (m *m3dMockServer) PID() int                                       { return 0 }
func (m *m3dMockServer) Close(context.Context) error {
	m.once.Do(func() { m.cancel(); _ = m.conn.Close(); m.srv.Stop(); m.wg.Wait(); <-m.exited })
	return nil
}
func m3dMockDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
func m3dMockCapabilities(cfg m3dMockConfig) *proxypb.GetCapabilitiesResponse {
	lang := strings.ToLower(cfg.profile)
	s := map[string][7]bool{
		"go": {true, true, true, true, true, true, false}, "cpp": {false, false, true, false, false, false, true}, "rust": {true, false, false, false, true, false, true}, "java": {true, false, true, true, true, true, false},
		"python": {false, false, true, true, false, false, false}, "ruby": {true, false, true, true, true, true, false}, "node": {true, false, false, true, true, true, false}, "dotnet": {true, false, true, true, true, true, true},
	}[lang]
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage: lang, ClientLibrary: "pubsub-" + lang, ClientVersion: "2.0.0",
		SupportsMonitoring: lang != "rust", SupportsExactlyOnceDelivery: lang != "php", SupportsAckWithResult: lang != "php",
		SupportsKeepalive: s[0], SupportsPerStreamFlowControl: s[1], SupportsSubscriberOrdering: s[2], SupportsEodReceiptModack: s[3], SupportsShutdownBehavior: s[4], SupportsShutdownTimeout: s[5], SupportsAutoNackOnDrop: s[6],
		Defaults: &proxypb.ClientDefaults{SubscriberMinAckExtensionMs: 35, SubscriberMaxAckExtensionMs: 600_000, SubscriberMaxExtensionMs: 3_600_000, RetryInitialDelayMs: 15, KeepalivePingIntervalMs: map[bool]int64{true: 500}[s[0]], KeepalivePongTimeoutMs: map[bool]int64{true: 1000}[s[0]]},
	}
	if cfg.capsOverride != nil {
		cfg.capsOverride(caps)
	}
	return caps
}
func m3dMockConnect(cfg m3dMockConfig) func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
	return func(context.Context, harness.TargetSpec) (harness.ProxyHandle, error) {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		srvCtx, cancel := context.WithCancel(context.Background())
		m := &m3dMockServer{cfg: cfg, caps: m3dMockCapabilities(cfg), srvCtx: srvCtx, cancel: cancel, srv: grpc.NewServer(), exited: make(chan error, 1)}
		proxypb.RegisterPubSubProxyServer(m.srv, m)
		go func() { m.exited <- m.srv.Serve(lis) }()
		if m.conn, err = m3dMockDial(lis.Addr().String()); err == nil {
			m.client = proxypb.NewPubSubProxyClient(m.conn)
			return m, nil
		}
		cancel()
		m.srv.Stop()
		<-m.exited
		return nil, err
	}
}
func (m *m3dMockServer) hasFault(f string) bool   { return m.cfg.fault == f }
func (m *m3dMockServer) hasAny(fs ...string) bool { return slices.Contains(fs, m.cfg.fault) }
func (m *m3dMockServer) outCtx(ctx context.Context, sub string, checkAck bool) (string, context.Context) {
	if checkAck && m.hasFault("missing-ack-req-params") {
		return sub, ctx
	}
	val := map[bool]string{true: sub, false: url.QueryEscape(sub)}[strings.EqualFold(m.caps.GetClientLanguage(), "rust")]
	if checkAck && m.hasFault("wrong-ack-sub") {
		sub += "-wrong"
	}
	return sub, metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "subscription="+val)
}
func (m *m3dMockServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return m.caps, nil
}
func (m *m3dMockServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (m *m3dMockServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *m3dMockServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	if m.hasFault("emit-unexpected-span") {
		return &proxypb.DrainTracesResponse{Spans: []*proxypb.TraceSpan{{TraceId: "0123456789abcdef0123456789abcdef", SpanId: "0123456789abcdef", TraceFlags: 1, Name: "unexpected", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL}}}, nil
	}
	return &proxypb.DrainTracesResponse{}, nil
}

type m3dFC struct {
	mu                                       sync.Mutex
	cond                                     *sync.Cond
	maxMsgs, maxBytes, curMsgs, curBytes     int64
	maxConc, curConc                         int32
	nodeAllowExcess, ignoreCount, ignoreByte bool
}

func m3dNewFC(maxMsgs, maxBytes int64, maxConc int32, nodeExcess, ignoreCount, ignoreByte bool) *m3dFC {
	fc := &m3dFC{maxMsgs: maxMsgs, maxBytes: maxBytes, maxConc: maxConc, nodeAllowExcess: nodeExcess, ignoreCount: ignoreCount, ignoreByte: ignoreByte}
	fc.cond = sync.NewCond(&fc.mu)
	return fc
}
func (fc *m3dFC) acquire(ctx context.Context, msgBytes int64) (int64, bool) {
	bounded := msgBytes
	if fc.maxBytes > 0 && bounded > fc.maxBytes {
		bounded = fc.maxBytes
	}
	defer context.AfterFunc(ctx, func() { fc.mu.Lock(); fc.cond.Broadcast(); fc.mu.Unlock() })()
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for ctx.Err() == nil {
		msgsOK := fc.ignoreCount || fc.maxMsgs <= 0 || fc.curMsgs+1 <= fc.maxMsgs
		bytesOK := fc.ignoreByte || fc.maxBytes <= 0 || fc.curBytes+bounded <= fc.maxBytes || (fc.nodeAllowExcess && fc.curBytes < fc.maxBytes)
		if msgsOK && bytesOK && (fc.maxConc <= 0 || fc.curConc+1 <= fc.maxConc) {
			fc.curMsgs, fc.curBytes, fc.curConc = fc.curMsgs+1, fc.curBytes+bounded, fc.curConc+1
			return bounded, true
		}
		fc.cond.Wait()
	}
	return 0, false
}
func (fc *m3dFC) release(bounded int64) {
	fc.mu.Lock()
	fc.curMsgs, fc.curBytes, fc.curConc = max(fc.curMsgs-1, 0), max(fc.curBytes-bounded, 0), max(fc.curConc-1, 0)
	fc.cond.Broadcast()
	fc.mu.Unlock()
}

type m3dEntry struct {
	tok, ackID     string
	msg            *pubsubpb.PubsubMessage
	eod            bool
	fc             *m3dFC
	boundedFC      int64
	settleCh       chan proxypb.SettleMessageCommand_SettleAction
	settled, fcRel atomic.Bool
}

func (e *m3dEntry) releaseFC() {
	if e.fcRel.CompareAndSwap(false, true) && e.fc != nil {
		e.fc.release(e.boundedFC)
	}
}

type m3dSettleItem struct {
	entry        *m3dEntry
	isNack, want bool
	done         chan struct{}
}
type m3dMockSub struct {
	m                                               *m3dMockServer
	stream                                          proxypb.PubSubProxy_SubscriberSessionServer
	client                                          pubsubpb.SubscriberClient
	sub, clientID, fatalM                           string
	start                                           *proxypb.StartSubscriberCommand
	sessCtx, pullCtx, abortCtx                      context.Context
	sessCancel, pullCancel, abortCancel             context.CancelFunc
	sendCh                                          chan *proxypb.SubscriberEvent
	flushCh, autoCh, stopCh, extReset, secondStopCh chan struct{}
	stopOnce, secondStopOnce                        sync.Once
	mu                                              sync.Mutex
	active                                          map[string]*m3dEntry
	pendingAckIDs                                   map[string]bool
	keyChains                                       map[string]chan struct{}
	pendAcks, pendNacks                             []*m3dSettleItem
	isEOD, isOrdered                                atomic.Bool
	stopBh                                          proxypb.ShutdownBehavior
	stopTO                                          time.Duration
	hasStopTO, stopping                             bool
	fatalC                                          int32
	activeCB                                        atomic.Int32
	delivCnt                                        atomic.Int64
	cbWG, streamWG, leaseWG                         sync.WaitGroup
}

func m3dSig(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (s *m3dMockSub) emit(ev *proxypb.SubscriberEvent) {
	select {
	case s.sendCh <- ev:
	case <-s.sessCtx.Done():
	}
}
func (s *m3dMockSub) emitMD(tok, mid string) {
	s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: tok, MessageId: mid, IsExactlyOnce: s.isEOD.Load()}}})
}
func m3dSleepTimer(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
func (s *m3dMockSub) triggerStop(bh proxypb.ShutdownBehavior, tm *int64) {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		if s.stopping, s.stopBh = true, cmp.Or(bh, s.start.GetShutdownBehavior()); s.stopBh == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_UNSPECIFIED {
			s.stopBh = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING
		}
		if tm = cmp.Or(tm, s.start.ShutdownTimeoutMs); tm != nil {
			s.hasStopTO, s.stopTO = true, time.Duration(*tm)*time.Millisecond
		}
		s.mu.Unlock()
		close(s.stopCh)
	})
}
func (s *m3dMockSub) fatalStop(c int32, msg string) {
	s.mu.Lock()
	if s.fatalC == 0 {
		s.fatalC, s.fatalM = c, msg
	}
	s.mu.Unlock()
	s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
}
func (s *m3dMockSub) ackDeadlineSec(eod bool) int32 {
	if s.m.hasFault("eod01-no-60s-floor") {
		return 10
	}
	if s.m.hasFault("eod01-ignores-min-15s") {
		return 60
	}
	dl := map[bool]int32{true: 60, false: 10}[eod]
	if minMs := s.start.GetMinDurationPerAckExtensionMs(); minMs > 0 && slices.Contains([]string{"go", "java", "python", "node", "cpp"}, strings.ToLower(s.m.caps.GetClientLanguage())) {
		dl = int32(minMs / 1000)
	}
	if maxMs := s.start.GetMaxDurationPerAckExtensionMs(); maxMs > 0 {
		dl = min(dl, int32(maxMs/1000))
	}
	return min(max(dl, 10), 600)
}
func (s *m3dMockSub) sendModAck(ids []string, dl int32) {
	if len(ids) == 0 {
		return
	}
	nCtx, cancel := context.WithTimeout(s.m.srvCtx, 2*time.Second)
	defer cancel()
	_, cCtx := s.m.outCtx(nCtx, s.sub, false)
	_, _ = s.client.ModifyAckDeadline(cCtx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: s.sub, AckIds: ids, AckDeadlineSeconds: dl})
}
func (s *m3dMockSub) runPull(fc *m3dFC, readyCh chan<- struct{}) {
	defer s.streamWG.Done()
	defer func() {
		if readyCh != nil {
			readyCh <- struct{}{}
		}
	}()
	for s.pullCtx.Err() == nil {
		stCtx, stCancel := context.WithCancel(s.pullCtx)
		_, callCtx := s.m.outCtx(stCtx, s.sub, false)
		if s.m.hasFault("unencoded-streaming-pull") {
			callCtx = metadata.AppendToOutgoingContext(stCtx, "x-goog-request-params", "subscription="+s.sub)
		}
		sp, err := s.client.StreamingPull(callCtx)
		if err != nil {
			stCancel()
			if !m3dSleepTimer(s.pullCtx, 10*time.Millisecond) {
				return
			}
			continue
		}
		maxMsgs := map[bool]int64{true: 0, false: s.start.GetMaxOutstandingMessages()}[s.m.hasFault("subf01-bad-frame0-msgs")]
		maxBytes := map[bool]int64{true: 0, false: s.start.GetMaxOutstandingBytes()}[s.m.hasFault("subf01-bad-frame0-bytes")]
		if sp.Send(&pubsubpb.StreamingPullRequest{Subscription: s.sub, StreamAckDeadlineSeconds: 60, ClientId: s.clientID, ProtocolVersion: map[bool]int64{true: 1}[s.m.caps.GetSupportsKeepalive()], MaxOutstandingMessages: maxMsgs, MaxOutstandingBytes: maxBytes}) != nil {
			stCancel()
			return
		}
		if readyCh != nil {
			_, _ = sp.Header()
			readyCh <- struct{}{}
			readyCh = nil
		}
		if s.m.hasFault("life03-delivers-before-stop") {
			s.delivCnt.Add(1)
			s.emitMD("spurious", "spurious")
		}
		javaReceiptDone := false
		for resp, rerr := sp.Recv(); rerr == nil; resp, rerr = sp.Recv() {
			if props := resp.GetSubscriptionProperties(); props != nil {
				if s.isEOD.Swap(props.GetExactlyOnceDeliveryEnabled()) != props.GetExactlyOnceDeliveryEnabled() {
					m3dSig(s.extReset)
					if s.m.hasFault("eod01b-multi-stream") {
						if sp2, e2 := s.client.StreamingPull(callCtx); e2 == nil {
							defer func() { _ = sp2.CloseSend() }()
							if sp2.Send(&pubsubpb.StreamingPullRequest{Subscription: s.sub, StreamAckDeadlineSeconds: 60, ClientId: s.clientID, ProtocolVersion: map[bool]int64{true: 1}[s.m.caps.GetSupportsKeepalive()]}) == nil {
								_, _ = sp2.Header()
							}
						}
					}
					if strings.EqualFold(s.m.caps.GetClientLanguage(), "cpp") && s.start.GetMinDurationPerAckExtensionMs() > 0 && !s.m.hasFault("eod01-ignores-min-15s") {
						_ = sp.Send(&pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: int32(s.start.GetMinDurationPerAckExtensionMs() / 1000)})
					}
				}
				s.isOrdered.Store(props.GetMessageOrderingEnabled())
			}
			if !s.m.hasFault("life03-delivers-before-stop") && len(resp.GetReceivedMessages()) > 0 {
				s.handleBatch(resp.GetReceivedMessages(), fc, &javaReceiptDone)
			}
		}
		stCancel()
		if s.pullCtx.Err() != nil {
			return
		}
	}
}
func m3dEntryIDs(entries []*m3dEntry) []string {
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ackID
	}
	return ids
}
func (s *m3dMockSub) handleBatch(rms []*pubsubpb.ReceivedMessage, fc *m3dFC, javaRecDone *bool) {
	eod := s.isEOD.Load() && !s.m.hasFault("eod01b-ignores-toggle")
	ordered := ((s.isOrdered.Load() && s.m.caps.GetSupportsSubscriberOrdering()) || s.m.hasFault("subf02b-ignores-dynamic-ordering")) && !s.m.hasFault("subf02-ignores-ordering")
	entries := make([]*m3dEntry, 0, len(rms))
	for _, rm := range rms {
		mid, data, key := rm.GetMessage().GetMessageId(), rm.GetMessage().GetData(), rm.GetMessage().GetOrderingKey()
		if rw, ok := map[string][4]string{
			"subf01-dup-delivery-phase-a": {"msg-subf01-a3", "msg-subf01-a1", "", ""}, "subf02-wrong-initial-set": {"msg-subf02-kb1", "msg-subf02-extra", "", ""},
			"subf02-ka2-early": {"msg-subf02-kb1", "msg-subf02-ka2", "", ""}, "subf02-wrong-key": {"msg-subf02-kb1", "", "wrongKey", ""},
			"subf02-ka2-bad-data": {"msg-subf02-ka2", "", "", "bad"}, "subf03-wrong-msg": {"msg-subf03-after", "msg-subf03-bogus", "", ""},
		}[s.m.cfg.fault]; ok && mid == rw[0] {
			mid, key = cmp.Or(rw[1], mid), cmp.Or(rw[2], key)
			if rw[3] != "" {
				data = []byte(rw[3])
			}
		}
		m2 := &pubsubpb.PubsubMessage{MessageId: mid, Data: data, OrderingKey: key}
		mk := func() *m3dEntry {
			return &m3dEntry{tok: fmt.Sprintf("dt-%d", s.m.seq.Add(1)), ackID: rm.GetAckId(), msg: m2, eod: eod, fc: fc, settleCh: make(chan proxypb.SettleMessageCommand_SettleAction, 1)}
		}
		entries = append(entries, mk())
		if s.m.hasFault("subf03-triple-dup") && mid == "msg-subf03-dup" {
			entries = append(entries, mk())
		}
	}
	nackRem := func(rem []*m3dEntry) {
		if !s.m.hasAny("subf01-no-undispatched-nack", "life02-skips-queued-nack", "python-fast-stop-no-nack") {
			s.sendModAck(m3dEntryIDs(rem), 0)
		}
	}
	if len(entries) > 0 && s.m.caps.GetSupportsEodReceiptModack() {
		dl, ids := s.ackDeadlineSec(eod), m3dEntryIDs(entries)
		if strings.EqualFold(s.m.caps.GetClientLanguage(), "java") && !*javaRecDone && s.start.MinDurationPerAckExtensionMs == nil {
			*javaRecDone, dl = true, 10
		}
		switch {
		case !eod && !s.m.hasFault("eod01b-eod-rec-when-false"):
			if strings.EqualFold(s.m.caps.GetClientLanguage(), "python") {
				s.sendModAck(ids, dl)
			} else {
				s.leaseWG.Go(func() { s.sendModAck(ids, dl) })
			}
		case s.m.hasFault("eod04-hangs-receipt-modack"):
			<-s.pullCtx.Done()
			return
		case eod && s.m.hasAny("eod04-skips-receipt-modack", "eod04-skips-receipt-modack-slow"):
		case s.m.hasFault("eod04-no-gate"):
			s.leaseWG.Go(func() { _ = s.modAckRetry(s.pullCtx, ids, dl, true) })
		default:
			if res := s.modAckRetry(s.pullCtx, ids, dl, true); s.pullCtx.Err() != nil {
				nackRem(entries)
				return
			} else if s.m.hasFault("eod04-perm-first") {
				entries = slices.DeleteFunc(entries, func(e *m3dEntry) bool { return e.ackID == "eod04-surv" })
			} else if s.m.hasFault("eod04-perm-second") {
				slices.Reverse(entries)
			} else if !s.m.hasAny("eod04-delivers-perm-failed", "eod04-no-retry-transient") {
				entries = slices.DeleteFunc(entries, func(e *m3dEntry) bool { return res[e.ackID].st != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS })
			}
		}
	}
	seenBatch := map[string]bool{}
	for i, e := range entries {
		var b int64
		ok := true
		if !s.m.hasFault("subf01-cancel-ignores-fc") || e.ackID != "ack-subf01-q3" {
			b, ok = fc.acquire(s.pullCtx, int64(len(e.msg.GetData())))
		}
		if !ok {
			nackRem(entries[i:])
			return
		}
		e.boundedFC = b
		s.mu.Lock()
		dupFail := (s.pendingAckIDs[e.ackID] || seenBatch[e.ackID]) && (s.m.hasFault("subf03-dups-fail") || (ordered && s.m.hasFault("subf03-ord-dup-fail")))
		if stopping := s.stopping; stopping || dupFail || (strings.EqualFold(s.m.caps.GetClientLanguage(), "java") && !eod && s.pendingAckIDs[e.ackID]) {
			s.mu.Unlock()
			e.releaseFC()
			if !stopping && !dupFail {
				continue
			}
			if dupFail {
				s.fatalStop(int32(codes.Internal), "duplicate ack_id panic")
			} else {
				nackRem(entries[i:])
			}
			return
		}
		seenBatch[e.ackID], s.pendingAckIDs[e.ackID], s.active[e.tok] = true, true, e
		var prevCh, nextCh chan struct{}
		if k := e.msg.GetOrderingKey(); k != "" && ordered {
			prevCh, nextCh = s.keyChains[k], make(chan struct{})
			s.keyChains[k] = nextCh
			if s.m.hasFault("subf02-cross-key-unlock") && e.msg.GetMessageId() == "msg-subf02-ka2" {
				prevCh = make(chan struct{})
				s.keyChains["_x"] = prevCh
			} else if s.m.hasFault("subf02-cross-key-unlock") && e.msg.GetMessageId() == "msg-subf02-kb1" {
				nextCh = s.keyChains["_x"]
			}
		}
		s.cbWG.Add(1)
		s.mu.Unlock()
		go s.dispatch(e, prevCh, nextCh)
	}
}
func (s *m3dMockSub) dispatch(e *m3dEntry, prevCh, nextCh chan struct{}) {
	defer s.cbWG.Done()
	var skipAdvance bool
	defer func() {
		if nextCh != nil && !skipAdvance {
			close(nextCh)
		}
	}()
	defer e.releaseFC()
	if prevCh != nil {
		select {
		case <-prevCh:
		case <-s.abortCtx.Done():
			return
		}
	}
	delay := map[[2]string]time.Duration{
		{"subf03-triple-dup", "ack-subf03-after"}: 25 * time.Millisecond, {"subf01-ignores-count-fc", "ack-subf01-a3"}: 10 * time.Millisecond,
		{"subf01-ignores-byte-fc", "ack-subf01-b3"}: 10 * time.Millisecond, {"subf01-cancel-ignores-fc", "ack-subf01-q3"}: 10 * time.Millisecond,
		{"subf01b-always-per-stream", "ack-subf01b-s1"}: 10 * time.Millisecond, {"eod04-no-gate", "eod04-gate"}: 10 * time.Millisecond, {"eod04-perm-second", "eod04-perm"}: 10 * time.Millisecond, {"eod04-skips-receipt-modack-slow", "eod04-gate"}: 75 * time.Millisecond,
	}[[2]string{s.m.cfg.fault, e.ackID}]
	if s.abortCtx.Err() != nil || (delay > 0 && !m3dSleepTimer(s.abortCtx, delay)) {
		return
	}
	act := cmp.Or(map[string]int32{"subf01-active-cb-exceeded": 99, "subf01b-shared-bad-cb": 2, "m3d-low-active-cb": 1}[s.m.cfg.fault], s.activeCB.Add(1))
	defer s.activeCB.Add(-1)
	eofFault := s.m.hasAny("sub-send-eof-with-stopped", "sub-send-eof-only")
	if n := s.delivCnt.Add(1); n > 1 && eofFault {
		s.delivCnt.Add(-1)
		return
	}
	if s.m.hasFault("eod-premature-ack") && !strings.HasPrefix(e.ackID, "ack-subf01-q") && !strings.HasPrefix(e.ackID, "ack-life01-b") && !strings.HasPrefix(e.ackID, "ack-life01-c") {
		_ = s.settleWithRetry(s.sessCtx, []string{e.ackID}, true, false, e.eod, false, 0)
	}
	auto := s.start.AutoSettleMode
	hasAuto := auto == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT || auto == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT
	s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{
		DeliveryToken: e.tok, MessageId: e.msg.GetMessageId(), Data: e.msg.GetData(), Attributes: e.msg.GetAttributes(), OrderingKey: e.msg.GetOrderingKey(),
		ActiveCallbacks: act, IsExactlyOnce: (e.eod && !s.m.hasFault("eod01-wrong-is-eod") && !(hasAuto && s.m.hasFault("eod02-auto-wrong-is-eod")) && !(strings.HasPrefix(e.ackID, "eod03b-m-") && s.m.hasFault("eod03b-cpp-sb-wrong-is-eod"))) || s.m.hasFault("eod01b-wrong-eod-phase1"), AutoSettled: (hasAuto && !s.m.hasFault("eod02-auto-missing-flag")) || s.m.hasFault("eod-auto-settled-interactive"),
	}}})
	if eofFault {
		s.fatalStop(int32(codes.Internal), "stopped-before-settle")
	} else if s.m.hasFault("subf02-delivers-ka4-before-nack") && e.msg.GetMessageId() == "msg-subf02-ka3" && nextCh != nil {
		close(nextCh)
		skipAdvance = true
	}
	settleAct := map[bool]proxypb.SettleMessageCommand_SettleAction{true: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT, false: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT}[auto == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT]
	if !hasAuto {
		select {
		case settleAct = <-e.settleCh:
		case <-s.abortCtx.Done():
			return
		}
	}
	if !e.settled.CompareAndSwap(false, true) {
		return
	}
	isNack := settleAct == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK || settleAct == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
	if isNack && s.m.hasFault("subf02-no-advance-after-nack") {
		skipAdvance = true
	}
	want := settleAct == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT || settleAct == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
	item := &m3dSettleItem{entry: e, isNack: isNack, want: want, done: make(chan struct{})}
	s.mu.Lock()
	delete(s.active, e.tok)
	delete(s.pendingAckIDs, e.ackID)
	dst := map[bool]*[]*m3dSettleItem{true: &s.pendNacks, false: &s.pendAcks}[isNack]
	*dst = append(*dst, item)
	s.mu.Unlock()
	m3dSig(map[bool]chan struct{}{true: s.autoCh, false: s.flushCh}[hasAuto])
	if want {
		select {
		case <-item.done:
		case <-s.abortCtx.Done():
		}
	}
}

type m3dRes struct {
	st   proxypb.SettlementResultEvent_AckStatus
	code int32
	msg  string
}

func m3dErrInfo(err error) map[string]string {
	if st, ok := status.FromError(err); ok {
		for _, d := range st.Details() {
			if ei, ok := d.(*errdetails.ErrorInfo); ok && len(ei.GetMetadata()) > 0 {
				return ei.GetMetadata()
			}
		}
	}
	return nil
}
func m3dMapCode(c codes.Code) proxypb.SettlementResultEvent_AckStatus {
	return cmp.Or(map[codes.Code]proxypb.SettlementResultEvent_AckStatus{
		codes.OK: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, codes.PermissionDenied: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED,
		codes.FailedPrecondition: proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, codes.Unavailable: proxypb.SettlementResultEvent_ACK_STATUS_UNAVAILABLE,
	}[c], proxypb.SettlementResultEvent_ACK_STATUS_OTHER)
}
func (s *m3dMockSub) settleWithRetry(ctx context.Context, ids []string, isAck, isNack, isEOD, isReceipt bool, dl int32) map[string]m3dRes {
	out, pending := make(map[string]m3dRes, len(ids)), slices.Clone(ids)
	noSplit := s.m.hasAny("eod03b-no-split-perm", "eod03b-retries-perm", "eod03b-retries-ok", "eod04-retries-perm")
	retryPerm := s.m.hasAny("eod03-retries-permanent", "eod03b-retries-perm", "eod03b-retries-ok", "eod04-retries-perm") || (!isAck && s.m.hasFault("eod03b-cpp-modack-retries-perm")) || (isAck && s.m.hasFault("eod03b-cpp-ack-retries-perm")) || (isNack && s.m.hasFault("eod03-nack-retries-perm"))
	for attempt := 0; len(pending) > 0; attempt++ {
		if !isReceipt && s.m.hasFault("eod03-dups-in-retry") && len(pending) == 1 && attempt > 0 {
			pending = append(pending, pending[0])
		}
		var err error
		if !(isAck && !isEOD && s.m.hasFault("eod02-skips-noneod-ack")) {
			sub, cCtx := s.m.outCtx(ctx, s.sub, isAck)
			if isAck {
				_, err = s.client.Acknowledge(cCtx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: pending})
				if !isEOD && s.m.hasFault("eod01b-detached-retry-when-false") && slices.Contains(pending, "ack-eod01b-3") {
					ids2 := slices.Clone(pending)
					s.m.wg.Go(func() {
						if m3dSleepTimer(s.m.srvCtx, 25*time.Millisecond) {
							sub2, cCtx2 := s.m.outCtx(s.m.srvCtx, s.sub, true)
							_, _ = s.client.Acknowledge(cCtx2, &pubsubpb.AcknowledgeRequest{Subscription: sub2, AckIds: ids2})
						}
					})
				}
			} else {
				_, err = s.client.ModifyAckDeadline(cCtx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: s.sub, AckIds: pending, AckDeadlineSeconds: dl})
			}
		}
		if err == nil {
			for _, id := range pending {
				out[id] = m3dRes{st: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
			}
			break
		}
		md, st := m3dErrInfo(err), status.Convert(err)
		transCode := slices.Contains([]codes.Code{codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unavailable}, st.Code())
		switch {
		case !isEOD && s.m.hasFault("eod01b-retries-when-false") && attempt == 0:
		case !isEOD || (isReceipt && s.m.hasFault("eod04-no-retry-transient")) || (!isReceipt && s.m.hasFault("eod03-no-retry-transient")) || (isNack && !isReceipt && s.m.hasFault("eod03-nack-no-retry")):
			for _, id := range pending {
				out[id] = map[bool]m3dRes{true: {st: m3dMapCode(st.Code()), code: int32(st.Code()), msg: st.Message()}, false: map[bool]m3dRes{true: {code: -1}, false: {st: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}}[!isReceipt && s.m.hasFault("eod02-noneod-hangs-ack-res")]}[isEOD || s.m.hasAny("eod01b-keeps-eod-err", "eod02-noneod-raw-status")]
			}
			pending = nil
		case len(md) > 0:
			var next []string
			for _, id := range pending {
				if reason, ok := md[id]; !ok {
					if s.m.hasFault("eod03b-retries-ok") && attempt == 0 {
						next = append(next, id)
					} else {
						out[id] = m3dRes{st: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
					}
				} else if (strings.HasPrefix(reason, "TRANSIENT_") || (attempt == 0 && retryPerm)) && attempt < 3 {
					next = append(next, id)
				} else {
					out[id] = m3dRes{st: map[bool]proxypb.SettlementResultEvent_AckStatus{true: proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, false: proxypb.SettlementResultEvent_ACK_STATUS_OTHER}[reason == fake.EODPermanentInvalidAckID], code: int32(st.Code()), msg: reason}
				}
			}
			if noSplit && len(next) > 0 {
				next = slices.Clone(pending)
			}
			pending = next
		case (transCode && !s.m.hasFault("eod03-no-fallback-retry") && !(isNack && s.m.hasFault("eod03-nack-no-retry")) || s.m.hasFault("eod03-retries-permanent") || (isNack && s.m.hasFault("eod03-nack-retries-perm"))) && attempt < 3:
		default:
			rst := m3dMapCode(st.Code())
			if s.m.hasFault("eod02-wrong-status") && rst == proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED {
				rst = proxypb.SettlementResultEvent_ACK_STATUS_OTHER
			}
			for _, id := range pending {
				out[id] = m3dRes{st: rst, code: int32(st.Code()), msg: st.Message()}
			}
			pending = nil
		}
		if len(pending) > 0 && !m3dSleepTimer(ctx, 15*time.Millisecond) {
			return out
		}
	}
	return out
}
func (s *m3dMockSub) modAckRetry(ctx context.Context, ids []string, dl int32, isRec bool) map[string]m3dRes {
	return s.settleWithRetry(ctx, ids, false, dl == 0, s.isEOD.Load() || s.m.hasFault("eod01b-eod-rec-when-false"), isRec, dl)
}
func (s *m3dMockSub) runSettleLoop(done <-chan struct{}) {
	bt, batchC := time.NewTimer(time.Hour), (<-chan time.Time)(nil)
	bt.Stop()
	defer bt.Stop()
	flushItems := func(items []*m3dSettleItem, isNack bool) {
		single := !isNack && ((strings.EqualFold(s.m.caps.GetClientLanguage(), "cpp") && s.m.hasAny("cpp-single-ack", "eod03b-cpp-modack-retries-perm", "eod03b-cpp-ack-retries-perm", "eod03b-cpp-sb-wrong-is-eod")) || s.m.hasFault("eod03b-unbatched-ack"))
		for i, end := 0, 0; i < len(items); i = end {
			end = map[bool]int{true: i + 1, false: len(items)}[single]
			grp, seen := items[i:end], map[string]bool{}
			eod, ids := grp[0].entry.eod, make([]string, 0, len(grp))
			for _, it := range grp {
				if !seen[it.entry.ackID] {
					seen[it.entry.ackID], ids = true, append(ids, it.entry.ackID)
				}
			}
			res := s.settleWithRetry(s.sessCtx, ids, !isNack, isNack, eod, false, 0)
			for _, it := range grp {
				if r := res[it.entry.ackID]; it.want && r.code >= 0 {
					if eod && s.m.hasFault("eod01b-swallows-eod-err") {
						r = m3dRes{st: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
					} else if r.st == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS && s.m.hasFault("eod02-nonzero-code-on-success") {
						r.code = int32(codes.Unavailable)
					} else if (s.m.hasFault("eod02-aa-wrong-status") && it.entry.ackID == "ack-eod02-aa-err") ||
						(s.m.hasFault("eod02-an-wrong-status") && it.entry.ackID == "nack-eod02-an-err") ||
						(s.m.hasFault("eod03b-wrong-status") && it.entry.ackID == "eod03b-a-perm") {
						r.st, r.code = proxypb.SettlementResultEvent_ACK_STATUS_OTHER, int32(codes.OutOfRange)
					}
					if s.m.hasFault("eod02-unexpected-md") {
						s.emitMD("spurious", "spurious")
					} else if s.m.hasFault("eod02-stops-on-settle") {
						s.fatalStop(int32(codes.Internal), "stop on settle")
					} else {
						s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: it.entry.tok, Status: r.st, GrpcErrorCode: r.code, ErrorMessage: r.msg, IsNack: isNack}}})
					}
				}
				close(it.done)
			}
		}
	}
	flush := func() {
		batchC = nil
		s.mu.Lock()
		acks, nacks := s.pendAcks, s.pendNacks
		s.pendAcks, s.pendNacks = nil, nil
		s.mu.Unlock()
		flushItems(acks, false)
		flushItems(nacks, true)
	}
	for {
		select {
		case <-done:
			flush()
			return
		case <-s.autoCh:
			if batchC == nil {
				bt.Reset(20 * time.Millisecond)
				batchC = bt.C
			}
		case <-batchC:
			flush()
		case <-s.flushCh:
			flush()
		}
	}
}
func (s *m3dMockSub) runLeaseLoop(done <-chan struct{}) {
	t := time.NewTimer(35 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-s.extReset:
			t.Reset(35 * time.Millisecond)
		case <-t.C:
			s.mu.Lock()
			ids, seen, eod := []string(nil), map[string]bool{}, s.isEOD.Load()
			for _, e := range s.active {
				if !seen[e.ackID] {
					seen[e.ackID], ids = true, append(ids, e.ackID)
				}
			}
			s.mu.Unlock()
			if len(ids) > 0 && !s.m.hasAny("eod01-no-periodic", "eod04-skips-receipt-modack-slow") {
				dl := s.ackDeadlineSec(eod)
				if strings.EqualFold(s.m.caps.GetClientLanguage(), "cpp") && s.start.MaxDurationPerAckExtensionMs == nil {
					dl = 600
				}
				s.modAckRetry(s.pullCtx, ids, dl, false)
			}
			t.Reset(35 * time.Millisecond)
		}
	}
}
func (s *m3dMockSub) bulkNackActive() {
	s.mu.Lock()
	var ids []string
	for tok, e := range s.active {
		if e.settled.CompareAndSwap(false, true) {
			delete(s.active, tok)
			ids = append(ids, e.ackID)
		}
	}
	s.mu.Unlock()
	if !s.m.hasFault("life02-skips-active-nack") {
		s.sendModAck(ids, 0)
	}
}
func (m *m3dMockServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	if m.hasFault("runtime-unimpl-sub") || strings.EqualFold(m.caps.GetClientLanguage(), "php") {
		return status.Error(codes.Unimplemented, "SubscriberSession unimplemented")
	}
	first, err := stream.Recv()
	if err != nil || first.GetStart() == nil {
		return cmp.Or(err, status.Error(codes.InvalidArgument, "expected StartSubscriberCommand"))
	}
	start := first.GetStart()
	cppNoEOD := strings.EqualFold(m.caps.GetClientLanguage(), "cpp") && (!start.GetUseExactlyOnce() || m.hasFault("cpp-ignores-use-exactly-once"))
	if cppNoEOD && (start.GetAutoSettleMode() == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT || start.GetAutoSettleMode() == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT) {
		return status.Error(codes.Unimplemented, "cpp requires use_exactly_once")
	}
	conn, err := m3dMockDial(start.GetEndpoint())
	if err != nil {
		return err
	}
	defer conn.Close()
	sessCtx, sessCancel := context.WithCancel(stream.Context())
	defer context.AfterFunc(m.srvCtx, sessCancel)()
	defer sessCancel()
	pullCtx, pullCancel := context.WithCancel(sessCtx)
	abortCtx, abortCancel := context.WithCancel(sessCtx)
	s := &m3dMockSub{
		m: m, stream: stream, client: pubsubpb.NewSubscriberClient(conn), sub: start.GetSubscription(), clientID: fmt.Sprintf("m3d-cid-%d", m.seq.Add(1)), start: start,
		sessCtx: sessCtx, sessCancel: sessCancel, pullCtx: pullCtx, pullCancel: pullCancel, abortCtx: abortCtx, abortCancel: abortCancel,
		sendCh: make(chan *proxypb.SubscriberEvent, 128), flushCh: make(chan struct{}, 8), autoCh: make(chan struct{}, 8), stopCh: make(chan struct{}), extReset: make(chan struct{}, 1), secondStopCh: make(chan struct{}),
		active: map[string]*m3dEntry{}, pendingAckIDs: map[string]bool{}, keyChains: map[string]chan struct{}{},
	}
	senderDone := make(chan struct{})
	m.wg.Go(func() {
		defer close(senderDone)
		for ev := range s.sendCh {
			if stream.Send(ev) != nil {
				sessCancel()
				return
			}
		}
	})
	m.wg.Go(func() {
		for cmd, rerr := stream.Recv(); rerr == nil; cmd, rerr = stream.Recv() {
			if st := cmd.GetStop(); st != nil {
				if m.hasFault("sub-stop-inband-unimpl") {
					s.fatalStop(int32(codes.Unimplemented), "stop unimplemented")
					return
				}
				s.mu.Lock()
				already := s.stopping
				s.mu.Unlock()
				if already {
					s.secondStopOnce.Do(func() { close(s.secondStopCh) })
					if m.hasFault("life03-stop-fails-inband") {
						s.fatalStop(int32(codes.FailedPrecondition), "second stop failed")
						return
					}
				}
				s.triggerStop(st.GetShutdownBehavior(), st.ShutdownTimeoutMs)
			} else if sc := cmd.GetSettle(); sc != nil {
				if a := sc.GetAction(); cppNoEOD && (a == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT || a == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT) {
					s.fatalStop(int32(codes.FailedPrecondition), "cpp requires use_exactly_once")
					return
				}
				s.mu.Lock()
				e := s.active[sc.GetDeliveryToken()]
				s.mu.Unlock()
				if e != nil {
					select {
					case e.settleCh <- sc.GetAction():
					default:
					}
				}
			}
		}
		s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
	})
	leaseStop := make(chan struct{})
	s.leaseWG.Go(func() { s.runSettleLoop(leaseStop) })
	s.leaseWG.Go(func() { s.runLeaseLoop(leaseStop) })
	perStream := (start.GetEnablePerStreamFlowControl() && m.caps.GetSupportsPerStreamFlowControl() && !m.hasFault("subf01b-ignores-per-stream")) || m.hasFault("subf01b-always-per-stream")
	maxConc := map[bool]int32{true: 1, false: start.GetMaxConcurrency()}[m.hasFault("subf02-serializes-all-keys")]
	newFC := func() *m3dFC {
		return m3dNewFC(start.GetMaxOutstandingMessages(), start.GetMaxOutstandingBytes(), maxConc, strings.EqualFold(m.caps.GetClientLanguage(), "node"), m.hasFault("subf01-ignores-count-fc"), m.hasFault("subf01-ignores-byte-fc"))
	}
	nStreams := int(start.GetNumStreams())
	if start.NumStreams == nil {
		nStreams = map[string]int{"node": 5, "dotnet": 2}[strings.ToLower(m.caps.GetClientLanguage())]
		if nStreams == 5 && start.GetMaxOutstandingMessages() > 0 {
			nStreams = min(5, int(start.GetMaxOutstandingMessages()))
		}
	}
	nStreams = map[bool]int{true: 2, false: max(nStreams, 1)}[m.hasFault("life03-extra-stream")]
	var readyCh chan struct{}
	if nStreams > 1 {
		readyCh = make(chan struct{}, nStreams)
	}
	for i, fc := 0, newFC(); i < nStreams; i++ {
		s.streamWG.Add(1)
		if i > 0 && perStream {
			fc = newFC()
		}
		go s.runPull(fc, readyCh)
	}
	for ; readyCh != nil && nStreams > 0; nStreams-- {
		select {
		case <-readyCh:
		case <-sessCtx.Done():
		}
	}
	s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: s.clientID}}})
	select {
	case <-s.stopCh:
	case <-sessCtx.Done():
	}
	subf01A := strings.Contains(s.sub, "subf01-count-sub")
	if m.hasAny("subf01-stop-deliv", "sub-empty-tok") {
		s.emitMD(map[bool]string{true: "", false: "spurious"}[m.hasFault("sub-empty-tok")], "spurious")
	} else if m.hasFault("subf01-stop-settle") && subf01A {
		s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: "dt-spurious"}}})
	} else if m.hasFault("sub-unexpected-ev") {
		s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: s.clientID}}})
	}
	if m.hasFault("life03-stop-fails-inband") {
		select {
		case <-s.secondStopCh:
		case <-sessCtx.Done():
		}
	}
	s.pullCancel()
	s.streamWG.Wait()
	s.mu.Lock()
	bh, hasTO, to := s.stopBh, s.hasStopTO, s.stopTO
	s.mu.Unlock()
	if m.hasAny("life01-ignores-wait", "python-fast-stop", "python-fast-stop-no-nack", "subf01-stop-deliv") || (subf01A && m.hasAny("python-fast-stop-err", "subf01-stop-settle", "subf01-bad-total-delivered")) {
		bh = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY
	}
	abortWait := func(nack bool) {
		s.abortCancel()
		if nack {
			s.bulkNackActive()
		}
		s.cbWG.Wait()
	}
	switch {
	case bh == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY:
		abortWait(!m.hasAny("python-fast-stop", "python-fast-stop-no-nack"))
	case m.hasFault("life01-hangs-finite-timeout") || !hasTO || to < 0:
		s.cbWG.Wait()
	default:
		cbDone, tm := make(chan struct{}), time.NewTimer(to)
		go func() { s.cbWG.Wait(); close(cbDone) }()
		select {
		case <-cbDone:
		case <-tm.C:
			abortWait(!strings.EqualFold(m.caps.GetClientLanguage(), "go"))
			<-cbDone
		}
		tm.Stop()
	}
	close(leaseStop)
	s.leaseWG.Wait()
	if m.hasFault("life03-spurious-ack") {
		_ = s.settleWithRetry(m.srvCtx, []string{"spurious"}, true, false, false, false, 0)
	}
	if m.hasFault("life01-bad-nack") && hasTO && to > 0 {
		s.sendModAck([]string{"ack-unexpected"}, 0)
	}
	s.mu.Lock()
	fcCode, fcMsg := s.fatalC, s.fatalM
	s.mu.Unlock()
	if m.hasFault("life03-nonzero-stopped") || (subf01A && m.hasFault("python-fast-stop-err")) {
		fcCode, fcMsg = int32(codes.Internal), "err"
	} else if m.hasFault("sub-inband-unimpl") {
		fcCode, fcMsg = int32(codes.Unimplemented), "inband unimplemented"
	}
	totDel := map[bool]int64{true: 99, false: s.delivCnt.Load()}[m.hasFault("life03-bad-total") || (subf01A && m.hasFault("subf01-bad-total-delivered"))]
	if m.hasFault("life01-deliv-on-stop") {
		s.emitMD("spurious", "spurious")
	} else if m.hasFault("sub-stop-bad-settle") {
		s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: "dt-bad", Status: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, GrpcErrorCode: int32(codes.PermissionDenied)}}})
	}
	if !m.hasFault("sub-send-eof-only") {
		s.emit(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: fcCode, ErrorMessage: fcMsg, TotalDelivered: totDel}}})
	}
	close(s.sendCh)
	<-senderDone
	if m.hasFault("life02-post-stop-modack") && m3dSleepTimer(m.srvCtx, 12*time.Millisecond) {
		s.sendModAck([]string{"ack-life02-1"}, 60)
	}
	return nil
}

type m3dFaultCase struct {
	prof, fault, sc, sub string
	timeout              time.Duration
	want                 harness.Status
	capsOverride         func(*proxypb.GetCapabilitiesResponse)
}

func m3dRunScenarios(t *testing.T, cfg m3dMockConfig, list []harness.Scenario, filter string, timeoutCap time.Duration) *harness.TargetReport {
	t.Helper()
	reg := scenarios.NewRegistry()
	for _, sc := range list {
		if timeoutCap > 0 && (sc.Timeout == 0 || sc.Timeout > timeoutCap) {
			sc.Timeout = timeoutCap
		}
		reg.MustRegister(sc)
	}
	target := harness.TargetSpec{Name: cfg.profile, Connect: m3dMockConnect(cfg)}
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
func m3dAssertUnitMeta(t *testing.T, list, want []harness.Scenario) {
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
func m3dRunProfileTests(t *testing.T, list []harness.Scenario, total int, to time.Duration, skipOverrides map[string]int) {
	t.Helper()
	for _, prof := range []string{"go", "cpp", "rust", "java", "python", "ruby", "node", "dotnet", "php"} {
		t.Run("profile_"+prof, func(t *testing.T) {
			wantP, rep := total-skipOverrides[prof], m3dRunScenarios(t, m3dMockConfig{profile: prof}, list, "", to)
			if rep.Summary.Pass != wantP || rep.Summary.UnsupportedByClient != total-wantP || rep.Summary.Fail != 0 || rep.Summary.TimeoutCount != 0 || rep.Summary.ErrorCount != 0 {
				t.Fatalf("profile=%s want pass=%d unsup=%d, got %+v scenarios=%+v", prof, wantP, total-wantP, rep.Summary, rep.Scenarios)
			}
		})
	}
}
func m3dRunFaultCases(t *testing.T, list []harness.Scenario, cases []m3dFaultCase) {
	t.Helper()
	for _, tc := range cases {
		to, want := cmp.Or(tc.timeout, 2*time.Second), cmp.Or(tc.want, harness.StatusFail)
		name := tc.sc + "_" + tc.prof + "_" + tc.fault
		t.Run(name, func(t *testing.T) {
			sr := m3dRunScenarios(t, m3dMockConfig{profile: tc.prof, fault: tc.fault, capsOverride: tc.capsOverride}, list, "^"+tc.sc+"$", to).Scenarios[0]
			if sr.Status != want || (tc.sub != "" && !strings.Contains(sr.Error, tc.sub)) {
				t.Errorf("%s: want %s (%q), got %s (%q)", name, want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}
func m3dGoF(sc, fault, sub string) m3dFaultCase {
	return m3dFaultCase{prof: "go", fault: fault, sc: sc, sub: sub}
}
func m3dCountFDs() int {
	if ents, err := os.ReadDir("/proc/self/fd"); err == nil {
		return len(ents)
	}
	return -1
}
func m3dRunNoLeakFaults(t *testing.T, list []harness.Scenario, to time.Duration, cases ...[3]string) {
	t.Helper()
	settled := func(maxG, maxFD int) {
		dl, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(10*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for ; ; <-tk.C {
			if s, fd := string(buf[:runtime.Stack(buf, true)]), m3dCountFDs(); !strings.Contains(s, "m3dMock") && !strings.Contains(s, "startSubPump") && (maxG < 0 || runtime.NumGoroutine() <= maxG) && (maxFD < 0 || fd <= maxFD) {
				return
			} else if time.Now().After(dl) {
				t.Fatalf("leak after timeout: g=%d(max=%d) fd=%d(max=%d)\n%s", runtime.NumGoroutine(), maxG, fd, maxFD, s)
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc[0]+"_"+tc[1], func(t *testing.T) {
			var coldG, coldFD int
			for i := range 4 {
				if sr := m3dRunScenarios(t, m3dMockConfig{profile: tc[0], fault: tc[1]}, list, tc[2], to).Scenarios[0]; sr.Status != harness.StatusTimeout {
					t.Fatalf("want TIMEOUT, got %s (%s)", sr.Status, sr.Error)
				}
				if i == 0 {
					settled(-1, -1)
					coldG, coldFD = runtime.NumGoroutine(), m3dCountFDs()
				}
			}
			settled(coldG, coldFD)
		})
	}
}

func TestSubscriberFlowScenarios_Unit(t *testing.T) {
	m3dAssertUnitMeta(t, scenarios.ExportSubscriberFlowScenarios(), []harness.Scenario{
		{ID: "SUBF-01", Category: "flow_control"}, {ID: "SUBF-01b", Category: "flow_control", RequiredCapabilities: []harness.Capability{harness.CapPerStreamFlowControl}}, {ID: "SUBF-02", Category: "ordering", RequiredCapabilities: []harness.Capability{harness.CapSubscriberOrdering}}, {ID: "SUBF-02b", Category: "ordering", RequiredCapabilities: []harness.Capability{harness.CapSubscriberOrdering}}, {ID: "SUBF-03", Category: "flow_control"},
	})
}
func TestSubscriberFlowScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportSubscriberFlowScenarios()
	m3dRunProfileTests(t, list, 5, 5*time.Second, map[string]int{"cpp": 1, "rust": 3, "python": 1, "ruby": 1, "node": 3, "dotnet": 1, "php": 5, "java": 1})
	m3dRunFaultCases(t, list, []m3dFaultCase{{prof: "python", fault: "python-fast-stop", sc: "SUBF-01", timeout: 5 * time.Second, want: harness.StatusPass}})
}
func TestSubscriberFlowScenarios_FaultDetection(t *testing.T) {
	m3dRunFaultCases(t, scenarios.ExportSubscriberFlowScenarios(), []m3dFaultCase{
		{prof: "go", fault: "runtime-unimpl-sub", sc: "SUBF-01", want: harness.StatusUnsupportedByClient}, m3dGoF("SUBF-01", "sub-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("SUBF-01", "eod-premature-ack", "premature Ack"), m3dGoF("SUBF-01", "eod-auto-settled-interactive", "auto_settled=true"), m3dGoF("SUBF-01", "subf01-bad-frame0-msgs", "max_outstanding_messages mismatch"), m3dGoF("SUBF-01", "subf01-bad-frame0-bytes", "max_outstanding_bytes mismatch"), m3dGoF("SUBF-01", "subf01-ignores-count-fc", "delivered 3rd message"), m3dGoF("SUBF-01", "subf01-cancel-ignores-fc", "delivered 3rd queued message"), m3dGoF("SUBF-01", "subf01-ignores-byte-fc", "delivered 3rd 80B message"), m3dGoF("SUBF-01", "subf01-no-undispatched-nack", "nacked ack_ids set mismatch"), m3dGoF("SUBF-01", "subf01-active-cb-exceeded", "exceeds 2"), m3dGoF("SUBF-01", "subf01-dup-delivery-phase-a", "expected 3 distinct messages"),
		{prof: "python", fault: "subf01-no-undispatched-nack", sc: "SUBF-01", sub: "nacked ack_ids set mismatch"}, {prof: "python", fault: "python-fast-stop-no-nack", sc: "SUBF-01", sub: "not nacked on close"}, {prof: "python", fault: "subf01-stop-deliv", sc: "SUBF-01", sub: "unexpected delivery"}, {prof: "python", fault: "subf01-stop-settle", sc: "SUBF-01", sub: "unexpected SettlementResultEvent"}, {prof: "python", fault: "subf01-bad-total-delivered", sc: "SUBF-01", sub: "total_delivered mismatch"}, {prof: "python", fault: "life03-nonzero-stopped", sc: "SUBF-01", sub: "StoppedEvent failed"}, {prof: "python", fault: "python-fast-stop-err", sc: "SUBF-01", sub: "StoppedEvent failed"},
		m3dGoF("SUBF-01b", "subf01b-always-per-stream", "delivered concurrently when shared cap=1"), m3dGoF("SUBF-01b", "subf01b-shared-bad-cb", "expected active_callbacks=1"), m3dGoF("SUBF-01b", "subf01-bad-frame0-msgs", "max_outstanding_messages mismatch"), m3dGoF("SUBF-01b", "m3d-low-active-cb", "expected concurrent active_callbacks=2"), m3dGoF("SUBF-01b", "eod-auto-settled-interactive", "auto_settled=true"), {prof: "go", fault: "subf01b-ignores-per-stream", sc: "SUBF-01b", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("SUBF-02", "subf02-ignores-ordering", "out of order"), m3dGoF("SUBF-02", "subf02-ka2-early", "delivered msg-subf02-ka2 out of order"), m3dGoF("SUBF-02", "subf02-wrong-initial-set", "expected msg-subf02-kb1"), m3dGoF("SUBF-02", "subf02-wrong-key", "delivered metadata mismatch"), m3dGoF("SUBF-02", "subf02-ka2-bad-data", "delivered data mismatch"), m3dGoF("SUBF-02", "subf02-cross-key-unlock", "delivered out of order"), m3dGoF("SUBF-02", "subf02-delivers-ka4-before-nack", "delivered before msg-subf02-ka3 nacked"), m3dGoF("SUBF-02", "eod-auto-settled-interactive", "auto_settled=true"), {prof: "go", fault: "subf02-serializes-all-keys", sc: "SUBF-02", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout}, {prof: "go", fault: "subf02-no-advance-after-nack", sc: "SUBF-02", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout}, {prof: "go", fault: "subf02b-ignores-dynamic-ordering", sc: "SUBF-02b", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("SUBF-02b", "m3d-low-active-cb", "expected concurrent same-key active_callbacks=2"), m3dGoF("SUBF-02b", "wrong-ack-sub", "subscription mismatch"), m3dGoF("SUBF-02b", "missing-ack-req-params", "x-goog-request-params"), m3dGoF("SUBF-02b", "emit-unexpected-span", "expected zero spans"), m3dGoF("SUBF-02b", "sub-stop-bad-settle", "unexpected settlement failure"), m3dGoF("SUBF-02b", "eod-auto-settled-interactive", "auto_settled=true"), m3dGoF("SUBF-03", "subf03-dups-fail", "duplicate ack_id panic"), m3dGoF("SUBF-03", "subf03-ord-dup-fail", "duplicate ack_id panic"), m3dGoF("SUBF-03", "subf03-wrong-msg", "unexpected message_id"), m3dGoF("SUBF-03", "subf03-triple-dup", "expected 1 or 2 deliveries"), m3dGoF("SUBF-03", "sub-send-eof-with-stopped", "stopped-before-settle"), m3dGoF("SUBF-03", "sub-send-eof-only", "settle EOF"), m3dGoF("SUBF-03", "eod-auto-settled-interactive", "auto_settled=true"),
		m3dGoF("SUBF-01", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("SUBF-01b", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("SUBF-02", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("SUBF-02b", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("SUBF-03", "sub-stop-inband-unimpl", "UNIMPLEMENTED"),
	})
}
func TestSubscriberFlowScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3dRunNoLeakFaults(t, scenarios.ExportSubscriberFlowScenarios(), 650*time.Millisecond, [3]string{"go", "subf01b-ignores-per-stream", "^SUBF-01b$"}, [3]string{"go", "subf02-no-advance-after-nack", "^SUBF-02$"}, [3]string{"go", "subf02b-ignores-dynamic-ordering", "^SUBF-02b$"})
}

func TestSubscriberFlow_NoShutdownBehavior_TimeoutToAckFallback(t *testing.T) {
	rep := m3dRunScenarios(t, m3dMockConfig{
		profile: "go",
		capsOverride: func(c *proxypb.GetCapabilitiesResponse) {
			c.SupportsShutdownBehavior = false
			c.SupportsShutdownTimeout = false
		},
	}, scenarios.ExportSubscriberFlowScenarios(), "^SUBF-01$", 5*time.Second)
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusPass {
		t.Fatalf("SUBF-01 without CapShutdownBehavior expected PASS, got %+v", rep.Scenarios)
	}
}
