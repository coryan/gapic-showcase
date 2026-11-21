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

package scenarios

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type tcpDropPair struct {
	cc, sc net.Conn
	once   sync.Once
}

func (p *tcpDropPair) closeConn(rst bool) {
	p.once.Do(func() {
		if tc, ok := p.cc.(*net.TCPConn); rst && ok {
			_ = tc.SetLinger(0)
		}
		_, _ = p.cc.Close(), p.sc.Close()
	})
}

type tcpDropForwarder struct {
	lis    net.Listener
	target string
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	pairs  map[*tcpDropPair]struct{}
	wg     sync.WaitGroup
}

func newTCPDropForwarder(targetAddr string) (*tcpDropForwarder, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen tcpDropForwarder: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &tcpDropForwarder{lis: lis, target: targetAddr, ctx: ctx, cancel: cancel, pairs: map[*tcpDropPair]struct{}{}}
	f.wg.Go(f.acceptLoop)
	return f, nil
}
func (f *tcpDropForwarder) KillActive() { f.closePairs(true, false) }
func (f *tcpDropForwarder) Close()      { f.closePairs(false, true) }
func (f *tcpDropForwarder) acceptLoop() {
	for d := (net.Dialer{Timeout: 2 * time.Second}); ; {
		cc, err := f.lis.Accept()
		if err != nil {
			return
		}
		sc, err := d.DialContext(f.ctx, "tcp", f.target)
		if err != nil {
			_ = cc.Close()
			continue
		}
		p := &tcpDropPair{cc: cc, sc: sc}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			p.closeConn(false)
			return
		}
		f.pairs[p] = struct{}{}
		for _, ab := range [2][2]net.Conn{{sc, cc}, {cc, sc}} {
			f.wg.Go(func() {
				_, _ = io.Copy(ab[0], ab[1])
				p.closeConn(false)
				f.mu.Lock()
				delete(f.pairs, p)
				f.mu.Unlock()
			})
		}
		f.mu.Unlock()
	}
}
func (f *tcpDropForwarder) closePairs(rst, stop bool) {
	f.mu.Lock()
	if stop && f.closed {
		f.mu.Unlock()
		return
	}
	list := slices.Collect(maps.Keys(f.pairs))
	clear(f.pairs)
	if stop {
		f.closed, f.pairs = true, nil
		f.cancel()
		_ = f.lis.Close()
	}
	f.mu.Unlock()
	for _, p := range list {
		p.closeConn(rst)
	}
	if stop {
		f.wg.Wait()
	}
}

type subPumpEv struct {
	ev  *proxypb.SubscriberEvent
	err error
}
type subPump struct {
	stream  proxypb.PubSubProxy_SubscriberSessionClient
	cancel  context.CancelFunc
	evs     chan subPumpEv
	done    chan struct{}
	stopped bool
}

func checkPostStartErr(env *harness.ScenarioEnv, err error, op string) error {
	if status.Code(err) == codes.Unimplemented {
		return env.Failf("%s: unexpected post-start UNIMPLEMENTED: %v", op, err)
	}
	return fmt.Errorf("%s: %w", op, env.CheckProxyErr(err))
}
func startSubPump(ctx context.Context, env *harness.ScenarioEnv, cmd *proxypb.StartSubscriberCommand) (*subPump, *proxypb.StartedEvent, error) {
	sessCtx, cancel := context.WithCancel(ctx)
	stream, err := env.Client.SubscriberSession(sessCtx)
	if err := env.CheckProxyErr(err); err != nil {
		cancel()
		return nil, nil, fmt.Errorf("SubscriberSession: %w", err)
	}
	if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: cmd}}); err != nil {
		if errors.Is(err, io.EOF) {
			for err = error(nil); err == nil; _, err = stream.Recv() {
			}
		}
		_ = stream.CloseSend()
		cancel()
		return nil, nil, fmt.Errorf("send StartSubscriberCommand: %w", env.CheckProxyErr(err))
	}
	p := &subPump{stream: stream, cancel: cancel, evs: make(chan subPumpEv, 64), done: make(chan struct{})}
	go func() {
		defer func() { close(p.evs); close(p.done) }()
		for {
			ev, rerr := stream.Recv()
			select {
			case p.evs <- subPumpEv{ev: ev, err: rerr}:
			case <-sessCtx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()
	item, ok := <-p.evs
	if ok && item.err == nil && item.ev.GetStarted() != nil {
		return p, item.ev.GetStarted(), nil
	}
	switch {
	case !ok:
		p.stopped, err = true, fmt.Errorf("recv StartedEvent: stream closed")
	case item.err != nil:
		p.stopped, err = true, fmt.Errorf("recv StartedEvent: %w", env.CheckProxyErr(item.err))
	case item.ev.GetStopped() != nil:
		p.stopped, err = true, stoppedEventErr(item.ev.GetStopped(), "start")
	default:
		err = fmt.Errorf("expected StartedEvent, got %T", item.ev.GetEvent())
	}
	p.close(ctx, env)
	return nil, nil, err
}
func startSubAndStream(ctx context.Context, env *harness.ScenarioEnv, cmd *proxypb.StartSubscriberCommand, readyIdx int, op string) (*subPump, *fake.StreamHandle, error) {
	p, _, err := startSubPump(ctx, env, cmd)
	if err != nil {
		return nil, nil, err
	}
	if sh, err := waitReadyStreamOrStop(ctx, env, p, readyIdx, op); err != nil {
		p.close(ctx, env)
		return nil, nil, err
	} else {
		return p, sh, nil
	}
}
func (p *subPump) close(ctx context.Context, env *harness.ScenarioEnv) {
	if p == nil {
		return
	}
	if !p.stopped && ctx.Err() == nil {
		stopTimer := time.AfterFunc(2*time.Second, p.cancel)
		stopCmd := &proxypb.StopSubscriberCommand{}
		switch {
		case env.HasCapability(harness.CapShutdownBehavior):
			stopCmd.ShutdownBehavior = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY
		case env.HasCapability(harness.CapShutdownTimeout):
			stopCmd.ShutdownTimeoutMs = proto.Int64(0)
		}
		_ = p.stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: stopCmd}})
		_ = p.stream.CloseSend()
		for item := range p.evs {
			if item.err != nil || item.ev.GetStopped() != nil {
				p.stopped = true
				break
			}
		}
		stopTimer.Stop()
	}
	p.cancel()
	<-p.done
}
func (p *subPump) decodeEv(env *harness.ScenarioEnv, item subPumpEv, ok bool, op string) (*proxypb.MessageDeliveredEvent, error) {
	if !ok {
		return nil, fmt.Errorf("%s: SubscriberSession stream closed", op)
	}
	if item.err != nil {
		return nil, checkPostStartErr(env, item.err, op+": SubscriberSession error")
	}
	if st := item.ev.GetStopped(); st != nil {
		p.stopped = true
		_ = p.stream.CloseSend()
		return nil, cmp.Or(rejectInBandUnimpl(env, st.GetErrorCode(), st.GetErrorMessage(), op), stoppedEventErr(st, op))
	}
	if md := item.ev.GetMessageDelivered(); md != nil {
		if md.GetDeliveryToken() == "" {
			return nil, fmt.Errorf("MessageDeliveredEvent has empty delivery_token")
		}
		return md, nil
	}
	return nil, fmt.Errorf("%s: unexpected SubscriberEvent %T", op, item.ev.GetEvent())
}
func (p *subPump) recvDelivery(ctx context.Context, env *harness.ScenarioEnv) (*proxypb.MessageDeliveredEvent, error) {
	select {
	case item, ok := <-p.evs:
		return p.decodeEv(env, item, ok, "recv MessageDeliveredEvent")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (p *subPump) awaitStopped(ctx context.Context, env *harness.ScenarioEnv, watchCh <-chan error, maxNew int, checkSettle bool, op string) (*proxypb.StoppedEvent, error) {
	for {
		select {
		case err, ok := <-watchCh:
			if ok && err == nil {
				return nil, env.Failf("%s: retried non-retryable error (opened > %d stream(s))", op, maxNew)
			}
			watchCh = nil
		case item, ok := <-p.evs:
			if !ok {
				return nil, fmt.Errorf("%s: stream closed before StoppedEvent", op)
			}
			if item.err != nil {
				return nil, checkPostStartErr(env, item.err, op+": recv StoppedEvent")
			}
			switch ev := item.ev.GetEvent().(type) {
			case *proxypb.SubscriberEvent_Stopped:
				p.stopped = true
				_ = p.stream.CloseSend()
				return ev.Stopped, rejectInBandUnimpl(env, ev.Stopped.GetErrorCode(), ev.Stopped.GetErrorMessage(), op)
			case *proxypb.SubscriberEvent_SettlementResult:
				if sr := ev.SettlementResult; checkSettle && (sr.GetGrpcErrorCode() != 0 || (sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED && sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS)) {
					return nil, fmt.Errorf("unexpected settlement failure for %q: status=%s code=%d", sr.GetDeliveryToken(), sr.GetStatus(), sr.GetGrpcErrorCode())
				}
			default:
				return nil, fmt.Errorf("%s: unexpected SubscriberEvent %T waiting StoppedEvent", op, ev)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func (p *subPump) stopAndCheck(ctx context.Context, env *harness.ScenarioEnv, wantDelivered int64) error {
	if err := p.stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{}}}); err != nil {
		return checkPostStartErr(env, err, "send StopSubscriberCommand")
	}
	st, err := p.awaitStopped(ctx, env, nil, 0, true, "StoppedEvent")
	if err != nil {
		return err
	}
	if err := checkInBandErr(st.GetErrorCode(), st.GetErrorMessage(), "StoppedEvent"); err != nil {
		return err
	}
	if st.GetTotalDelivered() != wantDelivered {
		return fmt.Errorf("StoppedEvent total_delivered mismatch: want %d, got %d", wantDelivered, st.GetTotalDelivered())
	}
	return nil
}
func sendSettleAction(env *harness.ScenarioEnv, p *subPump, tok string, action proxypb.SettleMessageCommand_SettleAction) error {
	if err := p.stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: action}}}); err != nil {
		return checkPostStartErr(env, err, fmt.Sprintf("send SettleMessageCommand(%s)", action))
	}
	return nil
}
func ackAndWait(ctx context.Context, env *harness.ScenarioEnv, p *subPump, tok, ackID, op string) error {
	if err := sendSettleAction(env, p, tok, proxypb.SettleMessageCommand_SETTLE_ACTION_ACK); err != nil {
		return err
	}
	return waitFakeOrStop(ctx, env, p, func(s fake.Snapshot) bool { return snapHasCompletedAckFor(s, ackID) }, op)
}
func spawnWatch(ctx context.Context, fn func(context.Context) error) (<-chan error, func()) {
	wCtx, wCancel := context.WithCancel(ctx)
	ch, done := make(chan error, 1), make(chan struct{})
	go func() { defer close(done); ch <- fn(wCtx) }()
	return ch, func() { wCancel(); <-done }
}
func spawnFakeWatch(ctx context.Context, fs *fake.Server, pred func(fake.Snapshot) bool) (<-chan error, func()) {
	return spawnWatch(ctx, func(c context.Context) error { return fs.WaitFor(c, pred) })
}
func awaitErrCh(env *harness.ScenarioEnv, p *subPump, ch <-chan error, op string) error {
	select {
	case err := <-ch:
		return err
	case item, ok := <-p.evs:
		if _, derr := p.decodeEv(env, item, ok, op); derr != nil {
			return derr
		}
		return fmt.Errorf("%s: unexpected MessageDeliveredEvent", op)
	}
}
func waitFakeOrStop(ctx context.Context, env *harness.ScenarioEnv, p *subPump, pred func(fake.Snapshot) bool, op string) error {
	ch, stop := spawnFakeWatch(ctx, env.Fake, pred)
	defer stop()
	return awaitErrCh(env, p, ch, op)
}
func waitReadyStreamOrStop(ctx context.Context, env *harness.ScenarioEnv, p *subPump, n int, op string) (*fake.StreamHandle, error) {
	var sh *fake.StreamHandle
	ch, stop := spawnWatch(ctx, func(c context.Context) (err error) { sh, err = env.Fake.WaitForStreamingPullN(c, n); return err })
	err := awaitErrCh(env, p, ch, op)
	stop()
	return sh, err
}
func recvDeliveryWatchingFake(ctx context.Context, env *harness.ScenarioEnv, p *subPump, pred func(fake.Snapshot) bool, failMsg string) (*proxypb.MessageDeliveredEvent, error) {
	errCh, stop := spawnFakeWatch(ctx, env.Fake, pred)
	defer stop()
	for {
		select {
		case err, ok := <-errCh:
			if ok && err == nil {
				return nil, env.Failf("%s", failMsg)
			}
			errCh = nil
		case item, ok := <-p.evs:
			return p.decodeEv(env, item, ok, "recv MessageDeliveredEvent")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func runSubFatalPhase(ctx context.Context, env *harness.ScenarioEnv, sub string, baseStreams int, wm int64, wantCode codes.Code, label string) error {
	p, _, err := startSubPump(ctx, env, interactiveCmd(env, env.Endpoint(), sub))
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	watchCh, stop := spawnFakeWatch(ctx, env.Fake, func(s fake.Snapshot) bool { return len(s.Streams)-baseStreams > 1 })
	st, err := p.awaitStopped(ctx, env, watchCh, 1, false, label)
	stop()
	if err != nil {
		return err
	}
	if st.GetErrorCode() == 0 {
		return env.Failf("%s: expected non-OK StoppedEvent (%s), got 0", label, wantCode)
	}
	if wantCode != codes.OK && codes.Code(st.GetErrorCode()) != wantCode {
		return env.Failf("%s: StoppedEvent code mismatch: want %s, got %s", label, wantCode, codes.Code(st.GetErrorCode()))
	}
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	v.Require(len(snap.Streams)-baseStreams == 1 && len(snap.StreamingPullCalls()) >= 1, "%s expected 1 fatal stream, got %d", label, len(snap.Streams)-baseStreams)
	checkSubInitialCalls(env, v, label+"-init", sub, callsAfterSeq(snap, wm))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	return finishPublishScenario(ctx, env, v)
}
func stopAndVerifyStream(ctx context.Context, env *harness.ScenarioEnv, p *subPump, wantDelivered int64, label, sub string, minStreams, exactStreams int, ackIDs ...string) (fake.Snapshot, *fake.Verifier, error) {
	if err := p.stopAndCheck(ctx, env, wantDelivered); err != nil {
		return fake.Snapshot{}, nil, err
	}
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	if exactStreams > 0 {
		v.Require(len(snap.Streams) == exactStreams, "%s expected exactly %d streams (stream survival), got %d", label, exactStreams, len(snap.Streams))
	} else if minStreams > 0 {
		v.Require(len(snap.Streams) >= minStreams, "%s expected >= %d streams, got %d", label, minStreams, len(snap.Streams))
	}
	v.Check(m3cCheckClientIDs(env, snap))
	checkSubInitialCalls(env, v, label+"-init", sub, snap.Calls)
	checkSubStreamRPCs(env, v, snap, sub, ackIDs...)
	return snap, v, nil
}
func pushOneMsg(sh *fake.StreamHandle, ackID, msgID string, data []byte, opts ...fake.PushOption) error {
	return sh.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: ackID, Message: &pubsubpb.PubsubMessage{MessageId: msgID, Data: data}}}, opts...)
}
func pushRecvOne(ctx context.Context, env *harness.ScenarioEnv, p *subPump, sh *fake.StreamHandle, ackID, msgID string, data []byte, opts ...fake.PushOption) (*proxypb.MessageDeliveredEvent, error) {
	if err := pushOneMsg(sh, ackID, msgID, data, opts...); err != nil {
		return nil, err
	}
	return p.recvDelivery(ctx, env)
}
func pushRecvAckOne(ctx context.Context, env *harness.ScenarioEnv, p *subPump, sh *fake.StreamHandle, ackID, msgID string, data []byte, prematureQuiet time.Duration, op string) error {
	md, err := pushRecvOne(ctx, env, p, sh, ackID, msgID, data)
	if err != nil {
		return err
	}
	if err := assertDeliveredMsg(md, msgID, data, nil, ""); err != nil {
		return err
	}
	if prematureQuiet > 0 {
		if err := checkNoPrematureAck(ctx, env.Fake, md, prematureQuiet); err != nil {
			return err
		}
	}
	return ackAndWait(ctx, env, p, md.GetDeliveryToken(), ackID, op)
}
func m3cCheckClientIDs(env *harness.ScenarioEnv, snap fake.Snapshot) error {
	if strings.EqualFold(env.Language(), "dotnet") {
		if !slices.ContainsFunc(snap.Streams, func(s fake.StreamSummary) bool { return s.HasInitial }) {
			return fmt.Errorf("no StreamingPull streams with initial request observed")
		}
		return nil
	}
	return fake.AssertConsistentClientIDs(snap, false)
}
func checkSubInitialCalls(env *harness.ScenarioEnv, v *fake.Verifier, label, sub string, calls []fake.CapturedCall) {
	chk := fake.ExpectStreamingPullInitial(label, fake.StreamingPullInitialExpectation{
		Subscription: sub, StreamAckDeadlineMin: fake.MinLeaseExtensionSeconds, StreamAckDeadlineMax: fake.MaxLeaseExtensionSeconds,
		ProtocolVersion:    map[bool]*int64{true: proto.Int64(1)}[env.HasCapability(harness.CapKeepalive)],
		CheckRequestParams: true, AllowUnencodedRequestParams: cfgStreamingAllowUnencoded(env),
	})
	for _, c := range calls {
		if c.FullMethod == fake.MethodStreamingPull && c.StreamSeq == 0 && c.StreamingPullReq != nil {
			v.Check(chk.Check(c))
		}
	}
}
func checkAckModAckMeta(env *harness.ScenarioEnv, v *fake.Verifier, sub string, calls []fake.CapturedCall) {
	allow := allowUnencodedRequestParams(env)
	for _, c := range calls {
		if c.AckReq != nil {
			v.Check(fake.ExpectAck("Acknowledge", fake.AckSubscription(sub), fake.AckRequestParams(sub, allow)).Check(c))
		} else if c.ModAckReq != nil {
			v.Check(fake.ExpectModAck("ModifyAckDeadline", fake.ModAckSubscription(sub), fake.ModAckRequestParams(sub, allow)).Check(c))
		}
	}
}
func snapAfterSeq(s fake.Snapshot, seq int64) fake.Snapshot {
	s.Calls = callsAfterSeq(s, seq)
	return s
}
func modAckCallsAfterSeq(s fake.Snapshot, seq int64) []fake.CapturedCall {
	return filterCalls(s.ModAckCalls(), func(c fake.CapturedCall) bool { return c.Seq > seq })
}
func snapHasCompletedAckFor(snap fake.Snapshot, ackID string) bool {
	return slices.ContainsFunc(snap.AckCalls(), func(c fake.CapturedCall) bool {
		return c.Completed && slices.Contains(c.AckReq.GetAckIds(), ackID)
	}) || slices.ContainsFunc(snap.StreamingPullCalls(), func(c fake.CapturedCall) bool {
		return slices.Contains(c.StreamingPullReq.GetAckIds(), ackID)
	})
}
func pushSimpleBatch(sh *fake.StreamHandle, ackPrefix, msgPrefix string, startIdx, count, padAckBytes int, opts ...fake.PushOption) ([]string, [][]byte, error) {
	msgs, acks, payloads := make([]*pubsubpb.ReceivedMessage, count), make([]string, count), make([][]byte, count)
	pad := strings.Repeat("x", max(padAckBytes, 0))
	for i := range count {
		aid := fmt.Sprintf("%s-%04d", ackPrefix, startIdx+i)
		if padAckBytes > len(aid)+1 {
			aid = (aid + "-" + pad)[:padAckBytes]
		}
		mid, data := fmt.Sprintf("%s-%04d", msgPrefix, startIdx+i), []byte(fmt.Sprintf("%s-body-%04d", msgPrefix, startIdx+i))
		acks[i], payloads[i], msgs[i] = aid, data, &pubsubpb.ReceivedMessage{AckId: aid, Message: &pubsubpb.PubsubMessage{MessageId: mid, Data: data}}
	}
	return acks, payloads, sh.PushMessages(msgs, opts...)
}
func checkSubStreamRPCs(env *harness.ScenarioEnv, v *fake.Verifier, snap fake.Snapshot, sub string, ackIDs ...string) {
	if sub != "" {
		checkAckModAckMeta(env, v, sub, snap.Calls)
	}
	v.Check(fake.AssertAllAckedIDs(snap, ackIDs...))
	v.Check(fake.AssertAllNackedIDs(snap))
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
}
func stopVerifyAndFinishStream(ctx context.Context, env *harness.ScenarioEnv, p *subPump, wantDelivered int64, label, sub string, minStreams, exactStreams int, ackIDs ...string) error {
	if _, v, err := stopAndVerifyStream(ctx, env, p, wantDelivered, label, sub, minStreams, exactStreams, ackIDs...); err != nil {
		return err
	} else {
		return finishPublishScenario(ctx, env, v)
	}
}
