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
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/protobuf/proto"
)

type m3dSession struct {
	p       *subPump
	sh      *fake.StreamHandle
	sub     string
	wm      int64
	base    int
	auto    bool
	wantEOD *bool
	settles map[string]*proxypb.SettlementResultEvent
}

func m3dStart(ctx context.Context, env *harness.ScenarioEnv, pfx string, tweak func(*proxypb.StartSubscriberCommand), op string) (*m3dSession, error) {
	pre, sub := env.Fake.Snapshot(), pfx
	if !strings.HasPrefix(sub, "projects/") {
		sub = env.SubscriptionPath(env.UniqueName(pfx))
	}
	wm, base, cmd := lastCapturedSeq(pre), len(pre.Streams), interactiveCmd(env, env.Endpoint(), sub)
	cmd.NumStreams = proto.Int32(1)
	if tweak != nil {
		tweak(cmd)
	}
	p, sh, err := startSubAndStream(ctx, env, cmd, base+1, op)
	if err != nil {
		return nil, err
	}
	return &m3dSession{p: p, sh: sh, sub: sub, wm: wm, base: base, auto: cmd.GetAutoSettleMode() == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT || cmd.GetAutoSettleMode() == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT, settles: map[string]*proxypb.SettlementResultEvent{}}, nil
}
func m3dWithSession(ctx context.Context, env *harness.ScenarioEnv, pfx string, tweak func(*proxypb.StartSubscriberCommand), op string, fn func(*m3dSession) error) error {
	s, err := m3dStart(ctx, env, pfx, tweak, op)
	if err != nil {
		return err
	}
	defer s.close(ctx, env)
	return fn(s)
}
func (s *m3dSession) close(ctx context.Context, env *harness.ScenarioEnv) {
	if s != nil && s.p != nil {
		s.p.close(ctx, env)
	}
}
func (s *m3dSession) nextEv(ctx context.Context, env *harness.ScenarioEnv, wait time.Duration, op string) (*proxypb.MessageDeliveredEvent, *proxypb.SettlementResultEvent, *proxypb.StoppedEvent, bool, error) {
	var tC <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		tC = t.C
	}
	select {
	case item, ok := <-s.p.evs:
		if !ok {
			return nil, nil, nil, false, fmt.Errorf("%s: SubscriberSession stream closed", op)
		}
		if item.err != nil {
			return nil, nil, nil, false, checkPostStartErr(env, item.err, op+": SubscriberSession error")
		}
		switch ev := item.ev.GetEvent().(type) {
		case *proxypb.SubscriberEvent_Stopped:
			s.p.stopped = true
			_ = s.p.stream.CloseSend()
			return nil, nil, ev.Stopped, false, rejectInBandUnimpl(env, ev.Stopped.GetErrorCode(), ev.Stopped.GetErrorMessage(), op)
		case *proxypb.SubscriberEvent_MessageDelivered:
			md := ev.MessageDelivered
			if md.GetDeliveryToken() == "" {
				return nil, nil, nil, false, fmt.Errorf("MessageDeliveredEvent has empty delivery_token")
			}
			if md.GetAutoSettled() != s.auto {
				return nil, nil, nil, false, env.Failf("message %q: expected auto_settled=%t, got auto_settled=%t", md.GetMessageId(), s.auto, md.GetAutoSettled())
			}
			if s.wantEOD != nil && md.GetIsExactlyOnce() != *s.wantEOD {
				return nil, nil, nil, false, env.Failf("message %q: expected is_exactly_once=%t, got %t", md.GetMessageId(), *s.wantEOD, md.GetIsExactlyOnce())
			}
			return md, nil, nil, false, nil
		case *proxypb.SubscriberEvent_SettlementResult:
			s.settles[ev.SettlementResult.GetDeliveryToken()] = ev.SettlementResult
			return nil, ev.SettlementResult, nil, false, nil
		default:
			return nil, nil, nil, false, fmt.Errorf("%s: unexpected SubscriberEvent %T", op, ev)
		}
	case <-tC:
		return nil, nil, nil, true, nil
	case <-ctx.Done():
		return nil, nil, nil, false, ctx.Err()
	}
}
func (s *m3dSession) recvDelivery(ctx context.Context, env *harness.ScenarioEnv) (*proxypb.MessageDeliveredEvent, error) {
	md, _, err := s.tryRecvDelivery(ctx, env, 0)
	return md, err
}
func (s *m3dSession) tryRecvDelivery(ctx context.Context, env *harness.ScenarioEnv, wait time.Duration) (*proxypb.MessageDeliveredEvent, bool, error) {
	for {
		md, _, st, timedOut, err := s.nextEv(ctx, env, wait, "recv MessageDeliveredEvent")
		if err != nil || timedOut {
			return nil, false, err
		}
		if st != nil {
			return nil, false, stoppedEventErr(st, "delivery")
		}
		if md != nil {
			return md, true, nil
		}
	}
}
func (s *m3dSession) recvSettlement(ctx context.Context, env *harness.ScenarioEnv, tok string, wantStatus proxypb.SettlementResultEvent_AckStatus, wantNack bool) (*proxypb.SettlementResultEvent, error) {
	for {
		if sr, ok := s.settles[tok]; ok {
			delete(s.settles, tok)
			if sr.GetIsNack() != wantNack || sr.GetStatus() != wantStatus {
				return nil, env.Failf("SettlementResultEvent(%q) mismatch: want status=%s is_nack=%t, got status=%s is_nack=%t (code=%d msg=%q)", tok, wantStatus, wantNack, sr.GetStatus(), sr.GetIsNack(), sr.GetGrpcErrorCode(), sr.GetErrorMessage())
			}
			if wantStatus == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS && sr.GetGrpcErrorCode() != 0 {
				return nil, env.Failf("SettlementResultEvent(%q) expected grpc_error_code=0 on SUCCESS, got %d", tok, sr.GetGrpcErrorCode())
			}
			return sr, nil
		}
		md, _, st, _, err := s.nextEv(ctx, env, 0, "recv SettlementResultEvent")
		if err != nil {
			return nil, err
		}
		if st != nil {
			return nil, stoppedEventErr(st, "settlement")
		}
		if md != nil {
			return nil, env.Failf("unexpected MessageDeliveredEvent %q while waiting for SettlementResultEvent %q", md.GetMessageId(), tok)
		}
	}
}
func (s *m3dSession) drainEOF(ctx context.Context, env *harness.ScenarioEnv, op string, err error) error {
	for errors.Is(err, io.EOF) {
		if _, _, st, timedOut, derr := s.nextEv(ctx, env, 200*time.Millisecond, op+" EOF"); derr != nil || timedOut {
			return cmp.Or(derr, err)
		} else if st != nil {
			return stoppedEventErr(st, op)
		}
	}
	return err
}
func (s *m3dSession) sendStop(ctx context.Context, env *harness.ScenarioEnv, stopCmd *proxypb.StopSubscriberCommand, ignoreEOF bool) error {
	stopCmd = cmp.Or(stopCmd, &proxypb.StopSubscriberCommand{})
	if err := s.p.stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: stopCmd}}); err != nil && (!ignoreEOF || !errors.Is(err, io.EOF)) {
		return s.drainEOF(ctx, env, "send StopSubscriberCommand", checkPostStartErr(env, err, "send StopSubscriberCommand"))
	}
	return nil
}
func (s *m3dSession) awaitStoppedWith(ctx context.Context, env *harness.ScenarioEnv, checkSettle bool, wantDelivered int64) (*proxypb.StoppedEvent, error) {
	for {
		md, sr, st, _, err := s.nextEv(ctx, env, 0, "StoppedEvent")
		if err != nil {
			return nil, err
		}
		if md != nil {
			return nil, env.Failf("unexpected MessageDeliveredEvent %q while stopping", md.GetMessageId())
		}
		if sr != nil && checkSettle && (sr.GetGrpcErrorCode() != 0 || (sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED && sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS)) {
			return nil, env.Failf("unexpected settlement failure for %q: status=%s code=%d", sr.GetDeliveryToken(), sr.GetStatus(), sr.GetGrpcErrorCode())
		}
		if st != nil {
			if err := checkInBandErr(st.GetErrorCode(), st.GetErrorMessage(), "StoppedEvent"); err != nil {
				return nil, err
			}
			if wantDelivered >= 0 && st.GetTotalDelivered() != wantDelivered {
				return nil, env.Failf("StoppedEvent total_delivered mismatch: want %d, got %d", wantDelivered, st.GetTotalDelivered())
			}
			return st, nil
		}
	}
}
func (s *m3dSession) stopWith(ctx context.Context, env *harness.ScenarioEnv, stopCmd *proxypb.StopSubscriberCommand, secondStop, checkSettle bool, wantDelivered int64) (*proxypb.StoppedEvent, error) {
	for _, ig := range map[bool][]bool{true: {false, true}, false: {false}}[secondStop] {
		if err := s.sendStop(ctx, env, stopCmd, ig); err != nil {
			return nil, err
		}
	}
	return s.awaitStoppedWith(ctx, env, checkSettle, wantDelivered)
}
func (s *m3dSession) recvCapped(ctx context.Context, env *harness.ScenarioEnv, n int, capCB int32, label string) ([]*proxypb.MessageDeliveredEvent, error) {
	out := make([]*proxypb.MessageDeliveredEvent, 0, n)
	for range n {
		md, err := s.recvDelivery(ctx, env)
		if err != nil {
			return nil, err
		}
		if capCB > 0 && md.GetActiveCallbacks() > capCB {
			return nil, env.Failf("%s: active_callbacks=%d exceeds %d", label, md.GetActiveCallbacks(), capCB)
		}
		out = append(out, md)
	}
	return out, nil
}
func (s *m3dSession) pushAndRecvCapped(ctx context.Context, env *harness.ScenarioEnv, rms []*pubsubpb.ReceivedMessage, n int, capCB int32, label string, opts ...fake.PushOption) ([]*proxypb.MessageDeliveredEvent, error) {
	if err := s.sh.PushMessages(rms, opts...); err != nil {
		return nil, err
	}
	return s.recvCapped(ctx, env, n, capCB, label)
}
func (s *m3dSession) pushRecv(ctx context.Context, env *harness.ScenarioEnv, aid, mid string, data []byte, opts ...fake.PushOption) (*proxypb.MessageDeliveredEvent, error) {
	if err := pushOneMsg(s.sh, aid, mid, data, opts...); err != nil {
		return nil, err
	}
	return s.recvDelivery(ctx, env)
}
func (s *m3dSession) settle(ctx context.Context, env *harness.ScenarioEnv, tok string, action proxypb.SettleMessageCommand_SettleAction) error {
	return s.drainEOF(ctx, env, "settle", sendSettleAction(env, s.p, tok, action))
}
func (s *m3dSession) ack(ctx context.Context, env *harness.ScenarioEnv, toks ...string) error {
	for _, tok := range toks {
		if err := s.settle(ctx, env, tok, proxypb.SettleMessageCommand_SETTLE_ACTION_ACK); err != nil {
			return err
		}
	}
	return nil
}
func (s *m3dSession) ackWait(ctx context.Context, env *harness.ScenarioEnv, op string, toks, ackIDs []string) error {
	if err := s.ack(ctx, env, toks...); err != nil || len(ackIDs) == 0 {
		return err
	}
	return waitFakeOrStop(ctx, env, s.p, func(sn fake.Snapshot) bool {
		return !slices.ContainsFunc(ackIDs, func(id string) bool { return !snapHasCompletedAckFor(sn, id) })
	}, op)
}
func (s *m3dSession) waitGateOrStop(ctx context.Context, env *harness.ScenarioEnv, g *fake.Gate, op string) error {
	ch, stop := spawnWatch(ctx, g.WaitEntered)
	defer stop()
	return awaitErrCh(env, s.p, ch, op)
}
func (s *m3dSession) requireNoExtra(ctx context.Context, env *harness.ScenarioEnv, msg string) error {
	if extra, got, err := s.tryRecvDelivery(ctx, env, m3dQuietWindow(env)); err != nil {
		return err
	} else if got {
		return env.Failf("%s (delivered %q)", msg, extra.GetMessageId())
	}
	return nil
}
func (s *m3dSession) finish(env *harness.ScenarioEnv, v *fake.Verifier, snap fake.Snapshot, label string, checkInit, checkNack bool, wantAcked, wantNacked []string) {
	sSnap := snap
	sSnap.Calls, sSnap.Streams = callsAfterSeq(snap, s.wm), snap.Streams[min(s.base, len(snap.Streams)):]
	v.Check(m3cCheckClientIDs(env, sSnap))
	if checkInit {
		checkSubInitialCalls(env, v, label+"-init", s.sub, sSnap.Calls)
	}
	checkAckModAckMeta(env, v, s.sub, sSnap.Calls)
	v.Check(fake.AssertAllAckedIDs(sSnap, wantAcked...))
	if checkNack {
		v.Check(fake.AssertAllNackedIDs(sSnap, wantNacked...))
		return
	}
	for _, c := range sSnap.ModAckCalls() {
		if c.ModAckReq.GetAckDeadlineSeconds() == 0 {
			for _, id := range c.ModAckReq.GetAckIds() {
				v.Require(slices.Contains(wantNacked, id), "%s: unexpected nacked ack_id %q", label, id)
			}
		}
	}
}
func (s *m3dSession) stopCheck(ctx context.Context, env *harness.ScenarioEnv, v *fake.Verifier, label string, toks []string, checkSettle bool, wantDelivered int64, checkInit bool, wantAcked, wantNacked []string) (fake.Snapshot, error) {
	if len(toks) > 0 || (checkSettle && len(wantAcked) > 0) {
		if err := s.ackWait(ctx, env, label+" acks", toks, wantAcked); err != nil {
			return fake.Snapshot{}, err
		}
	}
	if _, err := s.stopWith(ctx, env, nil, false, checkSettle, wantDelivered); err != nil {
		return fake.Snapshot{}, err
	}
	s.close(ctx, env)
	snap := env.Fake.Snapshot()
	s.finish(env, v, snap, label, checkInit, true, wantAcked, wantNacked)
	return snap, nil
}
func (s *m3dSession) stopFinish(ctx context.Context, env *harness.ScenarioEnv, v *fake.Verifier, label string, toks []string, checkSettle bool, wantDelivered int64, checkInit bool, wantAcked, wantNacked []string) error {
	snap, err := s.stopCheck(ctx, env, v, label, toks, checkSettle, wantDelivered, checkInit, wantAcked, wantNacked)
	if err != nil {
		return err
	}
	return m3dFinishScenario(ctx, env, v, snap)
}
func m3dBoundedShutdownTimeoutMs(env *harness.ScenarioEnv) int64 {
	d := env.Defaults().GetSubscriberMinAckExtensionMs()
	return map[bool]int64{true: 250, false: 2_000}[d > 0 && d < 1500]
}
func m3dQuietWindow(env *harness.ScenarioEnv) time.Duration {
	d := env.Defaults().GetSubscriberMinAckExtensionMs()
	return map[bool]time.Duration{true: 40 * time.Millisecond, false: 120 * time.Millisecond}[d > 0 && d < 1500]
}
func m3dPushRec(ackID, msgID string, data []byte, key string) *pubsubpb.ReceivedMessage {
	return &pubsubpb.ReceivedMessage{AckId: ackID, Message: &pubsubpb.PubsubMessage{MessageId: msgID, Data: data, OrderingKey: key}}
}
func m3dAckCount(snap fake.Snapshot, afterSeq int64, ackID string) int {
	n := 0
	for _, c := range snap.AckCalls() {
		if c.Seq > afterSeq && c.Completed && slices.Contains(c.AckReq.GetAckIds(), ackID) {
			n++
		}
	}
	return n
}
func m3dModAckCount(snap fake.Snapshot, afterSeq int64, ackID string, minDL, maxDL int32) int {
	n := 0
	for _, c := range modAckCallsAfterSeq(snap, afterSeq) {
		if dl := c.ModAckReq.GetAckDeadlineSeconds(); c.Completed && dl >= minDL && dl <= maxDL && slices.Contains(c.ModAckReq.GetAckIds(), ackID) {
			n++
		}
	}
	return n
}
func m3dCheckFrame0FC(env *harness.ScenarioEnv, v *fake.Verifier, label, sub string, calls []fake.CapturedCall, wantMsgs, wantBytes int64) {
	chk, seen := fake.ExpectStreamingPullInitial(label, fake.StreamingPullInitialExpectation{
		Subscription: sub, StreamAckDeadlineMin: fake.MinLeaseExtensionSeconds, StreamAckDeadlineMax: fake.MaxLeaseExtensionSeconds,
		MaxOutstandingMessages: map[bool]*int64{true: &wantMsgs}[wantMsgs > 0], MaxOutstandingBytes: map[bool]*int64{true: &wantBytes}[wantBytes > 0],
		ProtocolVersion: map[bool]*int64{true: proto.Int64(1)}[env.HasCapability(harness.CapKeepalive)], CheckRequestParams: true, AllowUnencodedRequestParams: cfgStreamingAllowUnencoded(env),
	}), 0
	for _, c := range calls {
		if c.FullMethod == fake.MethodStreamingPull && c.StreamSeq == 0 && c.StreamingPullReq != nil {
			seen++
			v.Check(chk.Check(c))
		}
	}
	v.Require(seen > 0, "%s: no initial StreamingPull frame observed", label)
}
func m3dCheckNoPrematureAckAfter(env *harness.ScenarioEnv, wm int64, md *proxypb.MessageDeliveredEvent) error {
	if ids, calls := countAckOccurrences(snapAfterSeq(env.Fake.Snapshot(), wm)); ids != 0 || calls != 0 {
		return env.Failf("message %q: proxy sent premature Ack before settlement (%d IDs across %d Ack calls)", md.GetMessageId(), ids, calls)
	}
	return nil
}
func m3dFinishScenario(ctx context.Context, env *harness.ScenarioEnv, v *fake.Verifier, snap fake.Snapshot) error {
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	return finishPublishScenario(ctx, env, v)
}
