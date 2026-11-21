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
	"context"
	"fmt"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func tracingExtraScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "TRAC-01b", Name: "Tracing disabled (enable_monitoring=false)", Category: "traces", Feature: "TRAC-01", Description: "Verify enable_monitoring=false emits zero spans, suppresses googclient_traceparent wire injection, leaves callback_traceparent empty, and preserves caller attributes.", TraceOnly: true, IsolateProxy: true, Run: runTRAC01b},
		{ID: "TRAC-02", Name: "Hedged publish tracing (prefill and hedged attempt wins)", Category: "traces", Feature: "TRAC-02", Description: "Verify hedged publish emits publish start/end (hedged) events on the producer span after prefilling the hedging token bucket.", TraceOnly: true, IsolateProxy: true, RequiredCapabilities: []harness.Capability{harness.CapPublishHedging}, Run: runTRAC02},
		{ID: "TRAC-04", Name: "Subscriber trace lifecycle (ALO, async process, nack, periodic lease, EOD errors)", Category: "traces", Feature: "TRAC-04", Timeout: 60 * time.Second, Description: "Verify subscribe, concurrency control, scheduler, async process closure, ack/nack/periodic modack settlement spans, and EOD error status propagation.", TraceOnly: true, Run: runTRAC04},
	}
}

func runTRAC01b(ctx context.Context, env *harness.ScenarioEnv) error {
	ep, topic, sub := env.Endpoint(), env.TopicPath(env.UniqueName("trac01b-topic")), env.SubscriptionPath(env.UniqueName("trac01b-sub"))
	req := newPubReq(env, topic, 1, 0, 0)
	req.EnableMonitoring = false
	userAttrs, pubData := map[string]string{"user_k": "user_v", "trace_k": "preserved"}, []byte("trac01b-pub-payload")
	pubMsgID, err := m3ePublishOne(ctx, env, req, pubData, userAttrs, newSampledTraceparent(), "rojo=00f067aa0ba902b7", true)
	if err != nil {
		return err
	}
	cmd := interactiveCmd(env, ep, sub)
	cmd.EnableMonitoring = false
	pump, stream, err := startSubAndStream(ctx, env, cmd, 1, "trac01b sub start")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pump)
	subMsgID, subAckID, subPayload := env.UniqueName("msg-trac01b-sub"), env.UniqueName("ack-trac01b-sub"), []byte("trac01b-sub-payload")
	subAttrs := map[string]string{"user_k": "user_v", fake.TraceparentAttributeKey: newSampledTraceparent(), fake.TracestateAttributeKey: "rojo=00f067aa0ba902b7"}
	ev, err := pushRecvOne(ctx, env, pump, stream, subAckID, subMsgID, subPayload, fake.WithAttributes(subAttrs))
	if err != nil {
		return err
	}
	if ev.GetCallbackTraceparent() != "" {
		return fmt.Errorf("trac01b: expected empty callback_traceparent, got %q", ev.GetCallbackTraceparent())
	}
	if err := m3eAssertDeliveredBasic(ev, subMsgID, subPayload, "user_k", "user_v"); err != nil {
		return err
	}
	if err := ackAndWait(ctx, env, pump, ev.GetDeliveryToken(), subAckID, "trac01b ack"); err != nil {
		return err
	}
	if err := pump.stopAndCheck(ctx, env, 1); err != nil {
		return err
	}
	pump = nil
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	pubCalls, ackCalls := snap.PublishCalls(), snap.AckCalls()
	v.Require(len(pubCalls) == 1, "trac01b: expected 1 Publish call, got %d", len(pubCalls))
	if len(pubCalls) > 0 {
		v.Check(fake.ExpectPublish("trac01b publish", fake.PublishTopic(topic), fake.PublishMessages(fake.ExpectedMessage{Data: pubData, Attributes: userAttrs})).Check(pubCalls[0]))
	}
	v.Check(checkOKPublishMessageIDsMatch([]string{pubMsgID}, snap))
	v.Require(len(ackCalls) == 1, "trac01b: expected 1 Ack call, got %d", len(ackCalls))
	if len(ackCalls) > 0 {
		v.Check(fake.ExpectAck("trac01b ack", fake.AckSubscription(sub), fake.AckIDsExact(subAckID)).Check(ackCalls[0]))
	}
	v.Check(fake.AssertAllNackedIDs(snap))
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	if err := v.Err(); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func runTRAC02(ctx context.Context, env *harness.ScenarioEnv) error {
	topic := env.TopicPath(env.UniqueName("trac02-topic"))
	req := newPubReq(env, topic, 1, 0, 0)
	req.EnableMonitoring = true
	req.Hedging = &proxypb.HedgingSettings{Enabled: true, DelayMs: proto.Int64(100), MaxTokens: proto.Int64(5), RefillRatio: proto.Float64(0.2)}
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	const prefillCount = 6
	prefillParents, prefillIDs := make([]string, prefillCount), make([]string, prefillCount)
	prefillPayloads, prefillAttrs := make([][]byte, prefillCount), make([]map[string]string, prefillCount)
	for i := 0; i < prefillCount; i++ {
		prefillParents[i] = newSampledTraceparent()
		prefillPayloads[i] = []byte(fmt.Sprintf("trac02-prefill-%02d", i+1))
		prefillAttrs[i] = map[string]string{"seq": fmt.Sprintf("prefill-%02d", i+1)}
		id, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: prefillPayloads[i], Attributes: prefillAttrs[i], ParentTraceparent: prefillParents[i]})
		if err != nil {
			return err
		}
		prefillIDs[i] = id
	}
	if n := len(env.Fake.Snapshot().PublishCalls()); n != prefillCount {
		return fmt.Errorf("trac02: expected %d prefill Publish calls, got %d", prefillCount, n)
	}

	wmTarget := lastCapturedSeq(env.Fake.Snapshot())
	gate := fake.NewGate()
	defer gate.Release()
	targetData := []byte("trac02-hedge-target")
	env.Fake.AddPublishRule(fake.PublishRule{Name: "trac02-attempt0-gated", Match: fake.MatchPublishAll(fake.MatchPayloadContains(targetData), fake.MatchHedgedAttempt(0)), Times: 1, Gate: gate, MessageIDs: []string{"msg-trac02-attempt0-lose"}})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "trac02-attempt1-fastwin", Match: fake.MatchPublishAll(fake.MatchPayloadContains(targetData), fake.MatchHedgedAttemptGTE(1)), Times: 1, MessageIDs: []string{"msg-trac02-hedge-win"}})

	targetParent, targetAttrs := newSampledTraceparent(), map[string]string{"seq": "target"}
	targetID, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: targetData, Attributes: targetAttrs, ParentTraceparent: targetParent})
	if err != nil {
		return err
	}
	if targetID != "msg-trac02-hedge-win" {
		return fmt.Errorf("trac02: expected hedged winner id %q, got %q", "msg-trac02-hedge-win", targetID)
	}
	if err := waitHedgeLosersCanceled(ctx, env, wmTarget, 2, "trac02 wait hedge attempts"); err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	pubCalls := snap.PublishCalls()

	msgExps := make([]traces.PublishMessageExpectation, 0, prefillCount+1)
	for i := 0; i < prefillCount; i++ {
		if len(pubCalls[i].PublishReq.GetMessages()) != 1 {
			return fmt.Errorf("trac02: prefill call[%d] had %d messages, want 1", i, len(pubCalls[i].PublishReq.GetMessages()))
		}
		tp := pubCalls[i].PublishReq.GetMessages()[0].GetAttributes()[fake.TraceparentAttributeKey]
		if err := m3eVerifySampledTraceparent(tp, fmt.Sprintf("trac02 prefill[%d]", i)); err != nil {
			return err
		}
		msgExps = append(msgExps, traces.PublishMessageExpectation{
			MessageID: prefillIDs[i], Data: prefillPayloads[i], Attributes: prefillAttrs[i],
			ParentTraceparent: prefillParents[i], WireTraceparent: tp, Hedged: false,
		})
	}

	var targetWinnerTP string
	for _, c := range publishCallsAfterSeq(snap, wmTarget) {
		if c.StatusCode != codes.OK {
			continue
		}
		for _, m := range c.PublishReq.GetMessages() {
			if string(m.GetData()) == string(targetData) {
				targetWinnerTP = m.GetAttributes()[fake.TraceparentAttributeKey]
			}
		}
	}
	if err := m3eVerifySampledTraceparent(targetWinnerTP, "trac02 winner"); err != nil {
		return err
	}
	msgExps = append(msgExps, traces.PublishMessageExpectation{
		MessageID: targetID, Data: targetData, Attributes: targetAttrs,
		ParentTraceparent: targetParent, WireTraceparent: targetWinnerTP, Hedged: true, MinHedgedAttempts: 1,
	})
	if err := fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, &traces.TraceExpectation{
		Topic: topic, AllowExtraPubSubSpans: false,
		Publish: &traces.PublishExpectation{MessageCount: prefillCount + 1, Messages: msgExps},
	})
}

func runTRAC04(ctx context.Context, env *harness.ScenarioEnv) error {
	ep := env.Endpoint()
	subIDA := env.UniqueName("trac04-sub-alo")
	subA := env.SubscriptionPath(subIDA)
	cmdA := interactiveCmd(env, ep, subA)
	cmdA.EnableMonitoring = true
	cmdA.MinDurationPerAckExtensionMs = proto.Int64(2_000)
	cmdA.MaxDurationPerAckExtensionMs = proto.Int64(2_000)

	pumpA, streamA, err := startSubAndStream(ctx, env, cmdA, 1, "trac04 phase A start")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pumpA)

	type aloCase struct {
		msgID, ackID, tp, cb string
		data                 []byte
		attrs                map[string]string
		holdAsync, nack, mod bool
	}
	cases := []*aloCase{
		{msgID: env.UniqueName("m-a1"), ackID: env.UniqueName("ack-a1"), data: []byte("trac04-a1-imm"), attrs: map[string]string{"case": "a1"}},
		{msgID: env.UniqueName("m-a2"), ackID: env.UniqueName("ack-a2"), data: []byte("trac04-a2-async"), attrs: map[string]string{"case": "a2"}, holdAsync: true},
		{msgID: env.UniqueName("m-a3"), ackID: env.UniqueName("ack-a3"), data: []byte("trac04-a3-nack"), attrs: map[string]string{"case": "a3"}, nack: true},
		{msgID: env.UniqueName("m-a4"), ackID: env.UniqueName("ack-a4"), data: []byte("trac04-a4-modack"), attrs: map[string]string{"case": "a4"}, mod: true},
	}
	for i, c := range cases {
		c.tp = newSampledTraceparent()
		pushOpts := []fake.PushOption{fake.WithAttributes(map[string]string{"case": c.attrs["case"], fake.TraceparentAttributeKey: c.tp})}
		if i == 0 {
			pushOpts = append(pushOpts, fake.WithSubscriptionProperties(false, false))
		}
		wm := lastCapturedSeq(env.Fake.Snapshot())
		ev, err := pushRecvOne(ctx, env, pumpA, streamA, c.ackID, c.msgID, c.data, pushOpts...)
		if err != nil {
			return err
		}
		if err := m3eVerifySampledTraceparent(ev.GetCallbackTraceparent(), "trac04 "+c.attrs["case"]); err != nil {
			return err
		}
		c.cb = ev.GetCallbackTraceparent()
		if c.holdAsync {
			if err := sleepCtx(ctx, 55*time.Millisecond); err != nil {
				return err
			}
		}
		if c.mod {
			if err := m3eWaitModAck(ctx, env, pumpA, wm, c.ackID, true, 2, "trac04 a4 periodic modack"); err != nil {
				return err
			}
		}
		if c.nack {
			if err := sendSettleAction(env, pumpA, ev.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK); err != nil {
				return err
			}
			if err := m3eWaitModAck(ctx, env, pumpA, wm, c.ackID, false, 1, "trac04 a3 nack modack"); err != nil {
				return err
			}
		} else if err := ackAndWait(ctx, env, pumpA, ev.GetDeliveryToken(), c.ackID, "trac04 "+c.attrs["case"]+" ack"); err != nil {
			return err
		}
	}
	if err := pumpA.stopAndCheck(ctx, env, int64(len(cases))); err != nil {
		return err
	}
	pumpA = nil

	msgExpsA := make([]traces.SubscribeMessageExpectation, len(cases))
	for i, c := range cases {
		out, op := "acked", "ack"
		if c.nack {
			out, op = "nacked", "nack"
		}
		settles := []traces.SettlementExpectation{{Operation: op}}
		if c.mod {
			settles = []traces.SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}}
		}
		msgExpsA[i] = traces.SubscribeMessageExpectation{MessageID: c.msgID, AckID: c.ackID, Data: c.data, Attributes: c.attrs, WireTraceparent: c.tp, CallbackTraceparent: c.cb, ExpectedOutcome: out, Settlements: settles}
	}
	snapA, vA := env.Fake.Snapshot(), env.NewVerifier()
	vA.Check(fake.AssertAllAckedIDs(snapA, cases[0].ackID, cases[1].ackID, cases[3].ackID))
	vA.Check(fake.AssertAllNackedIDs(snapA, cases[2].ackID))
	for _, c := range snapA.Calls {
		switch {
		case c.AckReq != nil:
			vA.Check(fake.AckWithinBounds()(c))
		case c.ModAckReq != nil:
			vA.Check(fake.ModAckWithinBounds()(c))
			if c.ModAckReq.GetAckDeadlineSeconds() != 0 {
				vA.Check(fake.ModAckDeadlineRange(1, fake.MaxLeaseExtensionSeconds)(c))
			}
		case c.StreamingPullReq != nil:
			req := c.StreamingPullReq
			aN, mN := len(req.GetAckIds()), len(req.GetModifyDeadlineAckIds())
			vA.Require(mN == len(req.GetModifyDeadlineSeconds()) && aN+mN <= fake.MaxAckIDsPerRequest && proto.Size(req) <= fake.MaxAckRequestBytes, "seq=%d: StreamingPull bounds exceeded", c.Seq)
			if sdl := req.GetStreamAckDeadlineSeconds(); sdl != 0 {
				vA.Require(sdl >= 1 && sdl <= fake.MaxLeaseExtensionSeconds, "seq=%d: stream_ack_deadline_seconds=%d outside 0 or [1, %d]", c.Seq, sdl, fake.MaxLeaseExtensionSeconds)
			}
			for i, dl := range req.GetModifyDeadlineSeconds() {
				vA.Require(dl == 0 || (dl >= 1 && dl <= fake.MaxLeaseExtensionSeconds), "seq=%d: modify_deadline_seconds[%d]=%d outside 0 or [1, %d]", c.Seq, i, dl, fake.MaxLeaseExtensionSeconds)
			}
		}
	}
	vA.Check(fake.AssertNoUnexpectedRPCs(snapA, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	if err := vA.Err(); err != nil {
		return err
	}
	if err := env.MustVerifyTraces(ctx, &traces.TraceExpectation{
		Subscription: subA, AllowExtraPubSubSpans: false,
		Subscribe: &traces.SubscribeExpectation{ExactlyOnce: traces.BoolPtr(false), StrictSettlementOps: false, Messages: msgExpsA},
		CustomChecks: []traces.CustomCheck{
			m3eCheckTrac04AsyncProcess(subIDA, cases[1].msgID),
			m3eCheckTrac04PeriodicModAck(subIDA, cases[3].msgID),
		},
	}); err != nil {
		return err
	}
	if err := traces.ResetTracesIfSupported(ctx, env.Client, env.Caps); err != nil {
		return err
	}
	if !env.HasCapability(harness.CapExactlyOnceDelivery) || !env.HasCapability(harness.CapAckWithResult) {
		return nil
	}

	msgB1ID, ackB1, dataB1, tpB1 := env.UniqueName("m-b1"), env.UniqueName("ack-b1"), []byte("trac04-b1-eod-perm"), newSampledTraceparent()
	env.Fake.AddAckRule(fake.AckRule{
		Name: "trac04-eod-perm-invalid", Match: fake.MatchAckID(ackB1), Times: fake.RepeatAlways,
		Err: fake.EODErrorStatus(codes.InvalidArgument, "trac04-eod-perm", map[string]string{ackB1: fake.EODPermanentInvalidAckID}),
	})
	sessB, err := m3dStart(ctx, env, "trac04-sub-eod", func(cmd *proxypb.StartSubscriberCommand) {
		cmd.EnableMonitoring, cmd.UseExactlyOnce = true, true
	}, "trac04 phase B start")
	if err != nil {
		return err
	}
	defer func() {
		if sessB != nil {
			sessB.close(ctx, env)
		}
	}()

	evB, err := sessB.pushRecv(ctx, env, ackB1, msgB1ID, dataB1, fake.WithAttributes(map[string]string{"case": "b1", fake.TraceparentAttributeKey: tpB1}), fake.WithSubscriptionProperties(true, false))
	if err != nil {
		return err
	}
	if err := m3eVerifySampledTraceparent(evB.GetCallbackTraceparent(), "trac04 b1"); err != nil {
		return err
	}
	if err := sessB.settle(ctx, env, evB.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT); err != nil {
		return err
	}
	_, srB, stB, isEOF, err := sessB.nextEv(ctx, env, 30*time.Second, "trac04 b1 settlement")
	if err != nil {
		return err
	}
	if isEOF || stB != nil || srB == nil || srB.GetDeliveryToken() != evB.GetDeliveryToken() {
		return fmt.Errorf("trac04 b1: expected settlement result for %q, got sr=%v st=%v eof=%v", evB.GetDeliveryToken(), srB, stB, isEOF)
	}
	if srB.GetStatus() == proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED || srB.GetStatus() == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
		return fmt.Errorf("trac04 b1: expected permanent EOD failure status, got %v", srB.GetStatus())
	}
	vB := env.NewVerifier()
	snapB, err := sessB.stopCheck(ctx, env, vB, "trac04 phase B", nil, false, 1, false, []string{ackB1}, nil)
	if err != nil {
		return err
	}
	vB.Check(fake.AssertAckAndModAckWithinBounds(snapAfterSeq(snapB, sessB.wm)))
	vB.Check(fake.AssertNoUnexpectedRPCs(snapB, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	if err := vB.Err(); err != nil {
		return err
	}
	subB := sessB.sub
	sessB = nil

	return env.MustVerifyTraces(ctx, &traces.TraceExpectation{
		Subscription: subB, AllowExtraPubSubSpans: false,
		Subscribe: &traces.SubscribeExpectation{
			ExactlyOnce: traces.BoolPtr(true), StrictSettlementOps: false,
			Messages: []traces.SubscribeMessageExpectation{{
				MessageID: msgB1ID, AckID: ackB1, Data: dataB1, Attributes: map[string]string{"case": "b1"},
				WireTraceparent: tpB1, CallbackTraceparent: evB.GetCallbackTraceparent(),
				Settlements: []traces.SettlementExpectation{{Operation: "ack", ExpectError: true}},
			}},
		},
	})
}

func m3eFindSubSpan(spans []*proxypb.TraceSpan, subID, msgID string) *proxypb.TraceSpan {
	for _, s := range spans {
		if id, ok := traces.AttrString(s.GetAttributes(), "messaging.message.id"); ok && s.GetName() == subID+" subscribe" && id == msgID {
			return s
		}
	}
	return nil
}

func m3eViol(rule, msg string, s *proxypb.TraceSpan) []traces.Violation {
	v := traces.Violation{Code: traces.CodeCustomCheckFailed, Rule: rule, Message: msg}
	if s != nil {
		v.SpanName, v.TraceID, v.SpanID = s.GetName(), s.GetTraceId(), s.GetSpanId()
	}
	return []traces.Violation{v}
}

func m3eCheckTrac04AsyncProcess(subID, msgID string) traces.CustomCheck {
	return func(spans []*proxypb.TraceSpan, profile traces.Profile) []traces.Violation {
		if profile.Name != traces.ProfileGo {
			return nil
		}
		subSpan := m3eFindSubSpan(spans, subID, msgID)
		if subSpan == nil {
			return m3eViol("trac04.async_process", fmt.Sprintf("subscribe span missing for %q", msgID), nil)
		}
		procName, subTrace, subSpanID := subID+" process", traces.NormalizeHex(subSpan.GetTraceId()), traces.NormalizeHex(subSpan.GetSpanId())
		var ps *proxypb.TraceSpan
		for _, cand := range spans {
			if cand.GetName() == procName && traces.NormalizeHex(cand.GetTraceId()) == subTrace && traces.NormalizeHex(cand.GetParentSpanId()) == subSpanID {
				ps = cand
				break
			}
		}
		if ps == nil {
			return m3eViol("trac04.async_process.missing", fmt.Sprintf("missing child %q span for %q", procName, msgID), subSpan)
		}
		var ackCalled *proxypb.SpanEvent
		for _, ev := range ps.GetEvents() {
			if ev.GetName() == "ack called" {
				ackCalled = ev
				break
			}
		}
		if ackCalled == nil {
			return m3eViol("trac04.async_process", `missing "ack called" event on process span`, ps)
		}
		if dur := ps.GetEndTime().AsTime().Sub(ps.GetStartTime().AsTime()); dur < 25*time.Millisecond {
			return m3eViol("trac04.async_process.duration", fmt.Sprintf("process span duration %v < 25ms", dur), ps)
		}
		if ps.GetEndTime().AsTime().Before(ackCalled.GetTimestamp().AsTime()) {
			return m3eViol("trac04.async_process.order", `process span ended before "ack called" event`, ps)
		}
		return nil
	}
}

func m3eCheckTrac04PeriodicModAck(subID, msgID string) traces.CustomCheck {
	return func(spans []*proxypb.TraceSpan, profile traces.Profile) []traces.Violation {
		if profile.Name != traces.ProfileGo {
			return nil
		}
		subSpan := m3eFindSubSpan(spans, subID, msgID)
		if subSpan == nil {
			return m3eViol("trac04.periodic_modack", fmt.Sprintf("subscribe span missing for %q", msgID), nil)
		}
		modName, subTrace, subSpanID := subID+" modack", traces.NormalizeHex(subSpan.GetTraceId()), traces.NormalizeHex(subSpan.GetSpanId())
		for _, ms := range spans {
			if ms.GetName() != modName {
				continue
			}
			for _, lk := range ms.GetLinks() {
				if traces.NormalizeHex(lk.GetTraceId()) == subTrace && traces.NormalizeHex(lk.GetSpanId()) == subSpanID {
					if isRec, ok := traces.AttrBool(ms.GetAttributes(), "messaging.gcp_pubsub.is_receipt_modack"); ok && !isRec {
						return nil
					}
				}
			}
		}
		return m3eViol("trac04.periodic_modack", fmt.Sprintf("missing non-receipt %q span linked to %q", modName, msgID), nil)
	}
}
