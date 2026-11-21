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
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func smokeScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PUB-01", Name: "Basic Publish & Message IDs", Category: "publish", Feature: "PUB-01", Description: "Publish a single message and verify wire Publish RPC, payload/attributes preservation, routing metadata, server-assigned message_id, and zero spans when monitoring is disabled.", Run: runPUB01},
		{ID: "PORD-02", Name: "Sequential Per-Key Ordered Publish FIFO & Concurrent Keys", Category: "ordering", Feature: "PORD-02", Description: "Verify strict FIFO sequential per-key Publish execution for key k1 while key k2 and unordered messages publish concurrently without key mixing.", Run: runPORD02},
		{ID: "PULL-01", Name: "StreamingPull Delivery & Acknowledge", Category: "pull", Feature: "PULL-01", Description: "Verify StreamingPull strict initial handshake, empty-response tolerance, delivery payload/attributes, no premature ack before settle, Acknowledge RPC, and clean StopSubscriber shutdown.", FakeServerOptions: []fake.Option{fake.WithStrictStreamingPullValidation(true)}, Run: runPULL01},
		{ID: "PULL-04", Name: "Unary Synchronous Pull & Ack", Category: "pull", Feature: "PULL-04", Description: "Verify optional unary Subscriber.Pull + Acknowledge flow (single, multi-message, empty, DeadlineExceeded), returning UNSUPPORTED_BY_CLIENT when omitted or Unimplemented.", RequiredCapabilities: []harness.Capability{harness.CapUnaryPull}, Run: runPULL04},
		{ID: "TRAC-01", Name: "Publish OpenTelemetry Spans & Attribute Immutability", Category: "traces", Feature: "TRAC-01", Description: "Verify publisher OTel spans (<topic> create, flow control, batching, <topic> publish), googclient_traceparent injection, and caller attribute immutability when monitoring is supported.", TraceOnly: true, Run: runTRAC01},
		{ID: "TRAC-03", Name: "Functional + W3C googclient_traceparent Trace Propagation", Category: "traces", Feature: "TRAC-03", Description: "Verify end-to-end functional publish/subscribe, W3C googclient_traceparent trace propagation, and ROOT_CONTEXT fallback on missing/invalid headers, skipping trace assertions cleanly when monitoring is unsupported.", Run: runTRAC03},
	}
}

func runPUB01(ctx context.Context, env *harness.ScenarioEnv) error {
	_, snap, err := publishSingleAndVerify(ctx, env, singlePublishSpec{
		label: "PUB-01", topic: env.TopicPath(env.UniqueName("pub01-topic")),
		data: []byte("hello pubsub conformance"), attrs: map[string]string{"k1": "v1", "scenario": "PUB-01"}, checkImmut: true,
	})
	if err != nil {
		return err
	}
	if m := snap.PublishCalls()[0].PublishReq.GetMessages()[0]; m.GetMessageId() != "" {
		return fmt.Errorf("PUB-01 client pre-populated publish message_id %q", m.GetMessageId())
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func runPORD02(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pord02-topic")), allowUnencodedRequestParams(env)
	gateK1, gateK2 := fake.NewGate(), fake.NewGate()
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-02-gate-k1", Match: fake.MatchOrderingKey("k1"), Times: 1, Gate: gateK1})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-02-gate-k2", Match: fake.MatchOrderingKey("k2"), Times: 1, Gate: gateK2})
	createReq := newPubReq(env, topic, 2, 0, 200)
	createReq.NumWorkers = proto.Int32(4)
	setOrderingOptIn(env, createReq, true)
	pubID, err := createPublisher(ctx, env, createReq)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	defer gateK2.Release()
	defer gateK1.Release()

	fire := func(key string, payloads [][]byte, g *fake.Gate) ([]string, error) {
		toks := make([]string, len(payloads))
		for i, p := range payloads {
			if i == 2 && g != nil {
				if err := g.WaitEntered(ctx); err != nil {
					return nil, fmt.Errorf("%s batch 1: %w", key, err)
				}
			}
			if toks[i], err = publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: p, OrderingKey: key}); err != nil {
				return nil, fmt.Errorf("Publish(%s, %q): %w", key, string(p), err)
			}
		}
		return toks, nil
	}
	k1Payloads := [][]byte{[]byte("k1-b1-1"), []byte("k1-b1-2"), []byte("k1-b2-1"), []byte("k1-b2-2")}
	k2Payloads := [][]byte{[]byte("k2-b1-1"), []byte("k2-b1-2"), []byte("k2-b2-1"), []byte("k2-b2-2")}
	tokK1, err := fire("k1", k1Payloads, gateK1)
	if err != nil {
		return err
	}
	tokK2, err := fire("k2", k2Payloads, gateK2)
	if err != nil {
		return err
	}
	if err := waitKeyPublishAttempts(ctx, env.Fake, "k2", 2, 25*time.Millisecond); err != nil {
		return err
	}
	gateK2.Release()
	tokUnord, err := fire("", [][]byte{[]byte("unord-1"), []byte("unord-2")}, nil)
	if err != nil {
		return err
	}
	allIDs := make([]string, 0, 10)
	for _, grp := range []struct {
		gate  *fake.Gate
		toks  []string
		phase string
	}{{nil, tokUnord, "unordered while k1 gated"}, {nil, tokK2, "k2 while k1 gated"}, {gateK1, tokK1, "k1 after release"}} {
		if grp.gate != nil {
			grp.gate.Release()
		}
		ids, err := awaitAllPublishIDs(ctx, env, pubID, grp.toks)
		if err != nil {
			return fmt.Errorf("%s: %w", grp.phase, err)
		}
		for _, id := range ids {
			if slices.Contains(allIDs, id) {
				return fmt.Errorf("duplicate published message_id %q", id)
			}
			allIDs = append(allIDs, id)
		}
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish))
	var gotUnord []string
	for _, c := range snap.PublishCalls() {
		v.Check(fake.ExpectPublish("PORD-02-call", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishNoMixedOrderingKeys()).Check(c))
		for _, m := range c.PublishReq.GetMessages() {
			if m.GetOrderingKey() == "" {
				gotUnord = append(gotUnord, string(m.GetData()))
			}
		}
	}
	slices.Sort(gotUnord)
	v.Require(slices.Equal(gotUnord, []string{"unord-1", "unord-2"}), "unordered payload multiset mismatch: want [unord-1 unord-2], got %v", gotUnord)
	for _, chk := range []error{fake.AssertOrderedPublishFIFO(snap, "k1", k1Payloads), fake.AssertOrderedPublishFIFO(snap, "k2", k2Payloads), fake.AssertMaxConcurrentPublishForKey(snap, "k1", 1), fake.AssertMaxConcurrentPublishForKey(snap, "k2", 1), fake.AssertConcurrentPublishesObserved(snap, "k1", "k2"), checkSortedMessageIDsMatch(allIDs, snap)} {
		v.Check(chk)
	}
	return finishPublishScenario(ctx, env, v)
}

func runPULL01(ctx context.Context, env *harness.ScenarioEnv) error {
	_, snap, err := runInteractiveSingleSub(ctx, env, singleSubSpec{
		sub: env.SubscriptionPath(env.UniqueName("pull01-sub")), ackID: "ack-pull01-1", msgID: "msg-pull01-1",
		data: []byte("hello streaming pull conformance"), attrs: map[string]string{"scenario": "PULL-01", "seq": "1"},
		sendEmptyResp: true, strictInitial: true,
	})
	if err != nil {
		return err
	}
	if slices.ContainsFunc(snap.Streams, func(s fake.StreamSummary) bool { return s.ClientID != "" }) {
		if err := fake.AssertConsistentClientIDs(snap, false); err != nil {
			return err
		}
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func pullCheck(ctx context.Context, env *harness.ScenarioEnv, sub string, max int32, want ...*pubsubpb.ReceivedMessage) error {
	resp, err := env.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID, MaxMessages: max, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK})
	if err := env.CheckProxyErr(err); err != nil {
		return fmt.Errorf("UnaryPull: %w", err)
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "UnaryPull"); err != nil {
		return err
	}
	if msgs := resp.GetMessages(); len(msgs) != len(want) {
		return fmt.Errorf("UnaryPull returned %d messages, want %d", len(msgs), len(want))
	} else {
		for i, w := range want {
			m := w.GetMessage()
			if err := assertDeliveredMsg(msgs[i], m.GetMessageId(), m.GetData(), m.GetAttributes(), ""); err != nil {
				return err
			}
			if s := msgs[i]; s.GetSettleErrorCode() != 0 || (s.GetSettleStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS && s.GetSettleStatus() != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED) {
				return fmt.Errorf("pulled message settle failure: status=%s code=%d msg=%q", s.GetSettleStatus(), s.GetSettleErrorCode(), s.GetSettleErrorMessage())
			}
		}
	}
	return nil
}

func runPULL04(ctx context.Context, env *harness.ScenarioEnv) error {
	sub, allowUnencoded := env.SubscriptionPath(env.UniqueName("pull04-sub")), allowUnencodedRequestParams(env)
	mkRM := func(ackID, msgID, text, kind string) *pubsubpb.ReceivedMessage {
		return &pubsubpb.ReceivedMessage{AckId: ackID, Message: &pubsubpb.PubsubMessage{MessageId: msgID, Data: []byte(text), Attributes: map[string]string{"scenario": "PULL-04", "kind": kind}}}
	}
	rm1 := mkRM("ack-pull04-1", "msg-pull04-1", "hello unary pull conformance", "unary")
	env.Fake.AddPullRule(fake.PullRule{Name: "PULL-04-one-msg", Times: 1, Response: &pubsubpb.PullResponse{ReceivedMessages: []*pubsubpb.ReceivedMessage{rm1}}})
	if err := pullCheck(ctx, env, sub, 1, rm1); err != nil {
		return err
	}
	snap := env.Fake.Snapshot()
	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPull("PULL-04-pull", fake.PullSubscription(sub), fake.PullMaxMessages(1), fake.PullRequestParams(sub, allowUnencoded), fake.PullStatusCode(codes.OK)), fake.ExpectAck("PULL-04-ack", fake.AckSubscription(sub), fake.AckIDsExact(rm1.GetAckId()), fake.AckRequestParams(sub, allowUnencoded), fake.AckStatusCode(codes.OK))); err != nil {
		return err
	}
	wm2, v := lastCapturedSeq(snap), env.NewVerifier()
	rm2a, rm2b := mkRM("ack-pull04-2a", "msg-pull04-2a", "unary batch a", "batch"), mkRM("ack-pull04-2b", "msg-pull04-2b", "unary batch b", "batch")
	env.Fake.AddPullRule(fake.PullRule{Name: "PULL-04-two-msgs", Times: 1, Response: &pubsubpb.PullResponse{ReceivedMessages: []*pubsubpb.ReceivedMessage{rm2a, rm2b}}})
	if err := pullCheck(ctx, env, sub, 2, rm2a, rm2b); err != nil {
		return err
	}
	snap2 := snapAfterSeq(env.Fake.Snapshot(), wm2)
	v.Check(fake.AssertNoUnexpectedRPCs(snap2, fake.MethodPull, fake.MethodAcknowledge))
	v.Require(len(snap2.PullCalls()) == 1, "Phase 2 expected 1 Pull call, got %d", len(snap2.PullCalls()))
	if len(snap2.PullCalls()) == 1 {
		v.Check(fake.ExpectPull("PULL-04-p2-pull", fake.PullSubscription(sub), fake.PullMaxMessages(2), fake.PullRequestParams(sub, allowUnencoded), fake.PullStatusCode(codes.OK)).Check(snap2.PullCalls()[0]))
	}
	v.Require(len(snap2.AckCalls()) >= 1, "Phase 2 expected >= 1 Acknowledge call, got 0")
	for _, ac := range snap2.AckCalls() {
		v.Check(fake.ExpectAck("PULL-04-p2-ack", fake.AckSubscription(sub), fake.AckRequestParams(sub, allowUnencoded), fake.AckStatusCode(codes.OK)).Check(ac))
	}
	v.Check(fake.AssertAllAckedIDs(snap2, rm2a.GetAckId(), rm2b.GetAckId()))
	wm3 := lastCapturedSeq(env.Fake.Snapshot())
	if err := pullCheck(ctx, env, sub, 2); err != nil {
		return err
	}
	v.Check(fake.VerifySequence(snapAfterSeq(env.Fake.Snapshot(), wm3), fake.DefaultVerifyOptions(), fake.ExpectPull("PULL-04-p3-pull", fake.PullSubscription(sub), fake.PullMaxMessages(2), fake.PullRequestParams(sub, allowUnencoded), fake.PullStatusCode(codes.OK))))
	wm4 := lastCapturedSeq(env.Fake.Snapshot())
	env.Fake.AddPullRule(fake.PullRule{Name: "PULL-04-deadline", Times: fake.RepeatAlways, Err: status.Error(codes.DeadlineExceeded, "deadline exceeded")})
	resp4, err := env.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID, MaxMessages: 1, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK, MaxAttempts: proto.Int32(1)})
	if err != nil && status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("UnaryPull(deadline): %w", env.CheckProxyErr(err))
	}
	if err == nil && resp4.GetErrorCode() != 0 && codes.Code(resp4.GetErrorCode()) != codes.DeadlineExceeded {
		return checkInBandErr(resp4.GetErrorCode(), resp4.GetErrorMessage(), "UnaryPull(deadline)")
	}
	if err == nil && len(resp4.GetMessages()) != 0 {
		return fmt.Errorf("UnaryPull(deadline) returned %d messages, want 0", len(resp4.GetMessages()))
	}
	snap4 := snapAfterSeq(env.Fake.Snapshot(), wm4)
	v.Check(fake.AssertNoUnexpectedRPCs(snap4, fake.MethodPull))
	v.Require(len(snap4.PullCalls()) >= 1, "Phase 4 expected >= 1 Pull call, got 0")
	for _, pc := range snap4.PullCalls() {
		v.Check(fake.ExpectPull("PULL-04-p4-pull", fake.PullSubscription(sub), fake.PullRequestParams(sub, allowUnencoded), fake.PullStatusCode(codes.DeadlineExceeded)).Check(pc))
	}
	return finishPublishScenario(ctx, env, v)
}

func runTRAC01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, parentTP, payload := env.TopicPath(env.UniqueName("trac01-topic")), newSampledTraceparent(), []byte("hello otel publish span")
	msgID, snap, err := publishSingleAndVerify(ctx, env, singlePublishSpec{
		label: "TRAC-01", topic: topic, data: payload, attrs: map[string]string{"scenario": "TRAC-01", "immutability": "true"},
		enableMonitoring: true, parentTraceparent: parentTP, checkImmut: true, requireTraceparent: true,
	})
	if err != nil {
		return err
	}
	rawWire, _ := traces.ExtractTraceparent(snap.PublishCalls()[0].PublishReq.GetMessages()[0].GetAttributes())
	wireTP, err := verifySampledTraceparent(rawWire, parentTP, "wire "+fake.TraceparentAttributeKey)
	if err != nil {
		return err
	}
	pubExp := singlePublishTraceExp(env, msgID, parentTP, wireTP, len(payload))
	pubExp.Messages = []traces.PublishMessageExpectation{{MessageID: msgID, Data: payload, BodySize: traces.Int64Ptr(int64(len(payload))), OrderingKey: "", ParentTraceparent: parentTP, WireTraceparent: wireTP}}
	pubExp.Batches = []traces.PublishBatchExpectation{{MessageCount: 1}}
	return env.MustVerifyTraces(ctx, &traces.TraceExpectation{Topic: topic, ProjectID: env.ProjectID, Publish: pubExp})
}

func verifySubTrace(ctx context.Context, env *harness.ScenarioEnv, topic, sub string, pub *traces.PublishExpectation, wireTP, cbTP, ackID, msgID string) error {
	return env.MustVerifyTraces(ctx, &traces.TraceExpectation{
		Topic: topic, Subscription: sub, ProjectID: env.ProjectID, Publish: pub,
		Subscribe: &traces.SubscribeExpectation{
			MessageCount: 1, WireTraceparents: nonEmptyStringSlice(wireTP), CallbackTraceparents: nonEmptyStringSlice(cbTP),
			AckIDs: []string{ackID}, MessageIDs: []string{msgID}, ExpectedOutcome: "acked", ExpectedSettlements: []traces.SettlementExpectation{{Operation: "ack"}},
		},
	})
}

func runTRAC03Fallback(ctx context.Context, env *harness.ScenarioEnv, sub, ackID, msgID, wireTP, phaseATID string, mon bool) error {
	snap := env.Fake.Snapshot()
	wm, payload, readyIdx := lastCapturedSeq(snap), []byte("hello w3c fallback"), 1
	for _, c := range snap.StreamingPullCalls() {
		if c.StreamSeq == 0 && c.StatusCode == codes.OK {
			readyIdx++
		}
	}
	p, sh, err := startSubAndStream(ctx, env, &proxypb.StartSubscriberCommand{
		Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID, NumStreams: proto.Int32(1),
		EnableMonitoring: mon, AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
	}, readyIdx, "TRAC-03 fallback")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	var attrs map[string]string
	if wireTP != "" {
		attrs = map[string]string{fake.TraceparentAttributeKey: wireTP}
	}
	md, err := pushRecvOne(ctx, env, p, sh, ackID, msgID, slices.Clone(payload), fake.WithAttributes(attrs))
	if err != nil {
		return err
	}
	cbTP := md.GetCallbackTraceparent()
	if err := assertDeliveredMsg(md, msgID, payload, attrs, ""); err != nil {
		return err
	}
	if mon {
		if tp, perr := traces.ParseTraceparent(cbTP); !strings.HasPrefix(cbTP, "00-") || perr != nil || !tp.IsSampled() || (phaseATID != "" && tp.TraceID == phaseATID) {
			return fmt.Errorf("invalid fallback callback_traceparent %q (phaseATraceID=%q err=%v)", cbTP, phaseATID, perr)
		}
	}
	if md.GetAutoSettled() {
		return fmt.Errorf("proxy reported auto_settled=true in interactive mode")
	}
	if ids, calls := countAckOccurrences(snapAfterSeq(env.Fake.Snapshot(), wm)); ids != 0 || calls != 0 {
		return fmt.Errorf("premature ack observed before SettleMessageCommand (ids=%d, calls=%d)", ids, calls)
	}
	if err := ackAndWait(ctx, env, p, md.GetDeliveryToken(), ackID, "TRAC-03 fallback"); err != nil {
		return err
	}
	if err := p.stopAndCheck(ctx, env, 1); err != nil {
		return err
	}
	p.close(ctx, env)
	subSnap := snapAfterSeq(env.Fake.Snapshot(), wm)
	if err := verifyStreamingPullAndSettlementRPCs(env, subSnap, sub, ackID); err != nil {
		return err
	}
	return verifySubTrace(ctx, env, "", sub, nil, wireTP, cbTP, ackID, msgID)
}

func runTRAC03(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, sub, mon, parentTP := env.TopicPath(env.UniqueName("trac03-topic")), env.SubscriptionPath(env.UniqueName("trac03-sub")), env.SupportsMonitoring(), ""
	if mon {
		parentTP = newSampledTraceparent()
	}
	payload := []byte("hello w3c trace propagation")
	pubMsgID, pubSnap, err := publishSingleAndVerify(ctx, env, singlePublishSpec{
		label: "TRAC-03-pub", topic: topic, data: payload, attrs: map[string]string{"scenario": "TRAC-03", "prop": "w3c"},
		enableMonitoring: mon, parentTraceparent: parentTP, checkImmut: mon, requireTraceparent: mon,
	})
	if err != nil {
		return err
	}
	capturedAttrs := maps.Clone(pubSnap.PublishCalls()[0].PublishReq.GetMessages()[0].GetAttributes())
	wireTP, _ := traces.ExtractTraceparent(capturedAttrs)
	phaseATID := ""
	if mon {
		if _, err := verifySampledTraceparent(wireTP, parentTP, "wire "+fake.TraceparentAttributeKey); err != nil {
			return err
		}
		if p, err := traces.ParseTraceparent(wireTP); err == nil {
			phaseATID = p.TraceID
		}
	}
	cbTP, finalSnap, err := runInteractiveSingleSub(ctx, env, singleSubSpec{
		sub: sub, ackID: "ack-trac03-1", msgID: pubMsgID, parentTP: parentTP, numStreams: 1,
		data: payload, attrs: capturedAttrs, enableMonitoring: mon, extraAllowed: []string{fake.MethodPublish},
	})
	if err != nil {
		return err
	}
	if gotPubs := len(finalSnap.PublishCalls()); gotPubs != 1 {
		return fmt.Errorf("expected exactly 1 Publish RPC in TRAC-03, got %d", gotPubs)
	}
	if err := verifySubTrace(ctx, env, topic, sub, singlePublishTraceExp(env, pubMsgID, parentTP, wireTP, len(payload)), wireTP, cbTP, "ack-trac03-1", pubMsgID); err != nil {
		return err
	}
	for _, tc := range []struct{ name, ack, msg, wtp string }{{"trac03-sub-empty", "ack-trac03-2", "msg-trac03-2", ""}, {"trac03-sub-badtp", "ack-trac03-3", "msg-trac03-3", "not-a-valid-traceparent"}} {
		if err := runTRAC03Fallback(ctx, env, env.SubscriptionPath(env.UniqueName(tc.name)), tc.ack, tc.msg, tc.wtp, phaseATID, mon); err != nil {
			return err
		}
	}
	fullSnap := env.Fake.Snapshot()
	if gotPubs := len(fullSnap.PublishCalls()); gotPubs != 1 {
		return fmt.Errorf("expected exactly 1 Publish RPC in TRAC-03, got %d", gotPubs)
	}
	if err := fake.AssertNoUnexpectedRPCs(fullSnap, fake.MethodPublish, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline); err != nil {
		return err
	}
	return nil
}
