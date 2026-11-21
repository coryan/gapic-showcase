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

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func configScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "CFG-01", Name: "Emulator mode (PUBSUB_EMULATOR_HOST with empty endpoint)", Category: "config", Feature: "CFG-01", Description: "Verify PUBSUB_EMULATOR_HOST routing with empty endpoint skips TLS/auth while keeping client OpenTelemetry tracing enabled when supported.", IsolateProxy: true, UseEmulatorEnv: true, Run: runCFG01},
		{ID: "CFG-02", Name: "Wire metadata, client_id, and custom headers across reconnect", Category: "config", Feature: "CFG-02", Description: "Verify x-goog-request-params, consistent StreamingPull client_id across reconnect, and custom_headers across publish, subscribe, and unary pull.", IsolateProxy: true, Run: runCFG02},
		{ID: "CFG-03", Name: "Empty project_id validation vs full 4-segment resource paths", Category: "config", Feature: "CFG-03", Description: "Verify empty project_id rejects short resource IDs locally with zero wire RPCs while full 4-segment resource paths succeed verbatim.", Run: runCFG03},
		{ID: "CFG-04", Name: "Cross-project topic and subscription resource preservation", Category: "config", Feature: "CFG-04", Description: "Verify distinct client project_id never overwrites explicit cross-project topic or subscription resource paths on wire RPCs or routing headers.", Run: runCFG04},
	}
}

func runCFG01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, sub, mon := env.TopicPath(env.UniqueName("cfg01-topic")), env.SubscriptionPath(env.UniqueName("cfg01-sub")), env.SupportsMonitoring()
	pubReq := newPubReq(env, topic, 1, 0, 0)
	pubReq.Endpoint, pubReq.UseEmulatorEnv, pubReq.EnableMonitoring = "", true, mon
	pubParent := ""
	if mon {
		pubParent = newSampledTraceparent()
	}
	pubData := []byte("cfg01-emulator-pub")
	pubMsgID, err := m3ePublishOne(ctx, env, pubReq, pubData, map[string]string{"mode": "emulator"}, pubParent, "", false)
	if err != nil {
		return err
	}
	cmd := interactiveCmd(env, "", sub)
	cmd.UseEmulatorEnv, cmd.EnableMonitoring = true, mon
	pump, stream, err := startSubAndStream(ctx, env, cmd, 1, "cfg01 sub start")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pump)
	subMsgID, subAckID, subData := env.UniqueName("msg-cfg01-sub"), env.UniqueName("ack-cfg01-sub"), []byte("cfg01-emulator-sub")
	subAttrs, subWireTP := map[string]string{"mode": "emulator"}, ""
	if mon {
		subWireTP = newSampledTraceparent()
		subAttrs[fake.TraceparentAttributeKey] = subWireTP
	}
	ev, err := pushRecvOne(ctx, env, pump, stream, subAckID, subMsgID, subData, fake.WithAttributes(subAttrs))
	if err != nil {
		return err
	}
	if err := m3eAssertDeliveredBasic(ev, subMsgID, subData, "mode", "emulator"); err != nil {
		return err
	}
	if mon {
		if err := m3eVerifySampledTraceparent(ev.GetCallbackTraceparent(), "cfg01 sub"); err != nil {
			return err
		}
	}
	if err := ackAndWait(ctx, env, pump, ev.GetDeliveryToken(), subAckID, "cfg01 sub ack"); err != nil {
		return err
	}
	if err := pump.stopAndCheck(ctx, env, 1); err != nil {
		return err
	}
	pump = nil
	snap, v, allowUnenc := env.Fake.Snapshot(), env.NewVerifier(), allowUnencodedRequestParams(env)
	pubCalls, ackCalls := snap.PublishCalls(), snap.AckCalls()
	v.Require(len(pubCalls) == 1, "cfg01: expected 1 Publish call, got %d", len(pubCalls))
	if len(pubCalls) > 0 {
		v.Check(fake.ExpectPublish("cfg01 publish", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnenc), fake.PublishMessages(fake.ExpectedMessage{Data: pubData, Attributes: map[string]string{"mode": "emulator"}, RequireTraceparent: mon})).Check(pubCalls[0]))
		v.Check(checkOKPublishMessageIDsMatch([]string{pubMsgID}, snap))
	}
	m3eVerifyStreamingInitAndHeaders(v, env, snap, sub, 1, nil, "cfg01")
	v.Require(len(ackCalls) == 1, "cfg01: expected 1 Ack call, got %d", len(ackCalls))
	if len(ackCalls) > 0 {
		v.Check(fake.ExpectAck("cfg01 ack", fake.AckSubscription(sub), fake.AckIDsExact(subAckID), fake.AckRequestParams(sub, allowUnenc)).Check(ackCalls[0]))
	}
	v.Check(fake.AssertAllNackedIDs(snap))
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	if err := v.Err(); err != nil {
		return err
	}
	if mon {
		pubWireTP := ""
		if len(pubCalls[0].PublishReq.GetMessages()) > 0 {
			pubWireTP = pubCalls[0].PublishReq.GetMessages()[0].GetAttributes()[fake.TraceparentAttributeKey]
		}
		if _, err := verifySampledTraceparent(pubWireTP, pubParent, "cfg01 pub wire"); err != nil {
			return err
		}
		if err := env.MustVerifyTraces(ctx, &traces.TraceExpectation{
			Topic: topic, Subscription: sub, AllowExtraPubSubSpans: false,
			Publish:   &traces.PublishExpectation{MessageCount: 1, Messages: []traces.PublishMessageExpectation{{MessageID: pubMsgID, Data: pubData, Attributes: map[string]string{"mode": "emulator"}, ParentTraceparent: pubParent, WireTraceparent: pubWireTP}}},
			Subscribe: &traces.SubscribeExpectation{StrictSettlementOps: false, Messages: []traces.SubscribeMessageExpectation{{MessageID: subMsgID, AckID: subAckID, Data: subData, Attributes: map[string]string{"mode": "emulator"}, WireTraceparent: subWireTP, CallbackTraceparent: ev.GetCallbackTraceparent(), ExpectedOutcome: "acked", Settlements: []traces.SettlementExpectation{{Operation: "ack"}}}}},
		}); err != nil {
			return err
		}
	}
	if env.HasCapability(harness.CapUnaryPull) {
		pullMsgID, pullAckID, pullData := env.UniqueName("msg-cfg01-pull"), env.UniqueName("ack-cfg01-pull"), []byte("cfg01-emulator-pull")
		pullAttrs, pullWireTP := map[string]string{"mode": "emulator"}, ""
		if mon {
			pullWireTP = newSampledTraceparent()
			pullAttrs[fake.TraceparentAttributeKey] = pullWireTP
		}
		wmPull, err := m3eRunUnaryPullOne(ctx, env, &proxypb.UnaryPullRequest{
			Endpoint: "", Subscription: sub, ProjectId: env.ProjectID, UseEmulatorEnv: true, EnableMonitoring: mon,
			MaxMessages: 1, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		}, pullMsgID, pullAckID, pullData, pullAttrs, "cfg01 unary pull")
		if err != nil {
			return err
		}
		pullSnap, vp := env.Fake.Snapshot(), env.NewVerifier()
		m3eVerifySubAndPullAfter(vp, env, pullSnap, wmPull, wmPull, sub, "", "", sub, pullAckID, nil, "cfg01")
		vp.Check(fake.AssertNoUnexpectedRPCs(snapAfterSeq(pullSnap, wmPull), fake.MethodPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
		if err := vp.Err(); err != nil {
			return err
		}
		if mon && env.Profile.AllowUnaryPull {
			return env.MustVerifyTraces(ctx, &traces.TraceExpectation{
				Subscription: sub, AllowExtraPubSubSpans: false,
				UnaryPull: &traces.UnaryPullExpectation{Messages: []traces.UnaryPullMessageExpectation{{MessageID: pullMsgID, AckID: pullAckID, Data: pullData, Attributes: map[string]string{"mode": "emulator"}, WireTraceparent: pullWireTP, Settlement: &traces.SettlementExpectation{Operation: "ack"}}}},
			})
		}
	}
	if !mon {
		return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
	}
	return nil
}

func runCFG02(ctx context.Context, env *harness.ScenarioEnv) error {
	ep, topic, sub := env.Endpoint(), env.TopicPath(env.UniqueName("cfg02-topic")), env.SubscriptionPath(env.UniqueName("cfg02-sub"))
	clientID, hdrs := env.UniqueName("cfg02-cli"), map[string]string{"x-custom-test": "m3e-val"}
	pubReq := newPubReq(env, topic, 1, 0, 0)
	pubReq.ClientId, pubReq.CustomHeaders = clientID, hdrs
	if _, err := m3ePublishOne(ctx, env, pubReq, []byte("cfg02-pub"), nil, "", "", false); err != nil {
		return err
	}
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{
		Name: "cfg02-transient-unavailable", Times: 1, InitialError: status.Error(codes.Unavailable, "cfg02-reconnect"),
		Match: func(_ int, init *pubsubpb.StreamingPullRequest, _ metadata.MD) bool {
			return init.GetSubscription() == sub
		},
	})
	cmd := interactiveCmd(env, ep, sub)
	cmd.ClientId, cmd.CustomHeaders = clientID, hdrs
	pump, stream2, err := startSubAndStream(ctx, env, cmd, 1, "cfg02 sub reconnect")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pump)
	msg1ID, ack1ID, msg2ID, ack2ID := env.UniqueName("m-cfg02-1"), env.UniqueName("ack-cfg02-1"), env.UniqueName("m-cfg02-2"), env.UniqueName("ack-cfg02-2")
	if err := m3eSubAckThenNack(ctx, env, pump, stream2, ack1ID, msg1ID, ack2ID, msg2ID, "cfg02"); err != nil {
		return err
	}
	pump = nil
	wmPull, pullAckID := lastCapturedSeq(env.Fake.Snapshot()), env.UniqueName("ack-cfg02-pull")
	if env.HasCapability(harness.CapUnaryPull) {
		if wmPull, err = m3eRunUnaryPullOne(ctx, env, &proxypb.UnaryPullRequest{
			Endpoint: ep, Subscription: sub, ProjectId: env.ProjectID, MaxMessages: 1, ReturnImmediately: true,
			SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK, ClientId: clientID, CustomHeaders: hdrs,
		}, env.UniqueName("m-cfg02-pull"), pullAckID, []byte("cfg02-pull"), nil, "cfg02 unary pull"); err != nil {
			return err
		}
	}
	if err := m3eVerifyCfgTail(env, 0, wmPull, topic, sub, 2, ack1ID, ack2ID, sub, pullAckID, hdrs, "cfg02"); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func runCFG03(ctx context.Context, env *harness.ScenarioEnv) error {
	ep, shortSub := env.Endpoint(), env.UniqueName("cfg03-short-sub")
	if err := m3eCheckRejectedPubEmptyProj(ctx, env, &proxypb.CreatePublisherRequest{
		Endpoint: ep, Topic: env.UniqueName("cfg03-short-topic"), CountThreshold: proto.Int64(1),
	}, lastCapturedSeq(env.Fake.Snapshot())); err != nil {
		return err
	}
	if err := m3eCheckRejectedSubEmptyProj(ctx, env, &proxypb.StartSubscriberCommand{Endpoint: ep, Subscription: shortSub}, lastCapturedSeq(env.Fake.Snapshot())); err != nil {
		return err
	}
	if env.HasCapability(harness.CapUnaryPull) {
		if err := m3eCheckRejectedPullEmptyProj(ctx, env, &proxypb.UnaryPullRequest{
			Endpoint: ep, Subscription: shortSub, MaxMessages: 1, ReturnImmediately: true,
		}, lastCapturedSeq(env.Fake.Snapshot())); err != nil {
			return err
		}
	}
	wmB := lastCapturedSeq(env.Fake.Snapshot())
	fullTopic := fmt.Sprintf("projects/%s/topics/%s", env.UniqueName("cfg03-p1"), env.UniqueName("cfg03-ok-topic"))
	fullSub := fmt.Sprintf("projects/%s/subscriptions/%s", env.UniqueName("cfg03-p2"), env.UniqueName("cfg03-ok-sub"))
	pubReq := newPubReq(env, fullTopic, 1, 0, 0)
	pubReq.ProjectId = ""
	if _, err := m3ePublishOne(ctx, env, pubReq, []byte("cfg03-ok-pub"), nil, "", "", false); err != nil {
		return err
	}
	cmd := interactiveCmd(env, ep, fullSub)
	cmd.ProjectId = ""
	pump, stream, err := startSubAndStream(ctx, env, cmd, 1, "cfg03 ok sub start")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pump)
	msgID, ackID := env.UniqueName("m-cfg03-ok"), env.UniqueName("ack-cfg03-ok")
	ev, err := pushRecvOne(ctx, env, pump, stream, ackID, msgID, []byte("cfg03-ok-sub"))
	if err != nil {
		return err
	}
	if err := ackAndWait(ctx, env, pump, ev.GetDeliveryToken(), ackID, "cfg03 ok ack"); err != nil {
		return err
	}
	if err := pump.stopAndCheck(ctx, env, 1); err != nil {
		return err
	}
	pump = nil
	wmPullB, pullAckB := lastCapturedSeq(env.Fake.Snapshot()), env.UniqueName("ack-cfg03-pull")
	if env.HasCapability(harness.CapUnaryPull) {
		if wmPullB, err = m3eRunUnaryPullOne(ctx, env, &proxypb.UnaryPullRequest{
			Endpoint: ep, Subscription: fullSub, MaxMessages: 1, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		}, env.UniqueName("m-cfg03-pull"), pullAckB, []byte("cfg03-ok-pull"), nil, "cfg03 ok unary pull"); err != nil {
			return err
		}
	}
	if err := m3eVerifyCfgTail(env, wmB, wmPullB, fullTopic, fullSub, 1, ackID, "", fullSub, pullAckB, nil, "cfg03 ok"); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func runCFG04(ctx context.Context, env *harness.ScenarioEnv) error {
	ep, wm0 := env.Endpoint(), lastCapturedSeq(env.Fake.Snapshot())
	fullTopic := fmt.Sprintf("projects/pubsub-public-data/topics/%s", env.UniqueName("cfg04-topic"))
	fullSub := fmt.Sprintf("projects/%s/subscriptions/%s", env.UniqueName("cfg04-res-sub"), env.UniqueName("cfg04-sub"))
	pubReq := newPubReq(env, fullTopic, 1, 0, 0)
	pubReq.ProjectId = env.UniqueName("cfg04-cli-pub")
	if _, err := m3ePublishOne(ctx, env, pubReq, []byte("cfg04-cross-pub"), nil, "", "", false); err != nil {
		return err
	}
	cmd := interactiveCmd(env, ep, fullSub)
	cmd.ProjectId = env.UniqueName("cfg04-cli-sub")
	pump, stream, err := startSubAndStream(ctx, env, cmd, 1, "cfg04 sub start")
	if err != nil {
		return err
	}
	defer m3eClosePump(ctx, env, &pump)
	msg1, ack1, msg2, ack2 := env.UniqueName("m-cfg04-1"), env.UniqueName("ack-cfg04-1"), env.UniqueName("m-cfg04-2"), env.UniqueName("ack-cfg04-2")
	if err := m3eSubAckThenNack(ctx, env, pump, stream, ack1, msg1, ack2, msg2, "cfg04"); err != nil {
		return err
	}
	pump = nil
	wmPull, pullAck := lastCapturedSeq(env.Fake.Snapshot()), env.UniqueName("ack-cfg04-pull")
	fullPullSub := fmt.Sprintf("projects/%s/subscriptions/%s", env.UniqueName("cfg04-res-pull"), env.UniqueName("cfg04-pullsub"))
	if env.HasCapability(harness.CapUnaryPull) {
		if wmPull, err = m3eRunUnaryPullOne(ctx, env, &proxypb.UnaryPullRequest{
			Endpoint: ep, Subscription: fullPullSub, ProjectId: env.UniqueName("cfg04-cli-pull"),
			MaxMessages: 1, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		}, env.UniqueName("m-cfg04-pull"), pullAck, []byte("cfg04-pull"), nil, "cfg04 unary pull"); err != nil {
			return err
		}
	}
	if err := m3eVerifyCfgTail(env, wm0, wmPull, fullTopic, fullSub, 1, ack1, ack2, fullPullSub, pullAck, nil, "cfg04"); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}
