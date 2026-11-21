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
	"errors"
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
	"google.golang.org/grpc/metadata"
)

func m3eClosePump(ctx context.Context, env *harness.ScenarioEnv, p **subPump) {
	if *p != nil {
		(*p).close(ctx, env)
	}
}

func m3eVerifySampledTraceparent(rawTP, label string) error {
	if tp, err := traces.ParseTraceparent(rawTP); err != nil {
		return fmt.Errorf("%s invalid traceparent %q: %w", label, rawTP, err)
	} else if !tp.IsSampled() {
		return fmt.Errorf("%s traceparent %q is not sampled (flags=%#02x)", label, rawTP, tp.TraceFlags)
	}
	return nil
}

func cfgStreamingAllowUnencoded(env *harness.ScenarioEnv) bool {
	return slices.Contains([]string{"rust", "java", "node", "nodejs", "javascript", "js", "typescript", "ts", "python"}, strings.ToLower(strings.TrimSpace(env.Language())))
}

func m3eCheckCustomHeaders(md metadata.MD, want map[string]string, op string) error {
	for k, wantVal := range want {
		if got := md.Get(k); !slices.Contains(got, wantVal) {
			return fmt.Errorf("%s: custom header %q = %v, want %q", op, k, got, wantVal)
		}
	}
	return nil
}

func m3eVerifyStreamingInitAndHeaders(v *fake.Verifier, env *harness.ScenarioEnv, snap fake.Snapshot, sub string, minStreams int, customHeaders map[string]string, op string) {
	initExp := fake.ExpectStreamingPullInitial(op, fake.StreamingPullInitialExpectation{
		Subscription:                sub,
		CheckRequestParams:          true,
		AllowUnencodedRequestParams: cfgStreamingAllowUnencoded(env),
	})
	initChecked := 0
	for i, c := range snap.StreamingPullCalls() {
		if c.StreamSeq != 0 || c.StreamingPullReq == nil || c.StreamingPullReq.GetSubscription() != sub {
			continue
		}
		v.Check(initExp.Check(c))
		if len(customHeaders) > 0 {
			v.Check(m3eCheckCustomHeaders(c.Metadata, customHeaders, fmt.Sprintf("%s initial frame[%d]", op, i)))
		}
		initChecked++
	}
	v.Require(initChecked >= minStreams, "%s: expected >= %d initial StreamingPull frames for %q, got %d", op, minStreams, sub, initChecked)
	v.Check(m3cCheckClientIDs(env, snap))
}

func m3ePublishOne(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.CreatePublisherRequest, data []byte, attrs map[string]string, parentTP, parentTS string, checkImmut bool) (string, error) {
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return "", err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	resp, err := publishSync(ctx, env, &proxypb.PublishRequest{
		PublisherId: pubID, Data: data, Attributes: maps.Clone(attrs),
		ParentTraceparent: parentTP, ParentTracestate: parentTS, VerifyAttributesImmutability: checkImmut,
	})
	if err != nil {
		return "", err
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "Publish"); err != nil {
		return "", err
	}
	if checkImmut && resp.GetCallerAttributesMutated() {
		return "", fmt.Errorf("proxy reported caller attributes map was mutated during Publish")
	}
	if resp.GetMessageId() == "" {
		return "", fmt.Errorf("Publish returned empty message_id")
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return "", err
	}
	return resp.GetMessageId(), nil
}

func m3eRunUnaryPullOne(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.UnaryPullRequest, msgID, ackID string, data []byte, attrs map[string]string, op string) (int64, error) {
	wmPull := lastCapturedSeq(env.Fake.Snapshot())
	env.Fake.AddPullRule(fake.PullRule{
		Name: op, Match: func(mc *fake.PullMatchContext) bool { return mc.Request.GetSubscription() == req.GetSubscription() },
		Times: 1,
		Response: &pubsubpb.PullResponse{
			ReceivedMessages: []*pubsubpb.ReceivedMessage{{
				AckId: ackID, Message: &pubsubpb.PubsubMessage{MessageId: msgID, Data: data, Attributes: attrs},
			}},
		},
	})
	resp, err := env.Client.UnaryPull(ctx, req)
	if err := env.CheckProxyErr(err); err != nil {
		return wmPull, err
	}
	if err := rejectInBandUnimpl(env, resp.GetErrorCode(), resp.GetErrorMessage(), op); err != nil {
		return wmPull, err
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), op); err != nil {
		return wmPull, err
	}
	if len(resp.GetMessages()) != 1 || resp.GetMessages()[0].GetMessageId() != msgID {
		return wmPull, fmt.Errorf("%s: expected message %q, got %v", op, msgID, resp.GetMessages())
	}
	return wmPull, nil
}

func m3eWaitModAck(ctx context.Context, env *harness.ScenarioEnv, pump *subPump, wm int64, ackID string, positive bool, minCount int, op string) error {
	return waitFakeOrStop(ctx, env, pump, func(s fake.Snapshot) bool {
		n := 0
		for _, c := range callsAfterSeq(s, wm) {
			if !c.Completed || c.FullMethod != fake.MethodModifyAckDeadline || c.StatusCode != codes.OK || c.ModAckReq == nil || !slices.Contains(c.ModAckReq.GetAckIds(), ackID) {
				continue
			}
			sec := c.ModAckReq.GetAckDeadlineSeconds()
			if (positive && sec > 0) || (!positive && sec == 0) {
				n++
			}
		}
		return n >= minCount
	}, op)
}

func m3eLooksUnimplOrUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return harness.IsUnimplemented(err) || strings.Contains(msg, "unimplemented") || strings.Contains(msg, "unsupported")
}

func m3eCFG03Check(err error, code int32, msg, what, detail string) error {
	if err != nil {
		if m3eLooksUnimplOrUnsupported(err) {
			return fmt.Errorf("cfg03 empty project_id %s rejected as unimplemented/unsupported: %v", what, err)
		}
		return nil
	}
	if harness.IsInBandUnimplemented(code) {
		return fmt.Errorf("cfg03 empty project_id %s returned UNIMPLEMENTED: %s", what, msg)
	}
	if code == 0 {
		return fmt.Errorf("cfg03 empty project_id %s unexpectedly succeeded (%s)", what, detail)
	}
	return nil
}

func m3eCheckRejectedPubEmptyProj(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.CreatePublisherRequest, wm int64) error {
	resp, err := env.Client.CreatePublisher(ctx, req)
	if err != nil {
		if e := m3eCFG03Check(err, 0, "", "publisher", ""); e != nil {
			return e
		}
		return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
	}
	pubID := resp.GetPublisherId()
	defer cleanupPublisher(ctx, env, &pubID)
	pubResp, pubErr := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("cfg03-should-fail")})
	if e := m3eCFG03Check(pubErr, pubResp.GetErrorCode(), pubResp.GetErrorMessage(), "publish", req.GetTopic()); e != nil {
		return e
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
}

func m3eCheckRejectedSubEmptyProj(ctx context.Context, env *harness.ScenarioEnv, cmd *proxypb.StartSubscriberCommand, wm int64) error {
	pump, _, err := startSubPump(ctx, env, cmd)
	if err != nil {
		if e := m3eCFG03Check(err, 0, "", "subscriber", ""); e != nil {
			return e
		}
		return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	ev, waitErr := pump.recvDelivery(waitCtx, env)
	pump.close(ctx, env)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if waitErr == nil {
		return fmt.Errorf("cfg03 empty project_id subscriber did not reject with error (ev=%v)", ev)
	}
	if errors.Is(waitErr, context.DeadlineExceeded) {
		return fmt.Errorf("cfg03 empty project_id subscriber stayed active for short sub %q", cmd.GetSubscription())
	}
	if e := m3eCFG03Check(waitErr, 0, "", "subscriber", ""); e != nil {
		return e
	}
	return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
}

func m3eCheckRejectedPullEmptyProj(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.UnaryPullRequest, wm int64) error {
	resp, err := env.Client.UnaryPull(ctx, req)
	if e := m3eCFG03Check(err, resp.GetErrorCode(), resp.GetErrorMessage(), "pull", req.GetSubscription()); e != nil {
		return e
	}
	return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
}

func m3eAssertDeliveredBasic(ev *proxypb.MessageDeliveredEvent, wantID string, wantData []byte, wantAttrK, wantAttrV string) error {
	if ev.GetMessageId() != wantID || string(ev.GetData()) != string(wantData) || ev.GetOrderingKey() != "" {
		return fmt.Errorf("delivered mismatch: id=%q(want %q) data=%q(want %q) key=%q", ev.GetMessageId(), wantID, ev.GetData(), wantData, ev.GetOrderingKey())
	}
	if wantAttrK != "" && ev.GetAttributes()[wantAttrK] != wantAttrV {
		return fmt.Errorf("delivered attr %q = %q, want %q", wantAttrK, ev.GetAttributes()[wantAttrK], wantAttrV)
	}
	return nil
}

func m3eSubAckThenNack(ctx context.Context, env *harness.ScenarioEnv, pump *subPump, stream *fake.StreamHandle, ack1, msg1, ack2, msg2, label string) error {
	ev1, err := pushRecvOne(ctx, env, pump, stream, ack1, msg1, []byte(label+"-d1"))
	if err != nil {
		return err
	}
	if err := ackAndWait(ctx, env, pump, ev1.GetDeliveryToken(), ack1, label+" ack1"); err != nil {
		return err
	}
	wm2 := lastCapturedSeq(env.Fake.Snapshot())
	ev2, err := pushRecvOne(ctx, env, pump, stream, ack2, msg2, []byte(label+"-d2"))
	if err != nil {
		return err
	}
	if err := sendSettleAction(env, pump, ev2.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK); err != nil {
		return err
	}
	if err := m3eWaitModAck(ctx, env, pump, wm2, ack2, false, 1, label+" nack2"); err != nil {
		return err
	}
	return pump.stopAndCheck(ctx, env, 2)
}

func m3eVerifySubAndPullAfter(v *fake.Verifier, env *harness.ScenarioEnv, snap fake.Snapshot, wmBase, wmPull int64, sub, ackID, nackID, pullSub, pullAckID string, customHeaders map[string]string, op string) {
	allowUnenc := allowUnencodedRequestParams(env)
	var subAcks, modAcks, pullModAcks, pulls, pullAcks []fake.CapturedCall
	for _, c := range callsAfterSeq(snap, wmBase) {
		switch {
		case c.FullMethod == fake.MethodAcknowledge && c.Seq <= wmPull:
			subAcks = append(subAcks, c)
		case c.FullMethod == fake.MethodModifyAckDeadline && c.Seq <= wmPull:
			modAcks = append(modAcks, c)
		case c.FullMethod == fake.MethodModifyAckDeadline && c.Seq > wmPull:
			pullModAcks = append(pullModAcks, c)
		case c.FullMethod == fake.MethodPull && c.Seq <= wmPull:
			v.Checkf("%s: unexpected Pull RPC seq=%d before UnaryPull watermark %d", op, c.Seq, wmPull)
		case c.FullMethod == fake.MethodPull && c.Seq > wmPull:
			pulls = append(pulls, c)
		case c.FullMethod == fake.MethodAcknowledge && c.Seq > wmPull:
			pullAcks = append(pullAcks, c)
		}
	}
	if ackID != "" {
		m3eVerifyOneCall(v, subAcks, fake.ExpectAck(op+" ack", fake.AckSubscription(sub), fake.AckIDsExact(ackID), fake.AckRequestParams(sub, allowUnenc)), customHeaders, op+" ack")
	}
	foundNack := false
	for i, mc := range modAcks {
		if mc.ModAckReq.GetAckDeadlineSeconds() == 0 {
			if nackID == "" {
				v.Checkf("%s: unexpected ModifyAckDeadline(0) nack call %v", op, mc.ModAckReq)
			} else {
				v.Check(fake.ExpectModAck(fmt.Sprintf("%s nack[%d]", op, i), fake.ModAckSubscription(sub), fake.ModAckIDsExact(nackID), fake.ModAckDeadline(0), fake.ModAckRequestParams(sub, allowUnenc)).Check(mc))
				foundNack = true
			}
		}
		if len(customHeaders) > 0 {
			v.Check(m3eCheckCustomHeaders(mc.Metadata, customHeaders, fmt.Sprintf("%s modack[%d]", op, i)))
		}
	}
	if nackID != "" {
		v.Require(foundNack, "%s: missing ModifyAckDeadline(0) nack call on %q", op, sub)
	}
	for i, mc := range pullModAcks {
		if mc.ModAckReq.GetAckDeadlineSeconds() == 0 {
			v.Checkf("%s: unexpected post-pull ModifyAckDeadline(0) nack call %v", op, mc.ModAckReq)
		}
		if len(customHeaders) > 0 {
			v.Check(m3eCheckCustomHeaders(mc.Metadata, customHeaders, fmt.Sprintf("%s pull modack[%d]", op, i)))
		}
	}
	v.Check(fake.AssertAckAndModAckWithinBounds(snapAfterSeq(snap, wmBase)))
	if env.HasCapability(harness.CapUnaryPull) {
		m3eVerifyOneCall(v, pulls, fake.ExpectPull(op+" pull", fake.PullSubscription(pullSub), fake.PullMaxMessages(1), fake.PullRequestParams(pullSub, allowUnenc)), customHeaders, op+" pull")
		m3eVerifyOneCall(v, pullAcks, fake.ExpectAck(op+" pull ack", fake.AckSubscription(pullSub), fake.AckIDsExact(pullAckID), fake.AckRequestParams(pullSub, allowUnenc)), customHeaders, op+" pull ack")
	}
}

func m3eVerifyOneCall(v *fake.Verifier, calls []fake.CapturedCall, exp fake.CallExpectation, hdrs map[string]string, op string) {
	v.Require(len(calls) == 1, "%s: expected 1 call, got %d", op, len(calls))
	if len(calls) > 0 {
		v.Check(exp.Check(calls[0]))
		if len(hdrs) > 0 {
			v.Check(m3eCheckCustomHeaders(calls[0].Metadata, hdrs, op))
		}
	}
}

func m3eVerifyCfgTail(env *harness.ScenarioEnv, wmBase, wmPull int64, topic, sub string, minStreams int, ackID, nackID, pullSub, pullAck string, hdrs map[string]string, op string) error {
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	allowed := []string{fake.MethodPublish, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline}
	if env.HasCapability(harness.CapUnaryPull) {
		allowed = append(allowed, fake.MethodPull)
	}
	v.Check(fake.AssertNoUnexpectedRPCs(snapAfterSeq(snap, wmBase), allowed...))
	m3eVerifyOneCall(v, publishCallsAfterSeq(snap, wmBase), fake.ExpectPublish(op+" publish", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencodedRequestParams(env))), hdrs, op+" publish")
	m3eVerifyStreamingInitAndHeaders(v, env, snap, sub, minStreams, hdrs, op+" streamingpull")
	m3eVerifySubAndPullAfter(v, env, snap, wmBase, wmPull, sub, ackID, nackID, pullSub, pullAck, hdrs, op)
	return v.Err()
}
