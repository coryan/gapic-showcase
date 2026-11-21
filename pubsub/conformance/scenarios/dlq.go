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
	"google.golang.org/grpc/codes"
)

func dlqScenarios() []harness.Scenario {
	return []harness.Scenario{{ID: "DLQ-01", Name: "delivery_attempt Field Mapping", Category: "dlq", Feature: "DLQ-01", Description: "Verify dead-letter delivery_attempt unset/0 and positive integer mapping across StreamingPull and optional UnaryPull.", Run: runDLQ01}}
}
func checkDLQMsg(md interface {
	GetMessageId() string
	GetData() []byte
	GetAttributes() map[string]string
	GetOrderingKey() string
	GetHasDeliveryAttempt() bool
	GetDeliveryAttempt() int32
}, mid string, d []byte, want int32, label string) error {
	if err := assertDeliveredMsg(md, mid, d, nil, ""); err != nil {
		return err
	}
	if has, got := md.GetHasDeliveryAttempt(), md.GetDeliveryAttempt(); (want == 0 && got != 0) || (want != 0 && (!has || got != want)) {
		return fmt.Errorf("%s: delivery_attempt mismatch: want %d, got %d (has=%t)", label, want, got, has)
	}
	return nil
}
func runDLQ01(ctx context.Context, env *harness.ScenarioEnv) error {
	sub, allowUnencoded, hasUnary := env.SubscriptionPath(env.UniqueName("dlq01-sub")), allowUnencodedRequestParams(env), env.HasCapability(harness.CapUnaryPull)
	p, sh, err := startSubAndStream(ctx, env, interactiveCmd(env, env.Endpoint(), sub), 1, "DLQ-01 stream")
	if err != nil {
		if !harness.IsUnimplemented(err) || !hasUnary {
			return err
		}
	} else {
		defer p.close(ctx, env)
		for _, tc := range []struct {
			aid, mid string
			attempt  int32
			opts     []fake.PushOption
		}{{"ack-dlq01-s1", "msg-dlq01-s1", 0, nil}, {"ack-dlq01-s2", "msg-dlq01-s2", 3, []fake.PushOption{fake.WithDeliveryAttempt(3)}}} {
			d := []byte(tc.mid)
			md, rerr := pushRecvOne(ctx, env, p, sh, tc.aid, tc.mid, d, tc.opts...)
			if rerr != nil {
				return rerr
			}
			if cerr := checkDLQMsg(md, tc.mid, d, tc.attempt, "DLQ-01 StreamingPull "+tc.mid); cerr != nil {
				return env.Failf("%v", cerr)
			}
			if aerr := ackAndWait(ctx, env, p, md.GetDeliveryToken(), tc.aid, "DLQ-01 "+tc.aid); aerr != nil {
				return aerr
			}
		}
		if _, vS, serr := stopAndVerifyStream(ctx, env, p, 2, "DLQ-01", sub, 0, 0, "ack-dlq01-s1", "ack-dlq01-s2"); serr != nil {
			return serr
		} else if verr := vS.Err(); verr != nil {
			return verr
		}
	}
	v := env.NewVerifier()
	if hasUnary {
		wmU := lastCapturedSeq(env.Fake.Snapshot())
		var wantUnary []fake.CallExpectation
		for i, tc := range []struct {
			aid, mid string
			attempt  int32
		}{{"ack-dlq01-u1", "msg-dlq01-u1", 0}, {"ack-dlq01-u2", "msg-dlq01-u2", 5}} {
			d := []byte(tc.mid)
			env.Fake.AddPullRule(fake.PullRule{Name: "DLQ-01-" + tc.mid, Times: 1, Response: &pubsubpb.PullResponse{ReceivedMessages: []*pubsubpb.ReceivedMessage{{AckId: tc.aid, DeliveryAttempt: tc.attempt, Message: &pubsubpb.PubsubMessage{MessageId: tc.mid, Data: d}}}}})
			resp, err := env.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID, MaxMessages: 1, ReturnImmediately: true, SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK})
			if err != nil {
				return checkPostStartErr(env, err, "UnaryPull("+tc.mid+")")
			}
			if err := rejectInBandUnimpl(env, resp.GetErrorCode(), resp.GetErrorMessage(), "UnaryPull"); err != nil {
				return err
			}
			if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "UnaryPull"); err != nil {
				return err
			}
			msgs := resp.GetMessages()
			if len(msgs) != 1 {
				return env.Failf("UnaryPull(%s) returned %d messages, want 1", tc.mid, len(msgs))
			}
			if err := checkDLQMsg(msgs[0], tc.mid, d, tc.attempt, "DLQ-01 UnaryPull "+tc.mid); err != nil {
				return env.Failf("%v", err)
			}
			if msgs[0].GetSettleErrorCode() != 0 || (msgs[0].GetSettleStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS && msgs[0].GetSettleStatus() != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED) {
				return env.Failf("UnaryPull(%s) settle failed: status=%s code=%d msg=%q", tc.mid, msgs[0].GetSettleStatus(), msgs[0].GetSettleErrorCode(), msgs[0].GetSettleErrorMessage())
			}
			wantUnary = append(wantUnary,
				fake.ExpectPull(fmt.Sprintf("DLQ-01-pull-%d", i+1), fake.PullSubscription(sub), fake.PullMaxMessages(1), fake.PullRequestParams(sub, allowUnencoded), fake.PullStatusCode(codes.OK)),
				fake.ExpectAck(fmt.Sprintf("DLQ-01-ack-%d", i+1), fake.AckSubscription(sub), fake.AckIDsExact(tc.aid), fake.AckRequestParams(sub, allowUnencoded), fake.AckStatusCode(codes.OK)),
			)
		}
		v.Check(fake.VerifySequence(snapAfterSeq(env.Fake.Snapshot(), wmU), fake.DefaultVerifyOptions(), wantUnary...))
	}
	v.Check(fake.AssertNoUnexpectedRPCs(env.Fake.Snapshot(), fake.MethodStreamingPull, fake.MethodPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	return finishPublishScenario(ctx, env, v)
}
