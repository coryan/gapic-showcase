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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func allowUnencodedRequestParams(env *harness.ScenarioEnv) bool {
	return strings.EqualFold(env.Language(), "rust")
}

func newSampledTraceparent() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	b[0], b[16] = b[0]|1, b[16]|1
	return fmt.Sprintf("00-%x-%x-01", b[:16], b[16:])
}

func checkInBandErr(code int32, msg, op string) error {
	if code == 0 {
		return nil
	}
	if harness.IsInBandUnimplemented(code) {
		return harness.Unsupportedf("%s returned UNIMPLEMENTED: %s", op, msg)
	}
	return fmt.Errorf("%s failed (%s): %s", op, codes.Code(code), msg)
}

func createPublisher(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.CreatePublisherRequest) (string, error) {
	resp, err := env.Client.CreatePublisher(ctx, req)
	if err := env.CheckProxyErr(err); err != nil {
		return "", fmt.Errorf("CreatePublisher: %w", err)
	}
	if resp == nil || resp.GetPublisherId() == "" {
		return "", fmt.Errorf("CreatePublisher returned empty publisher_id")
	}
	return resp.GetPublisherId(), nil
}

func closePublisher(ctx context.Context, env *harness.ScenarioEnv, pubID string) error {
	if pubID == "" {
		return nil
	}
	if _, err := env.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubID}); err != nil {
		return fmt.Errorf("ClosePublisher: %w", env.CheckProxyErr(err))
	}
	return nil
}

func cleanupPublisher(ctx context.Context, env *harness.ScenarioEnv, pubID *string) {
	if pubID == nil || *pubID == "" || ctx.Err() != nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, _ = env.Client.ClosePublisher(cctx, &proxypb.ClosePublisherRequest{PublisherId: *pubID})
	*pubID = ""
}

func awaitPublishMessageID(ctx context.Context, env *harness.ScenarioEnv, pubID, token string) (string, error) {
	resp, err := env.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubID, PublishToken: token})
	if err := env.CheckProxyErr(err); err != nil {
		return "", fmt.Errorf("AwaitPublish: %w", err)
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "AwaitPublish"); err != nil {
		return "", err
	}
	if resp.GetMessageId() == "" {
		return "", fmt.Errorf("AwaitPublish returned empty message_id")
	}
	return resp.GetMessageId(), nil
}

type singlePublishSpec struct {
	label, topic, parentTraceparent                  string
	data                                             []byte
	attrs                                            map[string]string
	enableMonitoring, checkImmut, requireTraceparent bool
}

func publishSingleAndVerify(ctx context.Context, env *harness.ScenarioEnv, sp singlePublishSpec) (string, fake.Snapshot, error) {
	pubID, err := createPublisher(ctx, env, &proxypb.CreatePublisherRequest{
		Endpoint: env.Endpoint(), Topic: sp.topic, ProjectId: env.ProjectID,
		EnableMonitoring: sp.enableMonitoring, CountThreshold: proto.Int64(1), DelayThresholdMs: proto.Int64(0),
	})
	if err != nil {
		return "", fake.Snapshot{}, err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	resp, err := env.Client.Publish(ctx, &proxypb.PublishRequest{
		PublisherId: pubID, Data: sp.data, Attributes: maps.Clone(sp.attrs),
		ParentTraceparent: sp.parentTraceparent, VerifyAttributesImmutability: sp.checkImmut,
	})
	if err := env.CheckProxyErr(err); err != nil {
		return "", fake.Snapshot{}, fmt.Errorf("Publish: %w", err)
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "Publish"); err != nil {
		return "", fake.Snapshot{}, err
	}
	if sp.checkImmut && resp.GetCallerAttributesMutated() {
		return "", fake.Snapshot{}, fmt.Errorf("proxy reported caller attributes map was mutated during Publish")
	}
	if resp.GetMessageId() == "" {
		return "", fake.Snapshot{}, fmt.Errorf("Publish returned empty message_id")
	}
	if err := closePublisher(ctx, env, pubID); err != nil {
		return "", fake.Snapshot{}, err
	}
	pubID = ""
	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPublish(
		sp.label, fake.PublishTopic(sp.topic),
		fake.PublishRequestParams(sp.topic, allowUnencodedRequestParams(env)), fake.PublishStatusCode(codes.OK), fake.PublishBatchSize(1),
		fake.PublishMessages(fake.ExpectedMessage{Data: sp.data, Attributes: sp.attrs, RequireTraceparent: sp.requireTraceparent}),
	)))
	v.Check(checkSortedMessageIDsMatch([]string{resp.GetMessageId()}, snap))
	return resp.GetMessageId(), snap, v.Err()
}

func verifySampledTraceparent(rawTP, parentTP, label string) (string, error) {
	pParent, err := traces.ParseTraceparent(parentTP)
	if err != nil {
		return "", fmt.Errorf("internal invalid parent traceparent %q: %w", parentTP, err)
	}
	if p, err := traces.ParseTraceparent(rawTP); err != nil {
		return "", fmt.Errorf("invalid %s %q: %w", label, rawTP, err)
	} else if p.TraceID != pParent.TraceID || !p.IsSampled() {
		return "", fmt.Errorf("%s %q trace_id/sampled mismatch vs parent %q", label, rawTP, parentTP)
	}
	return rawTP, nil
}

func nonEmptyStringSlice(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

func singlePublishTraceExp(env *harness.ScenarioEnv, msgID, parentTP, wireTP string, bodySize int) *traces.PublishExpectation {
	return &traces.PublishExpectation{
		MessageCount: 1, BatchCount: 1, ParentTraceparent: parentTP, MessageIDs: []string{msgID},
		WireTraceparents: nonEmptyStringSlice(wireTP), BodySizes: []int64{int64(bodySize)},
		FlowControlIgnored: env.Profile.AllowOmittedFlowControlIfIgnored && env.Defaults().GetPublishFlowControlBehavior() == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE,
	}
}

func stoppedEventErr(st *proxypb.StoppedEvent, op string) error {
	if err := checkInBandErr(st.GetErrorCode(), st.GetErrorMessage(), op); err != nil {
		return err
	}
	return fmt.Errorf("SubscriberSession stopped unexpectedly during %s", op)
}

func drainSubscriberSendEOF(env *harness.ScenarioEnv, stream proxypb.PubSubProxy_SubscriberSessionClient, sendErr error, op, label string) error {
	_ = stream.CloseSend()
	for stopped := (*proxypb.StoppedEvent)(nil); errors.Is(sendErr, io.EOF); {
		ev, rerr := stream.Recv()
		if rerr == nil {
			if st := ev.GetStopped(); st != nil {
				stopped = st
			}
			continue
		}
		if stopped != nil && (errors.Is(rerr, io.EOF) || status.Code(rerr) != codes.Unimplemented) {
			return stoppedEventErr(stopped, op)
		}
		if !errors.Is(rerr, io.EOF) {
			return fmt.Errorf("send %s: %w", label, env.CheckProxyErr(rerr))
		}
		break
	}
	return fmt.Errorf("send %s: %w", label, env.CheckProxyErr(sendErr))
}

func startSubscriber(ctx context.Context, env *harness.ScenarioEnv, cmd *proxypb.StartSubscriberCommand) (proxypb.PubSubProxy_SubscriberSessionClient, *proxypb.StartedEvent, error) {
	stream, err := env.Client.SubscriberSession(ctx)
	if err := env.CheckProxyErr(err); err != nil {
		return nil, nil, fmt.Errorf("SubscriberSession: %w", err)
	}
	if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: cmd}}); err != nil {
		return nil, nil, drainSubscriberSendEOF(env, stream, err, "start", "StartSubscriberCommand")
	}
	ev, err := stream.Recv()
	if err != nil {
		_ = stream.CloseSend()
		return nil, nil, fmt.Errorf("recv StartedEvent: %w", env.CheckProxyErr(err))
	}
	if started := ev.GetStarted(); started != nil {
		return stream, started, nil
	}
	_ = stream.CloseSend()
	if st := ev.GetStopped(); st != nil {
		return nil, nil, stoppedEventErr(st, "start")
	}
	return nil, nil, fmt.Errorf("expected StartedEvent, got %T", ev.GetEvent())
}

func recvMessageDelivered(env *harness.ScenarioEnv, stream proxypb.PubSubProxy_SubscriberSessionClient) (*proxypb.MessageDeliveredEvent, error) {
	ev, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv MessageDeliveredEvent: %w", env.CheckProxyErr(err))
	}
	if st := ev.GetStopped(); st != nil {
		return nil, stoppedEventErr(st, "delivery")
	}
	md := ev.GetMessageDelivered()
	if md == nil {
		return nil, fmt.Errorf("expected MessageDeliveredEvent, got %T", ev.GetEvent())
	}
	if md.GetDeliveryToken() == "" {
		return nil, fmt.Errorf("MessageDeliveredEvent has empty delivery_token")
	}
	return md, nil
}

func assertDeliveredMsg(m interface {
	GetMessageId() string
	GetData() []byte
	GetAttributes() map[string]string
	GetOrderingKey() string
}, wantMsgID string, wantData []byte, wantAttrs map[string]string, wantOrderingKey string) error {
	if m.GetMessageId() != wantMsgID || m.GetOrderingKey() != wantOrderingKey {
		return fmt.Errorf("delivered metadata mismatch: want id=%q key=%q, got id=%q key=%q", wantMsgID, wantOrderingKey, m.GetMessageId(), m.GetOrderingKey())
	}
	if d := cmp.Diff(wantData, m.GetData()); d != "" {
		return fmt.Errorf("delivered data mismatch (-want +got):\n%s", d)
	}
	if d := cmp.Diff(wantAttrs, m.GetAttributes()); d != "" {
		return fmt.Errorf("delivered attributes mismatch (-want +got):\n%s", d)
	}
	return nil
}

func checkNoPrematureAck(ctx context.Context, fakeSrv *fake.Server, md *proxypb.MessageDeliveredEvent, quiet time.Duration) error {
	if md.GetAutoSettled() {
		return fmt.Errorf("proxy reported auto_settled=true in interactive mode")
	}
	if quiet > 0 {
		wCtx, cancel := context.WithTimeout(ctx, quiet)
		defer cancel()
		_ = fakeSrv.WaitFor(wCtx, func(s fake.Snapshot) bool { ids, calls := countAckOccurrences(s); return ids > 0 || calls > 0 })
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if ids, calls := countAckOccurrences(fakeSrv.Snapshot()); ids != 0 || calls != 0 {
		return fmt.Errorf("premature ack observed before SettleMessageCommand (ids=%d, calls=%d)", ids, calls)
	}
	return nil
}

func sendSettleAck(env *harness.ScenarioEnv, stream proxypb.PubSubProxy_SubscriberSessionClient, deliveryToken string) error {
	if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: deliveryToken, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK}}}); err != nil {
		return drainSubscriberSendEOF(env, stream, err, "settle", "SettleMessageCommand(ACK)")
	}
	return nil
}

func stopSubscriberAndCheckDelivered(env *harness.ScenarioEnv, stream proxypb.PubSubProxy_SubscriberSessionClient, wantDelivered int64) error {
	if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{}}}); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("send StopSubscriberCommand: %w", env.CheckProxyErr(err))
	}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("SubscriberSession stream closed before StoppedEvent")
		}
		if err != nil {
			return fmt.Errorf("recv StoppedEvent: %w", env.CheckProxyErr(err))
		}
		switch p := ev.GetEvent().(type) {
		case *proxypb.SubscriberEvent_Stopped:
			_ = stream.CloseSend()
			if err := checkInBandErr(p.Stopped.GetErrorCode(), p.Stopped.GetErrorMessage(), "StoppedEvent"); err != nil {
				return err
			}
			if p.Stopped.GetTotalDelivered() != wantDelivered {
				return fmt.Errorf("StoppedEvent total_delivered mismatch: want %d, got %d", wantDelivered, p.Stopped.GetTotalDelivered())
			}
			return nil
		case *proxypb.SubscriberEvent_SettlementResult:
			if sr := p.SettlementResult; sr.GetGrpcErrorCode() != 0 || (sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED && sr.GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS) {
				return fmt.Errorf("unexpected settlement failure for %q: status=%s code=%d msg=%q", sr.GetDeliveryToken(), sr.GetStatus(), sr.GetGrpcErrorCode(), sr.GetErrorMessage())
			}
		default:
			return fmt.Errorf("unexpected SubscriberEvent %T while waiting for StoppedEvent", p)
		}
	}
}

func waitCompletedAck(ctx context.Context, fakeSrv *fake.Server) error {
	return fakeSrv.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return slices.ContainsFunc(snap.AckCalls(), func(c fake.CapturedCall) bool { return c.Completed }) ||
			slices.ContainsFunc(snap.StreamingPullCalls(), func(c fake.CapturedCall) bool { return len(c.StreamingPullReq.GetAckIds()) > 0 })
	})
}

func waitKeyPublishAttempts(ctx context.Context, fakeSrv *fake.Server, key string, want int, wait time.Duration) error {
	wCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	_ = fakeSrv.WaitFor(wCtx, func(snap fake.Snapshot) bool {
		n := 0
		for _, c := range snap.PublishCalls() {
			if msgs := c.PublishReq.GetMessages(); len(msgs) > 0 && msgs[0].GetOrderingKey() == key {
				n++
			}
		}
		return n >= want
	})
	return ctx.Err()
}

func countAckOccurrences(snap fake.Snapshot) (totalIDs, ackCalls int) {
	for _, c := range snap.Calls {
		if c.AckReq != nil {
			ackCalls++
			totalIDs += len(c.AckReq.GetAckIds())
		} else if c.StreamingPullReq != nil {
			totalIDs += len(c.StreamingPullReq.GetAckIds())
		}
	}
	return totalIDs, ackCalls
}

func verifyStreamingPullAndSettlementRPCs(env *harness.ScenarioEnv, snap fake.Snapshot, sub, wantAckID string, extraAllowed ...string) error {
	allowUnencoded := allowUnencodedRequestParams(env)
	v := env.NewVerifier()
	v.Check(fake.AssertNoUnexpectedRPCs(snap, append([]string{fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline}, extraAllowed...)...))
	initExp := fake.StreamingPullInitialExpectation{
		Subscription: sub, StreamAckDeadlineMin: fake.MinLeaseExtensionSeconds, StreamAckDeadlineMax: fake.MaxLeaseExtensionSeconds,
		ProtocolVersion: map[bool]*int64{true: proto.Int64(1)}[env.HasCapability(harness.CapKeepalive)], CheckRequestParams: true, AllowUnencodedRequestParams: cfgStreamingAllowUnencoded(env),
	}
	initChk := fake.ExpectStreamingPullInitial("StreamingPull-init", initExp)
	initFrames := 0
	for _, c := range snap.StreamingPullCalls() {
		if c.StreamSeq == 0 && c.StreamingPullReq != nil {
			initFrames++
			v.Check(initChk.Check(c))
		}
	}
	v.Require(initFrames > 0, "expected at least one initial StreamingPullRequest frame")
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertAllAckedIDs(snap, wantAckID))
	v.Check(fake.AssertAllNackedIDs(snap))
	totalIDs, ackCalls := countAckOccurrences(snap)
	v.Require(totalIDs == 1, "expected exactly 1 acknowledged ack_id occurrence, got %d (ack_calls=%d)", totalIDs, ackCalls)
	for _, c := range snap.AckCalls() {
		v.Check(fake.ExpectAck("Acknowledge", fake.AckSubscription(sub), fake.AckRequestParams(sub, allowUnencoded), fake.AckStatusCode(codes.OK)).Check(c))
	}
	for _, c := range snap.ModAckCalls() {
		v.Check(fake.ExpectModAck("ModifyAckDeadline", fake.ModAckSubscription(sub), fake.ModAckRequestParams(sub, allowUnencoded), fake.ModAckStatusCode(codes.OK)).Check(c))
	}
	return v.Err()
}

type singleSubSpec struct {
	sub, ackID, msgID, parentTP                    string
	data                                           []byte
	attrs                                          map[string]string
	enableMonitoring, sendEmptyResp, strictInitial bool
	numStreams                                     int32
	extraAllowed                                   []string
}

func cleanupSubscriber(ctx context.Context, cancel context.CancelFunc, env *harness.ScenarioEnv, s proxypb.PubSubProxy_SubscriberSessionClient, deliveryToken string, settled bool) {
	if s == nil || ctx.Err() != nil {
		return
	}
	defer time.AfterFunc(2*time.Second, cancel).Stop()
	stopCmd := &proxypb.StopSubscriberCommand{}
	switch {
	case env.HasCapability(harness.CapShutdownBehavior):
		stopCmd.ShutdownBehavior = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY
	case env.HasCapability(harness.CapShutdownTimeout):
		stopCmd.ShutdownTimeoutMs = proto.Int64(0)
	case deliveryToken != "" && !settled:
		_ = s.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: deliveryToken, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK}}})
	}
	_ = s.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: stopCmd}})
	_ = s.CloseSend()
	for ev, err := s.Recv(); err == nil && ev.GetStopped() == nil; ev, err = s.Recv() {
	}
}

func runInteractiveSingleSub(ctx context.Context, env *harness.ScenarioEnv, sp singleSubSpec) (string, fake.Snapshot, error) {
	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var stream proxypb.PubSubProxy_SubscriberSessionClient
	deliveryToken, cbTP, settled := "", "", false
	defer func() { cleanupSubscriber(ctx, cancel, env, stream, deliveryToken, settled) }()
	var numStreams *int32
	if sp.numStreams > 0 {
		numStreams = proto.Int32(sp.numStreams)
	}
	stream, started, err := startSubscriber(sessCtx, env, &proxypb.StartSubscriberCommand{
		Endpoint: env.Endpoint(), Subscription: sp.sub, ProjectId: env.ProjectID, NumStreams: numStreams,
		EnableMonitoring: sp.enableMonitoring, AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
	})
	if err != nil {
		return "", fake.Snapshot{}, err
	}
	subCtx, subCancel := context.WithCancel(sessCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if sp.strictInitial && env.Fake.WaitFor(subCtx, func(s fake.Snapshot) bool { return len(s.Streams) > 0 && s.Streams[0].Closed }) == nil {
			subCancel()
		}
	}()
	sh, err := env.Fake.WaitForStreamingPull(subCtx)
	subCancel()
	<-done
	if err != nil {
		if snap := env.Fake.Snapshot(); sp.strictInitial && sessCtx.Err() == nil && len(snap.Streams) > 0 && snap.Streams[0].Closed {
			return "", fake.Snapshot{}, verifyStreamingPullAndSettlementRPCs(env, snap, sp.sub, sp.ackID, sp.extraAllowed...)
		}
		return "", fake.Snapshot{}, fmt.Errorf("WaitForStreamingPull: %w", err)
	}
	if sp.sendEmptyResp {
		err = sh.PushResponse(&pubsubpb.StreamingPullResponse{})
	}
	if err == nil {
		err = sh.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: sp.ackID, Message: &pubsubpb.PubsubMessage{MessageId: sp.msgID, Data: slices.Clone(sp.data)}}}, fake.WithAttributes(sp.attrs))
	}
	if err != nil {
		return "", fake.Snapshot{}, fmt.Errorf("PushMessages: %w", err)
	}
	md, err := recvMessageDelivered(env, stream)
	if err == nil {
		deliveryToken, cbTP = md.GetDeliveryToken(), md.GetCallbackTraceparent()
		err = assertDeliveredMsg(md, sp.msgID, sp.data, sp.attrs, "")
	}
	if err == nil && sp.enableMonitoring {
		_, err = verifySampledTraceparent(cbTP, sp.parentTP, "callback_traceparent")
	}
	if err == nil {
		if err = checkNoPrematureAck(sessCtx, env.Fake, md, 25*time.Millisecond); err == nil {
			err = sendSettleAck(env, stream, deliveryToken)
		}
	}
	if err != nil {
		return "", fake.Snapshot{}, err
	}
	settled = true
	if err := waitCompletedAck(sessCtx, env.Fake); err != nil {
		return "", fake.Snapshot{}, fmt.Errorf("waiting for Acknowledge RPC: %w", err)
	}
	if err := stopSubscriberAndCheckDelivered(env, stream, 1); err != nil {
		return "", fake.Snapshot{}, err
	}
	stream = nil
	snap := env.Fake.Snapshot()
	if cid := started.GetClientId(); cid != "" {
		initCount := 0
		for _, st := range snap.Streams {
			if st.HasInitial {
				initCount++
				if st.ClientID != cid {
					return cbTP, snap, fmt.Errorf("client_id mismatch: want %q, got %q", cid, st.ClientID)
				}
			}
		}
		if initCount == 0 {
			return cbTP, snap, fmt.Errorf("client_id mismatch: want %q, got no initial StreamingPull stream", cid)
		}
	}
	return cbTP, snap, verifyStreamingPullAndSettlementRPCs(env, snap, sp.sub, sp.ackID, sp.extraAllowed...)
}

func checkSortedMessageIDsMatch(returnedIDs []string, snap fake.Snapshot) error {
	var serverIDs []string
	for _, c := range snap.PublishCalls() {
		serverIDs = append(serverIDs, c.ResponseMessageIDs...)
	}
	want, got := serverIDs, slices.Clone(returnedIDs)
	slices.Sort(want)
	slices.Sort(got)
	if diff := cmp.Diff(want, got); diff != "" {
		return fmt.Errorf("published message_ids mismatch against server response (-server +proxy):\n%s", diff)
	}
	return nil
}
