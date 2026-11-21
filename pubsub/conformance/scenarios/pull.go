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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func pullScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PULL-02", Name: "StreamingPull Transient Recovery & Permanent Failure", Category: "pull", Feature: "PULL-02", Description: "Verify StreamingPull recovers from transient Unavailable/Internal/Aborted stream faults with consistent client_id and terminates fatally on permanent PermissionDenied.", Run: runPULL02},
		{ID: "PULL-02b", Name: "StreamingPull Reconnect After Abrupt TCP Reset", Category: "pull", Feature: "PULL-02b", IsolateProxy: true, Description: "Verify StreamingPull reconnects and delivers subsequent messages after an abrupt trailerless TCP RST mid-stream.", Run: runPULL02b},
		{ID: "PULL-02c", Name: "StreamingPull ResourceExhausted Divergence", Category: "pull", Feature: "PULL-02c", Description: "Verify Go terminates fatally on mid-stream ResourceExhausted while peer streaming SDKs reopen the stream and deliver subsequent messages.", Run: runPULL02c},
		{ID: "PULL-02d", Name: "StreamingPull Server shutdownNow Divergence", Category: "pull", Feature: "PULL-02d", Description: "Verify Go and Java fail fatally on Unavailable 'Server shutdownNow invoked' while peer streaming SDKs retry and recover.", Run: runPULL02d},
		{ID: "PULL-03", Name: "StreamingPull Keepalive Ping & Pong Timeout Reconnect", Category: "pull", Feature: "PULL-03", Timeout: 120 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapKeepalive}, Description: "Verify protocol_version=1 idle keepalive pings keep the stream healthy and suppressed pongs trigger stream reconnection.", Run: runPULL03},
		{ID: "PULL-03b", Name: "StreamingPull Keepalive Immune to Flow-Control Pause", Category: "pull", Feature: "PULL-03b", Timeout: 120 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapKeepalive}, Description: "Verify holding MaxOutstandingMessages=1 full past ping+pong timeout with pongs enabled neither drops the stream nor delivers buffered messages early.", Run: runPULL03b},
	}
}

func interactiveCmd(env *harness.ScenarioEnv, ep, sub string) *proxypb.StartSubscriberCommand {
	return &proxypb.StartSubscriberCommand{Endpoint: ep, Subscription: sub, ProjectId: env.ProjectID, AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE}
}
func subMatch(sub string) func(int, *pubsubpb.StreamingPullRequest, metadata.MD) bool {
	return func(_ int, req *pubsubpb.StreamingPullRequest, _ metadata.MD) bool {
		return req.GetSubscription() == sub
	}
}
func posInt64(v, fallback int64) int64 { return cmp.Or(max(v, 0), fallback) }
func runPULL02(ctx context.Context, env *harness.ScenarioEnv) error {
	subA := env.SubscriptionPath(env.UniqueName("pull02-rec-sub"))
	matchA := subMatch(subA)
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "PULL-02-prerecv", Times: 1, FailBeforeRecvErr: status.Error(codes.Unavailable, "pre-recv-unavailable")})
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "PULL-02-init-internal", Times: 1, Match: matchA, InitialError: status.Error(codes.Internal, "init-internal")})
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "PULL-02-mid-abort", Times: 1, Match: matchA, OnOpen: func(h *fake.StreamHandle) { h.Abort(status.Error(codes.Aborted, "mid-abort")) }})

	pA, sh4, err := startSubAndStream(ctx, env, interactiveCmd(env, env.Endpoint(), subA), 2, "PULL-02 stream 4")
	if err != nil {
		return err
	}
	defer pA.close(ctx, env)
	if err := pushRecvAckOne(ctx, env, pA, sh4, "ack-pull02-1", "msg-pull02-1", []byte("pull02-recovered"), 15*time.Millisecond, "PULL-02 ack1"); err != nil {
		return err
	}
	snapA, vA, err := stopAndVerifyStream(ctx, env, pA, 1, "PULL-02", subA, 4, 0, "ack-pull02-1")
	if err == nil {
		err = vA.Err()
	}
	if err != nil {
		return err
	}
	subB := env.SubscriptionPath(env.UniqueName("pull02-perm-sub"))
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "PULL-02-perm-denied", Times: fake.RepeatAlways, Match: subMatch(subB), InitialError: status.Error(codes.PermissionDenied, "permission denied")})
	return runSubFatalPhase(ctx, env, subB, len(snapA.Streams), lastCapturedSeq(snapA), codes.PermissionDenied, "PULL-02 Phase B")
}
func runPULL02b(ctx context.Context, env *harness.ScenarioEnv) error {
	fwd, err := newTCPDropForwarder(env.Fake.Addr())
	if err != nil {
		return err
	}
	defer fwd.Close()
	sub := env.SubscriptionPath(env.UniqueName("pull02b-sub"))
	p, sh1, err := startSubAndStream(ctx, env, interactiveCmd(env, fwd.lis.Addr().String(), sub), 1, "PULL-02b stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	if err := pushRecvAckOne(ctx, env, p, sh1, "ack-pull02b-1", "msg-pull02b-1", []byte("pull02b-m1"), 15*time.Millisecond, "PULL-02b ack1"); err != nil {
		return err
	}
	fwd.KillActive()
	sh2, err := waitReadyStreamOrStop(ctx, env, p, 2, "PULL-02b stream 2")
	if err != nil {
		return err
	}
	if err := pushRecvAckOne(ctx, env, p, sh2, "ack-pull02b-2", "msg-pull02b-2", []byte("pull02b-m2"), 0, "PULL-02b ack2"); err != nil {
		return err
	}
	return stopVerifyAndFinishStream(ctx, env, p, 2, "PULL-02b", sub, 2, 0, "ack-pull02b-1", "ack-pull02b-2")
}
func runPULL02c(ctx context.Context, env *harness.ScenarioEnv) error {
	return runPULL02Branch(ctx, env, "pull02c-sub", fake.StreamingPullScript{Name: "PULL-02c-resexhausted", Times: 1, InitialResponses: []*pubsubpb.StreamingPullResponse{{}}, AfterInitialError: status.Error(codes.ResourceExhausted, "quota")}, strings.EqualFold(env.Language(), "go"), codes.ResourceExhausted, 2, "PULL-02c")
}
func runPULL02d(ctx context.Context, env *harness.ScenarioEnv) error {
	lang := strings.ToLower(env.Language())
	return runPULL02Branch(ctx, env, "pull02d-sub", fake.StreamingPullScript{Name: "PULL-02d-shutdownnow", Times: 1, InitialError: status.Error(codes.Unavailable, "Server shutdownNow invoked")}, lang == "go" || lang == "java", codes.Unavailable, 1, "PULL-02d")
}
func runPULL02Branch(ctx context.Context, env *harness.ScenarioEnv, subPrefix string, script fake.StreamingPullScript, wantFatal bool, wantCode codes.Code, readyIdx int, label string) error {
	sub := env.SubscriptionPath(env.UniqueName(subPrefix))
	env.Fake.AddStreamingPullScript(script)
	if wantFatal {
		return runSubFatalPhase(ctx, env, sub, 0, 0, wantCode, label)
	}
	p, _, err := startSubPump(ctx, env, interactiveCmd(env, env.Endpoint(), sub))
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	sh2, err := waitReadyStreamOrStop(ctx, env, p, readyIdx, label+" reconnect")
	if err != nil {
		return err
	}
	ackID := "ack-" + subPrefix
	if err := pushRecvAckOne(ctx, env, p, sh2, ackID, "msg-"+subPrefix, []byte(subPrefix+"-ok"), 0, label+" ack"); err != nil {
		return err
	}
	return stopVerifyAndFinishStream(ctx, env, p, 1, label, sub, 2, 0, ackID)
}
func waitStreamPing(ctx context.Context, env *harness.ScenarioEnv, p *subPump, sid, op string) error {
	if err := waitFakeOrStop(ctx, env, p, func(s fake.Snapshot) bool {
		return slices.ContainsFunc(s.Streams, func(st fake.StreamSummary) bool {
			return st.StreamID == sid && (st.KeepalivePingCount >= 1 || st.Closed)
		})
	}, op); err != nil {
		return err
	}
	if slices.ContainsFunc(env.Fake.Snapshot().Streams, func(st fake.StreamSummary) bool {
		return st.StreamID == sid && st.Closed && st.KeepalivePingCount == 0
	}) {
		return env.Failf("%s: stream %s closed before sending keepalive ping", op, sid)
	}
	return nil
}
func holdStreamStable(ctx context.Context, env *harness.ScenarioEnv, p *subPump, holdDur time.Duration, wantStreams int, op, extraMsg string) error {
	brokeCh, stopWatch := spawnFakeWatch(ctx, env.Fake, func(s fake.Snapshot) bool {
		return len(s.Streams) > wantStreams || (len(s.Streams) == wantStreams && s.Streams[wantStreams-1].Closed)
	})
	t := time.NewTimer(holdDur)
	defer func() { t.Stop(); stopWatch() }()
	select {
	case wErr := <-brokeCh:
		if wErr == nil {
			wErr = env.Failf("%s: expected exactly %d streams (closed or restarted unexpectedly)", op, wantStreams)
		}
		return wErr
	case item, ok := <-p.evs:
		if _, derr := p.decodeEv(env, item, ok, op); derr != nil {
			return derr
		}
		return env.Failf("%s: %s", op, extraMsg)
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func runPULL03(ctx context.Context, env *harness.ScenarioEnv) error {
	sub := env.SubscriptionPath(env.UniqueName("pull03-sub"))
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "PULL-03-suppress-pong", Times: 1, SuppressPong: proto.Bool(true), Match: subMatch(sub)})
	p, sh1, err := startSubAndStream(ctx, env, interactiveCmd(env, env.Endpoint(), sub), 1, "PULL-03 stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	if err := waitStreamPing(ctx, env, p, sh1.ID(), "PULL-03 stream 1 ping"); err != nil {
		return err
	}
	sh2, err := waitReadyStreamOrStop(ctx, env, p, 2, "PULL-03 stream 2")
	if err != nil {
		return err
	}
	if err := waitStreamPing(ctx, env, p, sh2.ID(), "PULL-03 stream 2 ping"); err != nil {
		return err
	}
	pongDur := time.Duration(posInt64(env.Defaults().GetKeepalivePongTimeoutMs(), 15_000)) * time.Millisecond
	if err := holdStreamStable(ctx, env, p, pongDur+min(pongDur/2, 10*time.Second), 2, "PULL-03 stream 2 stable", "unexpected MessageDeliveredEvent"); err != nil {
		return err
	}
	if err := pushRecvAckOne(ctx, env, p, sh2, "ack-pull03-2", "msg-pull03-2", []byte("pull03-m2"), 0, "PULL-03 ack2"); err != nil {
		return err
	}
	snap, v, err := stopAndVerifyStream(ctx, env, p, 1, "PULL-03", sub, 0, 2, "ack-pull03-2")
	if err != nil {
		return err
	}
	if len(snap.Streams) >= 2 {
		v.Require(snap.Streams[0].KeepalivePingCount >= 1 && snap.Streams[1].KeepalivePingCount >= 1,
			"PULL-03 expected >=1 ping on both streams, got (%d,%d)", snap.Streams[0].KeepalivePingCount, snap.Streams[1].KeepalivePingCount)
	}
	v.Check(fake.AssertKeepalivePings(snap, 2))
	kaChk := fake.ExpectStreamingPullKeepalive("PULL-03-keepalive")
	for _, c := range snap.StreamingPullCalls() {
		if c.IsKeepalivePing() {
			v.Check(kaChk.Check(c))
		}
	}
	return finishPublishScenario(ctx, env, v)
}
func runPULL03b(ctx context.Context, env *harness.ScenarioEnv) error {
	sub := env.SubscriptionPath(env.UniqueName("pull03b-sub"))
	cmd := interactiveCmd(env, env.Endpoint(), sub)
	cmd.MaxOutstandingMessages, cmd.MaxOutstandingBytes = proto.Int64(1), proto.Int64(1<<20)
	p, sh1, err := startSubAndStream(ctx, env, cmd, 1, "PULL-03b stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	acks, bodies, err := pushSimpleBatch(sh1, "ack-pull03b", "msg-pull03b", 1, 3, 0)
	if err != nil {
		return err
	}
	pingMs, pongMs := posInt64(env.Defaults().GetKeepalivePingIntervalMs(), 30_000), posInt64(env.Defaults().GetKeepalivePongTimeoutMs(), 15_000)
	holdDur := time.Duration(pingMs+pongMs+min(pongMs/2, 5_000))*time.Millisecond + map[bool]time.Duration{true: 6 * time.Second}[pongMs >= 10_000]
	for i := range 3 {
		md, rerr := p.recvDelivery(ctx, env)
		if rerr != nil {
			return rerr
		}
		if err := assertDeliveredMsg(md, fmt.Sprintf("msg-pull03b-%04d", i+1), bodies[i], nil, ""); err != nil {
			return err
		}
		if i == 0 {
			if err := checkNoPrematureAck(ctx, env.Fake, md, 0); err != nil {
				return err
			}
			if err := holdStreamStable(ctx, env, p, holdDur, 1, "PULL-03b hold", "delivered extra message while m1 held under MaxOutstandingMessages=1"); err != nil {
				return err
			}
			if pre := env.Fake.Snapshot(); len(pre.Streams) != 1 || pre.Streams[0].Closed {
				return env.Failf("PULL-03b: expected single open stream after hold, got %d", len(pre.Streams))
			}
		}
		if err := sendSettleAction(env, p, md.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK); err != nil {
			return err
		}
	}
	if err := waitFakeOrStop(ctx, env, p, func(s fake.Snapshot) bool {
		return snapHasCompletedAckFor(s, acks[0]) && snapHasCompletedAckFor(s, acks[1]) && snapHasCompletedAckFor(s, acks[2])
	}, "PULL-03b all acks"); err != nil {
		return err
	}
	return stopVerifyAndFinishStream(ctx, env, p, 3, "PULL-03b", sub, 0, 1, acks...)
}
