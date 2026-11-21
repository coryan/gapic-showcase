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
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/protobuf/proto"
)

// LIFE-03 covers 8/9 streaming SDKs (PHP excluded via SubscriberSession UNIMPLEMENTED).
func lifecycleScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "LIFE-01", Name: "Graceful Shutdown WAIT_FOR_PROCESSING (Unbounded, Finite Timeout, Zero Bail)", Category: "lifecycle", Feature: "LIFE-01", RequiredCapabilities: []harness.Capability{harness.CapShutdownBehavior}, Description: "Verify WAIT_FOR_PROCESSING blocks until in-flight message Ack when ShutdownTimeoutMs is nil, exits cleanly when finite ShutdownTimeoutMs expires, and exits immediately when ShutdownTimeoutMs=0.", Run: runLIFE01},
		{ID: "LIFE-02", Name: "Fast Shutdown NACK_IMMEDIATELY & Lease Extension Halt", Category: "lifecycle", Feature: "LIFE-02", RequiredCapabilities: []harness.Capability{harness.CapShutdownBehavior}, Description: "Verify NACK_IMMEDIATELY (with non-zero ShutdownTimeoutMs when CapShutdownTimeout is set) stops extensions, NACKs active and queued unsettled messages, and emits StoppedEvent cleanly.", Run: runLIFE02},
		{ID: "LIFE-03", Name: "Pre-First-Response Cancel & Idempotent StopSubscriber", Category: "lifecycle", Feature: "LIFE-03", Description: "Verify 8/9 streaming SDKs cleanly tear down an idle pre-first-response StreamingPull stream across two consecutive StopSubscriberCommands with zero deliveries or spurious Ack/Nack RPCs. Note: Go also enforces single-active Receive via errReceiveInProgress.", Run: runLIFE03},
	}
}

func runLIFE01(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	if err := m3dWithSession(ctx, env, "life01-a-sub", func(c *proxypb.StartSubscriberCommand) {
		c.ShutdownBehavior = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING
	}, "LIFE-01 Subcase A", func(sA *m3dSession) error {
		mdA, err := sA.pushRecv(ctx, env, "ack-life01-a", "msg-life01-a", []byte("life01-a"))
		if err != nil {
			return err
		}
		if err := m3dCheckNoPrematureAckAfter(env, sA.wm, mdA); err != nil {
			return err
		}
		if err := sA.sendStop(ctx, env, &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING}, false); err != nil {
			return err
		}
		if _, _, st, timedOut, err := sA.nextEv(ctx, env, map[bool]time.Duration{true: 75 * time.Millisecond, false: 150 * time.Millisecond}[m3dBoundedShutdownTimeoutMs(env) < 1000], "LIFE-01 hold"); err != nil {
			return err
		} else if !timedOut || st != nil {
			return env.Failf("LIFE-01-A: subscriber stopped prematurely while message unsettled")
		}
		if err := sA.ack(ctx, env, mdA.GetDeliveryToken()); err != nil {
			return err
		}
		if _, err := sA.awaitStoppedWith(ctx, env, true, 1); err != nil {
			return err
		}
		if !snapHasCompletedAckFor(env.Fake.Snapshot(), "ack-life01-a") {
			return env.Failf("LIFE-01 Subcase A: StoppedEvent arrived before Acknowledge(%q) completed", "ack-life01-a")
		}
		sA.close(ctx, env)
		sA.finish(env, v, env.Fake.Snapshot(), "LIFE-01-A", true, true, []string{"ack-life01-a"}, nil)
		return nil
	}); err != nil {
		return err
	}
	if env.HasCapability(harness.CapShutdownTimeout) {
		for _, sc := range []struct {
			label, pfx, aid, mid string
			tmMs                 int64
		}{
			{"LIFE-01-B", "life01-b-sub", "ack-life01-b", "msg-life01-b", m3dBoundedShutdownTimeoutMs(env)},
			{"LIFE-01-C", "life01-c-sub", "ack-life01-c", "msg-life01-c", 0},
		} {
			if err := m3dWithSession(ctx, env, sc.pfx, func(c *proxypb.StartSubscriberCommand) {
				c.ShutdownBehavior, c.ShutdownTimeoutMs = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING, proto.Int64(sc.tmMs)
			}, sc.label, func(s *m3dSession) error {
				if _, err := s.pushRecv(ctx, env, sc.aid, sc.mid, []byte(sc.mid)); err != nil {
					return err
				}
				if _, err := s.stopWith(ctx, env, &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING, ShutdownTimeoutMs: proto.Int64(sc.tmMs)}, false, false, 1); err != nil {
					return err
				}
				s.close(ctx, env)
				s.finish(env, v, env.Fake.Snapshot(), sc.label, true, false, nil, []string{sc.aid})
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return m3dFinishScenario(ctx, env, v, env.Fake.Snapshot())
}

func runLIFE02(ctx context.Context, env *harness.ScenarioEnv) error {
	return m3dWithSession(ctx, env, "life02-sub", func(c *proxypb.StartSubscriberCommand) {
		c.ShutdownBehavior, c.MaxOutstandingMessages = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, proto.Int64(1)
		if env.HasCapability(harness.CapShutdownTimeout) {
			c.ShutdownTimeoutMs = proto.Int64(10_000)
		}
	}, "LIFE-02", func(s *m3dSession) error {
		mds, err := s.pushAndRecvCapped(ctx, env, []*pubsubpb.ReceivedMessage{m3dPushRec("ack-life02-1", "msg-life02-1", []byte("m1"), ""), m3dPushRec("ack-life02-2", "msg-life02-2", []byte("m2"), "")}, 1, 0, "LIFE-02")
		if err != nil {
			return err
		}
		if err := m3dCheckNoPrematureAckAfter(env, s.wm, mds[0]); err != nil {
			return err
		}
		stopCmd := &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, ShutdownTimeoutMs: map[bool]*int64{true: proto.Int64(10_000)}[env.HasCapability(harness.CapShutdownTimeout)]}
		if _, err := s.stopWith(ctx, env, stopCmd, false, false, 1); err != nil {
			return err
		}
		wmPostStop := lastCapturedSeq(env.Fake.Snapshot())
		s.close(ctx, env)
		if err := sleepCtx(ctx, m3dQuietWindow(env)); err != nil {
			return err
		}
		snap, v := env.Fake.Snapshot(), env.NewVerifier()
		for _, c := range modAckCallsAfterSeq(snap, wmPostStop) {
			if c.ModAckReq.GetAckDeadlineSeconds() > 0 {
				v.Checkf("LIFE-02: unexpected post-stop lease extension ModifyAckDeadline(%d) seq=%d", c.ModAckReq.GetAckDeadlineSeconds(), c.Seq)
			}
		}
		s.finish(env, v, snap, "LIFE-02", true, true, nil, []string{"ack-life02-1", "ack-life02-2"})
		return m3dFinishScenario(ctx, env, v, snap)
	})
}

func runLIFE03(ctx context.Context, env *harness.ScenarioEnv) error {
	sub := env.SubscriptionPath(env.UniqueName("life03-sub"))
	env.Fake.AddStreamingPullScript(fake.StreamingPullScript{Name: "LIFE-03-silent-pre-resp", Times: 1, Match: subMatch(sub)})
	return m3dWithSession(ctx, env, sub, nil, "LIFE-03 stream", func(s *m3dSession) error {
		if got := len(env.Fake.ActiveStreams()); got != 1 {
			return env.Failf("LIFE-03: expected 1 active stream before first response, got %d", got)
		}
		if _, err := s.stopWith(ctx, env, nil, true, true, 0); err != nil {
			return err
		}
		s.close(ctx, env)
		snap, v := env.Fake.Snapshot(), env.NewVerifier()
		v.Require(len(snap.AckCalls()) == 0 && len(snap.ModAckCalls()) == 0,
			"LIFE-03: expected 0 Ack/ModAck RPCs, got ack=%d modack=%d", len(snap.AckCalls()), len(snap.ModAckCalls()))
		s.finish(env, v, snap, "LIFE-03", true, true, nil, nil)
		return m3dFinishScenario(ctx, env, v, snap)
	})
}
