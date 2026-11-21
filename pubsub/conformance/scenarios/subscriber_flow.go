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
	"bytes"
	"context"
	"maps"
	"slices"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/protobuf/proto"
)

func subscriberFlowScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "SUBF-01", Name: "Subscriber Count & Byte Flow Control + Un-dispatched Cancel Nack", Category: "flow_control", Feature: "SUBF-01", Description: "Verify Frame-0 MaxOutstandingMessages/MaxOutstandingBytes, client count/byte admission bounds (tolerating Node !wasFull 2nd byte delivery), and un-dispatched queued Nack on cancel. Note: C++ gates locally by MaxConcurrencyOption, Rust uses server-side Frame-0 FC, and Ruby pauses between response batches.", Run: runSUBF01},
		{ID: "SUBF-01b", Name: "Subscriber Shared vs Per-Stream Flow Control", Category: "flow_control", Feature: "SUBF-01", RequiredCapabilities: []harness.Capability{harness.CapPerStreamFlowControl}, Description: "Verify EnablePerStreamFlowControl=false shares MaxOutstandingMessages=1 across NumStreams=2 while true gives each stream an independent permit.", Run: runSUBF01b},
		{ID: "SUBF-02", Name: "Subscriber Per-Key Ordered Delivery & Nack Advancement", Category: "ordering", Feature: "SUBF-02", RequiredCapabilities: []harness.Capability{harness.CapSubscriberOrdering}, FakeServerOptions: []fake.Option{fake.WithDefaultSubscriptionProperties(false, true)}, Description: "Verify sequential per-key delivery, concurrent delivery across distinct/empty ordering keys, and key advancement on Ack and Nack.", Run: runSUBF02},
		{ID: "SUBF-02b", Name: "Subscriber Concurrent Same-Key Delivery When Ordering Disabled", Category: "ordering", Feature: "SUBF-02", RequiredCapabilities: []harness.Capability{harness.CapSubscriberOrdering}, FakeServerOptions: []fake.Option{fake.WithDefaultSubscriptionProperties(false, false)}, Description: "Verify concurrent same-key delivery when SubscriptionProperties.message_ordering_enabled=false (Go/Java/C# check dynamically; C++/Python/Ruby ignore the dynamic flag).", Run: runSUBF02b},
		{ID: "SUBF-03", Name: "Subscriber Duplicate ack_id Safe Delivery & Settlement", Category: "flow_control", Feature: "SUBF-03", Description: "Verify duplicate ack_id batches do not panic or deadlock, tolerating 1 delivery (Java pending dedup) or 2 deliveries (peer SDKs), in both unordered and ordered modes.", Run: runSUBF03},
	}
}

func m3dTriplet(a1, m1 string, d1 []byte, a2, m2 string, d2 []byte, a3, m3 string, d3 []byte, key string) []*pubsubpb.ReceivedMessage {
	return []*pubsubpb.ReceivedMessage{m3dPushRec(a1, m1, d1, key), m3dPushRec(a2, m2, d2, key), m3dPushRec(a3, m3, d3, key)}
}

func runSUBF01(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	if err := m3dWithSession(ctx, env, "subf01-count-sub", func(c *proxypb.StartSubscriberCommand) {
		c.MaxOutstandingMessages, c.MaxOutstandingBytes = proto.Int64(2), proto.Int64(100_000)
	}, "SUBF-01 Phase A", func(sA *m3dSession) error {
		mdsA, err := sA.pushAndRecvCapped(ctx, env, m3dTriplet("ack-subf01-a1", "msg-subf01-a1", []byte("a1"), "ack-subf01-a2", "msg-subf01-a2", []byte("a2"), "ack-subf01-a3", "msg-subf01-a3", []byte("a3"), ""), 2, 2, "SUBF-01 Phase A")
		if err != nil {
			return err
		}
		if err := m3dCheckNoPrematureAckAfter(env, sA.wm, mdsA[0]); err != nil {
			return err
		}
		if err := sA.requireNoExtra(ctx, env, "SUBF-01 Phase A: delivered 3rd message while 2 held"); err != nil {
			return err
		}
		if err := sA.ack(ctx, env, mdsA[0].GetDeliveryToken()); err != nil {
			return err
		}
		mdsA3, err := sA.recvCapped(ctx, env, 1, 2, "SUBF-01 Phase A")
		if err != nil {
			return err
		}
		if seen := map[string]bool{mdsA[0].GetMessageId(): true, mdsA[1].GetMessageId(): true, mdsA3[0].GetMessageId(): true}; len(seen) != 3 {
			return env.Failf("SUBF-01 Phase A: expected 3 distinct messages, got %v", seen)
		}
		if err := sA.ackWait(ctx, env, "SUBF-01 Phase A acks", []string{mdsA[1].GetDeliveryToken(), mdsA3[0].GetDeliveryToken()}, []string{"ack-subf01-a1", "ack-subf01-a2", "ack-subf01-a3"}); err != nil {
			return err
		}
		mdsQ, err := sA.pushAndRecvCapped(ctx, env, m3dTriplet("ack-subf01-q1", "msg-subf01-q1", []byte("q1"), "ack-subf01-q2", "msg-subf01-q2", []byte("q2"), "ack-subf01-q3", "msg-subf01-q3", []byte("q3"), ""), 2, 2, "SUBF-01 cancel batch")
		if err != nil {
			return err
		}
		if err := sA.requireNoExtra(ctx, env, "SUBF-01 cancel batch: delivered 3rd queued message while cap=2 held"); err != nil {
			return err
		}
		aidByMsg := map[string]string{"msg-subf01-q1": "ack-subf01-q1", "msg-subf01-q2": "ack-subf01-q2", "msg-subf01-q3": "ack-subf01-q3"}
		undispatchedQ := slices.DeleteFunc([]string{"ack-subf01-q1", "ack-subf01-q2", "ack-subf01-q3"}, func(id string) bool {
			return id == aidByMsg[mdsQ[0].GetMessageId()] || id == aidByMsg[mdsQ[1].GetMessageId()]
		})
		stopCmdA, wantAckA, wantNackQ, strictNack := &proxypb.StopSubscriberCommand{}, []string{"ack-subf01-a1", "ack-subf01-a2", "ack-subf01-a3"}, []string{"ack-subf01-q1", "ack-subf01-q2", "ack-subf01-q3"}, true
		if env.HasCapability(harness.CapShutdownBehavior) {
			stopCmdA.ShutdownBehavior = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY
			if env.HasCapability(harness.CapShutdownTimeout) {
				stopCmdA.ShutdownTimeoutMs = proto.Int64(10_000)
			}
			if _, err := sA.stopWith(ctx, env, stopCmdA, false, true, 5); err != nil {
				return err
			}
		} else {
			if err := sA.sendStop(ctx, env, stopCmdA, false); err != nil {
				return err
			}
			for done := false; !done; {
				md, sr, st, timedOut, err := sA.nextEv(ctx, env, m3dQuietWindow(env), "SUBF-01 cancel wait")
				switch {
				case err != nil:
					return err
				case md != nil:
					return env.Failf("SUBF-01 cancel batch: unexpected delivery %q during stop", md.GetMessageId())
				case sr != nil:
					return env.Failf("SUBF-01 cancel batch: unexpected SettlementResultEvent %q during stop", sr.GetDeliveryToken())
				case st != nil:
					if err := checkInBandErr(st.GetErrorCode(), st.GetErrorMessage(), "StoppedEvent"); err != nil {
						return err
					}
					if st.GetTotalDelivered() != 5 {
						return env.Failf("StoppedEvent total_delivered mismatch: want 5, got %d", st.GetTotalDelivered())
					}
					strictNack, done = false, true
				case timedOut:
					wantAckA, wantNackQ = append(wantAckA, aidByMsg[mdsQ[0].GetMessageId()], aidByMsg[mdsQ[1].GetMessageId()]), undispatchedQ
					if err := sA.ack(ctx, env, mdsQ[0].GetDeliveryToken(), mdsQ[1].GetDeliveryToken()); err != nil {
						return err
					}
					if _, err := sA.awaitStoppedWith(ctx, env, true, 5); err != nil {
						return err
					}
					done = true
				}
			}
		}
		sA.close(ctx, env)
		snapA := env.Fake.Snapshot()
		sA.finish(env, v, snapA, "SUBF-01-A", false, strictNack, wantAckA, wantNackQ)
		if !strictNack {
			for _, id := range undispatchedQ {
				v.Require(m3dModAckCount(snapA, sA.wm, id, 0, 0) >= 1, "SUBF-01-A: un-dispatched queued %q not nacked on close", id)
			}
		}
		m3dCheckFrame0FC(env, v, "SUBF-01-A", sA.sub, callsAfterSeq(snapA, sA.wm), 2, 100_000)
		return nil
	}); err != nil {
		return err
	}
	return m3dWithSession(ctx, env, "subf01-byte-sub", func(c *proxypb.StartSubscriberCommand) {
		c.MaxOutstandingMessages, c.MaxOutstandingBytes = proto.Int64(10), proto.Int64(150)
	}, "SUBF-01 Phase B", func(sB *m3dSession) error {
		body80 := bytes.Repeat([]byte{'b'}, 80)
		heldB, err := sB.pushAndRecvCapped(ctx, env, m3dTriplet("ack-subf01-b1", "msg-subf01-b1", body80, "ack-subf01-b2", "msg-subf01-b2", body80, "ack-subf01-b3", "msg-subf01-b3", body80, ""), 1, 2, "SUBF-01 Phase B")
		if err != nil {
			return err
		}
		if mdB2, got2, err := sB.tryRecvDelivery(ctx, env, m3dQuietWindow(env)); err != nil {
			return err
		} else if got2 {
			heldB = append(heldB, mdB2)
			if err := sB.requireNoExtra(ctx, env, "SUBF-01 Phase B: delivered 3rd 80B message while max_outstanding_bytes=150 held"); err != nil {
				return err
			}
		}
		for _, md := range heldB {
			if err := sB.ack(ctx, env, md.GetDeliveryToken()); err != nil {
				return err
			}
		}
		for range 3 - len(heldB) {
			rem, err := sB.recvCapped(ctx, env, 1, 2, "SUBF-01 Phase B")
			if err != nil {
				return err
			}
			if err := sB.ack(ctx, env, rem[0].GetDeliveryToken()); err != nil {
				return err
			}
		}
		snap, err := sB.stopCheck(ctx, env, v, "SUBF-01-B", nil, true, 3, false, []string{"ack-subf01-b1", "ack-subf01-b2", "ack-subf01-b3"}, nil)
		if err != nil {
			return err
		}
		m3dCheckFrame0FC(env, v, "SUBF-01-B", sB.sub, callsAfterSeq(snap, sB.wm), 10, 150)
		return m3dFinishScenario(ctx, env, v, snap)
	})
}

func runSUBF01b(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	runSub := func(perStream bool, label, pfx, a0, m0, a1, m1 string) error {
		return m3dWithSession(ctx, env, pfx, func(c *proxypb.StartSubscriberCommand) {
			c.NumStreams, c.MaxOutstandingMessages, c.EnablePerStreamFlowControl = proto.Int32(2), proto.Int64(1), proto.Bool(perStream)
		}, label, func(s *m3dSession) error {
			sh2, err := waitReadyStreamOrStop(ctx, env, s.p, s.base+2, label)
			if err != nil {
				return err
			}
			for i, pair := range [2][2]string{{a0, m0}, {a1, m1}} {
				if err := pushOneMsg(map[bool]*fake.StreamHandle{true: sh2, false: s.sh}[i == 1], pair[0], pair[1], []byte(pair[1])); err != nil {
					return err
				}
			}
			var toks []string
			if !perStream {
				md1, err := s.recvDelivery(ctx, env)
				if err != nil {
					return err
				}
				if md1.GetActiveCallbacks() != 1 {
					return env.Failf("%s: expected active_callbacks=1, got %d", label, md1.GetActiveCallbacks())
				}
				if err := s.requireNoExtra(ctx, env, label+": delivered concurrently when shared cap=1"); err != nil {
					return err
				}
				if err := s.ack(ctx, env, md1.GetDeliveryToken()); err != nil {
					return err
				}
				md2, err := s.recvDelivery(ctx, env)
				if err != nil {
					return err
				}
				toks = []string{md2.GetDeliveryToken()}
			} else if mds, err := s.recvCapped(ctx, env, 2, 2, label); err != nil {
				return err
			} else if max(mds[0].GetActiveCallbacks(), mds[1].GetActiveCallbacks()) < 2 {
				return env.Failf("%s: expected concurrent active_callbacks=2, got (%d, %d)", label, mds[0].GetActiveCallbacks(), mds[1].GetActiveCallbacks())
			} else {
				toks = []string{mds[0].GetDeliveryToken(), mds[1].GetDeliveryToken()}
			}
			snap, err := s.stopCheck(ctx, env, v, label, toks, true, 2, false, []string{a0, a1}, nil)
			if err == nil {
				m3dCheckFrame0FC(env, v, label, s.sub, callsAfterSeq(snap, s.wm), 1, 0)
			}
			return err
		})
	}
	if err := runSub(false, "SUBF-01b-shared", "subf01b-shared-sub", "ack-subf01b-s0", "msg-subf01b-s0", "ack-subf01b-s1", "msg-subf01b-s1"); err != nil {
		return err
	}
	if err := runSub(true, "SUBF-01b-per", "subf01b-per-sub", "ack-subf01b-p0", "msg-subf01b-p0", "ack-subf01b-p1", "msg-subf01b-p1"); err != nil {
		return err
	}
	return m3dFinishScenario(ctx, env, v, env.Fake.Snapshot())
}

func runSUBF02(ctx context.Context, env *harness.ScenarioEnv) error {
	return m3dWithSession(ctx, env, "subf02-sub", func(c *proxypb.StartSubscriberCommand) { c.MaxConcurrency = proto.Int32(4) }, "SUBF-02", func(s *m3dSession) error {
		mds3, err := s.pushAndRecvCapped(ctx, env, []*pubsubpb.ReceivedMessage{
			m3dPushRec("ack-subf02-ka1", "msg-subf02-ka1", []byte("ka1"), "kA"), m3dPushRec("ack-subf02-ka2", "msg-subf02-ka2", []byte("ka2"), "kA"),
			m3dPushRec("ack-subf02-kb1", "msg-subf02-kb1", []byte("kb1"), "kB"), m3dPushRec("ack-subf02-u1", "msg-subf02-u1", []byte("u1"), ""),
		}, 3, 4, "SUBF-02", fake.WithSubscriptionProperties(false, true))
		if err != nil {
			return err
		}
		got3 := map[string]*proxypb.MessageDeliveredEvent{}
		for _, md := range mds3 {
			if md.GetMessageId() == "msg-subf02-ka2" {
				return env.Failf("SUBF-02: delivered msg-subf02-ka2 out of order before msg-subf02-ka1 settled")
			}
			got3[md.GetMessageId()] = md
		}
		for id, want := range map[string][2]string{"msg-subf02-ka1": {"ka1", "kA"}, "msg-subf02-kb1": {"kb1", "kB"}, "msg-subf02-u1": {"u1", ""}} {
			if md := got3[id]; md == nil {
				return env.Failf("SUBF-02: expected %s concurrently before ka1 settled, got %v", id, slices.Collect(maps.Keys(got3)))
			} else if err := assertDeliveredMsg(md, id, []byte(want[0]), nil, want[1]); err != nil {
				return err
			}
		}
		if err := s.ack(ctx, env, got3["msg-subf02-kb1"].GetDeliveryToken(), got3["msg-subf02-u1"].GetDeliveryToken()); err != nil {
			return err
		}
		if err := s.requireNoExtra(ctx, env, "SUBF-02: delivered out of order before msg-subf02-ka1 settled"); err != nil {
			return err
		}
		if err := s.ack(ctx, env, got3["msg-subf02-ka1"].GetDeliveryToken()); err != nil {
			return err
		}
		recvKA := func(mid, body string, nack bool) error {
			md, err := s.recvDelivery(ctx, env)
			if err != nil {
				return err
			}
			if err := assertDeliveredMsg(md, mid, []byte(body), nil, "kA"); err != nil {
				return err
			}
			if nack {
				if err := s.requireNoExtra(ctx, env, "SUBF-02: delivered before "+mid+" nacked"); err != nil {
					return err
				}
				return s.settle(ctx, env, md.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_NACK)
			}
			return s.ack(ctx, env, md.GetDeliveryToken())
		}
		if err := recvKA("msg-subf02-ka2", "ka2", false); err != nil {
			return err
		}
		if err := s.sh.PushMessages([]*pubsubpb.ReceivedMessage{
			m3dPushRec("ack-subf02-ka3", "msg-subf02-ka3", []byte("ka3"), "kA"), m3dPushRec("ack-subf02-ka4", "msg-subf02-ka4", []byte("ka4"), "kA"),
		}, fake.WithSubscriptionProperties(false, true)); err != nil {
			return err
		}
		if err := recvKA("msg-subf02-ka3", "ka3", true); err != nil {
			return err
		}
		if err := recvKA("msg-subf02-ka4", "ka4", false); err != nil {
			return err
		}
		if err := waitFakeOrStop(ctx, env, s.p, func(sn fake.Snapshot) bool {
			return snapHasCompletedAckFor(sn, "ack-subf02-ka2") && snapHasCompletedAckFor(sn, "ack-subf02-ka4") && m3dModAckCount(sn, 0, "ack-subf02-ka3", 0, 0) >= 1
		}, "SUBF-02 settlements"); err != nil {
			return err
		}
		return s.stopFinish(ctx, env, env.NewVerifier(), "SUBF-02", nil, true, 6, true, []string{"ack-subf02-ka1", "ack-subf02-ka2", "ack-subf02-kb1", "ack-subf02-u1", "ack-subf02-ka4"}, []string{"ack-subf02-ka3"})
	})
}

func runSUBF02b(ctx context.Context, env *harness.ScenarioEnv) error {
	return m3dWithSession(ctx, env, "subf02b-sub", func(c *proxypb.StartSubscriberCommand) { c.MaxConcurrency = proto.Int32(4) }, "SUBF-02b", func(s *m3dSession) error {
		mds, err := s.pushAndRecvCapped(ctx, env, []*pubsubpb.ReceivedMessage{
			m3dPushRec("ack-subf02b-1", "msg-subf02b-1", []byte("b1"), "kA"), m3dPushRec("ack-subf02b-2", "msg-subf02b-2", []byte("b2"), "kA"),
		}, 2, 4, "SUBF-02b", fake.WithSubscriptionProperties(false, false))
		if err != nil {
			return err
		}
		if max(mds[0].GetActiveCallbacks(), mds[1].GetActiveCallbacks()) < 2 {
			return env.Failf("SUBF-02b: expected concurrent same-key active_callbacks=2, got (%d, %d)", mds[0].GetActiveCallbacks(), mds[1].GetActiveCallbacks())
		}
		return s.stopFinish(ctx, env, env.NewVerifier(), "SUBF-02b", []string{mds[0].GetDeliveryToken(), mds[1].GetDeliveryToken()}, true, 2, true, []string{"ack-subf02b-1", "ack-subf02b-2"}, nil)
	})
}

func runSUBF03DupBatch(ctx context.Context, env *harness.ScenarioEnv, label, pfx, dupAck, dupMsg, afterAck, afterMsg, key string, v *fake.Verifier) error {
	return m3dWithSession(ctx, env, pfx, func(c *proxypb.StartSubscriberCommand) {
		if key != "" {
			c.MaxConcurrency = proto.Int32(4)
		}
	}, label, func(s *m3dSession) error {
		opts := map[bool][]fake.PushOption{true: {fake.WithSubscriptionProperties(false, true)}}[key != ""]
		if err := s.sh.PushMessages(m3dTriplet(dupAck, dupMsg, []byte(dupMsg), dupAck, dupMsg, []byte(dupMsg), afterAck, afterMsg, []byte(afterMsg), key), opts...); err != nil {
			return err
		}
		dupCount, sawAfter := 0, false
		handleMD := func(md *proxypb.MessageDeliveredEvent) error {
			switch md.GetMessageId() {
			case dupMsg:
				dupCount++
			case afterMsg:
				sawAfter = true
			default:
				return env.Failf("%s: unexpected message_id %q", label, md.GetMessageId())
			}
			return s.ack(ctx, env, md.GetDeliveryToken())
		}
		for !sawAfter || dupCount == 0 {
			if md, err := s.recvDelivery(ctx, env); err != nil {
				return err
			} else if err := handleMD(md); err != nil {
				return err
			}
		}
		if dupCount == 1 {
			if md, got, err := s.tryRecvDelivery(ctx, env, m3dQuietWindow(env)); err != nil {
				return err
			} else if got {
				if err := handleMD(md); err != nil {
					return err
				}
			}
		}
		if dupCount < 1 || dupCount > 2 {
			return env.Failf("%s: expected 1 or 2 deliveries of duplicate ack_id, got %d", label, dupCount)
		}
		_, err := s.stopCheck(ctx, env, v, label, nil, true, int64(dupCount+1), true, []string{dupAck, afterAck}, nil)
		return err
	})
}

func runSUBF03(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	if err := runSUBF03DupBatch(ctx, env, "SUBF-03-A", "subf03-dup-sub", "ack-subf03-dup", "msg-subf03-dup", "ack-subf03-after", "msg-subf03-after", "", v); err != nil {
		return err
	}
	if env.HasCapability(harness.CapSubscriberOrdering) {
		env.Fake.SetDefaultSubscriptionProperties(false, true)
		if err := runSUBF03DupBatch(ctx, env, "SUBF-03-B", "subf03-orddup-sub", "ack-subf03-odup", "msg-subf03-odup", "ack-subf03-oafter", "msg-subf03-oafter", "kDup", v); err != nil {
			return err
		}
	}
	return m3dFinishScenario(ctx, env, v, env.Fake.Snapshot())
}
