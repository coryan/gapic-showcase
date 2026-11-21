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
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func eodScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "EOD-01", Name: "Exactly-Once 60s Minimum Lease Floor & Min Override", Category: "eod", Feature: "EOD-01", Timeout: 75 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery}, Description: "Verify EOD SubscriptionProperties raises default ModifyAckDeadline to >= 60s (tolerating Java pre-tick 10s receipt ModAck) and MinDurationPerAckExtensionMs=15s overrides down on Go/Java/Python/Node (UpdateStreamDeadline on C++).", Run: runEOD01},
		{ID: "EOD-01b", Name: "Mid-Stream SubscriptionProperties EOD Toggle (false -> true -> false)", Category: "eod", Feature: "EOD-01", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery}, Description: "Verify batch-attached SubscriptionProperties toggles EOD dynamically across false -> true -> false on one stream, switching is_exactly_once, receipt ModAck, and EOD error propagation/retry.", Run: runEOD01b},
		{ID: "EOD-02", Name: "Confirmed AckWithResult / NackWithResult Status Mapping", Category: "eod", Feature: "EOD-02", IsolateProxy: true, RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}, Description: "Verify interactive and auto AckWithResult/NackWithResult status mapping (SUCCESS, INVALID_ACK_ID, PERMISSION_DENIED, FAILED_PRECONDITION, OTHER via OutOfRange) under EOD=true and immediate SUCCESS under EOD=false.", Run: runEOD02},
		{ID: "EOD-03", Name: "EOD ErrorInfo Transient Retry & Fallback Code Classification", Category: "eod", Feature: "EOD-03", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}, Description: "Verify TRANSIENT_FAILURE_* and empty-ErrorInfo Unavailable/Internal retry on Acknowledge and ModifyAckDeadline(0) while PERMANENT_FAILURE_INVALID_ACK_ID, PermissionDenied, and FailedPrecondition fail without retry.", Run: runEOD03},
		{ID: "EOD-03b", Name: "EOD Mixed-Batch Partial Failure Split & Selective Retry", Category: "eod", Feature: "EOD-03", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}, Description: "Verify AssertEODSplitRetry across mixed [ok, permanent, transient] batches using AUTO_ACK_WITH_RESULT (Acknowledge on batching SDKs; ExtendLeases ModifyAckDeadline fallback when real C++ sends 1-ID Acknowledge).", Run: runEOD03b},
		{ID: "EOD-04", Name: "Receipt ModifyAckDeadline Gating User Callback Under EOD", Category: "eod", Feature: "EOD-04", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapEodReceiptModack}, Description: "Verify receipt ModifyAckDeadline blocks delivery until completion, retries TRANSIENT_FAILURE_*, and drops PERMANENT_FAILURE_INVALID_ACK_ID pre-callback while delivering mixed-batch survivors.", Run: runEOD04},
	}
}

func runEOD01(ctx context.Context, env *harness.ScenarioEnv) error {
	lang, v := strings.ToLower(env.Language()), env.NewVerifier()
	honorsMin := lang == "go" || lang == "java" || lang == "python" || lang == "node"
	runPhase := func(label, pfx, aid, mid string, minMs int64, isB bool) (fake.Snapshot, error) {
		var snap fake.Snapshot
		err := m3dWithSession(ctx, env, pfx, func(c *proxypb.StartSubscriberCommand) {
			c.MaxExtensionMs = proto.Int64(90_000)
			if minMs > 0 {
				c.MinDurationPerAckExtensionMs = proto.Int64(minMs)
			}
		}, label, func(s *m3dSession) error {
			s.wantEOD = proto.Bool(true)
			md, err := s.pushRecv(ctx, env, aid, mid, []byte(mid), fake.WithSubscriptionProperties(true, false))
			if err != nil {
				return err
			}
			if err := m3dCheckNoPrematureAckAfter(env, s.wm, md); err != nil {
				return err
			}
			var badDL int32
			if err := waitFakeOrStop(ctx, env, s.p, func(sn fake.Snapshot) bool {
				for _, c := range modAckCallsAfterSeq(sn, s.wm) {
					if !c.Completed || !slices.Contains(c.ModAckReq.GetAckIds(), aid) {
						continue
					}
					dl := c.ModAckReq.GetAckDeadlineSeconds()
					switch {
					case !isB && dl > 0 && dl < fake.MinEODLeaseExtensionSecond && lang != "java", isB && honorsMin && dl >= fake.MinEODLeaseExtensionSecond:
						badDL = dl
						return true
					case !isB && dl >= fake.MinEODLeaseExtensionSecond && dl <= fake.MaxLeaseExtensionSeconds,
						isB && honorsMin && dl >= fake.MinLeaseExtensionSeconds && dl < fake.MinEODLeaseExtensionSecond,
						isB && !honorsMin && dl >= fake.MinLeaseExtensionSeconds && dl <= fake.MaxLeaseExtensionSeconds:
						return true
					}
				}
				return false
			}, label+" ModAck"); err != nil {
				return err
			}
			if badDL > 0 {
				return env.Failf("%s (%s): invalid EOD ack_deadline_seconds=%d", label, lang, badDL)
			}
			if snap, err = s.stopCheck(ctx, env, v, label, []string{md.GetDeliveryToken()}, true, 1, true, []string{aid}, nil); err != nil {
				return err
			}
			if isB && lang == "cpp" {
				hasCppMin := m3dModAckCount(snap, s.wm, aid, 10, 59) > 0 || slices.ContainsFunc(callsAfterSeq(snap, s.wm), func(c fake.CapturedCall) bool {
					return c.IsStreamAckDeadlineUpdate() && c.StreamingPullReq.GetStreamAckDeadlineSeconds() == 15
				})
				v.Require(hasCppMin, "EOD-01-B (cpp): expected UpdateStreamDeadline=15 or ModAck<60")
			}
			return nil
		})
		return snap, err
	}
	if _, err := runPhase("EOD-01-A", "eod01-a-sub", "ack-eod01-1", "msg-eod01-1", 0, false); err != nil {
		return err
	}
	snap, err := runPhase("EOD-01-B", "eod01-b-sub", "ack-eod01-2", "msg-eod01-2", 15_000, true)
	if err != nil {
		return err
	}
	return m3dFinishScenario(ctx, env, v, snap)
}

func runEOD01b(ctx context.Context, env *harness.ScenarioEnv) error {
	lang, hasRes, hasRec := strings.ToLower(env.Language()), env.HasCapability(harness.CapAckWithResult), env.HasCapability(harness.CapEodReceiptModack)
	m3dAddSettleRule(env.Fake, "EOD-01b-", "ack-eod01b-1", false, 1, status.Error(codes.Internal, "non-eod-1"))
	m3dAddSettleRule(env.Fake, "EOD-01b-", "ack-eod01b-2", false, 1, map[bool]error{true: status.Error(codes.PermissionDenied, "eod-2"), false: status.Error(codes.Unavailable, "eod-2-trans")}[hasRes])
	m3dAddSettleRule(env.Fake, "EOD-01b-", "ack-eod01b-3", false, 1, status.Error(codes.Internal, "non-eod-3"))
	for _, aid := range []string{"ack-eod01b-1", "ack-eod01b-3"} {
		env.Fake.AddModAckRule(fake.ModAckRule{
			Name: "EOD-01b-rec-" + aid, Err: fake.EODModAckErrorStatus(codes.InvalidArgument, "non-eod-rec", map[string]string{aid: fake.EODPermanentInvalidAckID}),
			Match: func(mc *fake.ModAckMatchContext) bool {
				return mc.Request != nil && mc.Request.GetAckDeadlineSeconds() > 0 && slices.Contains(mc.Request.GetAckIds(), aid)
			},
		})
	}
	return m3dWithSession(ctx, env, "eod01b-sub", func(c *proxypb.StartSubscriberCommand) {
		c.UseExactlyOnce, c.MaxExtensionMs = hasRes, proto.Int64(90_000)
	}, "EOD-01b", func(s *m3dSession) error {
		runPhase := func(ackID, msgID, phase string, eod bool, wm int64) error {
			s.wantEOD = proto.Bool(eod)
			var md *proxypb.MessageDeliveredEvent
			var err error
			if eod && hasRec {
				minRec := map[bool]int32{true: fake.MinLeaseExtensionSeconds, false: fake.MinEODLeaseExtensionSecond}[lang == "java"]
				gate := fake.NewGate()
				defer gate.Release()
				env.Fake.AddModAckRule(fake.ModAckRule{
					Name: "EOD-01b-p2-gate", Gate: gate,
					Match: func(mc *fake.ModAckMatchContext) bool {
						return mc.Request != nil && mc.Request.GetAckDeadlineSeconds() >= minRec && slices.Contains(mc.Request.GetAckIds(), ackID)
					},
				})
				if err = pushOneMsg(s.sh, ackID, msgID, []byte(msgID), fake.WithSubscriptionProperties(eod, false)); err != nil {
					return err
				}
				if err = s.waitGateOrStop(ctx, env, gate, "EOD-01b Phase 2: delivered before receipt ModAck"); err != nil {
					return err
				}
				gate.Release()
				md, err = s.recvDelivery(ctx, env)
			} else {
				md, err = s.pushRecv(ctx, env, ackID, msgID, []byte(msgID), fake.WithSubscriptionProperties(eod, false))
			}
			if err != nil {
				return err
			}
			if err := m3dCheckNoPrematureAckAfter(env, wm, md); err != nil {
				return err
			}
			if err := s.settle(ctx, env, md.GetDeliveryToken(), map[bool]proxypb.SettleMessageCommand_SettleAction{true: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT, false: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK}[eod && hasRes]); err != nil {
				return err
			}
			if eod && hasRes {
				if _, err := s.recvSettlement(ctx, env, md.GetDeliveryToken(), proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, false); err != nil {
					return err
				}
			}
			if err := waitFakeOrStop(ctx, env, s.p, func(sn fake.Snapshot) bool {
				return m3dAckCount(sn, wm, ackID) >= map[bool]int{true: 2, false: 1}[eod && !hasRes]
			}, phase+" ack"); err != nil {
				return err
			}
			if !eod {
				return sleepCtx(ctx, m3dQuietWindow(env))
			}
			return nil
		}
		for _, p := range []struct {
			aid, mid, phase string
			eod             bool
		}{{"ack-eod01b-1", "msg-eod01b-1", "Phase 1", false}, {"ack-eod01b-2", "msg-eod01b-2", "Phase 2", true}, {"ack-eod01b-3", "msg-eod01b-3", "Phase 3", false}} {
			if err := runPhase(p.aid, p.mid, p.phase, p.eod, lastCapturedSeq(env.Fake.Snapshot())); err != nil {
				return err
			}
		}
		v := env.NewVerifier()
		snap, err := s.stopCheck(ctx, env, v, "EOD-01b", nil, false, 3, true, []string{"ack-eod01b-1", "ack-eod01b-2", "ack-eod01b-3"}, nil)
		if err != nil {
			return err
		}
		v.Require(m3dAckCount(snap, 0, "ack-eod01b-1") == 1 && m3dAckCount(snap, 0, "ack-eod01b-3") == 1 && len(snap.Streams) == 1,
			"EOD-01b: expected 1 non-retried Ack on 1 stream, got p1=%d p3=%d streams=%d", m3dAckCount(snap, 0, "ack-eod01b-1"), m3dAckCount(snap, 0, "ack-eod01b-3"), len(snap.Streams))
		return m3dFinishScenario(ctx, env, v, snap)
	})
}

func m3dPushSettleStep(ctx context.Context, env *harness.ScenarioEnv, s *m3dSession, aid, mid string, eod, nack bool, want proxypb.SettlementResultEvent_AckStatus, label string) error {
	s.wantEOD = proto.Bool(eod)
	md, err := s.pushRecv(ctx, env, aid, mid, []byte(mid), fake.WithSubscriptionProperties(eod, false))
	if err != nil {
		return err
	}
	act := map[bool]proxypb.SettleMessageCommand_SettleAction{true: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT, false: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT}[nack]
	if err := s.settle(ctx, env, md.GetDeliveryToken(), act); err != nil {
		return err
	}
	_, err = s.recvSettlement(ctx, env, md.GetDeliveryToken(), want, nack)
	return err
}

func m3dAddSettleRule(f *fake.Server, pfx, id string, nack bool, times int, err error) {
	if nack {
		f.AddModAckRule(fake.ModAckRule{Name: pfx + id, Match: func(mc *fake.ModAckMatchContext) bool {
			return mc.Request != nil && mc.Request.GetAckDeadlineSeconds() == 0 && slices.Contains(mc.Request.GetAckIds(), id)
		}, Times: times, Err: err})
	} else {
		f.AddAckRule(fake.AckRule{Name: pfx + id, Match: fake.MatchAckID(id), Times: times, Err: err})
	}
}

func runEOD02(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	steps1 := []struct {
		aid, mid string
		nack     bool
		want     proxypb.SettlementResultEvent_AckStatus
		err      error
	}{
		{"ack-eod02-ok", "msg-eod02-1", false, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, nil}, {"nack-eod02-ok", "msg-eod02-2", true, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, nil},
		{"ack-eod02-inv", "msg-eod02-3", false, proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, fake.EODErrorStatus(codes.InvalidArgument, "inv", map[string]string{"ack-eod02-inv": fake.EODPermanentInvalidAckID})}, {"ack-eod02-perm", "msg-eod02-4", false, proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, status.Error(codes.PermissionDenied, "perm")},
		{"ack-eod02-pre", "msg-eod02-5", false, proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, status.Error(codes.FailedPrecondition, "pre")}, {"ack-eod02-oth", "msg-eod02-6", false, proxypb.SettlementResultEvent_ACK_STATUS_OTHER, status.Error(codes.OutOfRange, "oth")},
		{"nack-eod02-perm", "msg-eod02-7", true, proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, status.Error(codes.PermissionDenied, "nperm")}, {"nack-eod02-inv", "msg-eod02-8", true, proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, fake.EODModAckErrorStatus(codes.InvalidArgument, "ninv", map[string]string{"nack-eod02-inv": fake.EODPermanentInvalidAckID})},
	}
	if err := m3dWithSession(ctx, env, "eod02-int-sub", func(c *proxypb.StartSubscriberCommand) {
		c.UseExactlyOnce, c.MaxExtensionMs = true, proto.Int64(90_000)
	}, "EOD-02 interactive", func(s1 *m3dSession) error {
		for _, stp := range steps1 {
			if stp.err != nil {
				m3dAddSettleRule(env.Fake, "EOD-02-", stp.aid, stp.nack, 1, stp.err)
			}
			if err := m3dPushSettleStep(ctx, env, s1, stp.aid, stp.mid, true, stp.nack, stp.want, "EOD-02"); err != nil {
				return err
			}
		}
		_, err := s1.stopCheck(ctx, env, v, "EOD-02-int", nil, false, int64(len(steps1)), true,
			[]string{"ack-eod02-ok", "ack-eod02-inv", "ack-eod02-perm", "ack-eod02-pre", "ack-eod02-oth"}, []string{"nack-eod02-ok", "nack-eod02-perm", "nack-eod02-inv"})
		return err
	}); err != nil {
		return err
	}
	m3dAddSettleRule(env.Fake, "EOD-02-", "ack-eod02-aa-err", false, 1, status.Error(codes.PermissionDenied, "aa-err"))
	m3dAddSettleRule(env.Fake, "EOD-02-", "nack-eod02-an-err", true, 1, status.Error(codes.PermissionDenied, "an-err"))
	for _, ap := range []struct {
		label, pfx, a1, m1, a2, m2 string
		mode                       proxypb.StartSubscriberCommand_AutoSettleMode
		nack                       bool
	}{
		{"EOD-02-AA", "eod02-aa-sub", "ack-eod02-aa-ok", "msg-eod02-aa-1", "ack-eod02-aa-err", "msg-eod02-aa-2", proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT, false},
		{"EOD-02-AN", "eod02-an-sub", "nack-eod02-an-ok", "msg-eod02-an-1", "nack-eod02-an-err", "msg-eod02-an-2", proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT, true},
	} {
		if err := m3dWithSession(ctx, env, ap.pfx, func(c *proxypb.StartSubscriberCommand) {
			c.UseExactlyOnce, c.AutoSettleMode, c.MaxExtensionMs = true, ap.mode, proto.Int64(90_000)
		}, ap.label, func(s *m3dSession) error {
			s.wantEOD = proto.Bool(true)
			for i, it := range [2][2]string{{ap.a1, ap.m1}, {ap.a2, ap.m2}} {
				md, err := s.pushRecv(ctx, env, it[0], it[1], []byte(it[1]), fake.WithSubscriptionProperties(true, false))
				if err != nil {
					return err
				}
				want := map[bool]proxypb.SettlementResultEvent_AckStatus{false: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, true: proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED}[i == 1]
				if _, err := s.recvSettlement(ctx, env, md.GetDeliveryToken(), want, ap.nack); err != nil {
					return err
				}
			}
			acks, nacks := map[bool][]string{false: {ap.a1, ap.a2}}[ap.nack], map[bool][]string{true: {ap.a1, ap.a2}}[ap.nack]
			_, err := s.stopCheck(ctx, env, v, ap.label, nil, false, 2, true, acks, nacks)
			return err
		}); err != nil {
			return err
		}
	}
	m3dAddSettleRule(env.Fake, "EOD-02-", "ack-eod02-noneod", false, 1, status.Error(codes.PermissionDenied, "ignored"))
	m3dAddSettleRule(env.Fake, "EOD-02-", "nack-eod02-noneod", true, 1, status.Error(codes.PermissionDenied, "ignored"))
	return m3dWithSession(ctx, env, "eod02-noneod-sub", func(c *proxypb.StartSubscriberCommand) { c.UseExactlyOnce = true }, "EOD-02 EOD=false", func(s4 *m3dSession) error {
		for i, p := range [2][2]string{{"ack-eod02-noneod", "msg-eod02-ne-1"}, {"nack-eod02-noneod", "msg-eod02-ne-2"}} {
			if err := m3dPushSettleStep(ctx, env, s4, p[0], p[1], false, i == 1, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, "EOD-02 Part 4"); err != nil {
				return err
			}
		}
		return s4.stopFinish(ctx, env, v, "EOD-02-noneod", nil, false, 2, true, []string{"ack-eod02-noneod"}, []string{"nack-eod02-noneod"})
	})
}

func runEOD03(ctx context.Context, env *harness.ScenarioEnv) error {
	lang := strings.ToLower(env.Language())
	cases := []struct {
		aid, mid       string
		nack, rubySkip bool
		wantCalls      int
		wantStatus     proxypb.SettlementResultEvent_AckStatus
		err            error
	}{
		{"ack-eod03-ei-t", "msg-eod03-1", false, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, fake.EODErrorStatus(codes.InvalidArgument, "ei-t", map[string]string{"ack-eod03-ei-t": fake.EODTransientUnorderedAckID})}, {"ack-eod03-unav", "msg-eod03-2", false, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, status.Error(codes.Unavailable, "unav")},
		{"ack-eod03-int", "msg-eod03-3", false, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, status.Error(codes.Internal, "int")}, {"ack-eod03-ei-p", "msg-eod03-4", false, true, 1, proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, fake.EODErrorStatus(codes.InvalidArgument, "ei-p", map[string]string{"ack-eod03-ei-p": fake.EODPermanentInvalidAckID})},
		{"ack-eod03-perm", "msg-eod03-5", false, false, 1, proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, status.Error(codes.PermissionDenied, "perm")}, {"ack-eod03-pre", "msg-eod03-6", false, false, 1, proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, status.Error(codes.FailedPrecondition, "pre")},
		{"nack-eod03-ei-t", "msg-eod03-7", true, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, fake.EODModAckErrorStatus(codes.InvalidArgument, "nei-t", map[string]string{"nack-eod03-ei-t": fake.EODTransientUnorderedAckID})}, {"nack-eod03-unav", "msg-eod03-8", true, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, status.Error(codes.Unavailable, "nunav")},
		{"nack-eod03-int", "msg-eod03-9", true, false, 2, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, status.Error(codes.Internal, "nint")}, {"nack-eod03-ei-p", "msg-eod03-10", true, true, 1, proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, fake.EODModAckErrorStatus(codes.InvalidArgument, "nei-p", map[string]string{"nack-eod03-ei-p": fake.EODPermanentInvalidAckID})},
		{"nack-eod03-perm", "msg-eod03-11", true, false, 1, proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, status.Error(codes.PermissionDenied, "nperm")}, {"nack-eod03-pre", "msg-eod03-12", true, false, 1, proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION, status.Error(codes.FailedPrecondition, "npre")},
	}
	return m3dWithSession(ctx, env, "eod03-sub", func(c *proxypb.StartSubscriberCommand) {
		c.UseExactlyOnce, c.MaxExtensionMs = true, proto.Int64(90_000)
	}, "EOD-03", func(s *m3dSession) error {
		var ackIDs, nackIDs []string
		for _, tc := range cases {
			m3dAddSettleRule(env.Fake, "EOD-03-", tc.aid, tc.nack, map[bool]int{true: 1, false: fake.RepeatAlways}[tc.wantCalls > 1], tc.err)
			dst := map[bool]*[]string{true: &nackIDs, false: &ackIDs}[tc.nack]
			*dst = append(*dst, tc.aid)
			wm := lastCapturedSeq(env.Fake.Snapshot())
			if err := m3dPushSettleStep(ctx, env, s, tc.aid, tc.mid, true, tc.nack, tc.wantStatus, "EOD-03"); err != nil {
				return err
			}
			sn := env.Fake.Snapshot()
			if got := map[bool]int{true: m3dModAckCount(sn, wm, tc.aid, 0, 0), false: m3dAckCount(sn, wm, tc.aid)}[tc.nack]; !(tc.rubySkip && lang == "ruby") && got != tc.wantCalls {
				return env.Failf("EOD-03 %s: expected %d RPC attempt(s), got %d", tc.aid, tc.wantCalls, got)
			}
		}
		return s.stopFinish(ctx, env, env.NewVerifier(), "EOD-03", nil, false, int64(len(cases)), true, ackIDs, nackIDs)
	})
}

func runEOD03b(ctx context.Context, env *harness.ScenarioEnv) error {
	lang, v := strings.ToLower(env.Language()), env.NewVerifier()
	env.Fake.AddAckRule(fake.AckRule{
		Name: "EOD-03b-mixed-ack", Times: fake.RepeatAlways,
		Match: func(mc *fake.AckMatchContext) bool {
			return mc.Request != nil && slices.ContainsFunc(mc.Request.GetAckIds(), func(id string) bool { return strings.HasPrefix(id, "eod03b-a-") })
		},
		EOD: fake.NewEODScript(codes.InvalidArgument, "mixed ack batch", fake.EODReasonAckFailed, map[string]fake.EODAckBehavior{
			"eod03b-a-perm": {PermanentReason: fake.EODPermanentInvalidAckID}, "eod03b-a-trans": {TransientTimes: 1, TransientReason: fake.EODTransientUnorderedAckID},
		}),
	})
	var snapA fake.Snapshot
	if err := m3dWithSession(ctx, env, "eod03b-sub", func(c *proxypb.StartSubscriberCommand) {
		c.UseExactlyOnce, c.AutoSettleMode, c.MaxOutstandingMessages, c.MaxExtensionMs = true, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT, proto.Int64(10), proto.Int64(90_000)
	}, "EOD-03b", func(s *m3dSession) error {
		s.wantEOD = proto.Bool(true)
		mdsA, err := s.pushAndRecvCapped(ctx, env, m3dTriplet("eod03b-a-ok", "msg-eod03b-ok", []byte("ok"), "eod03b-a-perm", "msg-eod03b-perm", []byte("perm"), "eod03b-a-trans", "msg-eod03b-trans", []byte("trans"), ""), 3, 0, "EOD-03b", fake.WithSubscriptionProperties(true, false))
		if err != nil {
			return err
		}
		for _, md := range mdsA {
			want := map[string]proxypb.SettlementResultEvent_AckStatus{"msg-eod03b-ok": proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, "msg-eod03b-perm": proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, "msg-eod03b-trans": proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}[md.GetMessageId()]
			if _, err := s.recvSettlement(ctx, env, md.GetDeliveryToken(), want, false); err != nil {
				return err
			}
		}
		snapA, err = s.stopCheck(ctx, env, v, "EOD-03b-A", nil, false, 3, true, []string{"eod03b-a-ok", "eod03b-a-perm", "eod03b-a-trans"}, nil)
		return err
	}); err != nil {
		return err
	}
	if lang != "cpp" || slices.ContainsFunc(snapA.AckCalls(), func(c fake.CapturedCall) bool { return c.Completed && len(c.AckIDs()) >= 3 }) {
		v.Check(fake.AssertEODSplitRetry(snapA, []string{"eod03b-a-perm"}, []string{"eod03b-a-trans"}, []string{"eod03b-a-ok"}))
		return m3dFinishScenario(ctx, env, v, env.Fake.Snapshot())
	}
	v.Require(m3dAckCount(snapA, 0, "eod03b-a-ok") == 1 && m3dAckCount(snapA, 0, "eod03b-a-perm") == 1 && m3dAckCount(snapA, 0, "eod03b-a-trans") == 2,
		"EOD-03b (cpp): unexpected per-ID ack counts ok=%d perm=%d trans=%d", m3dAckCount(snapA, 0, "eod03b-a-ok"), m3dAckCount(snapA, 0, "eod03b-a-perm"), m3dAckCount(snapA, 0, "eod03b-a-trans"))
	env.Fake.AddModAckRule(fake.ModAckRule{
		Name: "EOD-03b-cpp-modack", Times: fake.RepeatAlways,
		Match: func(mc *fake.ModAckMatchContext) bool {
			return mc.Request != nil && mc.Request.GetAckDeadlineSeconds() > 0 && slices.ContainsFunc(mc.Request.GetAckIds(), func(id string) bool { return strings.HasPrefix(id, "eod03b-m-") })
		},
		EOD: fake.NewEODScript(codes.InvalidArgument, "mixed modack", fake.EODReasonModAckFailed, map[string]fake.EODAckBehavior{
			"eod03b-m-perm": {PermanentReason: fake.EODPermanentInvalidAckID}, "eod03b-m-trans": {TransientTimes: 1, TransientReason: fake.EODTransientUnorderedAckID},
		}),
	})
	return m3dWithSession(ctx, env, "eod03b-cpp-modack", func(c *proxypb.StartSubscriberCommand) {
		c.UseExactlyOnce, c.MaxOutstandingMessages, c.MaxDurationPerAckExtensionMs, c.MaxExtensionMs = true, proto.Int64(10), proto.Int64(10_000), proto.Int64(90_000)
	}, "EOD-03b cpp ModAck", func(sB *m3dSession) error {
		sB.wantEOD = proto.Bool(true)
		mdsB, err := sB.pushAndRecvCapped(ctx, env, m3dTriplet("eod03b-m-ok", "msg-eod03b-m-ok", []byte("mok"), "eod03b-m-perm", "msg-eod03b-m-perm", []byte("mperm"), "eod03b-m-trans", "msg-eod03b-m-trans", []byte("mtrans"), ""), 3, 0, "EOD-03b cpp ModAck", fake.WithSubscriptionProperties(true, false))
		if err != nil {
			return err
		}
		mToks := []string{mdsB[0].GetDeliveryToken(), mdsB[1].GetDeliveryToken(), mdsB[2].GetDeliveryToken()}
		if err := waitFakeOrStop(ctx, env, sB.p, func(sn fake.Snapshot) bool {
			return m3dModAckCount(sn, sB.wm, "eod03b-m-perm", 1, 600) >= 2 || slices.ContainsFunc(modAckCallsAfterSeq(sn, sB.wm), func(c fake.CapturedCall) bool {
				return c.Completed && c.StatusCode == codes.OK && slices.Contains(c.ModAckReq.GetAckIds(), "eod03b-m-trans")
			})
		}, "EOD-03b cpp ModAck retry"); err != nil {
			return err
		}
		postExtend := lastCapturedSeq(env.Fake.Snapshot())
		snapB, err := sB.stopCheck(ctx, env, v, "EOD-03b-B", mToks, true, 3, true, []string{"eod03b-m-ok", "eod03b-m-perm", "eod03b-m-trans"}, nil)
		if err != nil {
			return err
		}
		extSnap := snapAfterSeq(snapB, sB.wm)
		extSnap.Calls = slices.DeleteFunc(slices.Clone(extSnap.Calls), func(c fake.CapturedCall) bool { return c.Seq > postExtend && c.ModAckReq != nil })
		v.Check(fake.AssertEODSplitRetry(extSnap, []string{"eod03b-m-perm"}, []string{"eod03b-m-trans"}, []string{"eod03b-m-ok"}))
		return m3dFinishScenario(ctx, env, v, snapB)
	})
}

func runEOD04(ctx context.Context, env *harness.ScenarioEnv) error {
	gate := fake.NewGate()
	env.Fake.AddModAckRule(fake.ModAckRule{Name: "EOD-04-gate", Match: fake.MatchModAckIDsContaining("eod04-gate"), Times: 1, Gate: gate})
	env.Fake.AddModAckRule(fake.ModAckRule{Name: "EOD-04-trans", Match: fake.MatchModAckIDsContaining("eod04-trans"), Times: 1, Err: fake.EODModAckErrorStatus(codes.Unavailable, "trans-receipt", map[string]string{"eod04-trans": fake.EODTransientUnorderedAckID})})
	env.Fake.AddModAckRule(fake.ModAckRule{
		Name: "EOD-04-mixed", Match: fake.MatchModAckIDsContaining("eod04-perm"), Times: fake.RepeatAlways,
		EOD: fake.NewEODScript(codes.InvalidArgument, "mixed receipt", fake.EODReasonModAckFailed, map[string]fake.EODAckBehavior{
			"eod04-perm": {PermanentReason: fake.EODPermanentInvalidAckID}, "eod04-surv": {TransientTimes: 1, TransientReason: fake.EODTransientUnorderedAckID},
		}),
	})
	return m3dWithSession(ctx, env, "eod04-sub", func(c *proxypb.StartSubscriberCommand) { c.MaxExtensionMs = proto.Int64(90_000) }, "EOD-04", func(s *m3dSession) error {
		defer gate.Release()
		s.wantEOD = proto.Bool(true)
		if err := pushOneMsg(s.sh, "eod04-gate", "msg-eod04-gate", []byte("gate"), fake.WithSubscriptionProperties(true, false)); err != nil {
			return err
		}
		if err := s.waitGateOrStop(ctx, env, gate, "EOD-04 Phase A: wait receipt ModifyAckDeadline"); err != nil {
			return err
		}
		if err := s.requireNoExtra(ctx, env, "EOD-04 Phase A: delivered before receipt ModifyAckDeadline completed"); err != nil {
			return err
		}
		gate.Release()
		md1, err := s.recvDelivery(ctx, env)
		if err != nil {
			return err
		}
		if err := m3dCheckNoPrematureAckAfter(env, 0, md1); err != nil {
			return err
		}
		if err := ackAndWait(ctx, env, s.p, md1.GetDeliveryToken(), "eod04-gate", "EOD-04 gate ack"); err != nil {
			return err
		}
		md2, err := s.pushRecv(ctx, env, "eod04-trans", "msg-eod04-trans", []byte("trans"), fake.WithSubscriptionProperties(true, false))
		if err != nil {
			return err
		}
		if got := m3dModAckCount(env.Fake.Snapshot(), 0, "eod04-trans", 1, 600); got < 2 {
			return env.Failf("EOD-04 Phase B: expected >= 2 receipt ModAck attempts before delivery, got %d", got)
		}
		if err := ackAndWait(ctx, env, s.p, md2.GetDeliveryToken(), "eod04-trans", "EOD-04 trans ack"); err != nil {
			return err
		}
		if err := s.sh.PushMessages([]*pubsubpb.ReceivedMessage{m3dPushRec("eod04-perm", "msg-eod04-perm", []byte("perm"), ""), m3dPushRec("eod04-surv", "msg-eod04-surv", []byte("surv"), "")}, fake.WithSubscriptionProperties(true, false)); err != nil {
			return err
		}
		md3, err := s.recvDelivery(ctx, env)
		if err != nil {
			return err
		}
		if md3.GetMessageId() != "msg-eod04-surv" {
			return env.Failf("EOD-04 Phase C: expected only msg-eod04-surv to deliver, got %q", md3.GetMessageId())
		}
		if snC := env.Fake.Snapshot(); m3dModAckCount(snC, 0, "eod04-perm", 1, 600) != 1 || m3dModAckCount(snC, 0, "eod04-surv", 1, 600) < 2 {
			return env.Failf("EOD-04 Phase C: permanent eod04-perm should not be retried (perm=%d surv=%d)", m3dModAckCount(snC, 0, "eod04-perm", 1, 600), m3dModAckCount(snC, 0, "eod04-surv", 1, 600))
		}
		if err := s.ack(ctx, env, md3.GetDeliveryToken()); err != nil {
			return err
		}
		if err := s.requireNoExtra(ctx, env, "EOD-04 Phase C: expected only msg-eod04-surv to deliver"); err != nil {
			return err
		}
		return s.stopFinish(ctx, env, env.NewVerifier(), "EOD-04", nil, true, 3, true, []string{"eod04-gate", "eod04-trans", "eod04-surv"}, nil)
	})
}
