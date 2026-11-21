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
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func boundedMaxExtMs(env *harness.ScenarioEnv) int64 {
	if d := env.Defaults().GetSubscriberMaxExtensionMs(); d > 0 && d < 1500 {
		return d
	}
	return 2_000
}
func lease03MaxExtMs(env *harness.ScenarioEnv) int64 {
	if d := boundedMaxExtMs(env); d < 1500 {
		return d
	}
	return cmp.Or(map[string]int64{"cpp": 19_000, "rust": 4_000}[strings.ToLower(env.Language())], 2_000)
}
func leaseScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "LEASE-01", Name: "Batched Ack/Nack & 1000-ID / 512 KiB Batch Splitting", Category: "lease", Feature: "LEASE-01", Description: "Verify Ack/Nack settlement and batch splitting under <= 1000 ack_ids and <= 512 KiB encoded request bounds.", Run: runLEASE01},
		{ID: "LEASE-02", Name: "Periodic Lease Extension & [10s, 600s] Clamping", Category: "lease", Feature: "LEASE-02", Timeout: 60 * time.Second, Description: "Verify periodic ModifyAckDeadline renewals while messages remain unsettled and [10s, 600s] deadline clamping.", Run: runLEASE02},
		{ID: "LEASE-03", Name: "MaxExtension Lease Expiration & Flow-Control Release", Category: "lease", Feature: "LEASE-03", Timeout: 45 * time.Second, Description: "Verify lease extension stops cleanly past max_extension_ms without spurious Nack(0) and releases flow control.", Run: runLEASE03},
		{ID: "LEASE-04", Name: "At-Least-Once Ack/ModAck Error Isolation", Category: "lease", Feature: "LEASE-04", Description: "Verify non-fatal Ack and ModifyAckDeadline RPC errors under UseExactlyOnce=false do not crash the subscriber stream.", Run: runLEASE04},
		{ID: "LEASE-05", Name: "Handler Drop Without Settlement Behavior", Category: "lease", Feature: "LEASE-05", Description: "Verify RETURN_WITHOUT_SETTLE sends ModifyAckDeadline(0) when CapAutoNackOnDrop is true and sends zero Nacks when false.", Run: runLEASE05},
	}
}
func runLeaseBatchPhase(ctx context.Context, env *harness.ScenarioEnv, sub, ackPrefix, msgPrefix string, readyIdx, total, chunk, padBytes, nackFrom int) ([]string, int64, error) {
	wm, cmd := lastCapturedSeq(env.Fake.Snapshot()), interactiveCmd(env, env.Endpoint(), sub)
	if total > 10 {
		cmd.MaxOutstandingMessages, cmd.MaxOutstandingBytes = proto.Int64(2000), proto.Int64(32<<20)
	}
	p, sh, err := startSubAndStream(ctx, env, cmd, readyIdx, "LEASE-01 "+ackPrefix)
	if err != nil {
		return nil, wm, err
	}
	defer p.close(ctx, env)
	allAcks, toks := make([]string, 0, total), make([]string, 0, total)
	for off := 0; off < total; off += chunk {
		n := min(chunk, total-off)
		acks, bodies, perr := pushSimpleBatch(sh, ackPrefix, msgPrefix, off+1, n, padBytes)
		if perr != nil {
			return nil, wm, perr
		}
		allAcks = append(allAcks, acks...)
		for i := range n {
			md, rerr := p.recvDelivery(ctx, env)
			if rerr != nil {
				return nil, wm, rerr
			}
			if total <= 5 || (i == 0 && off == 0) {
				if aerr := assertDeliveredMsg(md, fmt.Sprintf("%s-%04d", msgPrefix, off+i+1), bodies[i], nil, ""); aerr != nil {
					return nil, wm, aerr
				}
			}
			toks = append(toks, md.GetDeliveryToken())
		}
	}
	for i, tok := range toks {
		act := map[bool]proxypb.SettleMessageCommand_SettleAction{true: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK, false: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK}[i >= nackFrom]
		if serr := sendSettleAction(env, p, tok, act); serr != nil {
			return nil, wm, serr
		}
	}
	return allAcks, wm, p.stopAndCheck(ctx, env, int64(total))
}
func runLEASE01(ctx context.Context, env *harness.ScenarioEnv) error {
	v := env.NewVerifier()
	for _, sp := range []struct {
		sub, ackPfx, msgPfx       string
		ready, tot, chnk, pd, nAt int
	}{
		{env.SubscriptionPath(env.UniqueName("lease01-a-sub")), "ack-l01a", "msg-l01a", 1, 5, 5, 0, 3},
		{env.SubscriptionPath(env.UniqueName("lease01-b-sub")), "ack-l01b", "msg-l01b", 2, 1005, 200, 0, 1005},
		{env.SubscriptionPath(env.UniqueName("lease01-c-sub")), "ack-l01c", "msg-l01c", 3, 540, 60, 1024, 540},
		{env.SubscriptionPath(env.UniqueName("lease01-d-sub")), "ack-l01d", "msg-l01d", 4, 1005, 200, 0, 0},
	} {
		ids, wm, err := runLeaseBatchPhase(ctx, env, sp.sub, sp.ackPfx, sp.msgPfx, sp.ready, sp.tot, sp.chnk, sp.pd, sp.nAt)
		if err != nil {
			return err
		}
		s := snapAfterSeq(env.Fake.Snapshot(), wm)
		checkAckModAckMeta(env, v, sp.sub, s.Calls)
		v.Check(fake.AssertAllAckedIDs(s, ids[:sp.nAt]...))
		v.Check(fake.AssertAllNackedIDs(s, ids[sp.nAt:]...))
		if sp.tot >= 1000 && sp.nAt > 0 {
			v.Require(len(s.AckCalls()) >= 2, "LEASE-01 %s expected >= 2 Acknowledge RPCs, got %d", sp.ackPfx, len(s.AckCalls()))
		}
		if sp.tot >= 1000 && sp.nAt == 0 {
			nacks := filterCalls(s.ModAckCalls(), func(c fake.CapturedCall) bool { return c.ModAckReq.GetAckDeadlineSeconds() == 0 })
			v.Require(len(nacks) >= 2, "LEASE-01 %s expected >= 2 ModifyAckDeadline(0) RPCs, got %d", sp.ackPfx, len(nacks))
		}
	}
	snap := env.Fake.Snapshot()
	v.Check(fake.AssertAckAndModAckWithinBounds(snap))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	return finishPublishScenario(ctx, env, v)
}
func startLease02Phase(ctx context.Context, env *harness.ScenarioEnv, sub string, minMs, maxMs, extMs int64, readyIdx int, ackID, msgID string, wantModAcks int, waitTO time.Duration) (int64, error) {
	wm, cmd := lastCapturedSeq(env.Fake.Snapshot()), interactiveCmd(env, env.Endpoint(), sub)
	cmd.MinDurationPerAckExtensionMs, cmd.MaxDurationPerAckExtensionMs, cmd.MaxExtensionMs = proto.Int64(minMs), proto.Int64(maxMs), proto.Int64(extMs)
	p, sh, err := startSubAndStream(ctx, env, cmd, readyIdx, "LEASE-02 "+ackID)
	if err != nil {
		return wm, err
	}
	defer p.close(ctx, env)
	md, err := pushRecvOne(ctx, env, p, sh, ackID, msgID, []byte(msgID))
	if err != nil {
		return wm, err
	}
	wCtx := ctx
	if waitTO > 0 {
		var cancel context.CancelFunc
		wCtx, cancel = context.WithTimeout(ctx, waitTO)
		defer cancel()
	}
	werr := waitFakeOrStop(wCtx, env, p, func(s fake.Snapshot) bool { return hasCompletedModAckFor(s, wm, ackID) >= wantModAcks }, "LEASE-02 wait "+ackID)
	if werr != nil && !(waitTO > 0 && ctx.Err() == nil && errors.Is(werr, context.DeadlineExceeded) && strings.EqualFold(env.Language(), "dotnet")) {
		return wm, werr
	}
	if err := fake.AssertAllNackedIDs(env.Fake.Snapshot()); err != nil {
		return wm, err
	}
	if err := ackAndWait(ctx, env, p, md.GetDeliveryToken(), ackID, "LEASE-02 ack "+ackID); err != nil {
		return wm, err
	}
	if err := sleepCtx(ctx, map[bool]time.Duration{true: 35 * time.Millisecond, false: 250 * time.Millisecond}[boundedMaxExtMs(env) < 1500]); err != nil {
		return wm, err
	}
	return lastCapturedSeq(env.Fake.Snapshot()), p.stopAndCheck(ctx, env, 1)
}
func hasCompletedModAckFor(s fake.Snapshot, afterSeq int64, ackID string) int {
	n := 0
	for _, c := range modAckCallsAfterSeq(s, afterSeq) {
		if c.Completed && slices.Contains(c.ModAckReq.GetAckIds(), ackID) {
			if c.ModAckReq.GetAckDeadlineSeconds() == 0 {
				return 999
			}
			n++
		}
	}
	return n
}

type lease02Phase struct {
	label, pfx, aid, mid, sub       string
	minMs, maxMs, extMs, lo, wmPost int64
	want                            int
	to                              time.Duration
}

func runLEASE02(ctx context.Context, env *harness.ScenarioEnv) error {
	waitB2 := map[bool]time.Duration{true: 150 * time.Millisecond, false: 12 * time.Second}[boundedMaxExtMs(env) < 1500]
	phases := []lease02Phase{
		{"A", "lease02-a-sub", "ack-lease02-1", "msg-lease02-1", "", 10_000, 15_000, 60_000, 0, 0, 2, 0},
		{"B1", "lease02-low-sub", "ack-lease02-2", "msg-lease02-2", "", 2_000, 2_000, 60_000, 0, 0, 1, 0},
		{"B2", "lease02-high-sub", "ack-lease02-3", "msg-lease02-3", "", 10_000, 700_000, 120_000, 0, 0, 1, waitB2},
	}
	for i := range phases {
		phases[i].sub, phases[i].lo = env.SubscriptionPath(env.UniqueName(phases[i].pfx)), lastCapturedSeq(env.Fake.Snapshot())
		wmPost, err := startLease02Phase(ctx, env, phases[i].sub, phases[i].minMs, phases[i].maxMs, phases[i].extMs, i+1, phases[i].aid, phases[i].mid, phases[i].want, phases[i].to)
		if err != nil {
			return err
		}
		phases[i].wmPost = wmPost
	}
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	v.Check(fake.AssertAllNackedIDs(snap))
	for i, ph := range phases {
		for _, c := range modAckCallsAfterSeq(snap, ph.wmPost) {
			if slices.Contains(c.ModAckReq.GetAckIds(), ph.aid) {
				v.Checkf("LEASE-02: unexpected post-Ack ModifyAckDeadline for %s (seq=%d)", ph.aid, c.Seq)
			}
		}
		var hi int64
		if i+1 < len(phases) {
			hi = phases[i+1].lo
		}
		calls := filterCalls(snap.Calls, func(c fake.CapturedCall) bool { return c.Seq > ph.lo && (hi == 0 || c.Seq <= hi) })
		checkSubInitialCalls(env, v, "LEASE-02-init-"+ph.label, ph.sub, calls)
		checkAckModAckMeta(env, v, ph.sub, calls)
	}
	checkSubStreamRPCs(env, v, snap, "", "ack-lease02-1", "ack-lease02-2", "ack-lease02-3")
	return finishPublishScenario(ctx, env, v)
}
func runLEASE03(ctx context.Context, env *harness.ScenarioEnv) error {
	maxExtMs, lang, autoNack := lease03MaxExtMs(env), strings.ToLower(env.Language()), env.HasCapability(harness.CapAutoNackOnDrop)
	sub := env.SubscriptionPath(env.UniqueName("lease03-sub"))
	cmd := interactiveCmd(env, env.Endpoint(), sub)
	cmd.MaxExtensionMs, cmd.MaxDurationPerAckExtensionMs, cmd.MaxOutstandingMessages = proto.Int64(maxExtMs), proto.Int64(10_000), proto.Int64(map[bool]int64{true: 2, false: 1}[autoNack || lang == "ruby"])
	p, sh1, err := startSubAndStream(ctx, env, cmd, 1, "LEASE-03 stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	md1, err := pushRecvOne(ctx, env, p, sh1, "ack-lease03-1", "msg-lease03-1", []byte("lease03-1"))
	if err != nil {
		return err
	}
	if lang == "go" {
		if err := sendSettleAction(env, p, md1.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE); err != nil {
			return err
		}
	}
	maxExtDur := time.Duration(maxExtMs) * time.Millisecond
	if err := sleepCtx(ctx, maxExtDur+min(max(maxExtDur/4, 25*time.Millisecond), 500*time.Millisecond)); err != nil {
		return err
	}
	wmAfterExpiry := lastCapturedSeq(env.Fake.Snapshot())
	postExpPred := func(s fake.Snapshot) bool {
		return slices.ContainsFunc(modAckCallsAfterSeq(s, wmAfterExpiry), func(c fake.CapturedCall) bool {
			return c.ModAckReq.GetAckDeadlineSeconds() != 0 && slices.Contains(c.ModAckReq.GetAckIds(), "ack-lease03-1")
		})
	}
	postExpEnd, d2 := time.Now().Add(min(max(maxExtDur*5, 220*time.Millisecond), 10*time.Second)), []byte("lease03-2")
	if err := pushOneMsg(sh1, "ack-lease03-2", "msg-lease03-2", d2); err != nil {
		return err
	}
	md2, err := recvDeliveryWatchingFake(ctx, env, p, postExpPred, "LEASE-03: unexpected ModifyAckDeadline for ack-lease03-1 after max_extension_ms")
	if err != nil {
		return err
	}
	if err := assertDeliveredMsg(md2, "msg-lease03-2", d2, nil, ""); err != nil {
		return err
	}
	wCtx, wCancel := context.WithTimeout(ctx, max(time.Until(postExpEnd), min(max(maxExtDur, 50*time.Millisecond), 500*time.Millisecond)))
	werr := waitFakeOrStop(wCtx, env, p, postExpPred, "LEASE-03 post-expiry")
	wCancel()
	if werr == nil {
		return env.Failf("LEASE-03: unexpected ModifyAckDeadline for ack-lease03-1 after max_extension_ms")
	}
	if ctx.Err() != nil || !errors.Is(werr, context.DeadlineExceeded) {
		return werr
	}
	if err := ackAndWait(ctx, env, p, md2.GetDeliveryToken(), "ack-lease03-2", "LEASE-03 ack2"); err != nil {
		return err
	}
	wantAcked := []string{"ack-lease03-2"}
	switch lang {
	case "cpp", "dotnet":
		if err := ackAndWait(ctx, env, p, md1.GetDeliveryToken(), "ack-lease03-1", "LEASE-03 ack1"); err != nil {
			return err
		}
		wantAcked = []string{"ack-lease03-1", "ack-lease03-2"}
	case "rust", "ruby":
		if err := sendSettleAction(env, p, md1.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE); err != nil {
			return err
		}
	}
	if err := p.stopAndCheck(ctx, env, 2); err != nil {
		return err
	}
	snap, v := env.Fake.Snapshot(), env.NewVerifier()
	minDL := map[bool]int32{true: 1, false: fake.MinLeaseExtensionSeconds}[lang == "java" || lang == "cpp"]
	if lang == "go" || lang == "cpp" || lang == "rust" {
		v.Require(slices.ContainsFunc(snap.ModAckCalls(), func(c fake.CapturedCall) bool {
			dl := c.ModAckReq.GetAckDeadlineSeconds()
			return c.Seq <= wmAfterExpiry && dl >= minDL && dl <= fake.MaxLeaseExtensionSeconds && slices.Contains(c.ModAckReq.GetAckIds(), "ack-lease03-1")
		}), "LEASE-03: expected >= 1 pre-expiry ModifyAckDeadline for ack-lease03-1")
	}
	v.Check(fake.AssertAllNackedIDs(snap))
	v.Require(!postExpPred(snap), "LEASE-03: unexpected ModifyAckDeadline for ack-lease03-1 after max_extension_ms")
	return finishSubStreamLeaseScenario(ctx, env, v, snap, sub, "LEASE-03", minDL, wantAcked...)
}
func runLEASE04(ctx context.Context, env *harness.ScenarioEnv) error {
	sub := env.SubscriptionPath(env.UniqueName("lease04-sub"))
	env.Fake.AddModAckRule(fake.ModAckRule{Name: "LEASE-04-modack-fail", Match: fake.MatchModAckIDsContaining("lease04-m0"), Times: 1, Err: status.Error(codes.InvalidArgument, "alo-modack-fail")})
	env.Fake.AddAckRule(fake.AckRule{Name: "LEASE-04-ack-fail", Match: fake.MatchAckID("lease04-m1"), Times: 1, Err: status.Error(codes.InvalidArgument, "alo-ack-fail")})
	cmd := interactiveCmd(env, env.Endpoint(), sub)
	cmd.MinDurationPerAckExtensionMs, cmd.MaxDurationPerAckExtensionMs, cmd.MaxExtensionMs = proto.Int64(10_000), proto.Int64(10_000), proto.Int64(30_000)
	p, sh1, err := startSubAndStream(ctx, env, cmd, 1, "LEASE-04 stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	md0, err := pushRecvOne(ctx, env, p, sh1, "lease04-m0", "msg-l04-0", []byte("msg-l04-0"))
	if err != nil {
		return err
	}
	if err := waitFakeOrStop(ctx, env, p, func(s fake.Snapshot) bool {
		return slices.ContainsFunc(s.ModAckCalls(), func(c fake.CapturedCall) bool {
			return c.Completed && c.StatusCode == codes.InvalidArgument && slices.Contains(c.ModAckReq.GetAckIds(), "lease04-m0")
		})
	}, "LEASE-04 failing ModAck"); err != nil {
		return err
	}
	if err := ackAndWait(ctx, env, p, md0.GetDeliveryToken(), "lease04-m0", "LEASE-04 ack m0"); err != nil {
		return err
	}
	tooManyStreams := func(s fake.Snapshot) bool { return len(s.Streams) != 1 || s.Streams[0].Closed }
	for _, step := range []struct {
		aid, mid string
		wantCode codes.Code
	}{{"lease04-m1", "msg-l04-1", codes.InvalidArgument}, {"lease04-m2", "msg-l04-2", codes.OK}} {
		if err := pushOneMsg(sh1, step.aid, step.mid, []byte(step.mid)); err != nil {
			if ctx.Err() == nil {
				return env.Failf("LEASE-04 expected stream survival (stream closed): %v", err)
			}
			return err
		}
		md, rerr := recvDeliveryWatchingFake(ctx, env, p, tooManyStreams, "LEASE-04 expected stream survival (1 stream)")
		if rerr != nil {
			return rerr
		}
		if serr := sendSettleAction(env, p, md.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_ACK); serr != nil {
			return serr
		}
		if werr := waitFakeOrStop(ctx, env, p, func(s fake.Snapshot) bool {
			return slices.ContainsFunc(s.AckCalls(), func(c fake.CapturedCall) bool {
				return c.Completed && c.StatusCode == step.wantCode && slices.Contains(c.AckReq.GetAckIds(), step.aid)
			})
		}, "LEASE-04 "+step.aid); werr != nil {
			return werr
		}
	}
	if pre := env.Fake.Snapshot(); len(pre.Streams) != 1 || pre.Streams[0].Closed || len(env.Fake.ActiveStreams()) != 1 {
		return env.Failf("LEASE-04 expected stream survival (1 open stream before stop), got %d closed=%v", len(pre.Streams), len(pre.Streams) > 0 && pre.Streams[0].Closed)
	}
	return stopVerifyAndFinishStream(ctx, env, p, 3, "LEASE-04", sub, 0, 1, "lease04-m0", "lease04-m1", "lease04-m2")
}
func runLEASE05(ctx context.Context, env *harness.ScenarioEnv) error {
	sub, maxExtMs, lang := env.SubscriptionPath(env.UniqueName("lease05-sub")), boundedMaxExtMs(env), strings.ToLower(env.Language())
	cmd := interactiveCmd(env, env.Endpoint(), sub)
	cmd.MaxExtensionMs = proto.Int64(maxExtMs)
	if lang == "node" {
		cmd.MaxDurationPerAckExtensionMs = proto.Int64(10_000)
	}
	p, sh1, err := startSubAndStream(ctx, env, cmd, 1, "LEASE-05 stream 1")
	if err != nil {
		return err
	}
	defer p.close(ctx, env)
	md1, err := pushRecvOne(ctx, env, p, sh1, "ack-lease05-1", "msg-lease05-1", []byte("lease05-1"))
	if err != nil {
		return err
	}
	if err := checkNoPrematureAck(ctx, env.Fake, md1, 15*time.Millisecond); err != nil {
		return err
	}
	if err := sendSettleAction(env, p, md1.GetDeliveryToken(), proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE); err != nil {
		return err
	}
	v, autoNack := env.NewVerifier(), env.HasCapability(harness.CapAutoNackOnDrop)
	if autoNack {
		dl, _ := ctx.Deadline()
		wCtx, wCancel := context.WithTimeout(ctx, min(max(time.Until(dl)/3, 30*time.Millisecond), 300*time.Millisecond))
		_ = env.Fake.WaitFor(wCtx, func(s fake.Snapshot) bool {
			return slices.ContainsFunc(s.ModAckCalls(), func(c fake.CapturedCall) bool {
				return c.Completed && c.ModAckReq.GetAckDeadlineSeconds() == 0 && slices.Contains(c.ModAckReq.GetAckIds(), "ack-lease05-1")
			})
		})
		wCancel()
	} else {
		if err := sleepCtx(ctx, 20*time.Millisecond); err != nil {
			return err
		}
		v.Require(fake.AssertAllNackedIDs(env.Fake.Snapshot()) == nil, "LEASE-05: unexpected nacked ack_ids after RETURN_WITHOUT_SETTLE")
		if lang == "node" {
			pad := map[bool]time.Duration{true: 9200 * time.Millisecond, false: 50 * time.Millisecond}[maxExtMs >= 1500]
			if err := sleepCtx(ctx, time.Duration(maxExtMs)*time.Millisecond+pad); err != nil {
				return err
			}
		}
	}
	if err := pushRecvAckOne(ctx, env, p, sh1, "ack-lease05-2", "msg-lease05-2", []byte("lease05-2"), 0, "LEASE-05 ack2"); err != nil {
		return err
	}
	if err := p.stopAndCheck(ctx, env, 2); err != nil {
		return err
	}
	snap := env.Fake.Snapshot()
	if autoNack {
		v.Check(fake.AssertAllNackedIDs(snap, "ack-lease05-1"))
		nacks := filterCalls(snap.ModAckCalls(), func(c fake.CapturedCall) bool { return c.ModAckReq.GetAckDeadlineSeconds() == 0 })
		v.Require(len(nacks) >= 1, "LEASE-05 expected ModifyAckDeadline(0) nack for ack-lease05-1")
		for _, c := range nacks {
			v.Check(fake.ExpectModAck("LEASE-05-nack", fake.ModAckSubscription(sub), fake.ModAckIDsExact("ack-lease05-1"), fake.ModAckDeadline(0), fake.ModAckRequestParams(sub, allowUnencodedRequestParams(env)), fake.ModAckStatusCode(codes.OK)).Check(c))
		}
	} else {
		v.Check(fake.AssertAllNackedIDs(snap))
	}
	return finishSubStreamLeaseScenario(ctx, env, v, snap, sub, "LEASE-05", map[bool]int32{true: 1, false: fake.MinLeaseExtensionSeconds}[lang == "java" || lang == "cpp"], "ack-lease05-2")
}
func finishSubStreamLeaseScenario(ctx context.Context, env *harness.ScenarioEnv, v *fake.Verifier, snap fake.Snapshot, sub, label string, minDL int32, wantAcked ...string) error {
	v.Check(m3cCheckClientIDs(env, snap))
	checkSubInitialCalls(env, v, label+"-init", sub, snap.Calls)
	checkAckModAckMeta(env, v, sub, snap.Calls)
	v.Check(fake.AssertAllAckedIDs(snap, wantAcked...))
	v.Check(fake.AssertNoUnexpectedRPCs(snap, fake.MethodStreamingPull, fake.MethodAcknowledge, fake.MethodModifyAckDeadline))
	for _, c := range snap.Calls {
		switch {
		case c.AckReq != nil:
			v.Check(fake.AckWithinBounds()(c))
		case c.ModAckReq != nil:
			v.Check(fake.ModAckWithinBounds()(c))
			if c.ModAckReq.GetAckDeadlineSeconds() != 0 {
				v.Check(fake.ModAckDeadlineRange(minDL, fake.MaxLeaseExtensionSeconds)(c))
			}
		case c.StreamingPullReq != nil:
			req := c.StreamingPullReq
			aN, mN := len(req.GetAckIds()), len(req.GetModifyDeadlineAckIds())
			v.Require(mN == len(req.GetModifyDeadlineSeconds()) && aN+mN <= fake.MaxAckIDsPerRequest && proto.Size(req) <= fake.MaxAckRequestBytes, "seq=%d: StreamingPull bounds exceeded", c.Seq)
			sdl := req.GetStreamAckDeadlineSeconds()
			v.Require(sdl == 0 || (sdl >= fake.MinLeaseExtensionSeconds && sdl <= fake.MaxLeaseExtensionSeconds), "seq=%d: stream_ack_deadline_seconds=%d outside [10, 600]", c.Seq, sdl)
			for _, dl := range req.GetModifyDeadlineSeconds() {
				v.Require(dl == 0 || (dl >= minDL && dl <= fake.MaxLeaseExtensionSeconds), "seq=%d: modify_deadline_seconds=%d outside [%d, %d]", c.Seq, dl, minDL, fake.MaxLeaseExtensionSeconds)
			}
		}
	}
	return finishPublishScenario(ctx, env, v)
}
