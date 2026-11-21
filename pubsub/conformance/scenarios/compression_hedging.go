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
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func compressionHedgingScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "CMP-01", Name: "Publish Compression & Threshold Boundaries", Category: "compression", Feature: "CMP-01", Description: "Verify publish compression defaults off, obeys explicit enable_compression=false, uses gzip/deflate above threshold, and enforces > vs >= threshold boundaries.", RequiredCapabilities: []harness.Capability{harness.CapPublishCompression}, Run: runCMP01},
		{ID: "HDG-01", Name: "Publish Hedging Validation, Clamping & Token Bucket", Category: "hedging", Feature: "HDG-01", Description: "Verify out-of-range hedging settings reject or clamp per SDK, bucket starts at 0, refills at refill_ratio, and caps at max_tokens.", RequiredCapabilities: []harness.Capability{harness.CapPublishHedging}, IsolateProxy: true, Run: runHDG01},
		{ID: "HDG-02", Name: "Publish Hedged Execution, Cancellation & Error Isolation", Category: "hedging", Feature: "HDG-02", Description: "Verify slow primary triggers hedge winner cancelling primary, fast primary avoids hedges, losing hedge errors are discarded, and all-fail returns cleanly.", RequiredCapabilities: []harness.Capability{harness.CapPublishHedging}, IsolateProxy: true, Run: runHDG02},
		{ID: "HDG-03", Name: "Publish Hedging Telemetry Header Sequence", Category: "hedging", Feature: "HDG-03", Description: "Verify x-goog-pubsub-client-telemetry reports sequential hedged_attempt_count [0,1,2] and identical publish_start_time across attempts.", RequiredCapabilities: []harness.Capability{harness.CapPublishHedging}, Run: runHDG03},
		{ID: "HDG-04", Name: "Publish Hedging & Ordering Conflict Semantics", Category: "hedging", Feature: "HDG-04", Description: "Verify hedging+ordering rejects when opt-in ordering is supported or suppresses hedging only for keyed publishes on unified ordering clients.", RequiredCapabilities: []harness.Capability{harness.CapPublishHedging}, Run: runHDG04},
	}
}

func runCMP01(ctx context.Context, env *harness.ScenarioEnv) error {
	if env.Defaults().GetPublishCompressionEnabledByDefault() {
		return env.Failf("expected publish compression disabled by default, got enabled")
	}
	topic, allowUnencoded := env.TopicPath(env.UniqueName("cmp01-topic")), allowUnencodedRequestParams(env)
	lang := strings.ToLower(env.Language())
	wantCodec, wantAlg := "gzip", proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP
	if lang == "cpp" {
		wantCodec, wantAlg = "deflate", proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE
	}
	if defAlg := env.Defaults().GetPublishCompressionAlgorithm(); defAlg != proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_UNSPECIFIED && defAlg != wantAlg {
		return env.Failf("defaults compression algorithm mismatch for %s: want %s, got %s", env.Language(), wantAlg, defAlg)
	}

	type cmpCase struct {
		label, codec string
		data         []byte
	}
	var allIDs []string
	var exps []cmpCase
	runBatch := func(req *proxypb.CreatePublisherRequest, cases ...cmpCase) error {
		pubID, err := createPublisher(ctx, env, req)
		if err != nil {
			return err
		}
		defer cleanupPublisher(ctx, env, &pubID)
		for _, c := range cases {
			id, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: c.data})
			if err != nil {
				return fmt.Errorf("%s: %w", c.label, err)
			}
			allIDs, exps = append(allIDs, id), append(exps, c)
		}
		return closePublisherCleared(ctx, env, &pubID)
	}

	if err := runBatch(newPubReq(env, topic, 1, 0, 10), cmpCase{"CMP-01-default-off", "", paddedPayload("cmp01-def:", 700)}); err != nil {
		return err
	}
	reqOff := newPubReq(env, topic, 1, 0, 10)
	reqOff.EnableCompression, reqOff.CompressionBytesThreshold = proto.Bool(false), proto.Int64(100)
	if err := runBatch(reqOff, cmpCase{"CMP-01-explicit-off", "", paddedPayload("cmp01-off:", 700)}); err != nil {
		return err
	}

	reqOn := newPubReq(env, topic, 1, 0, 10)
	reqOn.EnableCompression, reqOn.CompressionBytesThreshold = proto.Bool(true), proto.Int64(500)
	onCases := []cmpCase{
		{"CMP-01-below-100", "", paddedPayload("cmp01-b100:", 100)},
		{"CMP-01-below-350", "", paddedPayload("cmp01-m350:", 350)},
		{"CMP-01-above-700", wantCodec, paddedPayload("cmp01-a700:", 700)},
	}
	switch lang {
	case "go":
		onCases = append(onCases, cmpCase{"CMP-01-go-eq-500", "", paddedPayload("cmp01-g500:", 497)}, cmpCase{"CMP-01-go-gt-501", wantCodec, paddedPayload("cmp01-g501:", 498)})
	case "php", "dotnet":
		onCases = append(onCases, cmpCase{"CMP-01-lt-499", "", paddedPayload("cmp01-p499:", 496)}, cmpCase{"CMP-01-ge-500", wantCodec, paddedPayload("cmp01-p500:", 497)})
	}
	if err := runBatch(reqOn, onCases...); err != nil {
		return err
	}
	if lang == "cpp" {
		reqCpp := newPubReq(env, topic, 1, 0, 10)
		reqCpp.EnableCompression, reqCpp.CompressionBytesThreshold = proto.Bool(true), proto.Int64(520)
		if err := runBatch(reqCpp, cmpCase{"CMP-01-cpp-lt-519", "", paddedPayload("cmp01-c519:", 499)}, cmpCase{"CMP-01-cpp-ge-520", "deflate", paddedPayload("cmp01-c520:", 500)}); err != nil {
			return err
		}
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	seq := make([]fake.CallExpectation, len(exps))
	for i, c := range exps {
		seq[i] = fake.ExpectPublish(c.label, fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishCompression(c.codec), fake.PublishSingleMessage(fake.ExpectedMessage{Data: c.data, Attributes: map[string]string{}}))
	}
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), seq...))
	v.Check(checkOKPublishMessageIDsMatch(allIDs, snap))
	return finishPublishScenario(ctx, env, v)
}

func newHedgePubReq(env *harness.ScenarioEnv, topic string, maxTokens int64) *proxypb.CreatePublisherRequest {
	req := newPubReq(env, topic, 1, 0, 10)
	req.TotalTimeoutMs = proto.Int64(15_000)
	req.Hedging = &proxypb.HedgingSettings{Enabled: true, DelayMs: proto.Int64(100), MaxTokens: proto.Int64(maxTokens), RefillRatio: proto.Float64(0.2)}
	return req
}

func hedgeRule(env *harness.ScenarioEnv, name, substr string, attempt int32, gte bool, times int, gate *fake.Gate, msgIDs []string, err error) {
	attMatch := fake.MatchHedgedAttempt(attempt)
	if gte {
		attMatch = fake.MatchHedgedAttemptGTE(attempt)
	}
	env.Fake.AddPublishRule(fake.PublishRule{
		Name: name, Match: fake.MatchPublishAll(fake.MatchPayloadContains([]byte(substr)), attMatch),
		Times: times, Gate: gate, MessageIDs: msgIDs, Err: err,
	})
}

func publishGatedNoHedge(ctx context.Context, env *harness.ScenarioEnv, pubID, data, key, ruleName, op string, allIDs *[]string, allData *[][]byte) (int64, error) {
	g := fake.NewGate()
	if key != "" {
		env.Fake.AddPublishRule(fake.PublishRule{Name: ruleName, Match: fake.MatchOrderingKey(key), Times: 1, Gate: g})
	} else {
		hedgeRule(env, ruleName, data, 0, false, 1, g, nil, nil)
	}
	defer g.Release()
	wm := lastCapturedSeq(env.Fake.Snapshot())
	job := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte(data), OrderingKey: key})
	defer job.abort()
	if err := g.WaitEntered(ctx); err != nil {
		return 0, err
	}
	if err := assertStaysBlocked(ctx, env, wm, 1, job.done, 220*time.Millisecond, op); err != nil {
		return 0, err
	}
	g.Release()
	id, err := job.awaitOK(ctx)
	if err != nil {
		return 0, err
	}
	*allIDs, *allData = append(*allIDs, id), append(*allData, []byte(data))
	return wm, nil
}

func runHDG01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("hdg01-topic")), allowUnencodedRequestParams(env)
	var allIDs []string
	var allData [][]byte

	// (a) Out-of-range settings validation vs Rust clamping.
	reqBad := newPubReq(env, topic, 1, 0, 10)
	reqBad.TotalTimeoutMs = proto.Int64(15_000)
	reqBad.Hedging = &proxypb.HedgingSettings{Enabled: true, DelayMs: proto.Int64(10), MaxTokens: proto.Int64(251), RefillRatio: proto.Float64(0.5)}
	if strings.EqualFold(env.Language(), "rust") {
		pubClamp, err := createPublisher(ctx, env, reqBad)
		if err != nil {
			return err
		}
		defer cleanupPublisher(ctx, env, &pubClamp)
		if err := publishSeqOK(ctx, env, pubClamp, "hdg01-c-pre", 2, &allIDs, &allData); err != nil {
			return err
		}
		if _, err := publishGatedNoHedge(ctx, env, pubClamp, "hdg01-c-3", "", "HDG-01-c3", "HDG-01 rust clamped 0.2 refill", &allIDs, &allData); err != nil {
			return err
		}
		if err := publishSeqOK(ctx, env, pubClamp, "hdg01-c-post", 2, &allIDs, &allData); err != nil {
			return err
		}
		wm6, err := runGatedHedgeWinner1(ctx, env, pubClamp, "HDG-01-c6", "hdg01-c-6", "HDG-01(a)", 1, 0, &allIDs, &allData)
		if err != nil {
			return err
		}
		if c6 := publishCallsAfterSeq(env.Fake.Snapshot(), wm6); c6[1].Timestamp.Sub(c6[0].Timestamp) < 85*time.Millisecond {
			return env.Failf("Rust hedged too early (%s < 85ms); expected 10ms clamped to 100ms", c6[1].Timestamp.Sub(c6[0].Timestamp))
		}
		if err := closePublisherCleared(ctx, env, &pubClamp); err != nil {
			return err
		}
	} else if err := expectRejectedPublisher(ctx, env, reqBad, "hdg01-bad", "invalid hedging settings"); err != nil {
		return err
	}

	// (b) Bucket starts at 0, refills at 0.2 (5 OKs = 1 token), caps at max_tokens=1.
	pubID, err := createPublisher(ctx, env, newHedgePubReq(env, topic, 1))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	if _, err := publishGatedNoHedge(ctx, env, pubID, "hdg01-slow1", "", "HDG-01-s1", "HDG-01 initial empty bucket", &allIDs, &allData); err != nil {
		return err
	}
	if err := publishSeqOK(ctx, env, pubID, "hdg01-fast", 3, &allIDs, &allData); err != nil {
		return err
	}
	if _, err := publishGatedNoHedge(ctx, env, pubID, "hdg01-slow5", "", "HDG-01-s5", "HDG-01 4 OKs below 1 token", &allIDs, &allData); err != nil {
		return err
	}

	wm6, err := runGate0HedgeWins(ctx, env, pubID, "HDG-01-s6", "hdg01-slow6", "", "HDG-01(b) slow6", &allIDs, &allData)
	if err != nil {
		return err
	}
	if c6 := publishCallsAfterSeq(env.Fake.Snapshot(), wm6); c6[1].Telemetry.GetPublishOperation().GetHedgedAttemptCount() < 1 {
		return env.Failf("HDG-01(b) expected hedge attempt >= 1 after 5 OK refills, got calls=%d", len(c6))
	}

	if err := publishSeqOK(ctx, env, pubID, "hdg01-fill", 10, &allIDs, &allData); err != nil {
		return err
	}
	if _, err := runGatedHedgeWinner1(ctx, env, pubID, "HDG-01-cap", "hdg01-cap", "HDG-01 max_tokens=1 cap", fake.RepeatAlways, 220*time.Millisecond, &allIDs, &allData); err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "HDG-01", topic, allowUnencoded, allIDs, allData)
}

func runGate0HedgeWins(ctx context.Context, env *harness.ScenarioEnv, pubID, rulePrefix, data, wantWinID, op string, allIDs *[]string, allData *[][]byte) (int64, error) {
	g0 := fake.NewGate()
	hedgeRule(env, rulePrefix+"-0", data, 0, false, 1, g0, nil, nil)
	if wantWinID != "" {
		hedgeRule(env, rulePrefix+"-1", data, 1, true, 1, nil, []string{wantWinID}, nil)
	}
	defer g0.Release()
	wm := lastCapturedSeq(env.Fake.Snapshot())
	id, err := pubRecord(ctx, env, pubID, []byte(data), allIDs, allData)
	if err != nil {
		return 0, err
	}
	if wantWinID != "" && id != wantWinID {
		return 0, env.Failf("%s message_id mismatch: want %q, got %q", op, wantWinID, id)
	}
	if err := waitHedgeLosersCanceled(ctx, env, wm, 2, op); err != nil {
		return 0, err
	}
	return wm, nil
}

func runGatedHedgeWinner1(ctx context.Context, env *harness.ScenarioEnv, pubID, rulePrefix, data, op string, repeat1 int, hold time.Duration, allIDs *[]string, allData *[][]byte) (int64, error) {
	g0, g1 := fake.NewGate(), fake.NewGate()
	hedgeRule(env, rulePrefix+"-0", data, 0, false, 1, g0, nil, nil)
	hedgeRule(env, rulePrefix+"-1", data, 1, true, repeat1, g1, nil, nil)
	defer g0.Release()
	defer g1.Release()
	wm := lastCapturedSeq(env.Fake.Snapshot())
	job := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte(data)})
	defer job.abort()
	if err := g0.WaitEntered(ctx); err != nil {
		return 0, err
	}
	if err := g1.WaitEntered(ctx); err != nil {
		return 0, err
	}
	if hold > 0 {
		if err := assertStaysBlocked(ctx, env, wm, 2, job.done, hold, op); err != nil {
			return 0, err
		}
	}
	g1.Release()
	id, err := job.awaitOK(ctx)
	if err != nil {
		return 0, err
	}
	*allIDs, *allData = append(*allIDs, id), append(*allData, []byte(data))
	if err := waitHedgeLosersCanceled(ctx, env, wm, 2, op); err != nil {
		return 0, err
	}
	return wm, nil
}

func waitHedgeAtt1Error(ctx context.Context, env *harness.ScenarioEnv, wm int64, code codes.Code) error {
	return env.Fake.WaitFor(ctx, func(snap fake.Snapshot) bool {
		c := publishCallsAfterSeq(snap, wm)
		return len(c) >= 2 && c[1].Completed && c[1].StatusCode == code
	})
}

func runHDG02(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("hdg02-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newHedgePubReq(env, topic, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	allIDs, allData := []string(nil), [][]byte(nil)
	if err := publishSeqOK(ctx, env, pubID, "hdg02-pre", 30, &allIDs, &allData); err != nil {
		return err
	}

	// (a) Slow primary -> hedge wins -> primary CanceledByClient -> winner ID returned.
	if _, err := runGate0HedgeWins(ctx, env, pubID, "HDG-02-a", "hdg02-a", "hedge-win-id", "HDG-02(a)", &allIDs, &allData); err != nil {
		return err
	}

	// (b) Fast primary wins -> no hedge fires.
	wmB := lastCapturedSeq(env.Fake.Snapshot())
	if _, err := pubRecord(ctx, env, pubID, []byte("hdg02-b"), &allIDs, &allData); err != nil {
		return err
	}
	if err := sleepCtx(ctx, 150*time.Millisecond); err != nil {
		return err
	}
	if callsB := publishCallsAfterSeq(env.Fake.Snapshot(), wmB); len(callsB) != 1 {
		return env.Failf("HDG-02(b) fast primary triggered %d RPCs, want 1", len(callsB))
	}

	// (c) Losing hedge fails (Internal) -> primary succeeds cleanly.
	gPrimary := fake.NewGate()
	hedgeRule(env, "HDG-02-c0", "hdg02-c", 0, false, 1, gPrimary, []string{"primary-win-id"}, nil)
	hedgeRule(env, "HDG-02-c1", "hdg02-c", 1, true, fake.RepeatAlways, nil, nil, status.Error(codes.Internal, "losing hedge failed"))
	defer gPrimary.Release()
	wmC := lastCapturedSeq(env.Fake.Snapshot())
	jobC := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("hdg02-c")})
	defer jobC.abort()
	if err := gPrimary.WaitEntered(ctx); err != nil {
		return err
	}
	if err := waitHedgeAtt1Error(ctx, env, wmC, codes.Internal); err != nil {
		return err
	}
	tC := time.NewTimer(50 * time.Millisecond)
	defer tC.Stop()
	select {
	case <-jobC.done:
		return env.Failf("HDG-02(c) batch ended prematurely after losing hedge failed: %v", jobC.err)
	case <-ctx.Done():
		return ctx.Err()
	case <-tC.C:
	}
	gPrimary.Release()
	idC, err := jobC.awaitOK(ctx)
	if err != nil {
		return err
	}
	if idC != "primary-win-id" {
		return env.Failf("HDG-02(c) message_id mismatch: want %q, got %q", "primary-win-id", idC)
	}
	allIDs, allData = append(allIDs, idC), append(allData, []byte("hdg02-c"))

	// (d) All attempts fail cleanly without hang.
	gAllFail := fake.NewGate()
	hedgeRule(env, "HDG-02-d0", "hdg02-d", 0, false, 1, gAllFail, nil, status.Error(codes.PermissionDenied, "perm 0"))
	hedgeRule(env, "HDG-02-d1", "hdg02-d", 1, true, fake.RepeatAlways, nil, nil, status.Error(codes.PermissionDenied, "perm hedge"))
	defer gAllFail.Release()
	wmD := lastCapturedSeq(env.Fake.Snapshot())
	jobD := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("hdg02-d")})
	defer jobD.abort()
	if err := waitHedgeAtt1Error(ctx, env, wmD, codes.PermissionDenied); err != nil {
		return err
	}
	gAllFail.Release()
	respD, err := jobD.awaitResp(ctx)
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, respD, "all-failed hedged publish"); err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "HDG-02", topic, allowUnencoded, allIDs, allData)
}

func runHDG03(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("hdg03-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newHedgePubReq(env, topic, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	allIDs, allData := []string(nil), [][]byte(nil)
	if err := publishSeqOK(ctx, env, pubID, "hdg03-pre", 10, &allIDs, &allData); err != nil {
		return err
	}

	wm := lastCapturedSeq(env.Fake.Snapshot())
	g0, g1 := fake.NewGate(), fake.NewGate()
	hedgeRule(env, "HDG-03-0", "hdg03-msg", 0, false, 1, g0, nil, nil)
	hedgeRule(env, "HDG-03-1", "hdg03-msg", 1, false, 1, g1, nil, nil)
	hedgeRule(env, "HDG-03-2", "hdg03-msg", 2, false, 1, nil, []string{"hedge-2-win"}, nil)
	defer g0.Release()
	defer g1.Release()

	winID, err := pubRecord(ctx, env, pubID, []byte("hdg03-msg"), &allIDs, &allData)
	if err != nil {
		return err
	}
	if winID != "hedge-2-win" {
		return env.Failf("HDG-03 message_id mismatch: want %q, got %q", "hedge-2-win", winID)
	}
	if err := waitHedgeLosersCanceled(ctx, env, wm, 3, "HDG-03"); err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyTopicPublishRPCs(v, "HDG-03", topic, allowUnencoded, snap.PublishCalls())
	v.Check(fake.AssertTelemetryHedgeSequence(fake.Snapshot{Calls: publishCallsAfterSeq(snap, wm)}, []int32{0, 1, 2}, true))
	if calls3 := publishCallsAfterSeq(snap, wm); len(calls3) > 0 {
		if ts := calls3[0].Telemetry.GetPublishOperation().GetPublishStartTime(); ts != nil && (ts.AsTime().IsZero() || (ts.GetSeconds() == 0 && ts.GetNanos() == 0)) {
			v.Checkf("HDG-03 publish_start_time is zero/epoch: %v", ts.AsTime())
		}
	}
	v.Check(checkOKPublishMessageIDsMatch(allIDs, snap))
	v.Check(checkOKPublishPayloadsMatch(allData, snap))
	return finishPublishScenario(ctx, env, v)
}

func runHDG04(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("hdg04-topic")), allowUnencodedRequestParams(env)
	req := newHedgePubReq(env, topic, 10)
	setOrderingOptIn(env, req, true)

	if env.HasCapability(harness.CapOrderingOptIn) {
		if err := expectRejectedPublisher(ctx, env, req, "hdg04-conflict", "hedging + ordering conflict"); err != nil {
			return err
		}
		return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
	}

	// Unified ordering without opt-in (Rust): unkeyed hedges, keyed suppresses hedging.
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	allIDs, allData := []string(nil), [][]byte(nil)
	if err := publishSeqOK(ctx, env, pubID, "hdg04-pre", 10, &allIDs, &allData); err != nil {
		return err
	}

	if _, err := runGate0HedgeWins(ctx, env, pubID, "HDG-04-u", "hdg04-unkeyed", "hdg04-unkeyed-win", "HDG-04", &allIDs, &allData); err != nil {
		return err
	}

	wmK, err := publishGatedNoHedge(ctx, env, pubID, "hdg04-k1", "k1", "HDG-04-k1", "HDG-04 keyed non-hedging", &allIDs, &allData)
	if err != nil {
		return err
	}
	if callsK := publishCallsAfterSeq(env.Fake.Snapshot(), wmK); len(callsK) != 1 || callsK[0].TelemetryDecodeErr != "" || callsK[0].Telemetry.GetPublishOperation().GetHedgedAttemptCount() != 0 {
		return env.Failf("HDG-04 keyed publish hedged unexpectedly: count=%d", len(callsK))
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "HDG-04", topic, allowUnencoded, allIDs, allData)
}
