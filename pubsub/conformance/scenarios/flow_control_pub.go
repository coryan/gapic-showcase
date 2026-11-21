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
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func publishFlowControlScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PFC-01", Name: "Publish Flow Control Ignore Overflow", Category: "flow_control", Feature: "PFC-01", Description: "Verify FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE permits >10x message and byte overflow without blocking or failing.", RequiredCapabilities: []harness.Capability{harness.CapPublishFlowControl}, Run: runPFC01},
		{ID: "PFC-02", Name: "Publish Flow Control Block & Release", Category: "flow_control", Feature: "PFC-02", Description: "Verify FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK suspends on count and byte limits until in-flight batches drain, releases permits cleanly, and handles idle oversized messages per SDK policy.", RequiredCapabilities: []harness.Capability{harness.CapPublishFlowControl}, Run: runPFC02},
		{ID: "PFC-03", Name: "Publish Flow Control SignalError Non-Leak", Category: "flow_control", Feature: "PFC-03", Description: "Verify FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR fails fast without wire RPCs on count/byte overflow, leaks zero permits, and handles idle oversized messages per SDK policy.", RequiredCapabilities: []harness.Capability{harness.CapPublishFlowControl}, Run: runPFC03},
		{ID: "PFC-03b", Name: "Publish Flow Control SignalError Ordering Divergence", Category: "flow_control", Feature: "PFC-03b", Description: "Verify ordered SignalError overflow pauses only the affected key on Go/Java/Ruby (recoverable via ResumePublish) while C++/Python reject without pausing.", RequiredCapabilities: []harness.Capability{harness.CapPublishFlowControl}, Run: runPFC03b},
	}
}

func newFCPubReq(env *harness.ScenarioEnv, topic string, maxMsgs, maxBytes int64, beh proxypb.FlowControlLimitBehavior) *proxypb.CreatePublisherRequest {
	req := newPubReq(env, topic, 1, 0, 10)
	req.MaxOutstandingMessages = proto.Int64(maxMsgs)
	req.MaxOutstandingBytes = proto.Int64(maxBytes)
	req.LimitExceededBehavior = beh
	return req
}

func fcHoldAtGate(ctx context.Context, env *harness.ScenarioEnv, topic, rule, substr string, maxMsgs, maxBytes int64, beh proxypb.FlowControlLimitBehavior, times int, payloads [][]byte) (pubID string, g *fake.Gate, wm int64, toks []string, err error) {
	g = fake.NewGate()
	env.Fake.AddPublishRule(fake.PublishRule{Name: rule, Match: fake.MatchPayloadContains([]byte(substr)), Times: times, Gate: g})
	pubID, err = createPublisher(ctx, env, newFCPubReq(env, topic, maxMsgs, maxBytes, beh))
	if err != nil {
		g.Release()
		return "", nil, 0, nil, err
	}
	wm = lastCapturedSeq(env.Fake.Snapshot())
	for i, data := range payloads {
		tok, pErr := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: data})
		if pErr != nil {
			g.Release()
			cleanupPublisher(ctx, env, &pubID)
			return "", nil, 0, nil, pErr
		}
		if wErr := g.WaitEnteredN(ctx, i+1); wErr != nil {
			g.Release()
			cleanupPublisher(ctx, env, &pubID)
			return "", nil, 0, nil, fmt.Errorf("waiting for %s[%d] at gate: %w", rule, i, wErr)
		}
		toks = append(toks, tok)
	}
	return pubID, g, wm, toks, nil
}

func runPFC01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pfc01-topic")), allowUnencodedRequestParams(env)
	const totalOv = 12
	payloads := make([][]byte, totalOv)
	for i := range totalOv {
		payloads[i] = paddedPayload(fmt.Sprintf("pfc01-ov-%02d:", i), 100)
	}
	pubID, g1, _, heldToks, err := fcHoldAtGate(ctx, env, topic, "PFC-01-hold", "pfc01-ov-", 1, 100, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE, fake.RepeatAlways, payloads[:1])
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	defer g1.Release()

	toks := append(heldToks, make([]string, totalOv-1)...)
	start := time.Now()
	for i := 1; i < totalOv; i++ {
		toks[i], err = publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: payloads[i]})
		if err != nil {
			return err
		}
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		return env.Failf("IGNORE publish overflow took %s (>= 2s), expected non-blocking", elapsed)
	}
	if !strings.EqualFold(env.Language(), "ruby") {
		if err := g1.WaitEnteredN(ctx, 2); err != nil {
			return fmt.Errorf("expected >1 concurrent wire RPC in IGNORE mode: %w", err)
		}
	}
	g1.Release()

	ids, err := awaitAllPublishIDs(ctx, env, pubID, toks)
	if err != nil {
		return err
	}
	if _, err := pubRecord(ctx, env, pubID, []byte("pfc01-followup"), &ids, &payloads); err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "PFC-01", topic, allowUnencoded, ids, payloads)
}

func runPFC02(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pfc02-topic")), allowUnencodedRequestParams(env)
	var allIDs []string
	var allData [][]byte

	// (a) Count limit + release (maxMsgs=2 w/ 1-of-2 re-saturation; maxMsgs=1 on Ruby single-batch "").
	isRuby := strings.EqualFold(env.Language(), "ruby")
	maxA, holdA := int64(2), [][]byte{[]byte("pfc02-a-1"), []byte("pfc02-a-2")}
	if isRuby {
		maxA, holdA = 1, holdA[:1]
	}
	pubA, g1, wmA, toksA, err := fcHoldAtGate(ctx, env, topic, "PFC-02-cnt", "pfc02-a-", maxA, 1_000_000, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK, int(maxA), holdA)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubA)
	defer g1.Release()

	a3, a4, a5 := []byte("pfc02-a-3"), []byte("pfc02-a-4"), []byte("pfc02-a-5")
	jobA3 := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: a3})
	defer jobA3.abort()
	if err := assertStaysBlocked(ctx, env, wmA, int(maxA), jobA3.done, 120*time.Millisecond, "PFC-02 count limit m3"); err != nil {
		return err
	}
	unblockA := time.Now()
	g1.Release()
	idA3, err := jobA3.awaitOK(ctx)
	if err != nil {
		return fmt.Errorf("m3 after gate release: %w", err)
	}
	if elapsed := time.Since(unblockA); elapsed >= 3*time.Second {
		return env.Failf("m3 unblock took %s (>= 3s)", elapsed)
	}
	idsA12, err := awaitAllPublishIDs(ctx, env, pubA, toksA)
	if err != nil {
		return err
	}
	allIDs, allData = append(append(allIDs, idsA12...), idA3), append(append(allData, holdA...), a3)
	if isRuby {
		idA4, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: a4})
		if err != nil {
			return fmt.Errorf("m4 follow-up after permit release: %w", err)
		}
		allIDs, allData = append(allIDs, idA4), append(allData, a4)
	} else {
		g1b := fake.NewGate()
		env.Fake.AddPublishRule(fake.PublishRule{Name: "PFC-02-cnt-sat", Match: fake.MatchPayloadContains(a4), Times: 1, Gate: g1b})
		defer g1b.Release()
		tA4, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: a4})
		if err != nil {
			return err
		}
		if err := g1b.WaitEntered(ctx); err != nil {
			return fmt.Errorf("waiting for a-4 re-saturation at gate: %w", err)
		}
		jobA5 := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: a5})
		defer jobA5.abort()
		tA5 := time.NewTimer(600 * time.Millisecond)
		defer tA5.Stop()
		select {
		case <-jobA5.done:
			idA5, err := jobA5.awaitOK(ctx)
			if err != nil {
				return fmt.Errorf("m5 during a-4 hold: %w", err)
			}
			allIDs = append(allIDs, idA5)
		case <-tA5.C:
			return env.Failf("PFC-02(a) m5 blocked while only m4 held (partial 1-of-2 permit leak)")
		case <-ctx.Done():
			return ctx.Err()
		}
		g1b.Release()
		idA4, err := awaitPublishMessageID(ctx, env, pubA, tA4)
		if err != nil {
			return err
		}
		allIDs, allData = append(allIDs, idA4), append(allData, a4, a5)
	}
	if err := closePublisherCleared(ctx, env, &pubA); err != nil {
		return err
	}

	// (b) Byte limit + FIFO non-starvation + release.
	b1, b2, s1, b3 := paddedPayload("pfc02-b1:", 60), paddedPayload("pfc02-b2:", 60), paddedPayload("pfc02-s1:", 10), paddedPayload("pfc02-b3:", 90)
	pubB, g2, wmB, toksB1, err := fcHoldAtGate(ctx, env, topic, "PFC-02-bytes", "pfc02-b1:", 100, 120, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK, 1, [][]byte{b1})
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubB)
	defer g2.Release()

	jobB2 := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubB, Data: b2})
	defer jobB2.abort()
	if err := assertStaysBlocked(ctx, env, wmB, 1, jobB2.done, 120*time.Millisecond, "PFC-02 byte limit b2"); err != nil {
		return err
	}
	jobS1 := startSyncPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubB, Data: s1})
	defer jobS1.abort()
	if lang := strings.ToLower(env.Language()); lang == "go" || lang == "java" || lang == "ruby" {
		if err := assertStaysBlocked(ctx, env, wmB, 1, jobS1.done, 120*time.Millisecond, "PFC-02 FIFO non-bypass s1"); err != nil {
			return err
		}
	} else {
		env.Diagnosticf("PFC-02 FIFO non-bypass check skipped for %s (non-FIFO / lazy reservation)", lang)
	}
	unblockB := time.Now()
	g2.Release()
	idB2, err := jobB2.awaitOK(ctx)
	if err != nil {
		return fmt.Errorf("b2 after gate release: %w", err)
	}
	if elapsed := time.Since(unblockB); elapsed >= 3*time.Second {
		return env.Failf("b2 unblock took %s (>= 3s)", elapsed)
	}
	idS1, err := jobS1.awaitOK(ctx)
	if err != nil {
		return fmt.Errorf("s1 after gate release: %w", err)
	}
	idB1, err := awaitPublishMessageID(ctx, env, pubB, toksB1[0])
	if err != nil {
		return err
	}
	idB3, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubB, Data: b3})
	if err != nil {
		return fmt.Errorf("b3 follow-up after byte permit release: %w", err)
	}
	allIDs, allData = append(allIDs, idB1, idB2, idS1, idB3), append(allData, b1, b2, s1, b3)
	if err := closePublisherCleared(ctx, env, &pubB); err != nil {
		return err
	}

	if err := runIdleOversized(ctx, env, topic, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK, "pfc02", &allIDs, &allData); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "PFC-02", topic, allowUnencoded, allIDs, allData)
}

func expectFastFCLocalErr(ctx context.Context, env *harness.ScenarioEnv, pubID string, data []byte, key, op string) error {
	wm, start := lastCapturedSeq(env.Fake.Snapshot()), time.Now()
	if _, _, err := publishAsyncExpectErr(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: data, OrderingKey: key}); err != nil {
		return err
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		return env.Failf("%s took %s (>= 2s)", op, elapsed)
	}
	return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
}

func runSigOverflow(ctx context.Context, env *harness.ScenarioEnv, topic, rule, substr string, maxMsgs, maxBytes int64, first, ov, follow []byte, op string) ([]string, [][]byte, error) {
	pubID, g, _, toks, err := fcHoldAtGate(ctx, env, topic, rule, substr, maxMsgs, maxBytes, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR, 1, [][]byte{first})
	if err != nil {
		return nil, nil, err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	defer g.Release()
	if err := expectFastFCLocalErr(ctx, env, pubID, ov, "", op); err != nil {
		return nil, nil, err
	}
	g.Release()
	id1, err := awaitPublishMessageID(ctx, env, pubID, toks[0])
	if err != nil {
		return nil, nil, err
	}
	id3, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: follow})
	if err != nil {
		return nil, nil, fmt.Errorf("follow-up after %s release: %w", op, err)
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return nil, nil, err
	}
	return []string{id1, id3}, [][]byte{first, follow}, nil
}

func runPFC03(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pfc03-topic")), allowUnencodedRequestParams(env)
	idsA, dataA, err := runSigOverflow(ctx, env, topic, "PFC-03-cnt", "pfc03-a-1", 1, 1_000_000, []byte("pfc03-a-1"), []byte("pfc03-a-2"), []byte("pfc03-a-3"), "SIGNAL_ERROR count reject")
	if err != nil {
		return err
	}
	idsB, dataB, err := runSigOverflow(ctx, env, topic, "PFC-03-bytes", "pfc03-b1:", 100, 100, paddedPayload("pfc03-b1:", 60), paddedPayload("pfc03-b2:", 60), paddedPayload("pfc03-b3:", 75), "SIGNAL_ERROR byte reject")
	if err != nil {
		return err
	}
	allIDs, allData := append(idsA, idsB...), append(dataA, dataB...)

	if err := runIdleOversized(ctx, env, topic, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR, "pfc03", &allIDs, &allData); err != nil {
		return err
	}
	return finishSimplePublish(ctx, env, "PFC-03", topic, allowUnencoded, allIDs, allData)
}

func runIdleOversized(ctx context.Context, env *harness.ScenarioEnv, topic string, beh proxypb.FlowControlLimitBehavior, prefix string, allIDs *[]string, allData *[][]byte) error {
	pubID, err := createPublisher(ctx, env, newFCPubReq(env, topic, 10, 100, beh))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	over, follow := paddedPayload(prefix+"-over:", 200), paddedPayload(prefix+"-ok:", 20)
	lang := strings.ToLower(env.Language())
	allow := lang == "cpp" || (beh == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK && lang == "go")
	if allow {
		kind := map[bool]string{true: "BLOCK", false: "SIGNAL_ERROR"}[beh == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK]
		if _, err := pubRecord(ctx, env, pubID, over, allIDs, allData); err != nil {
			return fmt.Errorf("idle oversized %s on %s: %w", kind, env.Language(), err)
		}
	} else if beh == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK {
		if lang == "java" || lang == "python" || lang == "ruby" {
			wm := lastCapturedSeq(env.Fake.Snapshot())
			resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: over})
			if err != nil {
				return err
			}
			if err := checkExpectedInBandErr(env, resp, fmt.Sprintf("idle oversized BLOCK on %s", env.Language())); err != nil {
				return err
			}
			if err := fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm); err != nil {
				return err
			}
		} else {
			env.Diagnosticf("PFC-02 idle oversized policy skipped for language=%q", env.Language())
		}
	} else if lang == "go" || lang == "java" || lang == "python" || lang == "ruby" {
		if err := expectFastFCLocalErr(ctx, env, pubID, over, "", "idle oversized SIGNAL_ERROR"); err != nil {
			return err
		}
	}
	if _, err := pubRecord(ctx, env, pubID, follow, allIDs, allData); err != nil {
		return fmt.Errorf("follow-up 20B after idle oversized: %w", err)
	}
	return closePublisherCleared(ctx, env, &pubID)
}

func runPFC03b(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pfc03b-topic")), allowUnencodedRequestParams(env)
	g1 := fake.NewGate()
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PFC-03b-k1", Match: fake.MatchOrderingKey("k1"), Times: 1, Gate: g1})

	req := newFCPubReq(env, topic, 1, 1_000_000, proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR)
	setOrderingOptIn(env, req, true)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)
	defer g1.Release()

	k1_1, k1_2, k1_3, k1_4, k2_1 := []byte("k1-1"), []byte("k1-2"), []byte("k1-3"), []byte("k1-4"), []byte("k2-1")
	t1, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: k1_1, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := g1.WaitEnteredN(ctx, 1); err != nil {
		return fmt.Errorf("waiting for k1-1 at gate: %w", err)
	}
	if err := expectFastFCLocalErr(ctx, env, pubID, k1_2, "k1", "ordered SIGNAL_ERROR k1-2"); err != nil {
		return err
	}
	g1.Release()

	id1, err := awaitPublishMessageID(ctx, env, pubID, t1)
	if err != nil {
		return err
	}
	idK2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: k2_1, OrderingKey: "k2"})
	if err != nil {
		return fmt.Errorf("k2-1 unaffected check: %w", err)
	}
	okIDs := []string{id1, idK2}

	lang := strings.ToLower(env.Language())
	var wantK1 [][]byte
	if lang == "cpp" || lang == "python" {
		id3, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: k1_3, OrderingKey: "k1"})
		if err != nil {
			return fmt.Errorf("k1-3 post-drain on %s (no pause expected): %w", lang, err)
		}
		okIDs, wantK1 = append(okIDs, id3), [][]byte{k1_1, k1_3}
	} else {
		if err := expectFastFCLocalErr(ctx, env, pubID, k1_3, "k1", "paused k1-3"); err != nil {
			return err
		}
		if err := env.RequireCapability(harness.CapResumePublish); err != nil {
			return err
		}
		if err := resumePublish(ctx, env, pubID, "k1"); err != nil {
			return err
		}
		id4, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: k1_4, OrderingKey: "k1"})
		if err != nil {
			return fmt.Errorf("k1-4 after ResumePublish: %w", err)
		}
		okIDs, wantK1 = append(okIDs, id4), [][]byte{k1_1, k1_4}
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	for i, c := range snap.PublishCalls() {
		v.Check(fake.ExpectPublish(fmt.Sprintf("PFC-03b-%d", i), fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishNoMixedOrderingKeys(), fake.PublishStatusCode(codes.OK)).Check(c))
	}
	v.Check(fake.AssertOrderedPublishFIFO(snap, "k1", wantK1))
	v.Check(fake.AssertOrderedPublishFIFO(snap, "k2", [][]byte{k2_1}))
	v.Check(checkOKPublishMessageIDsMatch(okIDs, snap))
	return finishPublishScenario(ctx, env, v)
}
