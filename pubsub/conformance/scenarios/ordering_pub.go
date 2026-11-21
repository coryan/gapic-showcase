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

func publishOrderingScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PORD-01", Name: "Ordering Opt-In Enforcement", Category: "ordering", Feature: "PORD-01", Description: "Verify publishing a non-empty ordering_key fails locally with zero wire RPCs when enable_message_ordering=false, while empty ordering_key succeeds.", RequiredCapabilities: []harness.Capability{harness.CapOrderingOptIn}, Run: runPORD01},
		{ID: "PORD-03", Name: "Pause Ordering Key on Error & ResumePublish", Category: "ordering", Feature: "PORD-03", Description: "Verify permanent publish error pauses only the failing ordering_key while independent keys continue, and ResumePublish restores publishing.", RequiredCapabilities: []harness.Capability{harness.CapResumePublish}, Run: runPORD03},
		{ID: "PORD-04", Name: "Ordered Publish Transient Retry & Timeout Semantics", Category: "ordering", Feature: "PORD-04", Description: "Verify transient errors retry in FIFO order without pausing the key, and document SDK timeout enforcement across short and extended deadlines.", Run: runPORD04},
	}
}

func runPORD01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pord01-topic")), allowUnencodedRequestParams(env)
	req := newPubReq(env, topic, 1, 0, 10)
	req.EnableMessageOrdering = proto.Bool(false)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("pord01-rejected"), OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, resp, "ordering_key publish with enable_message_ordering=false"); err != nil {
		return err
	}
	if calls := env.Fake.Snapshot().Calls; len(calls) != 0 {
		return env.Failf("expected 0 wire RPCs when ordering opt-in rejected, got %d", len(calls))
	}

	okData := []byte("pord01-unordered-ok")
	id, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: okData})
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPublish("PORD-01", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: okData, Attributes: map[string]string{}}))))
	v.Check(checkOKPublishMessageIDsMatch([]string{id}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPORD03(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pord03-topic")), allowUnencodedRequestParams(env)
	req := newPubReq(env, topic, 1, 0, 10)
	setOrderingOptIn(env, req, true)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	gate := fake.NewGate()
	defer gate.Release()
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-03-k1-perm", Match: fake.MatchOrderingKey("k1"), Times: 1, Gate: gate, Err: status.Error(codes.InvalidArgument, "permanent ordering error")})

	pK1_1, pK1_2, pK1_3 := []byte("k1-1"), []byte("k1-2"), []byte("k1-3")
	pK1_4, pK1_5 := []byte("k1-4"), []byte("k1-5")
	pK2_1, pK2_2 := []byte("k2-1"), []byte("k2-2")

	tok1, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK1_1, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := gate.WaitEntered(ctx); err != nil {
		return fmt.Errorf("waiting for k1-1 at gate: %w", err)
	}
	earlyResp, err := sendPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK1_2, OrderingKey: "k1"}, true)
	if err != nil {
		return err
	}
	k2ID1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK2_1, OrderingKey: "k2"})
	if err != nil {
		return fmt.Errorf("k2-1 while k1 gated: %w", err)
	}
	gate.Release()

	res1, err := awaitPublishResult(ctx, env, pubID, tok1, false)
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, res1, "k1-1"); err != nil {
		return err
	}
	if err := rejectInBandUnimpl(env, earlyResp.GetErrorCode(), earlyResp.GetErrorMessage(), "Publish(async, k1-2)"); err != nil {
		return err
	}
	if earlyResp.GetErrorCode() == 0 {
		if earlyResp.GetPublishToken() == "" {
			return env.Failf("Publish(async, k1-2) returned empty publish_token with error_code=0")
		}
		res2, err := awaitPublishResult(ctx, env, pubID, earlyResp.GetPublishToken(), false)
		if err != nil {
			return err
		}
		if err := checkExpectedInBandErr(env, res2, "queued k1-2"); err != nil {
			return err
		}
	}
	resp3, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK1_3, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, resp3, "k1-3 while k1 paused"); err != nil {
		return err
	}
	k2ID2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK2_2, OrderingKey: "k2"})
	if err != nil {
		return fmt.Errorf("k2-2 while k1 paused: %w", err)
	}
	if err := resumePublish(ctx, env, pubID, "k1"); err != nil {
		return err
	}
	k1ID4, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK1_4, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	k1ID5, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: pK1_5, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	for i, c := range snap.PublishCalls() {
		v.Check(fake.ExpectPublish(fmt.Sprintf("PORD-03-%d", i), fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishNoMixedOrderingKeys()).Check(c))
	}
	v.Check(fake.AssertOrderedPublishAttemptedFIFO(snap, "k1", [][]byte{pK1_1, pK1_4, pK1_5}))
	v.Check(fake.AssertOrderedPublishFIFO(snap, "k1", [][]byte{pK1_4, pK1_5}))
	v.Check(fake.AssertOrderedPublishFIFO(snap, "k2", [][]byte{pK2_1, pK2_2}))
	v.Check(fake.AssertMaxConcurrentPublishForKey(snap, "k1", 1))
	v.Check(checkOKPublishMessageIDsMatch([]string{k2ID1, k2ID2, k1ID4, k1ID5}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPORD04(ctx context.Context, env *harness.ScenarioEnv) error {
	env.Diagnosticf("publish_total_timeout_ms=%d", env.Defaults().GetPublishTotalTimeoutMs())
	if env.Caps != nil && env.Caps.GetDefaults() != nil && env.Caps.GetDefaults().GetPublishTotalTimeoutMs() < 0 {
		return env.Failf("invalid negative defaults.publish_total_timeout_ms=%d", env.Caps.GetDefaults().GetPublishTotalTimeoutMs())
	}

	topic, allowUnencoded := env.TopicPath(env.UniqueName("pord04-topic")), allowUnencodedRequestParams(env)
	reqA := newPubReq(env, topic, 1, 0, 10)
	reqA.TotalTimeoutMs, reqA.InitialRetryDelayMs = proto.Int64(10_000), proto.Int64(20)
	reqA.MaxRetryDelayMs, reqA.RetryDelayMultiplier = proto.Int64(60), proto.Float64(1.5)
	setOrderingOptIn(env, reqA, true)
	pubA, err := createPublisher(ctx, env, reqA)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubA)

	p1, p2, p3 := []byte("k1-1"), []byte("k1-2"), []byte("k1-3")
	gate := fake.NewGate()
	defer gate.Release()
	matchK1P1 := fake.MatchPublishAll(fake.MatchOrderingKey("k1"), fake.MatchPayloadContains(p1))
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-04-u1", Match: matchK1P1, Times: 1, Gate: gate, Err: status.Error(codes.Unavailable, "transient 1")})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-04-u2", Match: matchK1P1, Times: 1, Err: status.Error(codes.Unavailable, "transient 2")})

	tok1, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: p1, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	if err := gate.WaitEntered(ctx); err != nil {
		return fmt.Errorf("waiting for k1-1 at gate: %w", err)
	}
	tok2, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: p2, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	gate.Release()

	idsA, err := awaitAllPublishIDs(ctx, env, pubA, []string{tok1, tok2})
	if err != nil {
		return err
	}
	id3, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubA, Data: p3, OrderingKey: "k1"})
	if err != nil {
		return err
	}
	okIDs := append(idsA, id3)
	if err := closePublisherCleared(ctx, env, &pubA); err != nil {
		return err
	}

	snapA := env.Fake.Snapshot()
	vA := env.NewVerifier()
	vA.Check(assertNoUnexpectedPublishRPCs(snapA))
	vA.Check(fake.AssertOrderedPublishAttemptedFIFO(snapA, "k1", [][]byte{p1, p1, p1, p2, p3}))
	vA.Check(fake.AssertOrderedPublishFIFO(snapA, "k1", [][]byte{p1, p2, p3}))
	vA.Check(fake.AssertMaxConcurrentPublishForKey(snapA, "k1", 1))
	vA.Check(checkOKPublishMessageIDsMatch(okIDs, snapA))
	if err := vA.Err(); err != nil {
		return err
	}

	reqB := newPubReq(env, topic, 1, 0, 10)
	reqB.TotalTimeoutMs, reqB.InitialRetryDelayMs, reqB.MaxRetryDelayMs = proto.Int64(250), proto.Int64(40), proto.Int64(80)
	setOrderingOptIn(env, reqB, true)
	pubB, err := createPublisher(ctx, env, reqB)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubB)

	const keyB = "k1-timeout"
	lang := strings.ToLower(env.Language())
	if lang == "java" || lang == "python" {
		pExt := []byte("k1-timeout-ext")
		env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-04-ext", Match: fake.MatchOrderingKey(keyB), Times: 4, Delay: 75 * time.Millisecond, Err: status.Error(codes.Unavailable, "transient ext")})
		idExt, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubB, Data: pExt, OrderingKey: keyB})
		if err != nil {
			return err
		}
		okIDs = append(okIDs, idExt)
		if err := closePublisherCleared(ctx, env, &pubB); err != nil {
			return err
		}

		snapB := env.Fake.Snapshot()
		vB := env.NewVerifier()
		vB.Check(fake.AssertOrderedPublishAttemptedFIFO(snapB, keyB, [][]byte{pExt, pExt, pExt, pExt, pExt}))
		vB.Check(fake.AssertOrderedPublishFIFO(snapB, keyB, [][]byte{pExt}))
		if err := vB.Err(); err != nil {
			return err
		}
	} else {
		pTO := []byte("k1-timeout-msg")
		env.Fake.AddPublishRule(fake.PublishRule{Name: "PORD-04-to", Match: fake.MatchOrderingKey(keyB), Times: fake.RepeatAlways, Delay: 50 * time.Millisecond, Err: status.Error(codes.Unavailable, "transient to")})
		startB := time.Now()
		respB, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubB, Data: pTO, OrderingKey: keyB})
		elapsedB := time.Since(startB)
		if err != nil {
			return err
		}
		if err := checkExpectedInBandErr(env, respB, "k1-timeout publish after TotalTimeoutMs=250"); err != nil {
			return err
		}
		if elapsedB >= 2*time.Second {
			return env.Failf("ordered publish timeout took %s (>= 2s), expected bounded failure", elapsedB)
		}
		if err := closePublisherCleared(ctx, env, &pubB); err != nil {
			return err
		}

		snapB := env.Fake.Snapshot()
		if !slices.ContainsFunc(snapB.PublishCalls(), func(c fake.CapturedCall) bool {
			msgs := c.PublishReq.GetMessages()
			return len(msgs) > 0 && msgs[0].GetOrderingKey() == keyB
		}) {
			return env.Failf("expected >= 1 wire Publish attempt for %q before timeout", keyB)
		}
		if err := fake.AssertOrderedPublishFIFO(snapB, keyB, nil); err != nil {
			return err
		}
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	for i, c := range snap.PublishCalls() {
		v.Check(fake.ExpectPublish(fmt.Sprintf("PORD-04-%d", i), fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishNoMixedOrderingKeys()).Check(c))
	}
	v.Check(fake.AssertMaxConcurrentPublishForKey(snap, keyB, 1))
	v.Check(checkOKPublishMessageIDsMatch(okIDs, snap))
	return finishPublishScenario(ctx, env, v)
}
