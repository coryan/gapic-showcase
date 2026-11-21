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
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func publishRetryScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PRET-01", Name: "Publish Transient Retries & Fast Permanent Failure", Category: "retry", Feature: "PRET-01", Description: "Verify Unavailable and Aborted retry cleanly to success while PermissionDenied and InvalidArgument fail immediately in 1 wire attempt.", Run: runPRET01},
		{ID: "PRET-01b", Name: "Publish Custom Retry Codes & Backoff", Category: "retry", Feature: "PRET-01b", Description: "Verify custom retry_codes=[DataLoss] retries DataLoss to success while omitting Unavailable causes immediate failure in 1 wire attempt.", RequiredCapabilities: []harness.Capability{harness.CapCustomRetryCodes}, Run: runPRET01b},
		{ID: "PRET-02", Name: "Publish Total Timeout Enforcement", Category: "retry", Feature: "PRET-02", Description: "Verify total_timeout_ms bounds retries over repeated transient errors without hanging or leaking permits.", Run: runPRET02},
	}
}

func expTopicPub(label, topic string, allowUnencoded bool, data []byte, code codes.Code) fake.CallExpectation {
	return fake.ExpectPublish(label, fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(code), fake.PublishSingleMessage(fake.ExpectedMessage{Data: data, Attributes: map[string]string{}}))
}

func runPRET01(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pret01-topic")), allowUnencodedRequestParams(env)
	mUnavail, mAbort, mPerm, mInval := []byte("m-unavail"), []byte("m-abort"), []byte("m-perm"), []byte("m-inval")
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01-unavail", Match: fake.MatchPayloadContains(mUnavail), Times: 2, Err: status.Error(codes.Unavailable, "transient unavailable")})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01-abort", Match: fake.MatchPayloadContains(mAbort), Times: 1, Err: status.Error(codes.Aborted, "transient aborted")})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01-perm", Match: fake.MatchPayloadContains(mPerm), Times: fake.RepeatAlways, Err: status.Error(codes.PermissionDenied, "perm denied")})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01-inval", Match: fake.MatchPayloadContains(mInval), Times: fake.RepeatAlways, Err: status.Error(codes.InvalidArgument, "bad arg")})

	req := newPubReq(env, topic, 1, 0, 10)
	req.TotalTimeoutMs = proto.Int64(15_000)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	id1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mUnavail})
	if err != nil {
		return err
	}
	id2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mAbort})
	if err != nil {
		return err
	}
	for _, perm := range [][]byte{mPerm, mInval} {
		wm := lastCapturedSeq(env.Fake.Snapshot())
		resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: perm})
		if err != nil {
			return err
		}
		if err := checkExpectedInBandErr(env, resp, string(perm)); err != nil {
			return err
		}
		if calls := publishCallsAfterSeq(env.Fake.Snapshot(), wm); len(calls) != 1 {
			return env.Failf("permanent error %q emitted %d wire attempts, want 1", perm, len(calls))
		}
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		expTopicPub("PRET-01-u0", topic, allowUnencoded, mUnavail, codes.Unavailable),
		expTopicPub("PRET-01-u1", topic, allowUnencoded, mUnavail, codes.Unavailable),
		expTopicPub("PRET-01-u2", topic, allowUnencoded, mUnavail, codes.OK),
		expTopicPub("PRET-01-a0", topic, allowUnencoded, mAbort, codes.Aborted),
		expTopicPub("PRET-01-a1", topic, allowUnencoded, mAbort, codes.OK),
		expTopicPub("PRET-01-p", topic, allowUnencoded, mPerm, codes.PermissionDenied),
		expTopicPub("PRET-01-i", topic, allowUnencoded, mInval, codes.InvalidArgument),
	))
	v.Check(checkOKPublishMessageIDsMatch([]string{id1, id2}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPRET01b(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pret01b-topic")), allowUnencodedRequestParams(env)
	mDL, mUnavail := []byte("m-dataloss"), []byte("m-unavail-perm")
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01b-dl", Match: fake.MatchPayloadContains(mDL), Times: 4, Err: status.Error(codes.DataLoss, "custom retryable dataloss")})
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-01b-unavail", Match: fake.MatchPayloadContains(mUnavail), Times: fake.RepeatAlways, Err: status.Error(codes.Unavailable, "omitted unavailable")})

	req := newPubReq(env, topic, 1, 0, 10)
	req.RetryCodes = []int32{int32(codes.DataLoss)}
	req.InitialRetryDelayMs, req.MaxRetryDelayMs = proto.Int64(200), proto.Int64(1000)
	req.RetryDelayMultiplier, req.TotalTimeoutMs = proto.Float64(1.5), proto.Int64(10_000)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	idDL, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mDL})
	if err != nil {
		return err
	}
	wm := lastCapturedSeq(env.Fake.Snapshot())
	resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mUnavail})
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, resp, "omitted Unavailable under custom retry_codes"); err != nil {
		return err
	}
	if calls := publishCallsAfterSeq(env.Fake.Snapshot(), wm); len(calls) != 1 {
		return env.Failf("omitted Unavailable emitted %d wire attempts, want 1", len(calls))
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		expTopicPub("PRET-01b-d0", topic, allowUnencoded, mDL, codes.DataLoss),
		expTopicPub("PRET-01b-d1", topic, allowUnencoded, mDL, codes.DataLoss),
		expTopicPub("PRET-01b-d2", topic, allowUnencoded, mDL, codes.DataLoss),
		expTopicPub("PRET-01b-d3", topic, allowUnencoded, mDL, codes.DataLoss),
		expTopicPub("PRET-01b-d4", topic, allowUnencoded, mDL, codes.OK),
		expTopicPub("PRET-01b-u", topic, allowUnencoded, mUnavail, codes.Unavailable),
	))
	if calls := snap.PublishCalls(); len(calls) >= 5 && calls[4].Timestamp.Sub(calls[0].Timestamp) < 60*time.Millisecond {
		v.Checkf("expected positive retry backoff >= 60ms across 5 DataLoss attempts, got %s", calls[4].Timestamp.Sub(calls[0].Timestamp))
	}
	v.Check(checkOKPublishMessageIDsMatch([]string{idDL}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPRET02(ctx context.Context, env *harness.ScenarioEnv) error {
	env.Diagnosticf("publish_total_timeout_ms=%d", env.Defaults().GetPublishTotalTimeoutMs())
	if env.Caps != nil && env.Caps.GetDefaults() != nil && env.Caps.GetDefaults().GetPublishTotalTimeoutMs() < 0 {
		return env.Failf("invalid negative defaults.publish_total_timeout_ms=%d", env.Caps.GetDefaults().GetPublishTotalTimeoutMs())
	}

	topic, allowUnencoded := env.TopicPath(env.UniqueName("pret02-topic")), allowUnencodedRequestParams(env)
	timeoutMs, ruleDelay, maxWait := int64(400), 40*time.Millisecond, 3*time.Second
	if strings.EqualFold(env.Language(), "java") {
		timeoutMs, ruleDelay, maxWait = 10_000, 50*time.Millisecond, 16*time.Second
	}

	mTimeout, mOK := []byte("m-timeout"), []byte("m-ok")
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PRET-02-timeout", Match: fake.MatchPayloadContains(mTimeout), Times: fake.RepeatAlways, Delay: ruleDelay, Err: status.Error(codes.Unavailable, "forced transient timeout")})

	req := newPubReq(env, topic, 1, 0, 10)
	req.TotalTimeoutMs = proto.Int64(timeoutMs)
	req.InitialRetryDelayMs, req.MaxRetryDelayMs, req.RetryDelayMultiplier = proto.Int64(20), proto.Int64(50), proto.Float64(1.5)
	pubID, err := createPublisher(ctx, env, req)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	wmBefore := lastCapturedSeq(env.Fake.Snapshot())
	start := time.Now()
	resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mTimeout})
	elapsed := time.Since(start)
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, resp, "timed-out publish"); err != nil {
		return err
	}
	if elapsed >= maxWait {
		return env.Failf("timed-out publish took %s (>= %s)", elapsed, maxWait)
	}
	snapAfterTO := env.Fake.Snapshot()
	wmAfterTO := lastCapturedSeq(snapAfterTO)
	if calls := publishCallsAfterSeq(snapAfterTO, wmBefore); len(calls) < 2 {
		return env.Failf("expected >= 2 wire Publish attempts before total timeout, got %d", len(calls))
	}
	if err := sleepCtx(ctx, 80*time.Millisecond); err != nil {
		return err
	}

	okID, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: mOK})
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}
	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyTopicPublishRPCs(v, "PRET-02", topic, allowUnencoded, snap.PublishCalls())
	v.Check(fake.VerifySequence(fake.Snapshot{Calls: callsAfterSeq(snap, wmAfterTO)}, fake.DefaultVerifyOptions(), expTopicPub("PRET-02-ok", topic, allowUnencoded, mOK, codes.OK)))
	v.Check(checkOKPublishMessageIDsMatch([]string{okID}, snap))
	return finishPublishScenario(ctx, env, v)
}
