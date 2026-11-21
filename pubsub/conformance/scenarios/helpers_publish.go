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
	"sort"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func newPubReq(env *harness.ScenarioEnv, topic string, count, byteThresh, delayMs int64) *proxypb.CreatePublisherRequest {
	req := &proxypb.CreatePublisherRequest{
		Endpoint: env.Endpoint(), Topic: topic, ProjectId: env.ProjectID,
		CountThreshold: proto.Int64(count), DelayThresholdMs: proto.Int64(delayMs),
	}
	if byteThresh > 0 {
		req.ByteThreshold = proto.Int64(byteThresh)
	}
	return req
}

func sendPublish(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest, async bool) (*proxypb.PublishResponse, error) {
	r := proto.CloneOf(req)
	r.AsyncFire = async
	op := "Publish"
	if async {
		op = "Publish(async)"
	}
	resp, err := env.Client.Publish(ctx, r)
	if err := env.CheckProxyErr(err); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("%s returned nil response", op)
	}
	return resp, nil
}

func publishSync(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	return sendPublish(ctx, env, req, false)
}

func publishSyncOK(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest) (string, error) {
	resp, err := publishSync(ctx, env, req)
	if err != nil {
		return "", err
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "Publish"); err != nil {
		return "", err
	}
	if resp.GetMessageId() == "" {
		return "", fmt.Errorf("Publish returned empty message_id")
	}
	return resp.GetMessageId(), nil
}

func publishAsync(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest) (string, error) {
	resp, err := sendPublish(ctx, env, req, true)
	if err != nil {
		return "", err
	}
	if err := checkInBandErr(resp.GetErrorCode(), resp.GetErrorMessage(), "Publish(async)"); err != nil {
		return "", err
	}
	if resp.GetPublishToken() == "" {
		return "", fmt.Errorf("Publish(async) returned empty publish_token")
	}
	return resp.GetPublishToken(), nil
}

func publishAsyncPayloads(ctx context.Context, env *harness.ScenarioEnv, pubID string, payloads [][]byte) ([]string, error) {
	toks := make([]string, len(payloads))
	for i, data := range payloads {
		tok, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: data})
		if err != nil {
			return nil, err
		}
		toks[i] = tok
	}
	return toks, nil
}

func awaitPublishResult(ctx context.Context, env *harness.ScenarioEnv, pubID, token string, failOnInBandUnimpl bool) (*proxypb.PublishResponse, error) {
	resp, err := env.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubID, PublishToken: token})
	if err := env.CheckProxyErr(err); err != nil {
		return nil, fmt.Errorf("AwaitPublish: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("AwaitPublish returned nil response")
	}
	if failOnInBandUnimpl && harness.IsInBandUnimplemented(resp.GetErrorCode()) {
		return nil, harness.Unsupportedf("AwaitPublish returned UNIMPLEMENTED: %s", resp.GetErrorMessage())
	}
	return resp, nil
}

func awaitAllPublishIDs(ctx context.Context, env *harness.ScenarioEnv, pubID string, tokens []string) ([]string, error) {
	ids := make([]string, len(tokens))
	for i, tok := range tokens {
		id, err := awaitPublishMessageID(ctx, env, pubID, tok)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func publishAsyncExpectErr(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest) (int32, string, error) {
	resp, err := sendPublish(ctx, env, req, true)
	if err != nil {
		return 0, "", err
	}
	if resp.GetErrorCode() != 0 {
		if err := rejectInBandUnimpl(env, resp.GetErrorCode(), resp.GetErrorMessage(), "Publish(async)"); err != nil {
			return 0, "", err
		}
		return resp.GetErrorCode(), resp.GetErrorMessage(), nil
	}
	if resp.GetPublishToken() == "" {
		return 0, "", fmt.Errorf("Publish(async) returned empty publish_token with error_code=0")
	}
	aresp, err := awaitPublishResult(ctx, env, req.GetPublisherId(), resp.GetPublishToken(), false)
	if err != nil {
		return 0, "", err
	}
	if err := checkExpectedInBandErr(env, aresp, "async publish"); err != nil {
		return 0, "", err
	}
	return aresp.GetErrorCode(), aresp.GetErrorMessage(), nil
}

func rejectInBandUnimpl(env *harness.ScenarioEnv, code int32, msg, op string) error {
	if harness.IsInBandUnimplemented(code) {
		return env.Failf("%s returned unexpected UNIMPLEMENTED: %s", op, msg)
	}
	return nil
}

func checkExpectedInBandErr(env *harness.ScenarioEnv, resp *proxypb.PublishResponse, op string) error {
	if resp == nil {
		return env.Failf("expected %s to fail, got nil response", op)
	}
	if resp.GetErrorCode() == 0 {
		return env.Failf("expected %s to fail, got success (message_id=%q)", op, resp.GetMessageId())
	}
	return rejectInBandUnimpl(env, resp.GetErrorCode(), resp.GetErrorMessage(), op)
}

func checkFlushElapsed(env *harness.ScenarioEnv, op string, start time.Time, maxDur time.Duration) error {
	if elapsed := time.Since(start); elapsed >= maxDur {
		return env.Failf("%s took %s (>= %s); expected immediate trigger", op, elapsed, maxDur)
	}
	return nil
}

func waitCompletedPublishWithin(ctx context.Context, env *harness.ScenarioEnv, want int, waitMsg, op string, start time.Time, maxDur time.Duration) error {
	if err := waitCompletedPublishCount(ctx, env.Fake, want); err != nil {
		return fmt.Errorf("waiting for %s: %w", waitMsg, err)
	}
	return checkFlushElapsed(env, op, start, maxDur)
}

func flushAwaitAndClose(ctx context.Context, env *harness.ScenarioEnv, pubID *string, toks []string) ([]string, error) {
	if err := flushPublisher(ctx, env, *pubID); err != nil {
		return nil, err
	}
	ids, err := awaitAllPublishIDs(ctx, env, *pubID, toks)
	if err != nil {
		return nil, err
	}
	return ids, closePublisherCleared(ctx, env, pubID)
}

func finishPublishScenario(ctx context.Context, env *harness.ScenarioEnv, v *fake.Verifier) error {
	if err := v.Err(); err != nil {
		return err
	}
	return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
}

func closePublisherCleared(ctx context.Context, env *harness.ScenarioEnv, pubID *string) error {
	if err := closePublisher(ctx, env, *pubID); err != nil {
		return err
	}
	*pubID = ""
	return nil
}

func flushPublisher(ctx context.Context, env *harness.ScenarioEnv, pubID string) error {
	if _, err := env.Client.FlushPublisher(ctx, &proxypb.FlushPublisherRequest{PublisherId: pubID}); err != nil {
		return fmt.Errorf("FlushPublisher: %w", env.CheckProxyErr(err))
	}
	return nil
}

func stopPublisher(ctx context.Context, env *harness.ScenarioEnv, pubID string) error {
	if _, err := env.Client.StopPublisher(ctx, &proxypb.StopPublisherRequest{PublisherId: pubID}); err != nil {
		return fmt.Errorf("StopPublisher: %w", env.CheckProxyErr(err))
	}
	return nil
}

func resumePublish(ctx context.Context, env *harness.ScenarioEnv, pubID, orderingKey string) error {
	if _, err := env.Client.ResumePublish(ctx, &proxypb.ResumePublishRequest{PublisherId: pubID, OrderingKey: orderingKey}); err != nil {
		return fmt.Errorf("ResumePublish: %w", env.CheckProxyErr(err))
	}
	return nil
}

func setOrderingOptIn(env *harness.ScenarioEnv, req *proxypb.CreatePublisherRequest, enable bool) {
	if env.HasCapability(harness.CapOrderingOptIn) {
		req.EnableMessageOrdering = proto.Bool(enable)
	}
}

func waitCompletedPublish(ctx context.Context, fakeSrv *fake.Server, want int, perMsg bool) error {
	return fakeSrv.WaitFor(ctx, func(snap fake.Snapshot) bool {
		n := 0
		for _, c := range snap.PublishCalls() {
			if c.Completed {
				if !perMsg {
					n++
				} else if c.PublishReq != nil {
					n += len(c.PublishReq.GetMessages())
				}
			}
		}
		return n >= want
	})
}

func waitCompletedPublishCount(ctx context.Context, fakeSrv *fake.Server, want int) error {
	return waitCompletedPublish(ctx, fakeSrv, want, false)
}

func waitCompletedPublishMessageCount(ctx context.Context, fakeSrv *fake.Server, wantMsgs int) error {
	return waitCompletedPublish(ctx, fakeSrv, wantMsgs, true)
}

func lastCapturedSeq(snap fake.Snapshot) int64 {
	var maxSeq int64
	for _, c := range snap.Calls {
		if c.Seq > maxSeq {
			maxSeq = c.Seq
		}
	}
	return maxSeq
}

func filterCalls(calls []fake.CapturedCall, pred func(fake.CapturedCall) bool) []fake.CapturedCall {
	var out []fake.CapturedCall
	for _, c := range calls {
		if pred(c) {
			out = append(out, c)
		}
	}
	return out
}

func callsAfterSeq(snap fake.Snapshot, seq int64) []fake.CapturedCall {
	return filterCalls(snap.Calls, func(c fake.CapturedCall) bool { return c.Seq > seq })
}

func publishCallsAfterSeq(snap fake.Snapshot, seq int64) []fake.CapturedCall {
	return filterCalls(snap.PublishCalls(), func(c fake.CapturedCall) bool { return c.Seq > seq })
}

func okPublishCalls(snap fake.Snapshot) []fake.CapturedCall {
	return filterCalls(snap.PublishCalls(), func(c fake.CapturedCall) bool { return c.Completed && c.StatusCode == codes.OK })
}

func checkOKPublishMessageIDsMatch(returnedIDs []string, snap fake.Snapshot) error {
	var serverIDs []string
	for _, c := range okPublishCalls(snap) {
		serverIDs = append(serverIDs, c.ResponseMessageIDs...)
	}
	want, got := slices.Clone(serverIDs), slices.Clone(returnedIDs)
	sort.Strings(want)
	sort.Strings(got)
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		return fmt.Errorf("published OK message_ids mismatch against server response (-server +proxy):\n%s", diff)
	}
	return nil
}

func assertNoUnexpectedPublishRPCs(snap fake.Snapshot) error {
	return fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish)
}
