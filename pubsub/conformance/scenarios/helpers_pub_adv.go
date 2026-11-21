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
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc/codes"
)

type syncPublishJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	resp   *proxypb.PublishResponse
	err    error
}

func startSyncPublish(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.PublishRequest) *syncPublishJob {
	pCtx, cancel := context.WithCancel(ctx)
	j := &syncPublishJob{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(j.done)
		j.resp, j.err = publishSync(pCtx, env, req)
	}()
	return j
}

func (j *syncPublishJob) abort() {
	if j != nil {
		j.cancel()
		<-j.done
	}
}

func (j *syncPublishJob) awaitResp(ctx context.Context) (*proxypb.PublishResponse, error) {
	select {
	case <-j.done:
		j.cancel()
		return j.resp, j.err
	case <-ctx.Done():
		j.abort()
		return nil, ctx.Err()
	}
}

func (j *syncPublishJob) awaitOK(ctx context.Context) (string, error) {
	resp, err := j.awaitResp(ctx)
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

func assertStaysBlocked(ctx context.Context, env *harness.ScenarioEnv, wm int64, wantCalls int, done <-chan struct{}, wait time.Duration, op string) error {
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-done:
		return env.Failf("%s returned prematurely instead of blocking for %s", op, wait)
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	if got := len(publishCallsAfterSeq(env.Fake.Snapshot(), wm)); got != wantCalls {
		return env.Failf("%s: expected %d wire Publish RPCs while blocked, got %d", op, wantCalls, got)
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitHedgeLosersCanceled(ctx context.Context, env *harness.ScenarioEnv, wm int64, minCalls int, op string) error {
	wCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := env.Fake.WaitFor(wCtx, func(snap fake.Snapshot) bool {
		calls := publishCallsAfterSeq(snap, wm)
		return len(calls) >= minCalls && !slices.ContainsFunc(calls, func(c fake.CapturedCall) bool { return !c.Completed })
	}); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return env.Failf("%s: losing attempt(s) did not cancel within 2s: %v", op, err)
	}
	calls := publishCallsAfterSeq(env.Fake.Snapshot(), wm)
	winIdx := slices.IndexFunc(calls, func(c fake.CapturedCall) bool { return c.Completed && c.StatusCode == codes.OK })
	if winIdx < 0 {
		return env.Failf("%s: no winning OK Publish call after seq %d", op, wm)
	}
	for i, c := range calls {
		if i == winIdx {
			continue
		}
		if dt := c.EndTime.Sub(calls[winIdx].EndTime); dt >= 2*time.Second {
			return env.Failf("%s: loser call[%d] ended %s after winner (>= 2s)", op, i, dt)
		}
		if i < winIdx && (!c.CanceledByClient || c.StatusCode != codes.Canceled) {
			return env.Failf("%s: loser call[%d] was not canceled by client (canceled=%t, code=%s)", op, i, c.CanceledByClient, c.StatusCode)
		}
	}
	return nil
}

func expectRejectedPublisher(ctx context.Context, env *harness.ScenarioEnv, req *proxypb.CreatePublisherRequest, data, op string) error {
	wm := lastCapturedSeq(env.Fake.Snapshot())
	resp, err := env.Client.CreatePublisher(ctx, req)
	if err != nil {
		if harness.IsUnimplemented(err) {
			return env.CheckProxyErr(err)
		}
		return fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm)
	}
	pubID := resp.GetPublisherId()
	defer cleanupPublisher(ctx, env, &pubID)
	presp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte(data)})
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, presp, op); err != nil {
		return err
	}
	if err := fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm); err != nil {
		return err
	}
	return closePublisherCleared(ctx, env, &pubID)
}

func verifyTopicPublishRPCs(v *fake.Verifier, label, topic string, allowUnencoded bool, calls []fake.CapturedCall) {
	for i, c := range calls {
		v.Check(fake.ExpectPublish(fmt.Sprintf("%s-%d", label, i), fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded)).Check(c))
	}
}

func paddedPayload(prefix string, size int) []byte {
	b := bytes.Repeat([]byte{'p'}, size)
	copy(b, prefix)
	return b
}

func pubRecord(ctx context.Context, env *harness.ScenarioEnv, pubID string, data []byte, allIDs *[]string, allData *[][]byte) (string, error) {
	id, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: data})
	if err != nil {
		return "", err
	}
	*allIDs, *allData = append(*allIDs, id), append(*allData, data)
	return id, nil
}

func publishSeqOK(ctx context.Context, env *harness.ScenarioEnv, pubID, prefix string, n int, allIDs *[]string, allData *[][]byte) error {
	for i := range n {
		if _, err := pubRecord(ctx, env, pubID, []byte(fmt.Sprintf("%s-%d", prefix, i)), allIDs, allData); err != nil {
			return err
		}
	}
	return nil
}

func checkOKPublishPayloadsMatch(wantPayloads [][]byte, snap fake.Snapshot) error {
	var got []string
	for _, c := range okPublishCalls(snap) {
		for _, m := range c.PublishReq.GetMessages() {
			got = append(got, string(m.GetData()))
		}
	}
	want := make([]string, len(wantPayloads))
	for i, p := range wantPayloads {
		want[i] = string(p)
	}
	sort.Strings(want)
	sort.Strings(got)
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		return fmt.Errorf("published OK payloads mismatch (-want +got):\n%s", diff)
	}
	return nil
}

func finishSimplePublish(ctx context.Context, env *harness.ScenarioEnv, label, topic string, allowUnencoded bool, allIDs []string, wantPayloads [][]byte) error {
	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyTopicPublishRPCs(v, label, topic, allowUnencoded, snap.PublishCalls())
	v.Check(checkOKPublishMessageIDsMatch(allIDs, snap))
	v.Check(checkOKPublishPayloadsMatch(wantPayloads, snap))
	return finishPublishScenario(ctx, env, v)
}
