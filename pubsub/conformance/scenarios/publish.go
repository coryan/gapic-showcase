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
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func publishScenarios() []harness.Scenario {
	return []harness.Scenario{
		{ID: "PUB-02", Name: "Attributes, Binary Payloads & UTF-8 Validation", Category: "publish", Feature: "PUB-02", Description: "Verify binary payload and UTF-8 attributes are preserved verbatim, invalid UTF-8 raw_attributes fail without retries (<=1 wire RPC), and subsequent valid publishes succeed.", Run: runPUB02},
		{ID: "PUB-02b", Name: "Empty Payload vs Attributes-Only Publish Validation", Category: "publish", Feature: "PUB-02b", Description: "Verify attributes-only publishes succeed everywhere and completely empty messages follow each SDK's documented empty-message policy.", Run: runPUB02b},
		{ID: "PUB-03", Name: "Publish Batch Trigger: Count Threshold & 1000-Msg Chunking", Category: "publish", Feature: "PUB-03", Description: "Verify batching holds below count_threshold, flushes immediately upon reaching count_threshold, and chunks 1005 messages into batches of at most 1000.", Run: runPUB03},
		{ID: "PUB-04", Name: "Publish Batch Trigger: Byte Threshold Splitting", Category: "publish", Feature: "PUB-04", Description: "Verify byte_threshold triggers batch flush and splits 4x400B messages across multiple batches.", Run: runPUB04},
		{ID: "PUB-05", Name: "Publish Batch Trigger: Delay Threshold Auto-Flush", Category: "publish", Feature: "PUB-05", Description: "Verify buffered messages below count and byte thresholds flush automatically on delay_threshold_ms without manual Flush or Close.", Run: runPUB05},
		{ID: "PUB-06a", Name: "Singleton Large Message (> ByteThreshold, <= 10MB)", Category: "publish", Feature: "PUB-06a", Description: "Verify a single 2 MiB message exceeding byte_threshold publishes immediately as a singleton batch with byte-for-byte integrity.", Run: runPUB06a},
		{ID: "PUB-06b", Name: "Oversized > 10MB Rejection & Flow-Control Non-Leak", Category: "publish", Feature: "PUB-06b", Description: "Verify >10MB messages fail cleanly without retry and without leaking Block flow-control permits.", FakeServerOptions: []fake.Option{fake.WithMaxPublishRequestBytes(10_000_000)}, Run: runPUB06b},
		{ID: "PUB-07", Name: "Manual Flush & Stop Drain Synchronization", Category: "publish", Feature: "PUB-07", Description: "Verify FlushPublisher and StopPublisher block until in-flight and buffered batches drain in order.", Run: runPUB07},
		{ID: "PUB-08", Name: "Publish After Stop & Close Rejected Locally", Category: "publish", Feature: "PUB-08", Description: "Verify Publish calls after StopPublisher and ClosePublisher fail without emitting wire RPCs.", Run: runPUB08},
		{ID: "PUB-09", Name: "Blocking / Unbatched Single Publish", Category: "publish", Feature: "PUB-09", Description: "Verify UseBlockingPublisher bypasses batching and publishes immediately in unary batches when supported.", RequiredCapabilities: []harness.Capability{harness.CapBlockingPublish}, Run: runPUB09},
		{ID: "PUB-10", Name: "Mismatched PublishResponse message_ids Count Recovery", Category: "publish", Feature: "PUB-10", Description: "Verify mismatched PublishResponse.message_ids counts surface clean errors without hanging and leave the publisher healthy.", IsolateProxy: true, Run: runPUB10},
	}
}

func verifyBatchedPayloads(v *fake.Verifier, label, topic string, allowUnencoded bool, calls []fake.CapturedCall, minBatches, maxBatches, minSize, maxSize int, wantPayloads []string) {
	v.Require(len(calls) >= minBatches && (maxBatches == 0 || len(calls) <= maxBatches), "%s: batch count %d outside [%d, %d]", label, len(calls), minBatches, maxBatches)
	var gotPayloads []string
	for i, c := range calls {
		v.Check(fake.ExpectPublish(fmt.Sprintf("%s-%d", label, i),
			fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded),
			fake.PublishBatchSizeRange(minSize, maxSize), fake.PublishStatusCode(codes.OK),
		).Check(c))
		for _, m := range c.PublishReq.GetMessages() {
			gotPayloads = append(gotPayloads, string(m.GetData()))
		}
	}
	wantSorted, gotSorted := slices.Clone(wantPayloads), gotPayloads
	sort.Strings(wantSorted)
	sort.Strings(gotSorted)
	if diff := cmp.Diff(wantSorted, gotSorted); diff != "" {
		v.Checkf("%s payloads mismatch (-want +got):\n%s", label, diff)
	}
}

func runPUB02(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub02-topic")), allowUnencodedRequestParams(env)
	binData := []byte{0x00, 0x01, 0x80, 0xff, 0xfe, 'b', 'i', 'n'}
	utf8Attrs := map[string]string{"ascii": "v1", "utf8-\U0001f511": "val-\U0001f680"}

	pub1, err := createPublisher(ctx, env, newPubReq(env, topic, 1, 0, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pub1)
	id1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pub1, Data: binData, Attributes: utf8Attrs})
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pub1); err != nil {
		return err
	}

	req2 := newPubReq(env, topic, 1, 0, 10)
	if strings.EqualFold(env.Language(), "cpp") {
		req2.TotalTimeoutMs = proto.Int64(800)
	}
	pub2, err := createPublisher(ctx, env, req2)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pub2)

	for i, ra := range []*proxypb.RawAttribute{{Key: []byte("a\xc5z"), Value: []byte("valid-val")}, {Key: []byte("valid-key"), Value: []byte("a\xc5z")}} {
		wm := lastCapturedSeq(env.Fake.Snapshot())
		resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pub2, Data: []byte(fmt.Sprintf("bad-utf8-%d", i)), RawAttributes: []*proxypb.RawAttribute{ra}})
		if err != nil {
			return err
		}
		if err := checkExpectedInBandErr(env, resp, fmt.Sprintf("invalid UTF-8 RawAttribute[%d]", i)); err != nil {
			return err
		}
		newCalls := callsAfterSeq(env.Fake.Snapshot(), wm)
		if len(newCalls) > 1 {
			return env.Failf("invalid UTF-8 RawAttribute[%d] emitted %d wire RPCs (expected <= 1, no retries)", i, len(newCalls))
		}
		if len(newCalls) == 1 && (newCalls[0].FullMethod != fake.MethodPublish || newCalls[0].StatusCode == codes.OK) {
			return env.Failf("invalid UTF-8 RawAttribute[%d] unexpected wire call method=%s status=%s", i, newCalls[0].FullMethod, newCalls[0].StatusCode)
		}
	}

	validData, validAttrs := []byte("valid-after-bad-utf8"), map[string]string{"recovery": "ok"}
	id2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pub2, Data: validData, Attributes: validAttrs})
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pub2); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	oks := okPublishCalls(snap)
	v.Require(len(oks) == 2, "expected 2 OK Publish RPCs, got %d", len(oks))
	if len(oks) == 2 {
		v.Check(fake.ExpectPublish("PUB-02-bin", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: binData, Attributes: utf8Attrs})).Check(oks[0]))
		v.Check(fake.ExpectPublish("PUB-02-rec", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: validData, Attributes: validAttrs})).Check(oks[1]))
	}
	v.Check(checkOKPublishMessageIDsMatch([]string{id1, id2}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB02b(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub02b-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 1, 0, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	attrsOnly := map[string]string{"only": "attr"}
	id1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Attributes: attrsOnly})
	if err != nil {
		return err
	}
	returnedIDs := []string{id1}

	lang := strings.ToLower(env.Language())
	rejectsEmpty := lang == "node" || lang == "nodejs" || lang == "php"
	env.Diagnosticf("PUB-02b language=%s rejects_empty=%t", env.Language(), rejectsEmpty)

	wm := lastCapturedSeq(env.Fake.Snapshot())
	if rejectsEmpty {
		resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID})
		if err != nil {
			return err
		}
		if err := checkExpectedInBandErr(env, resp, fmt.Sprintf("empty message rejection for %s", env.Language())); err != nil {
			return err
		}
		if err := fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm); err != nil {
			return err
		}
	} else {
		id2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID})
		if err != nil {
			return err
		}
		returnedIDs = append(returnedIDs, id2)
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	exp := []fake.CallExpectation{fake.ExpectPublish("PUB-02b-attrs-only", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Attributes: attrsOnly}))}
	if !rejectsEmpty {
		exp = append(exp, fake.ExpectPublish("PUB-02b-empty", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Attributes: map[string]string{}})))
	}
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), exp...))
	v.Check(checkOKPublishMessageIDsMatch(returnedIDs, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB03(ctx context.Context, env *harness.ScenarioEnv) error {
	allowUnencoded := allowUnencodedRequestParams(env)
	topic1 := env.TopicPath(env.UniqueName("pub03-t1"))
	pub1, err := createPublisher(ctx, env, newPubReq(env, topic1, 5, 10_000_000, 30_000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pub1)

	p1Payloads := make([][]byte, 5)
	p1Exp := make([]fake.ExpectedMessage, 5)
	for i := range 5 {
		p1Payloads[i] = []byte(fmt.Sprintf("pub03-p1-%d", i))
		p1Exp[i] = fake.ExpectedMessage{Data: p1Payloads[i], Attributes: map[string]string{}}
	}
	toks1, err := publishAsyncPayloads(ctx, env, pub1, p1Payloads[:4])
	if err != nil {
		return err
	}
	if calls := env.Fake.Snapshot().PublishCalls(); len(calls) != 0 {
		return env.Failf("expected 0 Publish RPCs below count_threshold=5, got %d", len(calls))
	}
	start := time.Now()
	tok4, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pub1, Data: p1Payloads[4]})
	if err != nil {
		return err
	}
	if err := waitCompletedPublishWithin(ctx, env, 1, "count_threshold=5 batch before flush", "count_threshold=5 flush", start, 3*time.Second); err != nil {
		return err
	}
	allIDs, err := awaitAllPublishIDs(ctx, env, pub1, append(toks1, tok4))
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pub1); err != nil {
		return err
	}

	snap1 := env.Fake.Snapshot()
	if err := fake.VerifySequence(snap1, fake.DefaultVerifyOptions(), fake.ExpectPublish("PUB-03-c5", fake.PublishTopic(topic1), fake.PublishRequestParams(topic1, allowUnencoded), fake.PublishBatchSize(5), fake.PublishStatusCode(codes.OK), fake.PublishMessages(p1Exp...))); err != nil {
		return err
	}
	wm := lastCapturedSeq(snap1)

	topic2 := env.TopicPath(env.UniqueName("pub03-t2"))
	req2 := newPubReq(env, topic2, 1000, 32_000_000, 30_000)
	if env.HasCapability(harness.CapPublishFlowControl) {
		req2.MaxOutstandingMessages = proto.Int64(2000)
		req2.MaxOutstandingBytes = proto.Int64(32_000_000)
		req2.LimitExceededBehavior = proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE
	}
	pub2, err := createPublisher(ctx, env, req2)
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pub2)

	const bulkN = 1005
	p2Payloads := make([][]byte, bulkN)
	wantPayloads := make([]string, bulkN)
	for i := range bulkN {
		s := fmt.Sprintf("p03-%04d", i)
		p2Payloads[i], wantPayloads[i] = []byte(s), s
	}
	start2 := time.Now()
	toks2, err := publishAsyncPayloads(ctx, env, pub2, p2Payloads)
	if err != nil {
		return err
	}
	if err := waitCompletedPublishWithin(ctx, env, 2, "1000-msg batch before FlushPublisher", "count_threshold=1000 batch flush", start2, 5*time.Second); err != nil {
		return err
	}
	ids2, err := flushAwaitAndClose(ctx, env, &pub2, toks2)
	if err != nil {
		return err
	}
	allIDs = append(allIDs, ids2...)

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyBatchedPayloads(v, "PUB-03-bulk", topic2, allowUnencoded, publishCallsAfterSeq(snap, wm), 2, 4, 1, 1000, wantPayloads)
	v.Check(checkOKPublishMessageIDsMatch(allIDs, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB04(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub04-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 100, 1000, 30_000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	payloads, wantPayloads := make([][]byte, 4), make([]string, 4)
	for i := range 4 {
		buf := bytes.Repeat([]byte{'a' + byte(i)}, 400)
		copy(buf, fmt.Sprintf("pub04-msg-%d:", i))
		payloads[i], wantPayloads[i] = buf, string(buf)
	}
	start := time.Now()
	toks, err := publishAsyncPayloads(ctx, env, pubID, payloads)
	if err != nil {
		return err
	}
	if err := waitCompletedPublishWithin(ctx, env, 1, "byte_threshold=1000 batch before FlushPublisher", "byte_threshold=1000 batch flush", start, 3*time.Second); err != nil {
		return err
	}
	ids, err := flushAwaitAndClose(ctx, env, &pubID, toks)
	if err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyBatchedPayloads(v, "PUB-04", topic, allowUnencoded, snap.PublishCalls(), 2, 3, 1, 3, wantPayloads)
	v.Check(checkOKPublishMessageIDsMatch(ids, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB05(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub05-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 100, 1_000_000, 150))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	start := time.Now()
	p1, p2 := []byte("pub05-delay-1"), []byte("pub05-delay-2")
	toks, err := publishAsyncPayloads(ctx, env, pubID, [][]byte{p1, p2})
	if err != nil {
		return err
	}
	if err := waitCompletedPublishMessageCount(ctx, env.Fake, 2); err != nil {
		return fmt.Errorf("waiting for delay_threshold auto-flush: %w", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		return env.Failf("delay_threshold=150ms flushed too early in %s (< 50ms)", elapsed)
	}
	ids, err := awaitAllPublishIDs(ctx, env, pubID, toks)
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	verifyBatchedPayloads(v, "PUB-05", topic, allowUnencoded, snap.PublishCalls(), 1, 2, 1, 2, []string{string(p1), string(p2)})
	v.Check(checkOKPublishMessageIDsMatch(ids, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB06a(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub06a-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 100, 1024, 30_000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	payload := make([]byte, 2*1024*1024)
	for i := range payload {
		payload[i] = byte((i*31 + (i >> 8)) & 0xff)
	}
	start := time.Now()
	tok, err := publishAsync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: payload})
	if err != nil {
		return err
	}
	if err := waitCompletedPublishWithin(ctx, env, 1, "singleton 2MiB message flush before Close", "byte_threshold=1024 singleton flush", start, 3*time.Second); err != nil {
		return err
	}
	id, err := awaitPublishMessageID(ctx, env, pubID, tok)
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPublish(
		"PUB-06a", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded),
		fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK),
		fake.PublishSingleMessage(fake.ExpectedMessage{
			IgnoreData: true,
			Attributes: map[string]string{},
			CheckData: func(got []byte) error {
				if len(got) != len(payload) {
					return fmt.Errorf("2 MiB payload length mismatch: want %d, got %d", len(payload), len(got))
				}
				if bytes.Equal(got, payload) {
					return nil
				}
				for i := range payload {
					if got[i] != payload[i] {
						return fmt.Errorf("2 MiB payload mismatch at byte %d: want 0x%02x, got 0x%02x", i, payload[i], got[i])
					}
				}
				return nil
			},
		}),
	)))
	v.Check(checkOKPublishMessageIDsMatch([]string{id}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB06b(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub06b-topic")), allowUnencodedRequestParams(env)
	oversized := bytes.Repeat([]byte{'X'}, 10_000_100)

	checkOversized := func(pubID string, small []byte) (string, error) {
		wm := lastCapturedSeq(env.Fake.Snapshot())
		resp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: oversized})
		if err != nil {
			return "", err
		}
		if err := checkExpectedInBandErr(env, resp, "oversized >10MB message"); err != nil {
			return "", err
		}
		wireCalls := callsAfterSeq(env.Fake.Snapshot(), wm)
		if len(wireCalls) > 1 {
			return "", env.Failf("oversized publish emitted %d wire RPCs (expected <= 1)", len(wireCalls))
		}
		if len(wireCalls) == 1 && (wireCalls[0].FullMethod != fake.MethodPublish || wireCalls[0].StatusCode != codes.InvalidArgument) {
			return "", env.Failf("oversized publish wire call want Publish InvalidArgument, got %s %s", wireCalls[0].FullMethod, wireCalls[0].StatusCode)
		}
		return publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: small})
	}

	pub1, err := createPublisher(ctx, env, newPubReq(env, topic, 1, 0, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pub1)
	small1 := []byte("small-after-oversized")
	id1, err := checkOversized(pub1, small1)
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pub1); err != nil {
		return err
	}

	okIDs, wantSmall := []string{id1}, [][]byte{small1}
	if env.HasCapability(harness.CapPublishFlowControl) {
		req2 := newPubReq(env, topic, 1, 0, 10)
		req2.MaxOutstandingMessages = proto.Int64(1)
		req2.LimitExceededBehavior = proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK
		pub2, err := createPublisher(ctx, env, req2)
		if err != nil {
			return err
		}
		defer cleanupPublisher(ctx, env, &pub2)
		small2 := []byte("small-fc-after-oversized")
		id2, err := checkOversized(pub2, small2)
		if err != nil {
			return err
		}
		if err := closePublisherCleared(ctx, env, &pub2); err != nil {
			return err
		}
		okIDs, wantSmall = append(okIDs, id2), append(wantSmall, small2)
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(assertNoUnexpectedPublishRPCs(snap))
	oks := okPublishCalls(snap)
	v.Require(len(oks) == len(wantSmall), "expected %d OK Publish RPCs, got %d", len(wantSmall), len(oks))
	for i := range min(len(oks), len(wantSmall)) {
		v.Check(fake.ExpectPublish(fmt.Sprintf("PUB-06b-%d", i), fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: wantSmall[i], Attributes: map[string]string{}})).Check(oks[i]))
	}
	v.Check(checkOKPublishMessageIDsMatch(okIDs, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB07(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub07-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 100, 10_000_000, 30_000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	waitGatedAction := func(opName string, g *fake.Gate, fn func() error) error {
		wCtx, wCancel := context.WithCancel(ctx)
		defer wCancel()
		done, entered := make(chan error, 1), make(chan error, 1)
		go func() { done <- fn() }()
		go func() { entered <- g.WaitEntered(wCtx) }()
		premature := func(err error, stage string) error {
			if err != nil {
				return err
			}
			return env.Failf("%s returned prematurely %s", opName, stage)
		}
		select {
		case err := <-done:
			return premature(err, "before entering gate")
		case err := <-entered:
			if err != nil {
				return fmt.Errorf("waiting for %s at gate: %w", opName, err)
			}
		}
		select {
		case err := <-done:
			return premature(err, "before gated RPC completed")
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		g.Release()
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	m1, m2, m3, m4 := []byte("pub07-m1"), []byte("pub07-m2"), []byte("pub07-m3"), []byte("pub07-m4")
	g1 := fake.NewGate()
	defer g1.Release()
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PUB-07-g1", Match: fake.MatchPayloadContains(m1), Times: 1, Gate: g1})
	toks1, err := publishAsyncPayloads(ctx, env, pubID, [][]byte{m1, m2})
	if err != nil {
		return err
	}
	if calls := env.Fake.Snapshot().PublishCalls(); len(calls) != 0 {
		return env.Failf("expected 0 Publish RPCs before FlushPublisher, got %d", len(calls))
	}
	if err := waitGatedAction("FlushPublisher", g1, func() error { return flushPublisher(ctx, env, pubID) }); err != nil {
		return err
	}
	ids1, err := awaitAllPublishIDs(ctx, env, pubID, toks1)
	if err != nil {
		return err
	}

	g2 := fake.NewGate()
	defer g2.Release()
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PUB-07-g2", Match: fake.MatchPayloadContains(m3), Times: 1, Gate: g2})
	toks2, err := publishAsyncPayloads(ctx, env, pubID, [][]byte{m3, m4})
	if err != nil {
		return err
	}
	if calls := env.Fake.Snapshot().PublishCalls(); len(calls) != 1 {
		return env.Failf("expected 1 Publish RPC before StopPublisher, got %d", len(calls))
	}
	if err := waitGatedAction("StopPublisher", g2, func() error { return stopPublisher(ctx, env, pubID) }); err != nil {
		return err
	}
	ids2, err := awaitAllPublishIDs(ctx, env, pubID, toks2)
	if err != nil {
		return err
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("PUB-07-flush", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishMessages(fake.ExpectedMessage{Data: m1, Attributes: map[string]string{}}, fake.ExpectedMessage{Data: m2, Attributes: map[string]string{}})),
		fake.ExpectPublish("PUB-07-stop", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishStatusCode(codes.OK), fake.PublishMessages(fake.ExpectedMessage{Data: m3, Attributes: map[string]string{}}, fake.ExpectedMessage{Data: m4, Attributes: map[string]string{}})),
	))
	v.Check(checkOKPublishMessageIDsMatch(append(ids1, ids2...), snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB08(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub08-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 1, 0, 10))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	beforeData := []byte("before-stop")
	id1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: beforeData})
	if err != nil {
		return err
	}
	if err := waitCompletedPublishCount(ctx, env.Fake, 1); err != nil {
		return err
	}
	wm := lastCapturedSeq(env.Fake.Snapshot())

	if err := stopPublisher(ctx, env, pubID); err != nil {
		return err
	}
	postStopResp, err := publishSync(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("after-stop")})
	if err != nil {
		return err
	}
	if err := checkExpectedInBandErr(env, postStopResp, "Publish after StopPublisher"); err != nil {
		return err
	}
	if err := fake.AssertNoCallsAfter(env.Fake.Snapshot(), wm); err != nil {
		return err
	}

	closedID := pubID
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	postCloseResp, postCloseErr := env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: closedID, Data: []byte("after-close")})
	if postCloseErr == nil {
		if err := checkExpectedInBandErr(env, postCloseResp, "Publish after ClosePublisher"); err != nil {
			return err
		}
	} else if harness.IsUnimplemented(postCloseErr) {
		return env.Failf("Publish after ClosePublisher returned unexpected UNIMPLEMENTED: %v", postCloseErr)
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.AssertNoCallsAfter(snap, wm))
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPublish("PUB-08-before", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: beforeData, Attributes: map[string]string{}}))))
	v.Check(checkOKPublishMessageIDsMatch([]string{id1}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB09(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub09-topic")), allowUnencodedRequestParams(env)
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 100, 1_000_000, 30_000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	d1, d2 := []byte("blocking-1"), []byte("blocking-2")
	a1, a2 := map[string]string{"seq": "1"}, map[string]string{"seq": "2"}
	id1, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: d1, Attributes: a1, UseBlockingPublisher: true})
	if err != nil {
		return err
	}
	id2, err := publishSyncOK(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: d2, Attributes: a2, UseBlockingPublisher: true})
	if err != nil {
		return err
	}
	if err := waitCompletedPublishCount(ctx, env.Fake, 2); err != nil {
		return fmt.Errorf("waiting for blocking publishes before Close: %w", err)
	}
	if err := closePublisherCleared(ctx, env, &pubID); err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("PUB-09-1", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: d1, Attributes: a1})),
		fake.ExpectPublish("PUB-09-2", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(1), fake.PublishStatusCode(codes.OK), fake.PublishSingleMessage(fake.ExpectedMessage{Data: d2, Attributes: a2})),
	))
	v.Check(checkOKPublishMessageIDsMatch([]string{id1, id2}, snap))
	return finishPublishScenario(ctx, env, v)
}

func runPUB10(ctx context.Context, env *harness.ScenarioEnv) error {
	topic, allowUnencoded := env.TopicPath(env.UniqueName("pub10-topic")), allowUnencodedRequestParams(env)
	env.Fake.AddPublishRule(fake.PublishRule{Name: "PUB-10-short-ids", Times: 1, MessageIDCountDelta: -1})
	pubID, err := createPublisher(ctx, env, newPubReq(env, topic, 2, 10_000_000, 5000))
	if err != nil {
		return err
	}
	defer cleanupPublisher(ctx, env, &pubID)

	m1, m2, m3, m4 := []byte("pub10-m1"), []byte("pub10-m2"), []byte("pub10-m3"), []byte("pub10-m4")
	var toks1, okIDs1 []string
	var errCount int
	for _, msg := range [][]byte{m1, m2} {
		r, err := sendPublish(ctx, env, &proxypb.PublishRequest{PublisherId: pubID, Data: msg}, true)
		if err != nil {
			return err
		}
		if r.GetErrorCode() != 0 {
			if err := rejectInBandUnimpl(env, r.GetErrorCode(), r.GetErrorMessage(), "Publish(async)"); err != nil {
				return err
			}
			errCount++
			continue
		}
		if r.GetPublishToken() == "" {
			return env.Failf("Publish(async) returned empty publish_token with error_code=0")
		}
		toks1 = append(toks1, r.GetPublishToken())
	}
	for _, tok := range toks1 {
		res, err := awaitPublishResult(ctx, env, pubID, tok, false)
		if err != nil {
			return err
		}
		if res.GetErrorCode() != 0 {
			if err := rejectInBandUnimpl(env, res.GetErrorCode(), res.GetErrorMessage(), "PUB-10 AwaitPublish"); err != nil {
				return err
			}
			errCount++
		} else {
			if res.GetMessageId() == "" {
				return env.Failf("OK message in mismatched batch returned empty message_id")
			}
			okIDs1 = append(okIDs1, res.GetMessageId())
		}
	}
	if errCount == 0 {
		return env.Failf("expected >= 1 error on mismatched message_ids count, got 0 errors")
	}

	toks2, err := publishAsyncPayloads(ctx, env, pubID, [][]byte{m3, m4})
	if err != nil {
		return err
	}
	ids2, err := flushAwaitAndClose(ctx, env, &pubID, toks2)
	if err != nil {
		return err
	}

	snap := env.Fake.Snapshot()
	v := env.NewVerifier()
	v.Check(fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("PUB-10-mismatch", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(2), fake.PublishStatusCode(codes.OK), fake.PublishMessages(fake.ExpectedMessage{Data: m1, Attributes: map[string]string{}}, fake.ExpectedMessage{Data: m2, Attributes: map[string]string{}})),
		fake.ExpectPublish("PUB-10-recovery", fake.PublishTopic(topic), fake.PublishRequestParams(topic, allowUnencoded), fake.PublishBatchSize(2), fake.PublishStatusCode(codes.OK), fake.PublishMessages(fake.ExpectedMessage{Data: m3, Attributes: map[string]string{}}, fake.ExpectedMessage{Data: m4, Attributes: map[string]string{}})),
	))
	if calls := snap.PublishCalls(); len(calls) == 2 {
		if diff := cmp.Diff(calls[1].ResponseMessageIDs, ids2); diff != "" {
			v.Checkf("PUB-10 recovery message_ids mismatch (-server +proxy):\n%s", diff)
		}
		if len(okIDs1) > 0 {
			if len(calls[0].ResponseMessageIDs) == 0 {
				v.Checkf("PUB-10: expected server to return 1 message_id on batch 1, got 0")
			} else {
				for _, okID := range okIDs1 {
					if okID != calls[0].ResponseMessageIDs[0] {
						v.Checkf("PUB-10 batch 1 OK message_id %q mismatch vs server %v", okID, calls[0].ResponseMessageIDs)
					}
				}
			}
		}
	}
	return finishPublishScenario(ctx, env, v)
}
