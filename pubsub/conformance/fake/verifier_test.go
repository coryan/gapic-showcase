// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fake_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func int64Ptr(v int64) *int64 { return &v }

func TestVerifierSequenceUnorderedAndUnexpectedRPCs(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	pubCall := fake.CapturedCall{
		Seq:        1,
		Timestamp:  t0,
		EndTime:    t0.Add(10 * time.Millisecond),
		Completed:  true,
		FullMethod: fake.MethodPublish,
		StatusCode: codes.OK,
		PublishReq: &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m1")}},
		},
	}
	pingCall := fake.CapturedCall{
		Seq:              2,
		Timestamp:        t0.Add(15 * time.Millisecond),
		EndTime:          t0.Add(15 * time.Millisecond),
		Completed:        true,
		FullMethod:       fake.MethodStreamingPull,
		StreamID:         "stream-1",
		StreamSeq:        1,
		StreamingPullReq: &pubsubpb.StreamingPullRequest{},
	}
	ackCall := fake.CapturedCall{
		Seq:        3,
		Timestamp:  t0.Add(20 * time.Millisecond),
		EndTime:    t0.Add(25 * time.Millisecond),
		Completed:  true,
		FullMethod: fake.MethodAcknowledge,
		StatusCode: codes.OK,
		AckReq: &pubsubpb.AcknowledgeRequest{
			Subscription: "projects/p/subscriptions/s",
			AckIds:       []string{"a1", "a2"},
		},
	}

	snap := fake.Snapshot{Calls: []fake.CapturedCall{pubCall, pingCall, ackCall}}

	pubExp := fake.ExpectPublish("pub-1", fake.PublishTopic("projects/p/topics/t"))
	ackExp := fake.ExpectAck("ack-1", fake.AckSubscription("projects/p/subscriptions/s"), fake.AckIDsExact("a1", "a2"))

	// Positive VerifySequence ignoring keepalives.
	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), pubExp, ackExp); err != nil {
		t.Fatalf("VerifySequence positive failed: %v", err)
	}
	// Negative VerifySequence without IgnoreKeepalives.
	if err := fake.VerifySequence(snap, fake.VerifyOptions{IgnoreKeepalives: false}, pubExp, ackExp); err == nil {
		t.Fatalf("expected VerifySequence to fail when keepalive is not ignored")
	}
	// Positive VerifySequence including explicit keepalive expectation.
	if err := fake.VerifySequence(snap, fake.VerifyOptions{IgnoreKeepalives: false}, pubExp, fake.ExpectStreamingPullKeepalive("ping"), ackExp); err != nil {
		t.Fatalf("VerifySequence with keepalive failed: %v", err)
	}
	// Negative VerifySequence out-of-order.
	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), ackExp, pubExp); err == nil {
		t.Fatalf("expected VerifySequence out-of-order to fail")
	}

	// Positive VerifyUnordered (reversed & overlapping expectations).
	if err := fake.VerifyUnordered(snap, fake.DefaultVerifyOptions(), ackExp, pubExp); err != nil {
		t.Fatalf("VerifyUnordered positive failed: %v", err)
	}
	// Negative VerifyUnordered with wrong ack ID.
	badAckExp := fake.ExpectAck("bad-ack", fake.AckIDsExact("a1", "wrong"))
	if err := fake.VerifyUnordered(snap, fake.DefaultVerifyOptions(), pubExp, badAckExp); err == nil {
		t.Fatalf("expected VerifyUnordered mismatch to fail")
	}

	// Unexpected RPC check & allowedMethods check.
	unexpSnap := fake.Snapshot{
		Calls: []fake.CapturedCall{
			pubCall,
			{Seq: 2, FullMethod: "/google.pubsub.v1.Publisher/CreateTopic", Unexpected: true, StatusCode: codes.Unimplemented},
		},
	}
	if err := fake.AssertNoUnexpectedRPCs(unexpSnap); err == nil {
		t.Fatalf("expected AssertNoUnexpectedRPCs to fail on Unexpected call")
	}
	if err := fake.AssertNoUnexpectedRPCs(snap, fake.MethodPublish); err == nil {
		t.Fatalf("expected AssertNoUnexpectedRPCs to fail on method outside allowlist")
	}
	if err := fake.AssertNoCallsAfter(snap, 2); err == nil {
		t.Fatalf("expected AssertNoCallsAfter(2) to fail when Seq=3 present")
	}
	if err := fake.AssertNoCallsAfter(snap, 3); err != nil {
		t.Fatalf("AssertNoCallsAfter(3) failed: %v", err)
	}
}

func TestVerifierPublishChecksAndTraceparent(t *testing.T) {
	binData := []byte{0x00, 0xff, 0x80, 0xc5}
	validTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ts := timestamppb.New(time.Unix(1700000000, 0))
	tel := &pubsubpb.PubsubClientTelemetry{
		Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{
			PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{
				HedgedAttemptCount: 1,
				PublishStartTime:   ts,
			},
		},
	}

	c := fake.CapturedCall{
		Seq:           1,
		FullMethod:    fake.MethodPublish,
		StatusCode:    codes.OK,
		Compression:   "gzip",
		Metadata:      metadata.Pairs("x-custom", "val1", fake.RequestParamsHeaderName, "topic=projects%2Fp%2Ftopics%2Ft"),
		RequestParams: []string{"topic=projects%2Fp%2Ftopics%2Ft"},
		Telemetry:     tel,
		PublishReq: &pubsubpb.PublishRequest{
			Topic: "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{
				{
					Data:        binData,
					OrderingKey: "k1",
					Attributes: map[string]string{
						"k":                          "v",
						fake.TraceparentAttributeKey: validTP,
					},
				},
			},
		},
	}
	snap := fake.Snapshot{Calls: []fake.CapturedCall{c}}

	// Positive composite Publish check.
	good := fake.ExpectPublish("good-pub",
		fake.PublishTopic("projects/p/topics/t"),
		fake.PublishBatchSize(1),
		fake.PublishBatchSizeRange(1, 10),
		fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:               binData,
			Attributes:         map[string]string{"k": "v"},
			RequiredAttributes: map[string]string{"k": "v"},
			OrderingKey:        "k1",
			RequireTraceparent: true,
		}),
		fake.PublishNoMixedOrderingKeys(),
		fake.PublishCompression("gzip"),
		fake.PublishTelemetry(1, true),
		fake.PublishRequestParams("projects/p/topics/t", false),
		fake.PublishHeader("x-custom", "val1"),
		fake.PublishStatusCode(codes.OK),
	)
	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), good); err != nil {
		t.Fatalf("Publish checks positive failed: %v", err)
	}

	// Verify "" and "identity" equivalence.
	cUncomp := c.Clone()
	cUncomp.Compression = "identity"
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cUncomp}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("uncomp", fake.PublishCompression(""))); err != nil {
		t.Fatalf("identity compression equivalence failed: %v", err)
	}

	// Positive: Traceparent without RequireTraceparent + Attributes ignores dynamic/pinned traceparent.
	cTraceOnly := c.Clone()
	cTraceOnly.PublishReq.Messages[0].Attributes = map[string]string{
		"k":                          "v",
		fake.TraceparentAttributeKey: validTP,
	}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cTraceOnly}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("tp-plus-attrs", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:        binData,
			Attributes:  map[string]string{"k": "v"},
			OrderingKey: "k1",
			Traceparent: validTP,
		}))); err != nil {
		t.Fatalf("ExpectedMessage with Traceparent + Attributes failed: %v", err)
	}

	// Positive: Traceparent without RequireTraceparent + Attributes also omits optional googclient_tracestate unless pinned.
	cTraceWithTS := c.Clone()
	cTraceWithTS.PublishReq.Messages[0].Attributes = map[string]string{
		"k":                          "v",
		fake.TraceparentAttributeKey: validTP,
		fake.TracestateAttributeKey:  "rojo=00f067aa0ba902b7",
	}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cTraceWithTS}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("tp-plus-attrs-and-tracestate", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:        binData,
			Attributes:  map[string]string{"k": "v"},
			OrderingKey: "k1",
			Traceparent: validTP,
		}))); err != nil {
		t.Fatalf("ExpectedMessage with Traceparent + Attributes + googclient_tracestate failed: %v", err)
	}
	// Negative: Pinned googclient_tracestate mismatch with Traceparent fails; unrequested googclient_tracestate when neither Traceparent nor RequireTraceparent is set still fails.
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cTraceWithTS}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("pinned-ts-mismatch-with-tp", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:        binData,
			Attributes:  map[string]string{"k": "v", fake.TracestateAttributeKey: "wrong=1"},
			OrderingKey: "k1",
			Traceparent: validTP,
		}))); err == nil {
		t.Fatalf("expected pinned googclient_tracestate mismatch with Traceparent to fail")
	}
	cOnlyTS := c.Clone()
	cOnlyTS.PublishReq.Messages[0].Attributes = map[string]string{
		"k":                         "v",
		fake.TracestateAttributeKey: "rojo=00f067aa0ba902b7",
	}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cOnlyTS}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("unrequested-ts", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:        binData,
			Attributes:  map[string]string{"k": "v"},
			OrderingKey: "k1",
		}))); err == nil {
		t.Fatalf("expected unrequested googclient_tracestate without Traceparent/RequireTraceparent to fail")
	}

	// Positive: RequireTraceparent + Attributes also omits optional googclient_tracestate unless pinned.
	cWithTS := c.Clone()
	cWithTS.PublishReq.Messages[0].Attributes[fake.TracestateAttributeKey] = "rojo=00f067aa0ba902b7"
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cWithTS}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("tp-plus-tracestate", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:               binData,
			Attributes:         map[string]string{"k": "v"},
			OrderingKey:        "k1",
			RequireTraceparent: true,
		}))); err != nil {
		t.Fatalf("ExpectedMessage with RequireTraceparent + googclient_tracestate failed: %v", err)
	}
	// Negative: Pinned googclient_tracestate mismatch in Attributes still fails.
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cWithTS}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("pinned-ts-mismatch", fake.PublishSingleMessage(fake.ExpectedMessage{
			Data:               binData,
			Attributes:         map[string]string{"k": "v", fake.TracestateAttributeKey: "wrong=1"},
			OrderingKey:        "k1",
			RequireTraceparent: true,
		}))); err == nil {
		t.Fatalf("expected pinned googclient_tracestate mismatch to fail")
	}

	// Negative cases for every Publish check:
	negatives := []struct {
		name  string
		mut   func(*fake.CapturedCall)
		check fake.PublishCheck
	}{
		{"wrong topic", nil, fake.PublishTopic("wrong")},
		{"wrong batch size", nil, fake.PublishBatchSize(2)},
		{"out of batch range", nil, fake.PublishBatchSizeRange(2, 5)},
		{"wrong binary data", nil, fake.PublishSingleMessage(fake.ExpectedMessage{Data: []byte{0x01}})},
		{"wrong ordering key", nil, fake.PublishSingleMessage(fake.ExpectedMessage{IgnoreData: true, OrderingKey: "k2"})},
		{"missing traceparent", func(x *fake.CapturedCall) { delete(x.PublishReq.Messages[0].Attributes, fake.TraceparentAttributeKey) }, fake.PublishSingleMessage(fake.ExpectedMessage{IgnoreData: true, OrderingKey: "k1", RequireTraceparent: true})},
		{"all-zero trace id traceparent", func(x *fake.CapturedCall) {
			x.PublishReq.Messages[0].Attributes[fake.TraceparentAttributeKey] = "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
		}, fake.PublishSingleMessage(fake.ExpectedMessage{IgnoreData: true, OrderingKey: "k1", RequireTraceparent: true})},
		{"mixed ordering keys", func(x *fake.CapturedCall) {
			x.PublishReq.Messages = append(x.PublishReq.Messages, &pubsubpb.PubsubMessage{Data: []byte("2"), OrderingKey: "k2"})
		}, fake.PublishNoMixedOrderingKeys()},
		{"mixed empty and non-empty ordering keys", func(x *fake.CapturedCall) {
			x.PublishReq.Messages = append(x.PublishReq.Messages, &pubsubpb.PubsubMessage{Data: []byte("2"), OrderingKey: ""})
		}, fake.PublishNoMixedOrderingKeys()},
		{"wrong compression", nil, fake.PublishCompression("deflate")},
		{"wrong hedged attempt", nil, fake.PublishTelemetry(0, true)},
		{"unencoded request params strict", func(x *fake.CapturedCall) {
			x.RequestParams = []string{"topic=projects/p/topics/t"}
		}, fake.PublishRequestParams("projects/p/topics/t", false)},
		{"missing header", nil, fake.PublishHeader("x-custom", "missing")},
		{"wrong status code", nil, fake.PublishStatusCode(codes.Unavailable)},
	}
	for _, tc := range negatives {
		cloned := c.Clone()
		if tc.mut != nil {
			tc.mut(&cloned)
		}
		err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cloned}}, fake.DefaultVerifyOptions(), fake.ExpectPublish(tc.name, tc.check))
		if err == nil {
			t.Errorf("%s: expected failure, got nil", tc.name)
		}
	}

	// Unencoded request params passes when allowUnencoded=true (Rust client behavior).
	cUnenc := c.Clone()
	cUnenc.RequestParams = []string{"topic=projects/p/topics/t"}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{cUnenc}}, fake.DefaultVerifyOptions(),
		fake.ExpectPublish("rust-unenc", fake.PublishRequestParams("projects/p/topics/t", true))); err != nil {
		t.Fatalf("allowUnencoded=true should pass: %v", err)
	}

	// Telemetry hedge sequence positive & negative.
	c0 := c.Clone()
	c0.Seq = 1
	c0.Telemetry = &pubsubpb.PubsubClientTelemetry{
		Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{
			PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: 0, PublishStartTime: ts},
		},
	}
	c1 := c.Clone()
	c1.Seq = 2
	hedgeSnap := fake.Snapshot{Calls: []fake.CapturedCall{c0, c1}}
	if err := fake.AssertTelemetryHedgeSequence(hedgeSnap, []int32{0, 1}, true); err != nil {
		t.Fatalf("AssertTelemetryHedgeSequence positive failed: %v", err)
	}
	c1DiffTS := c1.Clone()
	c1DiffTS.Telemetry.GetPublishOperation().PublishStartTime = timestamppb.New(time.Unix(1700000999, 0))
	if err := fake.AssertTelemetryHedgeSequence(fake.Snapshot{Calls: []fake.CapturedCall{c0, c1DiffTS}}, []int32{0, 1}, true); err == nil {
		t.Fatalf("expected AssertTelemetryHedgeSequence to fail when PublishStartTime differs")
	}
}

func TestVerifierOrderingConcurrencyBoundsAndAckModAck(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	kA1 := fake.CapturedCall{
		Seq: 1, Timestamp: t0, EndTime: t0.Add(10 * time.Millisecond), Completed: true,
		FullMethod: fake.MethodPublish, InFlightAtStart: 1, MaxConcurrent: 2, KeyInFlightAtStart: 1, KeyMaxConcurrent: 1,
		PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a1"), OrderingKey: "A"}}},
	}
	kB1 := fake.CapturedCall{
		Seq: 2, Timestamp: t0.Add(2 * time.Millisecond), EndTime: t0.Add(8 * time.Millisecond), Completed: true,
		FullMethod: fake.MethodPublish, InFlightAtStart: 2, MaxConcurrent: 2, KeyInFlightAtStart: 1, KeyMaxConcurrent: 1,
		PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("b1"), OrderingKey: "B"}}},
	}
	kA2 := fake.CapturedCall{
		Seq: 3, Timestamp: t0.Add(15 * time.Millisecond), EndTime: t0.Add(20 * time.Millisecond), Completed: true,
		FullMethod: fake.MethodPublish, InFlightAtStart: 1, MaxConcurrent: 1, KeyInFlightAtStart: 1, KeyMaxConcurrent: 1,
		PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a2"), OrderingKey: "A"}}},
	}
	snap := fake.Snapshot{
		Calls:                     []fake.CapturedCall{kA1, kB1, kA2},
		MaxConcurrentPublishTotal: 2,
		MaxConcurrentPublishByKey: map[string]int{"A": 1, "B": 1},
	}

	if err := fake.AssertOrderedPublishFIFO(snap, "A", [][]byte{[]byte("a1"), []byte("a2")}); err != nil {
		t.Fatalf("AssertOrderedPublishFIFO positive: %v", err)
	}
	if err := fake.AssertOrderedPublishFIFO(snap, "A", [][]byte{[]byte("a2"), []byte("a1")}); err == nil {
		t.Fatalf("expected AssertOrderedPublishFIFO wrong order to fail")
	}
	if err := fake.AssertMaxConcurrentPublishForKey(snap, "A", 1); err != nil {
		t.Fatalf("AssertMaxConcurrentPublishForKey positive: %v", err)
	}
	if err := fake.AssertConcurrentPublishesObserved(snap, "A", "B"); err != nil {
		t.Fatalf("AssertConcurrentPublishesObserved positive: %v", err)
	}
	// Negative: sequential non-overlapping keys fail AssertConcurrentPublishesObserved.
	seqSnap := fake.Snapshot{
		Calls:                     []fake.CapturedCall{kA1, kA2},
		MaxConcurrentPublishTotal: 1,
		MaxConcurrentPublishByKey: map[string]int{"A": 1},
	}
	if err := fake.AssertConcurrentPublishesObserved(seqSnap, "A"); err == nil {
		t.Fatalf("expected AssertConcurrentPublishesObserved to fail for sequential calls")
	}
	// Negative: overlapping same-key calls fail AssertMaxConcurrentPublishForKey even if counters mocked to 1.
	badOverlap := kA2.Clone()
	badOverlap.Timestamp = t0.Add(5 * time.Millisecond)
	if err := fake.AssertMaxConcurrentPublishForKey(fake.Snapshot{Calls: []fake.CapturedCall{kA1, badOverlap}}, "A", 1); err == nil {
		t.Fatalf("expected AssertMaxConcurrentPublishForKey to catch interval overlap")
	}
	// Negative: zero-duration call [t0+5ms, t0+5ms] nested strictly inside [t0, t0+10ms] MUST fail overlap check.
	zeroNested := fake.CapturedCall{
		Seq: 4, Timestamp: t0.Add(5 * time.Millisecond), EndTime: t0.Add(5 * time.Millisecond), Completed: true,
		FullMethod: fake.MethodPublish, InFlightAtStart: 1, MaxConcurrent: 1, KeyInFlightAtStart: 1, KeyMaxConcurrent: 1,
		PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("az"), OrderingKey: "A"}}},
	}
	if err := fake.AssertMaxConcurrentPublishForKey(fake.Snapshot{Calls: []fake.CapturedCall{kA1, zeroNested}}, "A", 1); err == nil {
		t.Fatalf("expected AssertMaxConcurrentPublishForKey to catch zero-duration nested overlap")
	}
	// Positive: zero-duration call at exact end boundary [t0+10ms, t0+10ms] after [t0, t0+10ms] is sequential (overlap 1).
	zeroSeq := zeroNested.Clone()
	zeroSeq.Timestamp = t0.Add(10 * time.Millisecond)
	zeroSeq.EndTime = t0.Add(10 * time.Millisecond)
	if err := fake.AssertMaxConcurrentPublishForKey(fake.Snapshot{Calls: []fake.CapturedCall{kA1, zeroSeq}}, "A", 1); err != nil {
		t.Fatalf("sequential zero-duration boundary should pass overlap=1, got: %v", err)
	}
	// AssertOrderedPublishFIFO ignores transient failed retries, while AssertOrderedPublishAttemptedFIFO checks all attempts.
	kA1Failed := kA1.Clone()
	kA1Failed.StatusCode = codes.Unavailable
	retryFIFOSnap := fake.Snapshot{Calls: []fake.CapturedCall{kA1Failed, kA1, kA2}}
	if err := fake.AssertOrderedPublishFIFO(retryFIFOSnap, "A", [][]byte{[]byte("a1"), []byte("a2")}); err != nil {
		t.Fatalf("AssertOrderedPublishFIFO should ignore failed retry duplicate: %v", err)
	}
	if err := fake.AssertOrderedPublishAttemptedFIFO(retryFIFOSnap, "A", [][]byte{[]byte("a1"), []byte("a2")}); err == nil {
		t.Fatalf("expected AssertOrderedPublishAttemptedFIFO to reject transient duplicate attempt")
	}
	if err := fake.AssertOrderedPublishAttemptedFIFO(retryFIFOSnap, "A", [][]byte{[]byte("a1"), []byte("a1"), []byte("a2")}); err != nil {
		t.Fatalf("AssertOrderedPublishAttemptedFIFO with full attempt list failed: %v", err)
	}

	// Uncompleted in-flight Publish (Completed=false, StatusCode=0=OK) must be
	// excluded from AssertOrderedPublishFIFO while included in AssertOrderedPublishAttemptedFIFO.
	kA2InFlight := kA2.Clone()
	kA2InFlight.Completed = false
	kA2InFlight.EndTime = time.Time{}
	kA2InFlight.StatusCode = codes.OK
	inFlightSnap := fake.Snapshot{Calls: []fake.CapturedCall{kA1, kA2InFlight}}
	if err := fake.AssertOrderedPublishFIFO(inFlightSnap, "A", [][]byte{[]byte("a1")}); err != nil {
		t.Fatalf("AssertOrderedPublishFIFO should ignore uncompleted in-flight call: %v", err)
	}
	if err := fake.AssertOrderedPublishFIFO(inFlightSnap, "A", [][]byte{[]byte("a1"), []byte("a2")}); err == nil {
		t.Fatalf("expected AssertOrderedPublishFIFO to fail when expecting uncompleted in-flight call")
	}
	if err := fake.AssertOrderedPublishAttemptedFIFO(inFlightSnap, "A", [][]byte{[]byte("a1"), []byte("a2")}); err != nil {
		t.Fatalf("AssertOrderedPublishAttemptedFIFO should include uncompleted in-flight call: %v", err)
	}

	// Ack & ModAck bounds + deadline + AllAcked/AllNacked + EOD split retry.
	ids1000 := make([]string, 1000)
	for i := range ids1000 {
		ids1000[i] = "id"
	}
	ackOK := fake.CapturedCall{
		Seq: 1, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK,
		AckReq: &pubsubpb.AcknowledgeRequest{Subscription: "sub", AckIds: []string{"a1", "a2"}},
	}
	nackOK := fake.CapturedCall{
		Seq: 2, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.OK,
		ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{Subscription: "sub", AckIds: []string{"n1"}, AckDeadlineSeconds: 0},
	}
	extendOK := fake.CapturedCall{
		Seq: 3, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.OK,
		ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{Subscription: "sub", AckIds: []string{"e1"}, AckDeadlineSeconds: 60},
	}
	leaseSnap := fake.Snapshot{Calls: []fake.CapturedCall{ackOK, nackOK, extendOK}}

	if err := fake.AssertAckAndModAckWithinBounds(leaseSnap); err != nil {
		t.Fatalf("AssertAckAndModAckWithinBounds positive: %v", err)
	}
	if err := fake.AssertAllAckedIDs(leaseSnap, "a2", "a1"); err != nil {
		t.Fatalf("AssertAllAckedIDs positive: %v", err)
	}
	if err := fake.AssertAllNackedIDs(leaseSnap, "n1"); err != nil {
		t.Fatalf("AssertAllNackedIDs positive: %v", err)
	}
	if err := fake.AssertAllAckedIDs(leaseSnap, "a1"); err == nil {
		t.Fatalf("expected AssertAllAckedIDs mismatch to fail")
	}

	// Bounds negatives: 1001 IDs, >512 KiB, deadline < 10.
	over1000 := make([]string, 1001)
	for i := range over1000 {
		over1000[i] = "x"
	}
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{Seq: 1, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: over1000}},
	}}); err == nil {
		t.Fatalf("expected >1000 AckIDs to fail bounds check")
	}
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{Seq: 1, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{strings.Repeat("z", 513*1024)}}},
	}}); err == nil {
		t.Fatalf("expected >512KiB Ack request to fail bounds check")
	}
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{Seq: 1, ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"x"}, AckDeadlineSeconds: 5}},
	}}); err == nil {
		t.Fatalf("expected deadline=5 (<10) to fail bounds check")
	}
	// StreamingPull bounds negatives: slice length mismatch, StreamSeq == 0 with 1001 AckIds, StreamSeq > 0 with StreamAckDeadlineSeconds == 5.
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq: 1, FullMethod: fake.MethodStreamingPull, StreamID: "s1", StreamSeq: 1,
			StreamingPullReq: &pubsubpb.StreamingPullRequest{
				ModifyDeadlineAckIds:  []string{"a1", "a2"},
				ModifyDeadlineSeconds: []int32{60},
			},
		},
	}}); err == nil {
		t.Fatalf("expected StreamingPull ModifyDeadline slice length mismatch to fail bounds check")
	}
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq: 1, FullMethod: fake.MethodStreamingPull, StreamID: "s1", StreamSeq: 0,
			StreamingPullReq: &pubsubpb.StreamingPullRequest{
				Subscription:             "projects/p/subscriptions/s",
				StreamAckDeadlineSeconds: 60,
				AckIds:                   over1000,
			},
		},
	}}); err == nil {
		t.Fatalf("expected StreamSeq==0 with 1001 AckIds to fail bounds check")
	}
	if err := fake.AssertAckAndModAckWithinBounds(fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq: 1, FullMethod: fake.MethodStreamingPull, StreamID: "s1", StreamSeq: 1,
			StreamingPullReq: &pubsubpb.StreamingPullRequest{
				StreamAckDeadlineSeconds: 5,
			},
		},
	}}); err == nil {
		t.Fatalf("expected StreamSeq>0 with StreamAckDeadlineSeconds=5 to fail bounds check")
	}
	// Boundary 1000 succeeds.
	if err := fake.ExpectAck("1000-ok", fake.AckWithinBounds()).Check(fake.CapturedCall{
		FullMethod: fake.MethodAcknowledge,
		AckReq:     &pubsubpb.AcknowledgeRequest{AckIds: ids1000},
	}); err != nil {
		t.Fatalf("exact 1000 AckIDs boundary failed: %v", err)
	}
	// ModAck deadline range check.
	if err := fake.ExpectModAck("eod-range", fake.ModAckDeadlineRange(60, 600)).Check(extendOK); err != nil {
		t.Fatalf("ModAckDeadlineRange(60, 600) failed: %v", err)
	}
	if err := fake.ExpectModAck("eod-range-fail", fake.ModAckDeadlineRange(60, 600)).Check(fake.CapturedCall{
		FullMethod: fake.MethodModifyAckDeadline,
		ModAckReq:  &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"e1"}, AckDeadlineSeconds: 30},
	}); err == nil {
		t.Fatalf("expected deadline 30 to fail ModAckDeadlineRange(60, 600)")
	}

	// AssertEODSplitRetry negative when permanently failed or succeeded ID is retried,
	// when batch is unmixed, or when transient retry never succeeds with OK.
	badRetrySnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq:                     1,
			Completed:               true,
			FullMethod:              fake.MethodAcknowledge,
			StatusCode:              codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub,
			ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID, "trans": fake.EODTransientOther},
			AckReq:                  &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm", "trans", "ok"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans", "ok"}}},
	}}
	if err := fake.AssertEODSplitRetry(badRetrySnap, []string{"perm"}, []string{"trans"}, []string{"ok"}); err == nil {
		t.Fatalf("expected AssertEODSplitRetry to catch retried succeeded ID")
	}
	if err := fake.AssertEODSplitRetry(badRetrySnap, []string{"perm"}, []string{"missing"}, nil); err == nil {
		t.Fatalf("expected AssertEODSplitRetry to fail when transient ID missing from retry")
	}
	unmixedSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq:                     1,
			Completed:               true,
			FullMethod:              fake.MethodAcknowledge,
			StatusCode:              codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub,
			ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID},
			AckReq:                  &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans"}}},
	}}
	if err := fake.AssertEODSplitRetry(unmixedSnap, []string{"perm"}, []string{"trans"}, nil); err == nil {
		t.Fatalf("expected AssertEODSplitRetry to reject unmixed batch")
	}
	neverOKSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq:                     1,
			Completed:               true,
			FullMethod:              fake.MethodAcknowledge,
			StatusCode:              codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub,
			ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID, "trans": fake.EODTransientOther},
			AckReq:                  &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm", "trans", "ok"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.Unavailable, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans"}}},
	}}
	if err := fake.AssertEODSplitRetry(neverOKSnap, []string{"perm"}, []string{"trans"}, []string{"ok"}); err == nil {
		t.Fatalf("expected AssertEODSplitRetry to reject transient retry that never returned OK")
	}
	// Positive: transient fails once with Unavailable, then retries with OK; also ModAck extension split followed by later nack of ok.
	eventualOKSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq:                     1,
			Completed:               true,
			FullMethod:              fake.MethodAcknowledge,
			StatusCode:              codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub,
			ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID, "trans": fake.EODTransientOther},
			AckReq:                  &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm", "trans", "ok"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.Unavailable, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans"}}},
		{Seq: 3, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans"}}},
	}}
	if err := fake.AssertEODSplitRetry(eventualOKSnap, []string{"perm"}, []string{"trans"}, []string{"ok"}); err != nil {
		t.Fatalf("AssertEODSplitRetry should pass when transient eventually returns OK: %v", err)
	}
	modAckThenNackSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq:                     1,
			Completed:               true,
			FullMethod:              fake.MethodModifyAckDeadline,
			StatusCode:              codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub,
			ResponseErrorInfoReason: fake.EODReasonModAckFailed,
			ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID, "trans": fake.EODTransientOther},
			ModAckReq:               &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"perm", "trans", "ok"}, AckDeadlineSeconds: 60},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.OK, ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"trans"}, AckDeadlineSeconds: 60}},
		{Seq: 3, Completed: true, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.OK, ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"ok"}, AckDeadlineSeconds: 0}},
	}}
	if err := fake.AssertEODSplitRetry(modAckThenNackSnap, []string{"perm"}, []string{"trans"}, []string{"ok"}); err != nil {
		t.Fatalf("AssertEODSplitRetry should scope ModAck extension vs later nack: %v", err)
	}
}

func TestVerifierStreamingPullInitialAndClientIDs(t *testing.T) {
	id1 := uuid.NewString()
	id2 := uuid.NewString()

	initCall := fake.CapturedCall{
		Seq:           1,
		FullMethod:    fake.MethodStreamingPull,
		StreamID:      "stream-1",
		StreamSeq:     0,
		RequestParams: []string{"subscription=projects%2Fp%2Fsubscriptions%2Fs"},
		StreamingPullReq: &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 60,
			MaxOutstandingMessages:   100,
			MaxOutstandingBytes:      1024,
			ClientId:                 id1,
			ProtocolVersion:          1,
		},
	}
	snap := fake.Snapshot{
		Calls: []fake.CapturedCall{
			initCall,
			{
				Seq: 2, FullMethod: fake.MethodStreamingPull, StreamID: "stream-1", StreamSeq: 1,
				StreamingPullReq: &pubsubpb.StreamingPullRequest{},
			},
		},
		Streams: []fake.StreamSummary{
			{StreamID: "stream-1", HasInitial: true, ClientID: id1},
			{StreamID: "stream-2", HasInitial: true, ClientID: id1},
		},
	}

	exp := fake.ExpectStreamingPullInitial("init", fake.StreamingPullInitialExpectation{
		Subscription:           "projects/p/subscriptions/s",
		StreamAckDeadlineMin:   10,
		StreamAckDeadlineMax:   600,
		MaxOutstandingMessages: int64Ptr(100),
		MaxOutstandingBytes:    int64Ptr(1024),
		RequireUUIDClientID:    true,
		ProtocolVersion:        int64Ptr(1),
		CheckRequestParams:     true,
	})
	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), exp); err != nil {
		t.Fatalf("ExpectStreamingPullInitial positive failed: %v", err)
	}
	if err := fake.AssertConsistentClientIDs(snap, true); err != nil {
		t.Fatalf("AssertConsistentClientIDs positive failed: %v", err)
	}
	if err := fake.AssertKeepalivePings(snap, 1); err != nil {
		t.Fatalf("AssertKeepalivePings(1) failed: %v", err)
	}
	if err := fake.AssertKeepalivePings(snap, 2); err == nil {
		t.Fatalf("expected AssertKeepalivePings(2) to fail")
	}

	// Negative: inconsistent client_id across reconnects.
	badStreams := snap
	badStreams.Streams = []fake.StreamSummary{
		{StreamID: "stream-1", HasInitial: true, ClientID: id1},
		{StreamID: "stream-2", HasInitial: true, ClientID: id2},
	}
	if err := fake.AssertConsistentClientIDs(badStreams, true); err == nil {
		t.Fatalf("expected AssertConsistentClientIDs to fail on mismatched client IDs")
	}

	// Negative: non-UUID client_id when requireUUID=true.
	nonUUID := snap
	nonUUID.Streams = []fake.StreamSummary{{StreamID: "stream-1", HasInitial: true, ClientID: "not-a-uuid"}}
	if err := fake.AssertConsistentClientIDs(nonUUID, true); err == nil {
		t.Fatalf("expected AssertConsistentClientIDs to fail on non-UUID")
	}
	// Non-UUID passes when requireUUID=false.
	if err := fake.AssertConsistentClientIDs(nonUUID, false); err != nil {
		t.Fatalf("AssertConsistentClientIDs(requireUUID=false) failed: %v", err)
	}

	// Negative: VerificationError Unwrap & multi-error formatting.
	v := fake.NewVerifier()
	v.Require(false, "first failure")
	v.Check(errors.New("second failure"))
	err := v.Err()
	var ve *fake.VerificationError
	if !errors.As(err, &ve) || len(ve.Unwrap()) != 2 || !bytes.Contains([]byte(err.Error()), []byte("2 verification failure(s)")) {
		t.Fatalf("unexpected VerificationError: %v", err)
	}
}

func TestVerifierPullStreamingAckModAckAndDiffProto(t *testing.T) {
	// Streaming Ack + Streaming Nack aggregation alongside unary checks.
	snap := fake.Snapshot{
		Calls: []fake.CapturedCall{
			{
				Seq: 1, FullMethod: fake.MethodPull, StatusCode: codes.OK,
				RequestParams: []string{"subscription=projects%2Fp%2Fsubscriptions%2Fs"},
				PullReq:       &pubsubpb.PullRequest{Subscription: "projects/p/subscriptions/s", MaxMessages: 3},
			},
			{
				Seq: 2, FullMethod: fake.MethodStreamingPull, StreamID: "stream-1", StreamSeq: 1, StatusCode: codes.OK,
				StreamingPullReq: &pubsubpb.StreamingPullRequest{
					AckIds:                []string{"s-ack1"},
					ModifyDeadlineAckIds:  []string{"s-nack1", "s-ext1"},
					ModifyDeadlineSeconds: []int32{0, 60},
				},
			},
			{
				Seq: 3, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK,
				RequestParams: []string{"subscription=projects%2Fp%2Fsubscriptions%2Fs"},
				AckReq:        &pubsubpb.AcknowledgeRequest{Subscription: "projects/p/subscriptions/s", AckIds: []string{"u-ack1", "u-ack2"}},
			},
			{
				Seq: 4, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.OK,
				RequestParams: []string{"subscription=projects%2Fp%2Fsubscriptions%2Fs"},
				ModAckReq:     &pubsubpb.ModifyAckDeadlineRequest{Subscription: "projects/p/subscriptions/s", AckIds: []string{"u-nack1"}, AckDeadlineSeconds: 0},
			},
		},
	}

	if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(),
		fake.ExpectPull("pull",
			fake.PullSubscription("projects/p/subscriptions/s"),
			fake.PullMaxMessages(3),
			fake.PullRequestParams("projects/p/subscriptions/s", false),
			fake.PullStatusCode(codes.OK),
		),
		fake.CallExpectation{Name: "stream-ack-modack", FullMethod: fake.MethodStreamingPull},
		fake.ExpectAck("ack",
			fake.AckSubscription("projects/p/subscriptions/s"),
			fake.AckIDsUnordered("u-ack2", "u-ack1"),
			fake.AckWithinBounds(),
			fake.AckRequestParams("projects/p/subscriptions/s", false),
			fake.AckStatusCode(codes.OK),
		),
		fake.ExpectModAck("modack",
			fake.ModAckSubscription("projects/p/subscriptions/s"),
			fake.ModAckIDsExact("u-nack1"),
			fake.ModAckIDsUnordered("u-nack1"),
			fake.ModAckDeadline(0),
			fake.ModAckWithinBounds(),
			fake.ModAckRequestParams("projects/p/subscriptions/s", false),
			fake.ModAckStatusCode(codes.OK),
		),
	); err != nil {
		t.Fatalf("VerifySequence positive failed: %v", err)
	}

	if err := fake.AssertAllAckedIDs(snap, "s-ack1", "u-ack1", "u-ack2"); err != nil {
		t.Fatalf("AssertAllAckedIDs failed: %v", err)
	}
	if err := fake.AssertAllNackedIDs(snap, "s-nack1", "u-nack1"); err != nil {
		t.Fatalf("AssertAllNackedIDs failed: %v", err)
	}
	if err := fake.AssertAckAndModAckWithinBounds(snap); err != nil {
		t.Fatalf("AssertAckAndModAckWithinBounds failed: %v", err)
	}

	// Negative Pull & StreamingPullInitial mismatches.
	if err := fake.ExpectPull("bad-pull", fake.PullMaxMessages(99)).Check(snap.Calls[0]); err == nil {
		t.Fatalf("expected PullMaxMessages mismatch to fail")
	}
	if err := fake.ExpectPull("bad-pull-status", fake.PullStatusCode(codes.DeadlineExceeded)).Check(snap.Calls[0]); err == nil {
		t.Fatalf("expected PullStatusCode mismatch to fail")
	}
	badInit := fake.CapturedCall{
		Seq: 1, FullMethod: fake.MethodStreamingPull, StreamSeq: 0,
		StreamingPullReq: &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 5,
			MaxOutstandingMessages:   1,
			ProtocolVersion:          0,
		},
	}
	initExp := fake.ExpectStreamingPullInitial("strict-init", fake.StreamingPullInitialExpectation{
		Subscription:           "projects/p/subscriptions/s",
		StreamAckDeadlineMin:   10,
		StreamAckDeadlineMax:   600,
		MaxOutstandingMessages: int64Ptr(10),
		ProtocolVersion:        int64Ptr(1),
		RequireUUIDClientID:    true,
		CheckRequestParams:     true,
	})
	if err := initExp.Check(badInit); err == nil {
		t.Fatalf("expected ExpectStreamingPullInitial to fail")
	} else {
		for _, wantSub := range []string{"stream_ack_deadline_seconds", "max_outstanding_messages", "protocol_version", "client_id is empty", "x-goog-request-params"} {
			if !strings.Contains(err.Error(), wantSub) {
				t.Errorf("ExpectStreamingPullInitial error missing %q: %v", wantSub, err)
			}
		}
	}

	// DiffProto positive & negative.
	p1 := &pubsubpb.PubsubMessage{Data: []byte("hello"), OrderingKey: "k1"}
	p2 := &pubsubpb.PubsubMessage{Data: []byte("world"), OrderingKey: "k1"}
	if diff := fake.DiffProto(p1, p1); diff != "" {
		t.Fatalf("expected empty DiffProto for equal messages, got:\n%s", diff)
	}
	if diff := fake.DiffProto(p1, p2); diff == "" {
		t.Fatalf("expected non-empty DiffProto for differing messages")
	}
}

func TestVerifierEODSplitRetryStaggeredAndStrictness(t *testing.T) {
	ackCall := func(seq int64, code codes.Code, domain, reason string, eodFailures map[string]string, ids ...string) fake.CapturedCall {
		return fake.CapturedCall{
			Seq:                     seq,
			Completed:               true,
			FullMethod:              fake.MethodAcknowledge,
			StatusCode:              code,
			ResponseErrorInfoDomain: domain,
			ResponseErrorInfoReason: reason,
			ResponseEODFailures:     eodFailures,
			AckReq:                  &pubsubpb.AcknowledgeRequest{Subscription: "projects/p/subscriptions/s", AckIds: ids},
		}
	}
	modAckCall := func(seq int64, code codes.Code, dl int32, domain, reason string, eodFailures map[string]string, ids ...string) fake.CapturedCall {
		return fake.CapturedCall{
			Seq:                     seq,
			Completed:               true,
			FullMethod:              fake.MethodModifyAckDeadline,
			StatusCode:              code,
			ResponseErrorInfoDomain: domain,
			ResponseErrorInfoReason: reason,
			ResponseEODFailures:     eodFailures,
			ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             ids,
				AckDeadlineSeconds: dl,
			},
		}
	}

	// Positives:
	t.Run("positive_staggered_ack", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientInvalidAckID},
				"perm", "t1", "t2", "ok"),
			ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"t1": fake.EODTransientOther}, // t2 succeeds by omission!
				"t1", "t2"),
			ackCall(3, codes.OK, "", "", nil, "t1"),
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on staggered ack: %v", err)
		}
	})

	t.Run("positive_staggered_modack_ext_and_nack_scoped", func(t *testing.T) {
		snapExt := fake.Snapshot{Calls: []fake.CapturedCall{
			modAckCall(1, codes.InvalidArgument, 60, fake.EODDomainPubSub, fake.EODReasonModAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			modAckCall(2, codes.InvalidArgument, 60, fake.EODDomainPubSub, fake.EODReasonModAckFailed,
				map[string]string{"t1": fake.EODTransientOther}, "t1", "t2"),
			modAckCall(3, codes.OK, 60, "", "", nil, "t1"),
			modAckCall(4, codes.OK, 0, "", "", nil, "ok"), // trailing nack ignored
		}}
		if err := fake.AssertEODSplitRetry(snapExt, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on staggered modack extension: %v", err)
		}

		snapNack := fake.Snapshot{Calls: []fake.CapturedCall{
			modAckCall(1, codes.InvalidArgument, 0, fake.EODDomainPubSub, fake.EODReasonModAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			modAckCall(2, codes.InvalidArgument, 0, fake.EODDomainPubSub, fake.EODReasonModAckFailed,
				map[string]string{"t1": fake.EODTransientOther}, "t1", "t2"),
			modAckCall(3, codes.OK, 0, "", "", nil, "t1"),
			modAckCall(4, codes.OK, 60, "", "", nil, "ok"), // trailing extend ignored
		}}
		if err := fake.AssertEODSplitRetry(snapNack, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on staggered modack nack: %v", err)
		}
	})

	t.Run("positive_staggered_plus_intermediate_unavailable", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"t1": fake.EODTransientOther}, "t1", "t2"), // t2 recovers by omission
			ackCall(3, codes.Unavailable, "", "", nil, "t1"), // t1 hits transient Unavailable
			ackCall(4, codes.OK, "", "", nil, "t1"),          // t1 finally OK
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on staggered+Unavailable: %v", err)
		}
	})

	t.Run("positive_split_per_id_retries", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			ackCall(2, codes.OK, "", "", nil, "t1"),
			ackCall(3, codes.OK, "", "", nil, "t2"),
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on per-ID split retries: %v", err)
		}
	})

	t.Run("positive_pre_split_plain_unavailable_retry", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.Unavailable, "", "", nil, "perm", "t1", "ok"), // full batch before EOD ErrorInfo
			ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther},
				"perm", "t1", "ok"),
			ackCall(3, codes.OK, "", "", nil, "t1"),
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure when pre-split plain Unavailable preceded EOD ErrorInfo: %v", err)
		}
	})

	t.Run("positive_empty_map_eod_omission_recovery", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{}, // non-nil empty EOD map omits both t1 and t2!
				"t1", "t2"),
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on empty-map EOD omission recovery: %v", err)
		}
	})

	t.Run("positive_non_invalid_argument_eod_code", func(t *testing.T) {
		snap := fake.Snapshot{Calls: []fake.CapturedCall{
			ackCall(1, codes.FailedPrecondition, fake.EODDomainPubSub, fake.EODReasonAckFailed,
				map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
				"perm", "t1", "t2", "ok"),
			ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
		}}
		if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
			t.Fatalf("unexpected failure on non-InvalidArgument EOD code: %v", err)
		}
	})

	// Negatives:
	negatives := []struct {
		name  string
		calls []fake.CapturedCall
	}{
		{
			name: "initially_succeeded_absent_from_fail_call",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther},
					"perm", "t1"), // missing "ok"
				ackCall(2, codes.OK, "", "", nil, "t1"),
			},
		},
		{
			name: "t2_dropped_prematurely_after_still_failing_in_call2",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"t1", "t2"),
				ackCall(3, codes.OK, "", "", nil, "t1"),
			},
		},
		{
			name: "t2_dropped_prematurely_after_plain_unavailable",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.Unavailable, "", "", nil, "t1", "t2"),
				ackCall(3, codes.OK, "", "", nil, "t1"),
			},
		},
		{
			name: "t2_dropped_after_non_eod_error_info",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.ResourceExhausted, "googleapis.com", "QUOTA_EXCEEDED", nil, "t1", "t2"),
				ackCall(3, codes.OK, "", "", nil, "t1"),
			},
		},
		{
			name: "redundant_retry_of_t2_after_omission_recovery_in_call2",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"t1": fake.EODTransientOther}, "t1", "t2"), // t2 recovered here
				ackCall(3, codes.OK, "", "", nil, "t1", "t2"), // invalid retry of t2
			},
		},
		{
			name: "post_ok_redundant_retry_unavailable",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
				ackCall(3, codes.Unavailable, "", "", nil, "t1"),
			},
		},
		{
			name: "post_ok_redundant_retry_ok",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
				ackCall(3, codes.OK, "", "", nil, "t2"),
			},
		},
		{
			name: "perm_retried_in_later_round",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID}, "perm", "t1", "t2"),
			},
		},
		{
			name: "fail_call_error_info_failed_initially_succeeded",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther, "ok": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "fail_call_error_info_omitted_transient",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "duplicate_ack_id_in_fail_call",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "duplicate_unrelated_ack_id_in_fail_call",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok", "extra", "extra"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "duplicate_ack_id_in_retry_call_ok",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t1", "t2"),
			},
		},
		{
			name: "duplicate_ack_id_in_retry_call_eod_omission",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"t1": fake.EODTransientOther}, "t1", "t2", "t2"),
				ackCall(3, codes.OK, "", "", nil, "t1"),
			},
		},
		{
			name: "duplicate_ack_id_in_pre_fail_unavailable_call",
			calls: []fake.CapturedCall{
				ackCall(1, codes.Unavailable, "", "", nil, "perm", "t1", "t1", "t2", "ok"),
				ackCall(2, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				ackCall(3, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "pre_eod_unavailable_premature_drop_of_perm_and_ok",
			calls: []fake.CapturedCall{
				ackCall(1, codes.Unavailable, "", "", nil, "perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "non_eod_error_info_premature_drop_of_perm_and_ok",
			calls: []fake.CapturedCall{
				ackCall(1, codes.ResourceExhausted, "googleapis.com", "QUOTA_EXCEEDED", nil, "perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "pubsub_non_eod_reason_error_info_premature_drop",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, "SUBSCRIPTION_NOT_FOUND", nil, "perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "plain_invalid_argument_without_error_info_premature_drop",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, "", "", nil, "perm", "t1", "t2", "ok"),
				ackCall(2, codes.OK, "", "", nil, "t1", "t2"),
			},
		},
		{
			name: "in_flight_uncompleted_retry_call",
			calls: []fake.CapturedCall{
				ackCall(1, codes.InvalidArgument, fake.EODDomainPubSub, fake.EODReasonAckFailed,
					map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther, "t2": fake.EODTransientOther},
					"perm", "t1", "t2", "ok"),
				func() fake.CapturedCall {
					c := ackCall(2, codes.OK, "", "", nil, "t1", "t2")
					c.Completed = false
					return c
				}(),
			},
		},
	}

	for _, tc := range negatives {
		t.Run("negative_"+tc.name, func(t *testing.T) {
			snap := fake.Snapshot{Calls: tc.calls}
			if err := fake.AssertEODSplitRetry(snap, []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail for %s, got nil", tc.name)
			}
		})
	}
}

func TestVerifier_NegativeDiagnosticsAndBoundaries(t *testing.T) {
	// VerificationError nil/empty/unwrap + CallExpectation unnamed label.
	var nilVE *fake.VerificationError
	if nilVE.Error() != "" || nilVE.Unwrap() != nil {
		t.Fatalf("nil VerificationError methods mismatch")
	}
	multiVE := &fake.VerificationError{Errors: []error{errors.New("e1"), errors.New("e2")}}
	if !strings.Contains(multiVE.Error(), "2 verification failure(s)") || len(multiVE.Unwrap()) != 2 {
		t.Fatalf("multi VerificationError unexpected: %q", multiVE.Error())
	}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{{Seq: 1, FullMethod: fake.MethodPull}}}, fake.VerifyOptions{}, fake.CallExpectation{FullMethod: fake.MethodPublish}); err == nil || !strings.Contains(err.Error(), fake.MethodPublish) {
		t.Fatalf("expected FullMethod label fallback in error, got %v", err)
	}
	if err := fake.VerifySequence(fake.Snapshot{Calls: []fake.CapturedCall{{Seq: 1, FullMethod: fake.MethodPull}}}, fake.VerifyOptions{}, fake.CallExpectation{Check: func(fake.CapturedCall) error { return errors.New("bad") }}); err == nil || !strings.Contains(err.Error(), "unnamed expectation") {
		t.Fatalf("expected unnamed expectation fallback, got %v", err)
	}

	// VerifyUnordered empty and count mismatch.
	if err := fake.VerifyUnordered(fake.Snapshot{}, fake.VerifyOptions{}); err != nil {
		t.Fatalf("VerifyUnordered empty expected nil, got %v", err)
	}
	if err := fake.VerifyUnordered(fake.Snapshot{}, fake.VerifyOptions{}, fake.ExpectPublish("p")); err == nil || !strings.Contains(err.Error(), "unordered call count mismatch") {
		t.Fatalf("VerifyUnordered count mismatch expected error, got %v", err)
	}

	// PublishMessages failure branches.
	pubCall := fake.CapturedCall{
		Seq:        1,
		FullMethod: fake.MethodPublish,
		StatusCode: codes.OK,
		PublishReq: &pubsubpb.PublishRequest{
			Topic: "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{
				Data:        []byte("abc"),
				OrderingKey: "k1",
				Attributes:  map[string]string{"reqk": "wrong", fake.TraceparentAttributeKey: "bad-tp"},
			}},
		},
	}
	if err := fake.ExpectPublish("nil-req").Check(fake.CapturedCall{FullMethod: fake.MethodPublish}); err == nil {
		t.Fatalf("expected PublishReq nil error")
	}
	if err := fake.PublishMessages(fake.ExpectedMessage{}, fake.ExpectedMessage{})(pubCall); err == nil || !strings.Contains(err.Error(), "message count mismatch") {
		t.Fatalf("expected message count mismatch, got %v", err)
	}
	if err := fake.PublishMessages(fake.ExpectedMessage{
		IgnoreData:         true,
		CheckData:          func([]byte) error { return errors.New("bad payload") },
		OrderingKey:        "k1",
		Traceparent:        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		RequiredAttributes: map[string]string{"reqk": "want", "absent": "x"},
	})(pubCall); err == nil {
		t.Fatalf("expected PublishMessages checks to fail")
	} else {
		for _, want := range []string{"data check failed", "mismatch", "missing required key"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("PublishMessages error missing %q: %v", want, err)
			}
		}
	}
	noTP := fake.CapturedCall{FullMethod: fake.MethodPublish, PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x")}}}}
	if err := fake.PublishMessages(fake.ExpectedMessage{IgnoreData: true, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})(noTP); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected missing Traceparent error, got %v", err)
	}

	// PublishTelemetry & AssertTelemetryHedgeSequence failure paths.
	if err := fake.PublishTelemetry(0, false)(fake.CapturedCall{TelemetryDecodeErr: "corrupt"}); err == nil || !strings.Contains(err.Error(), "decode error") {
		t.Fatalf("expected decode error, got %v", err)
	}
	if err := fake.PublishTelemetry(0, false)(fake.CapturedCall{}); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected missing telemetry error, got %v", err)
	}
	nilStartTel := &pubsubpb.PubsubClientTelemetry{Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: 0}}}
	if err := fake.PublishTelemetry(0, true)(fake.CapturedCall{Telemetry: nilStartTel}); err == nil || !strings.Contains(err.Error(), "publish_start_time is nil") {
		t.Fatalf("expected nil start time error, got %v", err)
	}
	hedgeSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{Seq: 1, FullMethod: fake.MethodPublish, TelemetryDecodeErr: "bad"},
		{Seq: 2, FullMethod: fake.MethodPublish},
		{Seq: 3, FullMethod: fake.MethodPublish, Telemetry: nilStartTel},
	}}
	if err := fake.AssertTelemetryHedgeSequence(hedgeSnap, []int32{0}, true); err == nil || !strings.Contains(err.Error(), "Publish count mismatch") {
		t.Fatalf("expected hedge count mismatch, got %v", err)
	}
	if err := fake.AssertTelemetryHedgeSequence(hedgeSnap, []int32{0, 1, 2}, true); err == nil {
		t.Fatalf("expected AssertTelemetryHedgeSequence failure")
	} else {
		for _, want := range []string{"telemetry decode error", "missing PublishOperation", "hedged_attempt_count mismatch", "publish_start_time is nil"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("AssertTelemetryHedgeSequence error missing %q: %v", want, err)
			}
		}
	}

	// AssertConcurrentPublishesObserved failure paths (<2 keys, missing key, non-overlapping keys).
	t0 := time.Unix(1700000000, 0)
	concSnap := fake.Snapshot{
		MaxConcurrentPublishTotal: 2,
		Calls: []fake.CapturedCall{
			{Seq: 1, FullMethod: fake.MethodPublish, Completed: true, Timestamp: t0, EndTime: t0.Add(10 * time.Millisecond), PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{OrderingKey: "k1"}}}},
			{Seq: 2, FullMethod: fake.MethodPublish, Completed: true, Timestamp: t0.Add(20 * time.Millisecond), EndTime: t0.Add(30 * time.Millisecond), PublishReq: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{OrderingKey: "k2"}}}},
		},
	}
	if err := fake.AssertConcurrentPublishesObserved(concSnap, "k1"); err != nil {
		t.Fatalf("<2 keys expected nil when MaxConcurrentPublishTotal>=2, got %v", err)
	}
	if err := fake.AssertConcurrentPublishesObserved(concSnap, "k1", "missing"); err == nil || !strings.Contains(err.Error(), "no Publish calls observed") {
		t.Fatalf("expected missing key error, got %v", err)
	}
	if err := fake.AssertConcurrentPublishesObserved(concSnap, "k1", "k2"); err == nil || !strings.Contains(err.Error(), "never published concurrently") {
		t.Fatalf("expected non-overlapping keys error, got %v", err)
	}

	// Ack / ModAck / Pull negative diagnostics.
	badAck := fake.CapturedCall{Seq: 1, FullMethod: fake.MethodAcknowledge, StatusCode: codes.Internal, AckReq: &pubsubpb.AcknowledgeRequest{Subscription: "wrong", AckIds: nil}}
	if err := fake.ExpectAck("a", fake.AckSubscription("want"), fake.AckIDsUnordered("a1"), fake.AckWithinBounds(), fake.AckStatusCode(codes.OK), fake.AckRequestParams("want", false)).Check(badAck); err == nil {
		t.Fatalf("expected ExpectAck checks to fail")
	}
	if err := fake.ExpectAck("nil", fake.AckWithinBounds()).Check(fake.CapturedCall{FullMethod: fake.MethodAcknowledge}); err == nil {
		t.Fatalf("expected nil AckReq error")
	}
	bigAckID := strings.Repeat("a", 1024)
	bigAckIDs := make([]string, 600)
	for i := range bigAckIDs {
		bigAckIDs[i] = bigAckID
	}
	if err := fake.AckWithinBounds()(fake.CapturedCall{AckReq: &pubsubpb.AcknowledgeRequest{AckIds: bigAckIDs}}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected AckWithinBounds oversize error, got %v", err)
	}
	badMod := fake.CapturedCall{Seq: 2, FullMethod: fake.MethodModifyAckDeadline, StatusCode: codes.Internal, ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{Subscription: "wrong", AckIds: make([]string, 1001), AckDeadlineSeconds: 5}}
	if err := fake.ExpectModAck("m", fake.ModAckSubscription("want"), fake.ModAckIDsExact("m1"), fake.ModAckIDsUnordered("m1"), fake.ModAckDeadline(30), fake.ModAckWithinBounds(), fake.ModAckStatusCode(codes.OK)).Check(badMod); err == nil {
		t.Fatalf("expected ExpectModAck checks to fail")
	}
	if err := fake.ExpectModAck("nil").Check(fake.CapturedCall{FullMethod: fake.MethodModifyAckDeadline}); err == nil {
		t.Fatalf("expected nil ModAckReq error")
	}
	if err := fake.ModAckWithinBounds()(fake.CapturedCall{ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{AckIds: bigAckIDs}}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected ModAckWithinBounds oversize error, got %v", err)
	}
	if err := fake.ExpectPull("nil").Check(fake.CapturedCall{FullMethod: fake.MethodPull}); err == nil {
		t.Fatalf("expected nil PullReq error")
	}
	if err := fake.PullSubscription("want")(fake.CapturedCall{PullReq: &pubsubpb.PullRequest{Subscription: "got"}}); err == nil {
		t.Fatalf("expected PullSubscription error")
	}

	// AssertAckAndModAckWithinBounds StreamingPull violations.
	manyIDs := make([]string, 1001)
	halfIDs1 := make([]string, 600)
	halfIDs2 := make([]string, 600)
	halfSecs := make([]int32, 600)
	for i := range halfSecs {
		halfSecs[i] = 10
	}
	bigSP := &pubsubpb.StreamingPullRequest{AckIds: bigAckIDs}
	_ = proto.Size(bigSP)
	spBoundsSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{Seq: 1, FullMethod: fake.MethodStreamingPull, StreamingPullReq: &pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: manyIDs, ModifyDeadlineSeconds: make([]int32, 1001)}},
		{Seq: 2, FullMethod: fake.MethodStreamingPull, StreamingPullReq: &pubsubpb.StreamingPullRequest{AckIds: halfIDs1, ModifyDeadlineAckIds: halfIDs2, ModifyDeadlineSeconds: halfSecs}},
		{Seq: 3, FullMethod: fake.MethodStreamingPull, StreamingPullReq: bigSP},
		{Seq: 4, FullMethod: fake.MethodStreamingPull, StreamingPullReq: &pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 5, ModifyDeadlineAckIds: []string{"m1"}, ModifyDeadlineSeconds: []int32{601}}},
	}}
	if err := fake.AssertAckAndModAckWithinBounds(spBoundsSnap); err == nil {
		t.Fatalf("expected AssertAckAndModAckWithinBounds to fail on StreamingPull bounds")
	} else {
		for _, want := range []string{"modify_deadline_ack_ids count", "combined ack/modack", "request size", "stream_ack_deadline_seconds=5", "modify_deadline_seconds[0]=601"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("AssertAckAndModAckWithinBounds error missing %q: %v", want, err)
			}
		}
	}

	// AssertEODSplitRetry missing retry / no calls / never retried branches.
	if err := fake.AssertEODSplitRetry(fake.Snapshot{}, nil, nil, nil); err == nil {
		t.Fatalf("expected no Ack/ModAck error")
	}
	noRetrySnap := fake.Snapshot{Calls: []fake.CapturedCall{{
		Seq: 1, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.InvalidArgument,
		ResponseErrorInfoDomain: fake.EODDomainPubSub, ResponseErrorInfoReason: fake.EODReasonAckFailed,
		ResponseEODFailures: map[string]string{"perm": fake.EODPermanentInvalidAckID, "t1": fake.EODTransientOther},
		AckReq:              &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm", "t1"}},
	}}}
	if err := fake.AssertEODSplitRetry(noRetrySnap, []string{"perm"}, []string{"t1"}, nil); err == nil || !strings.Contains(err.Error(), "expected retry calls") {
		t.Fatalf("expected missing retry error, got %v", err)
	}

	// ExpectStreamingPullInitial, ExpectStreamingPullKeepalive, AssertConsistentClientIDs negatives.
	if err := fake.ExpectStreamingPullInitial("s0", fake.StreamingPullInitialExpectation{}).Check(fake.CapturedCall{FullMethod: fake.MethodStreamingPull, StreamSeq: 1}); err == nil {
		t.Fatalf("expected StreamSeq!=0 error")
	}
	if err := fake.ExpectStreamingPullInitial("nil", fake.StreamingPullInitialExpectation{}).Check(fake.CapturedCall{FullMethod: fake.MethodStreamingPull, StreamSeq: 0}); err == nil {
		t.Fatalf("expected nil StreamingPullReq error")
	}
	badInit := fake.CapturedCall{
		FullMethod: fake.MethodStreamingPull, StreamSeq: 0,
		StreamingPullReq: &pubsubpb.StreamingPullRequest{Subscription: "other", MaxOutstandingBytes: 9, ClientId: "not-a-uuid"},
	}
	if err := fake.ExpectStreamingPullInitial("init", fake.StreamingPullInitialExpectation{
		Subscription: "want", MaxOutstandingBytes: int64Ptr(100), ClientID: uuid.NewString(), RequireUUIDClientID: true,
	}).Check(badInit); err == nil {
		t.Fatalf("expected ExpectStreamingPullInitial field mismatch error")
	}
	if err := fake.ExpectStreamingPullKeepalive("ka").Check(badInit); err == nil {
		t.Fatalf("expected ExpectStreamingPullKeepalive failure")
	}
	if err := fake.AssertConsistentClientIDs(fake.Snapshot{}, false); err == nil || !strings.Contains(err.Error(), "no StreamingPull streams") {
		t.Fatalf("expected no streams error, got %v", err)
	}
	if err := fake.AssertConsistentClientIDs(fake.Snapshot{Streams: []fake.StreamSummary{{StreamID: "s1", HasInitial: true, ClientID: ""}}}, false); err == nil || !strings.Contains(err.Error(), "empty client_id") {
		t.Fatalf("expected empty client_id error, got %v", err)
	}
	if err := fake.AssertConsistentClientIDs(fake.Snapshot{Streams: []fake.StreamSummary{{StreamID: "s1", HasInitial: true, ClientID: "bad"}}}, true); err == nil || !strings.Contains(err.Error(), "not a valid UUID") {
		t.Fatalf("expected invalid UUID error, got %v", err)
	}
}

func TestVerifier_Tier5GapDCoverageBranches(t *testing.T) {
	// 1. AssertOrderedPublishFIFO mixed ordering keys in a single PublishRequest (verifier.go:620).
	mixedKeySnap := fake.Snapshot{Calls: []fake.CapturedCall{{
		Seq: 1, Completed: true, FullMethod: fake.MethodPublish, StatusCode: codes.OK,
		PublishReq: &pubsubpb.PublishRequest{
			Topic: "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{
				{Data: []byte("a"), OrderingKey: "k1"},
				{Data: []byte("b"), OrderingKey: "k2"},
			},
		},
	}}}
	if err := fake.AssertOrderedPublishFIFO(mixedKeySnap, "k1", [][]byte{[]byte("a")}); err == nil || !strings.Contains(err.Error(), `mixed ordering keys alongside "k1"`) {
		t.Fatalf("expected mixed ordering keys error, got %v", err)
	}

	// 2. AssertAckAndModAckWithinBounds unary ModifyAckDeadlineRequest > 1000 ack IDs (verifier.go:1073).
	unaryModAckBoundsSnap := fake.Snapshot{Calls: []fake.CapturedCall{{
		Seq: 1, FullMethod: fake.MethodModifyAckDeadline,
		ModAckReq: &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       "projects/p/subscriptions/s",
			AckIds:             make([]string, 1001),
			AckDeadlineSeconds: 30,
		},
	}}}
	if err := fake.AssertAckAndModAckWithinBounds(unaryModAckBoundsSnap); err == nil || !strings.Contains(err.Error(), "outside [1, 1000]") {
		t.Fatalf("expected unary ModifyAckDeadline >1000 ack_ids error, got %v", err)
	}

	// 3. AssertEODSplitRetry missing permanent ack_id in ResponseEODFailures (verifier.go:1185).
	missingPermFailSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq: 1, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub, ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures: map[string]string{"trans1": fake.EODTransientOther},
			AckReq:              &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm1", "trans1", "ok1"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans1"}}},
	}}
	if err := fake.AssertEODSplitRetry(missingPermFailSnap, []string{"perm1"}, []string{"trans1"}, []string{"ok1"}); err == nil || !strings.Contains(err.Error(), `missing permanent ack_id "perm1"`) {
		t.Fatalf("expected missing permanent ack_id error, got %v", err)
	}

	// 4. AssertEODSplitRetry !seenLater[id] when len(laterCalls) > 0 (verifier.go:1243).
	wrongRetryIDSnap := fake.Snapshot{Calls: []fake.CapturedCall{
		{
			Seq: 1, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.InvalidArgument,
			ResponseErrorInfoDomain: fake.EODDomainPubSub, ResponseErrorInfoReason: fake.EODReasonAckFailed,
			ResponseEODFailures: map[string]string{"perm1": fake.EODPermanentInvalidAckID, "trans1": fake.EODTransientOther},
			AckReq:              &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm1", "trans1", "ok1"}},
		},
		{Seq: 2, Completed: true, FullMethod: fake.MethodAcknowledge, StatusCode: codes.OK, AckReq: &pubsubpb.AcknowledgeRequest{AckIds: []string{"other"}}},
	}}
	if err := fake.AssertEODSplitRetry(wrongRetryIDSnap, []string{"perm1"}, []string{"trans1"}, []string{"ok1"}); err == nil || !strings.Contains(err.Error(), `transient ack_id "trans1" was not retried`) {
		t.Fatalf("expected unretried transient ack_id error, got %v", err)
	}
}

func TestVerifier_Tier6OpenIntervalOverlapAndIdentityCompression(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	mkPub := func(seq int64, key string, ts, end time.Time, completed bool) fake.CapturedCall {
		return fake.CapturedCall{
			Seq:        seq,
			Timestamp:  ts,
			EndTime:    end,
			Completed:  completed,
			FullMethod: fake.MethodPublish,
			StatusCode: codes.OK,
			PublishReq: &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("d"), OrderingKey: key}},
			},
		}
	}

	t.Run("intervalsOverlap_open_open", func(t *testing.T) {
		snapZeroEnd := fake.Snapshot{
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0, time.Time{}, false),
				mkPub(2, "k2", t0.Add(5*time.Millisecond), time.Time{}, false),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapZeroEnd, "k1", "k2"); err != nil {
			t.Fatalf("expected (open, open) zero EndTime overlap to pass, got %v", err)
		}

		// Even with non-zero tentative EndTime, Completed=false stays open.
		snapIncompleteNonZero := fake.Snapshot{
			MaxConcurrentPublishTotal: 2,
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0, t0.Add(1*time.Millisecond), false),
				mkPub(2, "k2", t0.Add(5*time.Millisecond), t0.Add(6*time.Millisecond), false),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapIncompleteNonZero, "k1", "k2"); err != nil {
			t.Fatalf("expected (open, open) Completed=false overlap to pass, got %v", err)
		}
	})

	t.Run("intervalsOverlap_open_closed_overlap_and_non_overlap", func(t *testing.T) {
		snapOverlap := fake.Snapshot{
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0.Add(5*time.Millisecond), time.Time{}, false),
				mkPub(2, "k2", t0, t0.Add(10*time.Millisecond), true),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapOverlap, "k1", "k2"); err != nil {
			t.Fatalf("expected (open, closed) overlapping to pass, got %v", err)
		}

		// MaxConcurrentPublishTotal=2 ensures intervalsOverlap arm 2 is actually evaluated.
		snapBoundary := fake.Snapshot{
			MaxConcurrentPublishTotal: 2,
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0.Add(10*time.Millisecond), time.Time{}, false),
				mkPub(2, "k2", t0, t0.Add(10*time.Millisecond), true),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapBoundary, "k1", "k2"); err == nil || !strings.Contains(err.Error(), "never published concurrently") {
			t.Fatalf("expected exact-boundary (open, closed) non-overlap to fail, got %v", err)
		}

		snapDisjoint := fake.Snapshot{
			MaxConcurrentPublishTotal: 2,
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0.Add(15*time.Millisecond), time.Time{}, false),
				mkPub(2, "k2", t0, t0.Add(10*time.Millisecond), true),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapDisjoint, "k1", "k2"); err == nil || !strings.Contains(err.Error(), "never published concurrently") {
			t.Fatalf("expected strictly disjoint (open, closed) non-overlap to fail, got %v", err)
		}
	})

	t.Run("intervalsOverlap_closed_open_overlap_and_non_overlap", func(t *testing.T) {
		snapOverlap := fake.Snapshot{
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0, t0.Add(10*time.Millisecond), true),
				mkPub(2, "k2", t0.Add(5*time.Millisecond), time.Time{}, false),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapOverlap, "k1", "k2"); err != nil {
			t.Fatalf("expected (closed, open) overlapping to pass, got %v", err)
		}

		// MaxConcurrentPublishTotal=2 ensures intervalsOverlap arm 3 is actually evaluated.
		snapBoundary := fake.Snapshot{
			MaxConcurrentPublishTotal: 2,
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0, t0.Add(10*time.Millisecond), true),
				mkPub(2, "k2", t0.Add(10*time.Millisecond), time.Time{}, false),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapBoundary, "k1", "k2"); err == nil || !strings.Contains(err.Error(), "never published concurrently") {
			t.Fatalf("expected exact-boundary (closed, open) non-overlap to fail, got %v", err)
		}

		snapDisjoint := fake.Snapshot{
			MaxConcurrentPublishTotal: 2,
			Calls: []fake.CapturedCall{
				mkPub(1, "k1", t0, t0.Add(10*time.Millisecond), true),
				mkPub(2, "k2", t0.Add(15*time.Millisecond), time.Time{}, false),
			},
		}
		if err := fake.AssertConcurrentPublishesObserved(snapDisjoint, "k1", "k2"); err == nil || !strings.Contains(err.Error(), "never published concurrently") {
			t.Fatalf("expected strictly disjoint (closed, open) non-overlap to fail, got %v", err)
		}
	})

	t.Run("PublishCompression_identity_canonicalization", func(t *testing.T) {
		for _, want := range []string{"identity", "IDENTITY", "Identity"} {
			if err := fake.PublishCompression(want)(fake.CapturedCall{FullMethod: fake.MethodPublish, Compression: ""}); err != nil {
				t.Fatalf("PublishCompression(%q) against empty Compression failed: %v", want, err)
			}
			if err := fake.PublishCompression(want)(fake.CapturedCall{FullMethod: fake.MethodPublish, Compression: "identity"}); err != nil {
				t.Fatalf("PublishCompression(%q) against identity Compression failed: %v", want, err)
			}
			if err := fake.PublishCompression(want)(fake.CapturedCall{FullMethod: fake.MethodPublish, Compression: "gzip"}); err == nil {
				t.Fatalf("PublishCompression(%q) against gzip Compression expected failure, got nil", want)
			}
		}
	})
}

func TestTier7_FakeVerifierAndCaptureAdversarial(t *testing.T) {
	t0 := time.Unix(1700000100, 0)

	t.Run("GAP-A7-F1_VerifyUnordered_DFSCycleAndCollisionProtection", func(t *testing.T) {
		mkAck := func(seq int, ackID string) fake.CapturedCall {
			return fake.CapturedCall{
				Seq:        int64(seq),
				Timestamp:  t0.Add(time.Duration(seq) * time.Millisecond),
				EndTime:    t0.Add(time.Duration(seq+1) * time.Millisecond),
				Completed:  true,
				FullMethod: fake.MethodAcknowledge,
				StatusCode: codes.OK,
				AckReq:     &pubsubpb.AcknowledgeRequest{Subscription: "projects/p/subscriptions/s", AckIds: []string{ackID}},
			}
		}

		// Two identical expectations both matching got[0] only (forces dfs(1)->0->dfs(0)->0 seen[0] check).
		snap2 := fake.Snapshot{Calls: []fake.CapturedCall{mkAck(1, "x"), mkAck(2, "y")}}
		err2 := fake.VerifyUnordered(snap2, fake.VerifyOptions{},
			fake.ExpectAck("ack-1", fake.AckSubscription("projects/p/subscriptions/s"), fake.AckIDsExact("x")),
			fake.ExpectAck("ack-2", fake.AckSubscription("projects/p/subscriptions/s"), fake.AckIDsExact("x")),
		)
		if err2 == nil {
			t.Fatal("expected VerifyUnordered mismatch error for duplicate colliding expectations, got nil")
		}

		// Three-node Hall violation cycle ({a1,a2}, {a1,a2}, {a1} vs [a1, a2, a3]).
		matchOneOf := func(name string, allowed ...string) fake.CallExpectation {
			return fake.CallExpectation{
				Name:       name,
				FullMethod: fake.MethodAcknowledge,
				Check: func(c fake.CapturedCall) error {
					ids := c.AckIDs()
					if len(ids) == 1 && slices.Contains(allowed, ids[0]) {
						return nil
					}
					return errors.New("mismatch")
				},
			}
		}
		snap3 := fake.Snapshot{Calls: []fake.CapturedCall{mkAck(1, "a1"), mkAck(2, "a2"), mkAck(3, "a3")}}
		err3 := fake.VerifyUnordered(snap3, fake.VerifyOptions{},
			matchOneOf("w0", "a1", "a2"),
			matchOneOf("w1", "a1", "a2"),
			matchOneOf("w2", "a1"),
		)
		if err3 == nil {
			t.Fatal("expected VerifyUnordered mismatch error on 3-node augmenting cycle, got nil")
		}

		// Positive augmenting path: w0 matches {a1,a2} (takes a1 first), w1 matches {a1} (re-assigns w0->a2).
		snapOK := fake.Snapshot{Calls: []fake.CapturedCall{mkAck(1, "a1"), mkAck(2, "a2")}}
		if errOK := fake.VerifyUnordered(snapOK, fake.VerifyOptions{},
			matchOneOf("w0", "a1", "a2"),
			matchOneOf("w1", "a1"),
		); errOK != nil {
			t.Fatalf("expected valid bipartite augmentation to pass, got %v", errOK)
		}
	})

	t.Run("GAP-A7-F2_AssertConsistentClientIDs_SkipsUninitializedStreams", func(t *testing.T) {
		cid := uuid.NewString()
		snap := fake.Snapshot{
			Streams: []fake.StreamSummary{
				{StreamID: "stream-no-init", HasInitial: false, ClientID: ""},
				{StreamID: "stream-1", HasInitial: true, ClientID: cid},
				{StreamID: "stream-2", HasInitial: true, ClientID: cid},
			},
		}
		if err := fake.AssertConsistentClientIDs(snap, true); err != nil {
			t.Fatalf("AssertConsistentClientIDs(requireUUID=true) failed: %v", err)
		}
		if err := fake.AssertConsistentClientIDs(snap, false); err != nil {
			t.Fatalf("AssertConsistentClientIDs(requireUUID=false) failed: %v", err)
		}

		onlyNoInit := fake.Snapshot{
			Streams: []fake.StreamSummary{
				{StreamID: "stream-no-init", HasInitial: false, ClientID: ""},
			},
		}
		if err := fake.AssertConsistentClientIDs(onlyNoInit, false); err == nil || !strings.Contains(err.Error(), "no StreamingPull streams with initial request observed") {
			t.Fatalf("expected missing-initial error when all streams have HasInitial=false, got %v", err)
		}
	})

	t.Run("GAP-A7-F3_CapturedCall_AckIDs_StreamingPullAndCloneIsolation", func(t *testing.T) {
		spCall := fake.CapturedCall{
			FullMethod: fake.MethodStreamingPull,
			StreamingPullReq: &pubsubpb.StreamingPullRequest{
				AckIds: []string{"sp-a1", "sp-a2"},
			},
		}
		got := spCall.AckIDs()
		if !slices.Equal(got, []string{"sp-a1", "sp-a2"}) {
			t.Fatalf("StreamingPull AckIDs() = %v, want [sp-a1 sp-a2]", got)
		}
		got[0] = "mutated"
		if spCall.StreamingPullReq.AckIds[0] != "sp-a1" {
			t.Fatalf("AckIDs() did not clone StreamingPullReq.AckIds; underlying mutated to %q", spCall.StreamingPullReq.AckIds[0])
		}

		emptySP := fake.CapturedCall{
			FullMethod:       fake.MethodStreamingPull,
			StreamingPullReq: &pubsubpb.StreamingPullRequest{},
		}
		if ids := emptySP.AckIDs(); ids != nil {
			t.Fatalf("empty StreamingPull AckIDs() = %v, want nil", ids)
		}
	})

	t.Run("GAP-A7-F4_F5_DecodeTelemetryHeader_EmptyAndMalformedProtobuf", func(t *testing.T) {
		rawEmpty, decEmpty, errEmpty := fake.DecodeTelemetryHeader(metadata.Pairs(fake.TelemetryHeaderName, ""))
		if errEmpty == nil || rawEmpty != "" || decEmpty != nil {
			t.Fatalf("DecodeTelemetryHeader(empty) = (%q, %v, %v), want non-nil error", rawEmpty, decEmpty, errEmpty)
		}

		// "gA==" decodes to 0x80 (truncated protobuf varint) — valid base64, invalid proto wire bytes.
		rawBad, decBad, errBad := fake.DecodeTelemetryHeader(metadata.Pairs(fake.TelemetryHeaderName, "gA=="))
		if errBad == nil || !strings.Contains(errBad.Error(), "unmarshal") || rawBad != "gA==" || decBad != nil {
			t.Fatalf("DecodeTelemetryHeader(malformed proto) = (%q, %v, %v), want unmarshal error", rawBad, decBad, errBad)
		}
	})
}

func TestAssertEODSplitRetry_RejectsNonPubSubDomainAndNonEODReason(t *testing.T) {
	retryOK := fake.CapturedCall{
		Seq:        2,
		Completed:  true,
		FullMethod: fake.MethodAcknowledge,
		StatusCode: codes.OK,
		AckReq:     &pubsubpb.AcknowledgeRequest{AckIds: []string{"trans"}},
	}
	baseFail := fake.CapturedCall{
		Seq:                     1,
		Completed:               true,
		FullMethod:              fake.MethodAcknowledge,
		StatusCode:              codes.InvalidArgument,
		ResponseErrorInfoDomain: fake.EODDomainPubSub,
		ResponseErrorInfoReason: fake.EODReasonAckFailed,
		ResponseEODFailures:     map[string]string{"perm": fake.EODPermanentInvalidAckID, "trans": fake.EODTransientOther},
		AckReq:                  &pubsubpb.AcknowledgeRequest{AckIds: []string{"perm", "trans", "ok"}},
	}

	if err := fake.AssertEODSplitRetry(fake.Snapshot{Calls: []fake.CapturedCall{baseFail, retryOK}}, []string{"perm"}, []string{"trans"}, []string{"ok"}); err != nil {
		t.Fatalf("positive control AssertEODSplitRetry failed: %v", err)
	}

	nonPubSub := baseFail.Clone()
	nonPubSub.ResponseErrorInfoDomain = "other.example.com"
	if err := fake.AssertEODSplitRetry(fake.Snapshot{Calls: []fake.CapturedCall{nonPubSub, retryOK}}, []string{"perm"}, []string{"trans"}, []string{"ok"}); err == nil {
		t.Fatal("expected AssertEODSplitRetry to reject non-PubSub ErrorInfo domain")
	}

	nonEODReason := baseFail.Clone()
	nonEODReason.ResponseErrorInfoReason = "UNRELATED_REASON"
	if err := fake.AssertEODSplitRetry(fake.Snapshot{Calls: []fake.CapturedCall{nonEODReason, retryOK}}, []string{"perm"}, []string{"trans"}, []string{"ok"}); err == nil {
		t.Fatal("expected AssertEODSplitRetry to reject non-EOD ErrorInfo reason")
	}
}
