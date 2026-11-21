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
	"compress/flate"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func dialFake(t *testing.T, srv *fake.Server, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)
	conn, err := grpc.NewClient(srv.Addr(), dialOpts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%q): %v", srv.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func startFakeServer(t *testing.T, opts ...fake.Option) *fake.Server {
	t.Helper()
	srv, err := fake.NewServer(opts...)
	if err != nil {
		t.Fatalf("fake.NewServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func TestDataPlaneRPCsAndNoRequestMutation(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish
	inMsg := &pubsubpb.PubsubMessage{
		Data:       []byte("hello-world"),
		Attributes: map[string]string{"k": "v"},
	}
	pubReq := &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{inMsg},
	}
	pubResp, err := pub.Publish(ctx, pubReq)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if len(pubResp.GetMessageIds()) != 1 || pubResp.GetMessageIds()[0] == "" {
		t.Fatalf("unexpected PublishResponse: %v", pubResp)
	}
	// Verify incoming proto pointers are never mutated in-place (unlike pstest).
	if inMsg.MessageId != "" || inMsg.PublishTime != nil {
		t.Fatalf("incoming PubsubMessage mutated in place: id=%q time=%v", inMsg.MessageId, inMsg.PublishTime)
	}

	// 2. Pull (scripted response then default empty)
	srv.AddPullRule(fake.PullRule{
		Response: &pubsubpb.PullResponse{
			ReceivedMessages: []*pubsubpb.ReceivedMessage{
				{
					AckId:           "pull-ack-1",
					DeliveryAttempt: 2,
					Message:         &pubsubpb.PubsubMessage{Data: []byte("pulled")},
				},
			},
		},
	})
	pullResp, err := sub.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: "projects/p/subscriptions/s",
		MaxMessages:  5,
	})
	if err != nil {
		t.Fatalf("Pull failed: %v", err)
	}
	if len(pullResp.GetReceivedMessages()) != 1 || pullResp.GetReceivedMessages()[0].GetAckId() != "pull-ack-1" {
		t.Fatalf("unexpected PullResponse: %v", pullResp)
	}
	emptyPull, err := sub.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: "projects/p/subscriptions/s",
		MaxMessages:  5,
	})
	if err != nil {
		t.Fatalf("second Pull failed: %v", err)
	}
	if len(emptyPull.GetReceivedMessages()) != 0 {
		t.Fatalf("expected empty PullResponse, got %v", emptyPull)
	}

	// 3. Acknowledge
	if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: "projects/p/subscriptions/s",
		AckIds:       []string{"pull-ack-1"},
	}); err != nil {
		t.Fatalf("Acknowledge failed: %v", err)
	}

	// 4. ModifyAckDeadline
	if _, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       "projects/p/subscriptions/s",
		AckIds:             []string{"pull-ack-2"},
		AckDeadlineSeconds: 60,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline failed: %v", err)
	}

	// 5. StreamingPull
	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open failed: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
		ClientId:                 "client-1",
		ProtocolVersion:          1,
	}); err != nil {
		t.Fatalf("StreamingPull Send failed: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
		{Message: &pubsubpb.PubsubMessage{Data: []byte("streamed")}},
	}); err != nil {
		t.Fatalf("PushMessages: %v", err)
	}
	spResp, err := sp.Recv()
	if err != nil {
		t.Fatalf("StreamingPull Recv: %v", err)
	}
	if len(spResp.GetReceivedMessages()) != 1 || string(spResp.GetReceivedMessages()[0].GetMessage().GetData()) != "streamed" {
		t.Fatalf("unexpected StreamingPullResponse: %v", spResp)
	}
	if err := sp.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	_, recvEOF := sp.Recv()
	if !errors.Is(recvEOF, io.EOF) {
		t.Fatalf("expected io.EOF after CloseSend, got %v", recvEOF)
	}

	snap := srv.Snapshot()
	if err := fake.AssertNoUnexpectedRPCs(snap); err != nil {
		t.Fatalf("AssertNoUnexpectedRPCs failed: %v", err)
	}
}

func TestUnregisteredAdminAndSchemaRPCsReturnUnimplementedAndFailVerifier(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)
	schema := pubsubpb.NewSchemaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	calls := []struct {
		name string
		fn   func() error
	}{
		{"CreateTopic", func() error {
			_, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p/topics/t"})
			return err
		}},
		{"GetTopic", func() error {
			_, err := pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: "projects/p/topics/t"})
			return err
		}},
		{"DetachSubscription", func() error {
			_, err := pub.DetachSubscription(ctx, &pubsubpb.DetachSubscriptionRequest{Subscription: "projects/p/subscriptions/s"})
			return err
		}},
		{"GetSubscription", func() error {
			_, err := sub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: "projects/p/subscriptions/s"})
			return err
		}},
		{"Seek", func() error {
			_, err := sub.Seek(ctx, &pubsubpb.SeekRequest{Subscription: "projects/p/subscriptions/s"})
			return err
		}},
		{"ModifyPushConfig", func() error {
			_, err := sub.ModifyPushConfig(ctx, &pubsubpb.ModifyPushConfigRequest{Subscription: "projects/p/subscriptions/s"})
			return err
		}},
		{"SchemaService.GetSchema", func() error {
			_, err := schema.GetSchema(ctx, &pubsubpb.GetSchemaRequest{Name: "projects/p/schemas/sc"})
			return err
		}},
		{"UnknownService", func() error {
			return conn.Invoke(ctx, "/google.iam.v1.IAMPolicy/GetIamPolicy", &pubsubpb.GetTopicRequest{}, &pubsubpb.Topic{})
		}},
	}

	for _, tc := range calls {
		err := tc.fn()
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("%s: want codes.Unimplemented, got %v", tc.name, err)
		}
	}

	snap := srv.Snapshot()
	if len(snap.UnexpectedCalls()) != len(calls) {
		t.Fatalf("expected %d unexpected calls, got %d", len(calls), len(snap.UnexpectedCalls()))
	}
	err := fake.AssertNoUnexpectedRPCs(snap)
	if err == nil {
		t.Fatalf("expected AssertNoUnexpectedRPCs to fail")
	}
	for _, wantSub := range []string{"CreateTopic", "GetTopic", "DetachSubscription", "Seek", "ModifyPushConfig", "GetSchema", "GetIamPolicy"} {
		if !strings.Contains(err.Error(), wantSub) {
			t.Errorf("AssertNoUnexpectedRPCs error missing %q: %v", wantSub, err)
		}
	}
}

func TestLargeMessagesAndMaxSizeRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Default 32 MiB server accepts > 10 MiB (31.9 MiB) without truncation.
	srvDefault := startFakeServer(t)
	connDefault := dialFake(t, srvDefault, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	pubDefault := pubsubpb.NewPublisherClient(connDefault)

	near32MiB := bytes.Repeat([]byte{0xab}, (32<<20)-4096)
	if _, err := pubDefault.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: near32MiB}},
	}); err != nil {
		t.Fatalf("default 32 MiB server rejected ~32 MiB payload: %v", err)
	}

	// 2. Explicit 40 MiB server roundtrips >= 32 MiB (exact 32 MiB data) cleanly.
	srv40 := startFakeServer(t, fake.WithMaxRecvMsgSize(40<<20), fake.WithMaxSendMsgSize(40<<20))
	conn40 := dialFake(t, srv40, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(40<<20), grpc.MaxCallSendMsgSize(40<<20)))
	pub40 := pubsubpb.NewPublisherClient(conn40)
	exact32MiB := bytes.Repeat([]byte{0xcd}, 32<<20)
	if _, err := pub40.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: exact32MiB}},
	}); err != nil {
		t.Fatalf("40 MiB server rejected 32 MiB payload: %v", err)
	}
	if len(srv40.Calls()) != 1 || len(srv40.Calls()[0].PublishReq.GetMessages()[0].GetData()) != 32<<20 {
		t.Fatalf("captured payload size mismatch")
	}

	// 3. WithMaxRecvMsgSize(10 MiB) rejects 10.5 MiB with ResourceExhausted & captures via stats.Handler.
	srvRecvCap := startFakeServer(t, fake.WithMaxRecvMsgSize(10<<20))
	connRecvCap := dialFake(t, srvRecvCap)
	pubRecvCap := pubsubpb.NewPublisherClient(connRecvCap)
	oversize := bytes.Repeat([]byte{0xef}, (10<<20)+512*1024)
	_, err := pubRecvCap.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: oversize}},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted on WithMaxRecvMsgSize exceed, got %v", err)
	}
	if _, err := srvRecvCap.WaitForPublish(ctx, 1); err != nil {
		t.Fatalf("WaitForPublish transport failure: %v", err)
	}
	capCall := srvRecvCap.Calls()[0]
	if capCall.StatusCode != codes.ResourceExhausted || capCall.CodecError == "" {
		t.Fatalf("expected captured transport error, got %+v", capCall)
	}

	// 4. WithMaxPublishRequestBytes(10_000_000) rejects >10 MB cleanly with InvalidArgument + full proto capture.
	srvPubCap := startFakeServer(t, fake.WithMaxPublishRequestBytes(10_000_000))
	connPubCap := dialFake(t, srvPubCap)
	pubCap := pubsubpb.NewPublisherClient(connPubCap)
	_, err = pubCap.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: bytes.Repeat([]byte{'x'}, 10_000_100)}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on WithMaxPublishRequestBytes exceed, got %v", err)
	}
	if got := srvPubCap.Calls(); len(got) != 1 || got[0].PublishReq == nil || got[0].StatusCode != codes.InvalidArgument {
		t.Fatalf("expected captured InvalidArgument call with PublishReq, got %+v", got)
	}
}

func TestCompressionGzipAndRFC1950Deflate(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	compressible := bytes.Repeat([]byte("pubsub-conformance-compression-"), 64)

	// 1. gzip via grpc.UseCompressor("gzip")
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: compressible}},
	}, grpc.UseCompressor("gzip")); err != nil {
		t.Fatalf("gzip Publish failed: %v", err)
	}

	// 2. deflate via grpc.UseCompressor("deflate")
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: compressible}},
	}, grpc.UseCompressor("deflate")); err != nil {
		t.Fatalf("deflate Publish failed: %v", err)
	}

	// 3. Raw RFC 1950 zlib payload over real HTTP/2 h2c with grpc-encoding: deflate
	rawWireReq := &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: compressible}},
	}
	wireBytes, err := proto.Marshal(rawWireReq)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var zlibBuf bytes.Buffer
	zw := zlib.NewWriter(&zlibBuf)
	if _, err := zw.Write(wireBytes); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zlib close: %v", err)
	}
	if code, body, err := sendRawGRPCH2C(ctx, srv.Addr(), fake.MethodPublish, "deflate", zlibBuf.Bytes(), true); err != nil || code != codes.OK {
		t.Fatalf("raw zlib HTTP/2 request failed: code=%v err=%v body=%q", code, err, body)
	}

	// 4. Negative: raw RFC 1951 flate (without zlib header) with grpc-encoding: deflate MUST fail decompression.
	var flateBuf bytes.Buffer
	fw, err := flate.NewWriter(&flateBuf, flate.DefaultCompression)
	if err != nil {
		t.Fatalf("flate.NewWriter: %v", err)
	}
	if _, err := fw.Write(wireBytes); err != nil {
		t.Fatalf("flate write: %v", err)
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("flate close: %v", err)
	}
	// Verify compressor directly rejects RFC 1951 raw flate:
	dc, err := encoding.GetCompressor("deflate").Decompress(bytes.NewReader(flateBuf.Bytes()))
	if err == nil {
		_, err = io.ReadAll(dc)
		if closer, ok := dc.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	if err == nil {
		t.Fatalf("expected raw RFC 1951 flate decompression to fail under RFC 1950 deflate compressor")
	}
	// Also verify over real HTTP/2 wire:
	if code, _, err := sendRawGRPCH2C(ctx, srv.Addr(), fake.MethodPublish, "deflate", flateBuf.Bytes(), true); err != nil || code != codes.Internal {
		t.Fatalf("expected codes.Internal over wire for raw RFC 1951 flate, got code=%v err=%v", code, err)
	}

	calls, err := srv.WaitForPublish(ctx, 4)
	if err != nil {
		t.Fatalf("WaitForPublish(4): %v", err)
	}
	if len(calls) != 4 {
		t.Fatalf("expected 4 calls, got %d", len(calls))
	}
	if calls[0].Compression != "gzip" || calls[0].CompressedBytes >= calls[0].UncompressedBytes {
		t.Fatalf("unexpected gzip capture: %+v", calls[0])
	}
	if calls[1].Compression != "deflate" || calls[1].CompressedBytes >= calls[1].UncompressedBytes {
		t.Fatalf("unexpected deflate capture: %+v", calls[1])
	}
	if calls[2].Compression != "deflate" || calls[2].StatusCode != codes.OK {
		t.Fatalf("unexpected raw zlib capture: %+v", calls[2])
	}
	if calls[3].Compression != "deflate" || calls[3].StatusCode != codes.Internal || calls[3].CodecError == "" {
		t.Fatalf("expected captured CodecError on raw flate rejection, got %+v", calls[3])
	}
}

type rawGRPCFrame struct {
	payload    []byte
	compressed bool
}

func sendRawGRPCH2C(ctx context.Context, addr, fullMethod, grpcEncoding string, payload []byte, compressed bool) (codes.Code, []byte, error) {
	return sendRawGRPCH2CFrames(ctx, addr, fullMethod, grpcEncoding, rawGRPCFrame{
		payload:    payload,
		compressed: compressed,
	})
}

func sendRawGRPCH2CFrames(ctx context.Context, addr, fullMethod, grpcEncoding string, frames ...rawGRPCFrame) (codes.Code, []byte, error) {
	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: p}
	defer tr.CloseIdleConnections()
	client := &http.Client{
		Transport: tr,
	}
	var bodyBuf bytes.Buffer
	for _, f := range frames {
		var hdr [5]byte
		if f.compressed {
			hdr[0] = 1
		}
		binary.BigEndian.PutUint32(hdr[1:5], uint32(len(f.payload)))
		bodyBuf.Write(hdr[:])
		bodyBuf.Write(f.payload)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+fullMethod, bytes.NewReader(bodyBuf.Bytes()))
	if err != nil {
		return codes.Unknown, nil, err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	if grpcEncoding != "" {
		req.Header.Set("Grpc-Encoding", grpcEncoding)
	}
	resp, err := client.Do(req)
	if err != nil {
		return codes.Unknown, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return codes.Unknown, nil, err
	}
	gs := resp.Trailer.Get("Grpc-Status")
	if gs == "" {
		gs = resp.Header.Get("Grpc-Status")
	}
	if gs == "" || gs == "0" {
		return codes.OK, body, nil
	}
	var c codes.Code
	if err := c.UnmarshalJSON([]byte(gs)); err != nil {
		return codes.Unknown, body, err
	}
	return c, body, nil
}

func decodeRawGRPCStreamingPullResponses(body []byte) ([]*pubsubpb.StreamingPullResponse, error) {
	var resps []*pubsubpb.StreamingPullResponse
	off := 0
	for off < len(body) {
		if off+5 > len(body) {
			return nil, fmt.Errorf("truncated gRPC frame header at offset %d (len=%d)", off, len(body))
		}
		compressed := body[off]
		if compressed != 0 {
			return nil, fmt.Errorf("unexpected compressed response frame flag=%d", compressed)
		}
		frameLen := int(binary.BigEndian.Uint32(body[off+1 : off+5]))
		off += 5
		if off+frameLen > len(body) {
			return nil, fmt.Errorf("truncated gRPC frame payload at offset %d (need=%d len=%d)", off, frameLen, len(body))
		}
		resp := &pubsubpb.StreamingPullResponse{}
		if err := proto.Unmarshal(body[off:off+frameLen], resp); err != nil {
			return nil, fmt.Errorf("proto.Unmarshal StreamingPullResponse: %w", err)
		}
		resps = append(resps, resp)
		off += frameLen
	}
	return resps, nil
}

func TestBinaryPayloadAndInvalidUTF8AttributesCapture(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Binary non-UTF-8 Data preserved losslessly.
	bin := []byte{0x00, 0x80, 0xff, 0xc5, 0xfe, 0x01}
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: bin, Attributes: map[string]string{"ok": "yes"}}},
	}); err != nil {
		t.Fatalf("binary Publish failed: %v", err)
	}
	gotData := srv.Calls()[0].PublishReq.GetMessages()[0].GetData()
	if !bytes.Equal(gotData, bin) {
		t.Fatalf("binary Data corrupted: want %x, got %x", bin, gotData)
	}

	// 2. Raw wire protobuf carrying invalid UTF-8 inside PubsubMessage.Attributes
	//    is rejected by proto3 unmarshaling (codes.Internal) and captured via stats.Handler.
	// Hand-encode PublishRequest: field 1 (topic="t"), field 2 (PubsubMessage: field 2 map entry key="k", val=0xc5).
	invalidAttrEntry := []byte{
		0x0a, 0x01, 'k', // key = "k"
		0x12, 0x03, 'a', 0xc5, 'z', // val = invalid UTF-8
	}
	msgBytes := append([]byte{0x12, byte(len(invalidAttrEntry))}, invalidAttrEntry...)
	rawPubReq := append([]byte{0x0a, 0x01, 't', 0x12, byte(len(msgBytes))}, msgBytes...)

	code, _, err := sendRawGRPCH2C(ctx, srv.Addr(), fake.MethodPublish, "", rawPubReq, false)
	if err != nil {
		t.Fatalf("sendRawGRPCH2C: %v", err)
	}
	if code != codes.Internal {
		t.Fatalf("expected codes.Internal on invalid UTF-8 attributes, got %v", code)
	}

	calls, err := srv.WaitForPublish(ctx, 2)
	if err != nil {
		t.Fatalf("WaitForPublish(2): %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}
	if calls[1].StatusCode != codes.Internal || !strings.Contains(calls[1].CodecError, "UTF-8") {
		t.Fatalf("expected UTF-8 CodecError capture, got %+v", calls[1])
	}
}

func TestTelemetryAndRequestParamsCapture(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	startTS := timestamppb.New(time.Unix(1700000000, 123000))
	tel := &pubsubpb.PubsubClientTelemetry{
		Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{
			PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{
				HedgedAttemptCount: 2,
				PublishStartTime:   startTS,
			},
		},
	}
	telBytes, err := proto.Marshal(tel)
	if err != nil {
		t.Fatalf("proto.Marshal telemetry: %v", err)
	}
	md := metadata.Pairs(
		fake.RequestParamsHeaderName, "topic=projects%2Fp%2Ftopics%2Ft",
		fake.TelemetryHeaderName, base64.StdEncoding.EncodeToString(telBytes),
	)
	outCtx := metadata.NewOutgoingContext(ctx, md)
	if _, err := pub.Publish(outCtx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hedged")}},
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Bad base64 header should populate TelemetryDecodeErr without crashing RPC.
	badCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(fake.TelemetryHeaderName, "!!!not-base64!!!"))
	if _, err := pub.Publish(badCtx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("bad-tel")}},
	}); err != nil {
		t.Fatalf("Publish with bad telemetry failed: %v", err)
	}

	calls := srv.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}
	if diff := cmp.Diff(tel, calls[0].Telemetry, protocmp.Transform()); diff != "" {
		t.Fatalf("decoded Telemetry diff (-want +got):\n%s", diff)
	}
	if len(calls[0].RequestParams) != 1 || calls[0].RequestParams[0] != "topic=projects%2Fp%2Ftopics%2Ft" {
		t.Fatalf("RequestParams mismatch: %v", calls[0].RequestParams)
	}
	if calls[1].TelemetryDecodeErr == "" {
		t.Fatalf("expected TelemetryDecodeErr for malformed header")
	}
}

func TestEODErrorInfoAndStatefulSplitRetry(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Direct EODErrorStatus builder on ModifyAckDeadline.
	srv.AddModAckRule(fake.ModAckRule{
		Err: fake.EODModAckErrorStatus(codes.InvalidArgument, "modack failed", map[string]string{
			"m1": fake.EODPermanentInvalidAckID,
		}),
	})
	_, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       "projects/p/subscriptions/s",
		AckIds:             []string{"m1"},
		AckDeadlineSeconds: 60,
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument status, got %v", err)
	}
	var gotInfo *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			gotInfo = ei
		}
	}
	if gotInfo == nil || gotInfo.GetDomain() != fake.EODDomainPubSub || gotInfo.GetReason() != fake.EODReasonModAckFailed || gotInfo.GetMetadata()["m1"] != fake.EODPermanentInvalidAckID {
		t.Fatalf("unexpected ErrorInfo: %+v", gotInfo)
	}

	// 2. Stateful EODScript on Acknowledge (mixed perm + transient-once + omitted success).
	script := fake.NewEODScript(codes.OK, "partial failure", fake.EODReasonAckFailed, map[string]fake.EODAckBehavior{
		"ack-perm":  {PermanentReason: fake.EODPermanentInvalidAckID},
		"ack-trans": {TransientReason: fake.EODTransientUnorderedAckID, TransientTimes: 1},
	})
	srv.AddAckRule(fake.AckRule{
		Times: fake.RepeatAlways,
		EOD:   script,
	})

	_, err = sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: "projects/p/subscriptions/s",
		AckIds:       []string{"ack-perm", "ack-trans", "ack-ok"},
	})
	st1, _ := status.FromError(err)
	if st1.Code() != codes.InvalidArgument {
		t.Fatalf("expected default InvalidArgument outer code when codes.OK passed, got %v", st1.Code())
	}
	ei1 := st1.Details()[0].(*errdetails.ErrorInfo)
	wantMD := map[string]string{
		"ack-perm":  fake.EODPermanentInvalidAckID,
		"ack-trans": fake.EODTransientUnorderedAckID,
	}
	if diff := cmp.Diff(wantMD, ei1.GetMetadata()); diff != "" {
		t.Fatalf("EOD ErrorInfo metadata mismatch (-want +got):\n%s", diff)
	}

	// Retry only transient ack_id -> succeeds!
	if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: "projects/p/subscriptions/s",
		AckIds:       []string{"ack-trans"},
	}); err != nil {
		t.Fatalf("expected retry of transient ack-trans to succeed, got %v", err)
	}

	if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"ack-perm"}, []string{"ack-trans"}, []string{"ack-ok"}); err != nil {
		t.Fatalf("AssertEODSplitRetry failed: %v", err)
	}
}

func TestGateNonBlockingOtherRPCsCancelAndPORD02Concurrency(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Hold ordering key "key-A" behind a Gate while concurrently calling key "key-B" AND Acknowledge/Pull!
	gateA := fake.NewGate()
	srv.AddPublishRule(fake.PublishRule{
		Match: fake.MatchOrderingKey("key-A"),
		Gate:  gateA,
		Times: 1,
	})

	var wg sync.WaitGroup
	ctxA, cancelA := context.WithCancel(ctx)
	var errA error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errA = pub.Publish(ctxA, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a1"), OrderingKey: "key-A"}},
		})
	}()

	if err := gateA.WaitEntered(ctx); err != nil {
		t.Fatalf("WaitEntered: %v", err)
	}

	// Concurrent key-B Publish and Acknowledge and Pull MUST NOT deadlock (unlike pstest).
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("b1"), OrderingKey: "key-B"}},
	}); err != nil {
		t.Fatalf("concurrent Publish key-B failed: %v", err)
	}
	if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
		Subscription: "projects/p/subscriptions/s",
		AckIds:       []string{"id-1"},
	}); err != nil {
		t.Fatalf("concurrent Acknowledge failed: %v", err)
	}
	if _, err := sub.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: "projects/p/subscriptions/s",
		MaxMessages:  1,
	}); err != nil {
		t.Fatalf("concurrent Pull failed: %v", err)
	}

	// Cancel key-A client context (simulating HDG-02 winner cancels loser).
	cancelA()
	wg.Wait()
	if status.Code(errA) != codes.Canceled {
		t.Fatalf("expected key-A canceled, got %v", errA)
	}

	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) >= 4 && calls[0].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls completion: %v", err)
	}

	snap := srv.Snapshot()
	if err := fake.AssertMaxConcurrentPublishForKey(snap, "key-A", 1); err != nil {
		t.Fatalf("AssertMaxConcurrentPublishForKey(key-A): %v", err)
	}
	if err := fake.AssertConcurrentPublishesObserved(snap, "key-A", "key-B"); err != nil {
		t.Fatalf("AssertConcurrentPublishesObserved: %v", err)
	}
	pubCalls := snap.PublishCalls()
	if !pubCalls[0].CanceledByClient || pubCalls[0].StatusCode != codes.Canceled {
		t.Fatalf("expected CanceledByClient on gated canceled call, got %+v", pubCalls[0])
	}
}

func TestStreamingPullDynamicsKeepaliveCloseAbortAndScripts(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. FailBeforeRecvErr script once.
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		Times:             1,
		FailBeforeRecvErr: status.Error(codes.Unavailable, "try again"),
	})
	spFail, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if _, err := spFail.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable from FailBeforeRecvErr, got %v", err)
	}

	// 2. InitialError script once (after frame 0).
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		Times:        1,
		InitialError: status.Error(codes.PermissionDenied, "denied"),
	})
	spInitErr, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open 2: %v", err)
	}
	_ = spInitErr.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
	if _, err := spInitErr.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied from InitialError, got %v", err)
	}

	// 3. Normal stream: early SendHeader (no spurious empty response!), dynamic PushMessages,
	//    mid-stream SubscriptionProperties toggle, keepalive ping/pong, SuppressPong(true), Close(nil).
	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open 3: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 60,
		ClientId:                 "c-1",
		ProtocolVersion:          1,
	}); err != nil {
		t.Fatalf("send initial: %v", err)
	}
	// Verify HTTP/2 headers arrived immediately (tonic compatibility) without waiting for PushMessages.
	hdr, err := sp.Header()
	if err != nil {
		t.Fatalf("Header() blocked or failed: %v", err)
	}
	_ = hdr

	// Wait for healthy stream #1 (WaitForStreamingPull skips FailBeforeRecvErr
	// and InitialError streams, while Snapshot still records all 3 attempts).
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	if len(srv.Snapshot().StreamingPullCalls()) != 3 {
		t.Fatalf("expected 3 captured StreamingPull calls (FailBeforeRecvErr + InitialError + healthy), got %d", len(srv.Snapshot().StreamingPullCalls()))
	}

	// Push message with options + SubscriptionProperties.
	pubTime := time.Unix(1700000100, 0).UTC()
	if err := h.PushMessages(
		[]*pubsubpb.ReceivedMessage{{Message: &pubsubpb.PubsubMessage{Data: []byte("eod-msg")}}},
		fake.WithSubscriptionProperties(true, true),
		fake.WithDeliveryAttempt(4),
		fake.WithOrderingKey("ord-1"),
		fake.WithPublishTime(pubTime),
	); err != nil {
		t.Fatalf("PushMessages: %v", err)
	}
	r1, err := sp.Recv()
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if !r1.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() || !r1.GetSubscriptionProperties().GetMessageOrderingEnabled() {
		t.Fatalf("expected EOD & Ordering properties true, got %+v", r1.GetSubscriptionProperties())
	}
	rm1 := r1.GetReceivedMessages()[0]
	if rm1.GetAckId() == "" || rm1.GetDeliveryAttempt() != 4 || rm1.GetMessage().GetOrderingKey() != "ord-1" || !rm1.GetMessage().GetPublishTime().AsTime().Equal(pubTime) {
		t.Fatalf("unexpected ReceivedMessage: %+v", rm1)
	}

	// Mid-stream toggle EOD off.
	if err := h.SetSubscriptionProperties(false, false); err != nil {
		t.Fatalf("SetSubscriptionProperties: %v", err)
	}
	r2, err := sp.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if r2.GetSubscriptionProperties() == nil || r2.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() {
		t.Fatalf("expected EOD toggled false, got %+v", r2.GetSubscriptionProperties())
	}

	// Keepalive ping -> empty pong.
	if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
		t.Fatalf("send ping 1: %v", err)
	}
	pong, err := sp.Recv()
	if err != nil {
		t.Fatalf("Recv pong: %v", err)
	}
	if len(pong.GetReceivedMessages()) != 0 || pong.GetSubscriptionProperties() != nil {
		t.Fatalf("expected empty pong, got %+v", pong)
	}

	// Suppress pong -> ping does NOT produce pong; subsequent PushMessages arrives instead.
	h.SetSuppressPong(true)
	if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
		t.Fatalf("send ping 2: %v", err)
	}
	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return h.Summary().KeepalivePingCount >= 2
	}); err != nil {
		t.Fatalf("WaitFor keepalive ping 2: %v", err)
	}
	if err := h.PushMessages([]*pubsubpb.ReceivedMessage{{Message: &pubsubpb.PubsubMessage{Data: []byte("after-suppressed-ping")}}}); err != nil {
		t.Fatalf("PushMessages after suppressed ping: %v", err)
	}
	if err := h.Close(status.Error(codes.Aborted, "graceful close err")); err != nil {
		t.Fatalf("CloseStream: %v", err)
	}
	r3, err := sp.Recv()
	if err != nil {
		t.Fatalf("expected FIFO queued message before Close err, got %v", err)
	}
	if string(r3.GetReceivedMessages()[0].GetMessage().GetData()) != "after-suppressed-ping" {
		t.Fatalf("unexpected r3: %v", r3)
	}
	if _, err := sp.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("expected Aborted after FIFO drain, got %v", err)
	}
	if err := h.WaitClosed(ctx); err != nil {
		t.Fatalf("WaitClosed: %v", err)
	}

	// 4. LIFE-03 cancel before first response & AbortStream.
	spCancelCtx, spCancel := context.WithCancel(ctx)
	sp4, err := sub.StreamingPull(spCancelCtx)
	if err != nil {
		t.Fatalf("StreamingPull 4 open: %v", err)
	}
	_ = sp4.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
	h4, err := srv.WaitForStreamingPullN(ctx, 2)
	if err != nil {
		t.Fatalf("WaitForStreamingPullN(2): %v", err)
	}
	spCancel()
	if err := h4.WaitClosed(ctx); err != nil {
		t.Fatalf("WaitClosed on pre-response cancel: %v", err)
	}

	// 5. AbortStream drops queued messages immediately.
	sp5, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull 5 open: %v", err)
	}
	_ = sp5.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
	h5, err := srv.WaitForStreamingPullN(ctx, 3)
	if err != nil {
		t.Fatalf("WaitForStreamingPullN(3): %v", err)
	}
	h5.Abort(status.Error(codes.Internal, "aborted now"))
	if _, err := sp5.Recv(); status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal on AbortStream, got %v", err)
	}
}

func TestStressResetNoDeadlockOrGoroutineLeak(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	// Warm up gRPC client transport pools before measuring baseline goroutines.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("warm")}},
	}); err != nil {
		t.Fatalf("warmup Publish: %v", err)
	}
	srv.Reset()
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for iter := range 15 {
		gate := fake.NewGate()
		srv.AddPublishRule(fake.PublishRule{Gate: gate, Times: 1})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = pub.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("held")}},
			})
		}()
		go func() {
			defer wg.Done()
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				return
			}
			_ = sp.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
			for {
				if _, err := sp.Recv(); err != nil {
					return
				}
			}
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("iter %d WaitEntered: %v", iter, err)
		}
		if _, err := srv.WaitForStreamingPull(ctx); err != nil {
			t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
		}

		// Reset while both a gated Publish and an idle StreamingPull are active!
		srv.Reset()
		wg.Wait()

		// Confirm clean new epoch immediately serves calls starting at Seq == 1.
		if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("post-reset")}},
		}); err != nil {
			t.Fatalf("iter %d post-reset Publish: %v", iter, err)
		}
		calls := srv.Calls()
		if len(calls) != 1 || calls[0].Seq != 1 {
			t.Fatalf("iter %d expected fresh epoch Seq=1, got %+v", iter, calls)
		}
	}

	srv.Reset()
	// Allow brief scheduling window for client recv loops to unwind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline+4 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak after stress Reset: baseline=%d final=%d", baseline, runtime.NumGoroutine())
}

func TestPublishRulesMatchersMismatchedIDsAndConcurrentSameKeyDetection(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. PUB-10 mismatched message IDs (fewer & extra) + MatchPayloadContains + MatchTopic.
	srv.AddPublishRule(fake.PublishRule{
		Match:               fake.MatchPublishAll(fake.MatchTopic("projects/p/topics/t"), fake.MatchPayloadContains([]byte("fewer"))),
		MessageIDCountDelta: -1,
		Times:               1,
	})
	srv.AddPublishRule(fake.PublishRule{
		Match:               fake.MatchPayloadContains([]byte("extra")),
		MessageIDCountDelta: 2,
		Times:               1,
	})
	rFewer, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("fewer")}, {Data: []byte("b")}},
	})
	if err != nil || len(rFewer.GetMessageIds()) != 1 {
		t.Fatalf("expected 1 ID on delta -1, got ids=%v err=%v", rFewer.GetMessageIds(), err)
	}
	rExtra, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("extra")}},
	})
	if err != nil || len(rExtra.GetMessageIds()) != 3 {
		t.Fatalf("expected 3 IDs on delta +2, got ids=%v err=%v", rExtra.GetMessageIds(), err)
	}

	// 2. MatchCallAttempt + Delay canceled by caller ctx + ResponseFunc.
	srv.Reset()
	srv.AddPublishRule(fake.PublishRule{
		Match: fake.MatchCallAttempt(1),
		Delay: 2 * time.Second,
	})
	srv.AddPublishRule(fake.PublishRule{
		Match: fake.MatchCallAttempt(2),
		ResponseFunc: func(_ context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
			return &pubsubpb.PublishResponse{MessageIds: []string{"custom-" + req.GetTopic()}}, nil
		},
	})
	shortCtx, shortCancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer shortCancel()
	if _, err := pub.Publish(shortCtx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("slow")}},
	}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded interrupting rule Delay, got %v", err)
	}
	rCustom, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "t2",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("fast")}},
	})
	if err != nil || len(rCustom.GetMessageIds()) != 1 || rCustom.GetMessageIds()[0] != "custom-t2" {
		t.Fatalf("expected custom ResponseFunc result, got ids=%v err=%v", rCustom.GetMessageIds(), err)
	}

	// 3. Same-key concurrent overlap detected by KeyInFlightAtStart, KeyMaxConcurrent, and AssertMaxConcurrentPublishForKey.
	srv.Reset()
	sameKeyGate := fake.NewGate()
	srv.AddPublishRule(fake.PublishRule{
		Match: fake.MatchOrderingKey("dup-key"),
		Gate:  sameKeyGate,
		Times: 2,
	})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pub.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x"), OrderingKey: "dup-key"}},
			})
		}()
	}
	if err := sameKeyGate.WaitEnteredN(ctx, 2); err != nil {
		t.Fatalf("WaitEnteredN(2): %v", err)
	}
	sameKeyGate.Release()
	wg.Wait()
	if err := fake.AssertMaxConcurrentPublishForKey(srv.Snapshot(), "dup-key", 1); err == nil {
		t.Fatalf("expected AssertMaxConcurrentPublishForKey(dup-key, 1) to fail when 2 overlapping same-key calls entered")
	}
}

func TestAckModAckPullRulesAndStrictStreamingPullValidation(t *testing.T) {
	srv := startFakeServer(t,
		fake.WithDefaultSubscriptionProperties(true, false),
		fake.WithStrictStreamingPullValidation(true),
	)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pull transient error then scripted response.
	srv.AddPullRule(fake.PullRule{Err: status.Error(codes.DeadlineExceeded, "timeout"), Times: 1})
	srv.AddPullRule(fake.PullRule{Response: &pubsubpb.PullResponse{
		ReceivedMessages: []*pubsubpb.ReceivedMessage{{AckId: "p1"}},
	}})
	if _, err := sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: "s", MaxMessages: 1}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	pr, err := sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: "s", MaxMessages: 1})
	if err != nil || len(pr.GetReceivedMessages()) != 1 {
		t.Fatalf("expected PullResponse, got %v err=%v", pr, err)
	}

	// Ack & ModAck matchers.
	srv.AddAckRule(fake.AckRule{
		Match: fake.MatchAckID("bad-ack"),
		Err:   status.Error(codes.PermissionDenied, "ack denied"),
	})
	if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: "s", AckIds: []string{"bad-ack"}}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied ack, got %v", err)
	}
	srv.AddModAckRule(fake.ModAckRule{
		Match: fake.MatchModAckDeadline(0),
		Err:   status.Error(codes.Unavailable, "nack unavailable"),
	})
	if _, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: "s", AckIds: []string{"m1"}, AckDeadlineSeconds: 0}); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable nack, got %v", err)
	}

	// Strict StreamingPull validation rejects empty subscription on frame 0 and mismatched ModAck slices.
	spBad, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	_ = spBad.Send(&pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 30}) // missing subscription
	if _, err := spBad.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on missing initial subscription, got %v", err)
	}

	// DefaultSubscriptionProperties emitted automatically when valid initial request sent,
	// plus strict check rejects setting subscription on frame 1.
	var onReqSeen sync.WaitGroup
	onReqSeen.Add(1)
	var once sync.Once
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		OnRequest: func(_ *fake.StreamHandle, req *pubsubpb.StreamingPullRequest) {
			if len(req.GetAckIds()) == 1 {
				once.Do(onReqSeen.Done)
			}
		},
	})
	spGood, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open 2: %v", err)
	}
	_ = spGood.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 60})
	firstResp, err := spGood.Recv()
	if err != nil || !firstResp.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() {
		t.Fatalf("expected default SubscriptionProperties EOD=true, got %+v err=%v", firstResp, err)
	}
	_ = spGood.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{"a1"}})
	onReqSeen.Wait()

	// ActiveStreams lookup & StreamByID.
	if active := srv.ActiveStreams(); len(active) != 1 {
		t.Fatalf("expected 1 active stream, got %d", len(active))
	} else if _, ok := srv.StreamByID(active[0].ID()); !ok {
		t.Fatalf("StreamByID failed for active stream %q", active[0].ID())
	}

	// Frame 1 with non-empty subscription triggers strict InvalidArgument abort.
	_ = spGood.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s"})
	if _, err := spGood.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on frame 1 subscription, got %v", err)
	}

	// Frame 1 with non-empty client_id triggers strict InvalidArgument abort.
	spClientID, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open 3: %v", err)
	}
	if err := spClientID.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 60,
		ClientId:                 "client-1",
	}); err != nil {
		t.Fatalf("send frame 0 with client_id: %v", err)
	}
	if rProps, err := spClientID.Recv(); err != nil || !rProps.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() {
		t.Fatalf("expected default SubscriptionProperties on frame 0 with client_id, got %+v err=%v", rProps, err)
	}
	if err := spClientID.Send(&pubsubpb.StreamingPullRequest{ClientId: "client-1"}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("send frame 1 with client_id: %v", err)
	}
	if _, err := spClientID.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on subsequent frame client_id, got %v", err)
	}
	if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return len(snap.Streams) >= 3 && snap.Streams[2].Closed
	}); err != nil {
		t.Fatalf("WaitFor strict client_id stream close: %v", err)
	}
	strictCalls := srv.Snapshot().StreamingPullCalls()
	lastCall := strictCalls[len(strictCalls)-1]
	if lastCall.StreamSeq != 1 || lastCall.StatusCode != codes.InvalidArgument || !strings.Contains(lastCall.StatusErr, "client_id") {
		t.Fatalf("expected captured InvalidArgument frame 1 for client_id, got %+v", lastCall)
	}
}

func TestStreamingPullUnreadBackpressureAbortAndResetNoDeadlock(t *testing.T) {
	t.Run("64KiB_window_unread_client_Abort_and_healthy_peer", func(t *testing.T) {
		srv := startFakeServer(t)
		connSlow := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		connHealthy := dialFake(t, srv)
		subSlow := pubsubpb.NewSubscriberClient(connSlow)
		pubHealthy := pubsubpb.NewPublisherClient(connHealthy)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sp, err := subSlow.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull open: %v", err)
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("send frame 0: %v", err)
		}
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}

		// Deliberately NEVER call sp.Recv(); overflow the 64 KiB HTTP/2 window
		// with 32 x 32 KiB pushes so stream.Send blocks inside flow control.
		payload32K := bytes.Repeat([]byte("w"), 32<<10)
		for range 32 {
			if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
				{Message: &pubsubpb.PubsubMessage{Data: payload32K}},
			}); err != nil {
				t.Fatalf("PushMessages: %v", err)
			}
		}
		time.Sleep(150 * time.Millisecond)

		h.Abort(status.Error(codes.DeadlineExceeded, "abort stalled stream"))
		waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
		defer waitCancel()
		if err := h.WaitClosed(waitCtx); err != nil {
			t.Fatalf("WaitClosed timed out on stalled stream after Abort: %v", err)
		}
		if sum := h.Summary(); sum.CloseCode != codes.DeadlineExceeded {
			t.Fatalf("expected CloseCode=DeadlineExceeded, got %s (%q)", sum.CloseCode, sum.CloseErr)
		}

		// Independent healthy ClientConn must remain completely unaffected.
		if _, err := pubHealthy.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("still-alive")}},
		}); err != nil {
			t.Fatalf("healthy peer Publish failed after sibling Abort: %v", err)
		}
	})

	t.Run("default_window_unread_client_Reset_and_Close", func(t *testing.T) {
		srv, err := fake.NewServer()
		if err != nil {
			t.Fatalf("fake.NewServer: %v", err)
		}
		defer func() { _ = srv.Close() }()

		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sp, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull open: %v", err)
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("send frame 0: %v", err)
		}
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}

		// Deliberately NEVER call sp.Recv(); push 40 x 1 MiB to fill default
		// grpc.NewClient HTTP/2 flow-control windows.
		payload1M := bytes.Repeat([]byte("m"), 1<<20)
		for range 40 {
			if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
				{Message: &pubsubpb.PubsubMessage{Data: payload1M}},
			}); err != nil {
				t.Fatalf("PushMessages: %v", err)
			}
		}
		time.Sleep(150 * time.Millisecond)

		resetDone := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDone)
		}()
		resetTimer := time.NewTimer(2 * time.Second)
		defer resetTimer.Stop()
		select {
		case <-resetDone:
		case <-resetTimer.C:
			t.Fatalf("srv.Reset() deadlocked >2s on stalled default-window StreamingPull")
		}

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- srv.Close()
		}()
		closeTimer := time.NewTimer(2 * time.Second)
		defer closeTimer.Stop()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("srv.Close(): %v", err)
			}
		case <-closeTimer.C:
			t.Fatalf("srv.Close() deadlocked >2s after Reset()")
		}
	})
}

func TestStreamingPullAbortPriorityZeroQueuedDrain(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// OnRequest executes on the sender goroutine: queueing 5 items and immediately
	// calling Abort leaves both st.queue/wakeCh and streamCtx.Done() ready simultaneously.
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		OnRequest: func(h *fake.StreamHandle, _ *pubsubpb.StreamingPullRequest) {
			for range 5 {
				_ = h.PushMessages([]*pubsubpb.ReceivedMessage{{Message: &pubsubpb.PubsubMessage{Data: []byte("must-not-drain")}}})
			}
			h.Abort(status.Error(codes.Aborted, "immediate abort"))
		},
	})

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30}); err != nil {
		t.Fatalf("send frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{AckIds: []string{"trigger-abort"}}); err != nil {
		t.Fatalf("send frame 1: %v", err)
	}
	if _, err := sp.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("expected immediate codes.Aborted without queued message drain, got %v", err)
	}
	if err := h.WaitClosed(ctx); err != nil {
		t.Fatalf("WaitClosed: %v", err)
	}
	if sent := len(h.Summary().SentMessages); sent != 0 {
		t.Fatalf("expected 0 SentMessages after Abort, got %d", sent)
	}
}

func TestStreamingPullRecvRejectionsCapturedAndCloseCode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Frame-0 > MaxRecvMsgSize (1 MiB)
	srvSize := startFakeServer(t, fake.WithMaxRecvMsgSize(1<<20))
	connSize := dialFake(t, srvSize)
	subSize := pubsubpb.NewSubscriberClient(connSize)

	sp0, err := subSize.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull 0 open: %v", err)
	}
	_ = sp0.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             strings.Repeat("s", (1<<20)+1024),
		StreamAckDeadlineSeconds: 30,
	})
	if _, err := sp0.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted on frame-0 oversize, got %v", err)
	}
	if err := srvSize.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return len(snap.StreamingPullCalls()) == 1 && len(snap.Streams) == 1 && snap.Streams[0].Closed
	}); err != nil {
		t.Fatalf("WaitFor frame-0 oversize capture: %v", err)
	}
	snap0 := srvSize.Snapshot()
	c0 := snap0.StreamingPullCalls()[0]
	if c0.StreamID == "" || c0.StreamSeq != 0 || c0.StatusCode != codes.ResourceExhausted || c0.CodecError == "" {
		t.Fatalf("unexpected frame-0 oversize capture: %+v", c0)
	}
	if snap0.Streams[0].CloseCode != codes.ResourceExhausted {
		t.Fatalf("expected StreamSummary.CloseCode=ResourceExhausted, got %s (%q)", snap0.Streams[0].CloseCode, snap0.Streams[0].CloseErr)
	}

	// 2. Frame-1 > MaxRecvMsgSize (1 MiB)
	srvSize.Reset()
	sp1, err := subSize.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull 1 open: %v", err)
	}
	if err := sp1.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("send frame 0: %v", err)
	}
	h1, err := srvSize.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	_ = sp1.Send(&pubsubpb.StreamingPullRequest{
		AckIds: []string{strings.Repeat("a", (1<<20)+1024)},
	})
	if _, err := sp1.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted on frame-1 oversize, got %v", err)
	}
	if err := h1.WaitClosed(ctx); err != nil {
		t.Fatalf("WaitClosed frame-1: %v", err)
	}
	snap1 := srvSize.Snapshot()
	calls1 := snap1.StreamingPullCalls()
	if len(calls1) != 2 {
		t.Fatalf("expected 2 StreamingPull frames (frame 0 OK + frame 1 ResourceExhausted), got %d: %+v", len(calls1), calls1)
	}
	if calls1[0].StreamSeq != 0 || calls1[0].StatusCode != codes.OK {
		t.Fatalf("unexpected frame 0 capture: %+v", calls1[0])
	}
	if calls1[1].StreamID != h1.ID() || calls1[1].StreamSeq != 1 || calls1[1].StatusCode != codes.ResourceExhausted || calls1[1].CodecError == "" {
		t.Fatalf("unexpected frame 1 oversize capture: %+v", calls1[1])
	}
	if sum1 := h1.Summary(); sum1.CloseCode != codes.ResourceExhausted {
		t.Fatalf("expected CloseCode=ResourceExhausted, got %s (%q)", sum1.CloseCode, sum1.CloseErr)
	}

	// 3. Raw h2c frame-0 invalid UTF-8 subscription
	srvRaw := startFakeServer(t)
	badSub := []byte{'p', 0xc5, 's'}
	rawBadUTF8 := append([]byte{0x0a, byte(len(badSub))}, badSub...)
	code, _, err := sendRawGRPCH2C(ctx, srvRaw.Addr(), fake.MethodStreamingPull, "", rawBadUTF8, false)
	if err != nil || code != codes.Internal {
		t.Fatalf("expected codes.Internal for frame-0 invalid UTF-8, got code=%v err=%v", code, err)
	}
	if err := srvRaw.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return len(snap.StreamingPullCalls()) == 1 && len(snap.Streams) == 1 && snap.Streams[0].Closed
	}); err != nil {
		t.Fatalf("WaitFor invalid UTF-8 capture: %v", err)
	}
	snapUTF8 := srvRaw.Snapshot()
	if snapUTF8.StreamingPullCalls()[0].StatusCode != codes.Internal || !strings.Contains(snapUTF8.StreamingPullCalls()[0].CodecError, "UTF-8") {
		t.Fatalf("expected UTF-8 CodecError capture, got %+v", snapUTF8.StreamingPullCalls()[0])
	}
	if snapUTF8.Streams[0].CloseCode != codes.Internal {
		t.Fatalf("expected StreamSummary.CloseCode=Internal, got %s", snapUTF8.Streams[0].CloseCode)
	}

	// 4. Raw h2c frame-0 grpc-encoding: deflate with raw RFC 1951 flate
	srvRaw.Reset()
	validWire, err := proto.Marshal(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var flateBuf bytes.Buffer
	fw, err := flate.NewWriter(&flateBuf, flate.DefaultCompression)
	if err != nil {
		t.Fatalf("flate.NewWriter: %v", err)
	}
	if _, err := fw.Write(validWire); err != nil {
		t.Fatalf("flate write: %v", err)
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("flate close: %v", err)
	}
	code, _, err = sendRawGRPCH2C(ctx, srvRaw.Addr(), fake.MethodStreamingPull, "deflate", flateBuf.Bytes(), true)
	if err != nil || code != codes.Internal {
		t.Fatalf("expected codes.Internal for raw RFC 1951 flate frame 0, got code=%v err=%v", code, err)
	}
	if err := srvRaw.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return len(snap.StreamingPullCalls()) == 1 && len(snap.Streams) == 1 && snap.Streams[0].Closed
	}); err != nil {
		t.Fatalf("WaitFor raw flate capture: %v", err)
	}
	snapFlate := srvRaw.Snapshot()
	cf := snapFlate.StreamingPullCalls()[0]
	if cf.Compression != "deflate" || cf.StreamSeq != 0 || cf.StatusCode != codes.Internal || cf.CodecError == "" {
		t.Fatalf("unexpected raw flate frame-0 capture: %+v", cf)
	}
	if snapFlate.Streams[0].CloseCode != codes.Internal {
		t.Fatalf("expected StreamSummary.CloseCode=Internal for raw flate, got %s", snapFlate.Streams[0].CloseCode)
	}
}

func TestStreamingPullStartupOrderingAndInitialErrorSkip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Part A: >= 100 iterations verifying strict FIFO order:
	// SubscriptionProperties -> InitialResponses -> OnOpen pushes -> immediate h.PushMessages (0 inversions).
	srv := startFakeServer(t, fake.WithDefaultSubscriptionProperties(true, false))
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	for iter := range 100 {
		srv.Reset()
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			InitialResponses: []*pubsubpb.StreamingPullResponse{
				{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "init-ack", Message: &pubsubpb.PubsubMessage{Data: []byte("init")}},
					},
				},
			},
			OnOpen: func(h *fake.StreamHandle) {
				_ = h.PushMessages([]*pubsubpb.ReceivedMessage{
					{AckId: "onopen-ack", Message: &pubsubpb.PubsubMessage{Data: []byte("onopen")}},
				})
			},
		})

		sp, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("iter %d open: %v", iter, err)
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("iter %d send: %v", iter, err)
		}

		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
		}
		if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
			{AckId: "push-ack", Message: &pubsubpb.PubsubMessage{Data: []byte("push")}},
		}); err != nil {
			t.Fatalf("iter %d PushMessages: %v", iter, err)
		}

		r1, err := sp.Recv()
		if err != nil || !r1.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() {
			t.Fatalf("iter %d response 1 must be SubscriptionProperties(EOD=true), got %+v err=%v", iter, r1, err)
		}
		r2, err := sp.Recv()
		if err != nil || len(r2.GetReceivedMessages()) != 1 || r2.GetReceivedMessages()[0].GetAckId() != "init-ack" {
			t.Fatalf("iter %d response 2 must be init-ack, got %+v err=%v", iter, r2, err)
		}
		r3, err := sp.Recv()
		if err != nil || len(r3.GetReceivedMessages()) != 1 || r3.GetReceivedMessages()[0].GetAckId() != "onopen-ack" {
			t.Fatalf("iter %d response 3 must be onopen-ack, got %+v err=%v", iter, r3, err)
		}
		r4, err := sp.Recv()
		if err != nil || len(r4.GetReceivedMessages()) != 1 || r4.GetReceivedMessages()[0].GetAckId() != "push-ack" {
			t.Fatalf("iter %d response 4 must be push-ack, got %+v err=%v", iter, r4, err)
		}
		_ = sp.CloseSend()
		_, _ = sp.Recv()
	}

	// Verify OnOpen pushes precede AfterInitialError.
	srv.Reset()
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		OnOpen: func(h *fake.StreamHandle) {
			_ = h.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: "before-after-err"}})
		},
		AfterInitialError: status.Error(codes.Unavailable, "after open"),
	})
	spAfter, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull AfterInitialError open: %v", err)
	}
	_ = spAfter.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
	// Expect default SubscriptionProperties, OnOpen message, then Unavailable.
	if _, err := spAfter.Recv(); err != nil {
		t.Fatalf("expected SubscriptionProperties before AfterInitialError, got %v", err)
	}
	msgResp, err := spAfter.Recv()
	if err != nil || len(msgResp.GetReceivedMessages()) != 1 || msgResp.GetReceivedMessages()[0].GetAckId() != "before-after-err" {
		t.Fatalf("expected OnOpen push before AfterInitialError, got %+v err=%v", msgResp, err)
	}
	if _, err := spAfter.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable from AfterInitialError, got %v", err)
	}

	// Part B: WaitForStreamingPull skips InitialError attempt and returns healthy reconnect stream.
	srv.Reset()
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		Times:        1,
		InitialError: status.Error(codes.Unavailable, "transient initial fail"),
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sp1, err := sub.StreamingPull(ctx)
		if err != nil {
			return
		}
		_ = sp1.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
		if _, err := sp1.Recv(); status.Code(err) != codes.Unavailable {
			t.Errorf("expected Unavailable on rejected stream 1, got %v", err)
		}

		sp2, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Errorf("reconnect StreamingPull 2: %v", err)
			return
		}
		_ = sp2.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30})
		// Expect default SubscriptionProperties, then pushed "healthy-msg".
		if _, err := sp2.Recv(); err != nil {
			t.Errorf("sp2 sub props: %v", err)
			return
		}
		r, err := sp2.Recv()
		if err != nil || len(r.GetReceivedMessages()) != 1 || string(r.GetReceivedMessages()[0].GetMessage().GetData()) != "healthy-msg" {
			t.Errorf("sp2 expected healthy-msg, got %+v err=%v", r, err)
		}
		_ = sp2.CloseSend()
		_, _ = sp2.Recv()
	}()

	hHealthy, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull skip InitialError: %v", err)
	}
	if hHealthy.Summary().Closed {
		t.Fatalf("WaitForStreamingPull returned closed stream %s", hHealthy.ID())
	}
	if err := hHealthy.PushMessages([]*pubsubpb.ReceivedMessage{{Message: &pubsubpb.PubsubMessage{Data: []byte("healthy-msg")}}}); err != nil {
		t.Fatalf("PushMessages on healthy stream failed: %v", err)
	}
	wg.Wait()
}

func TestUnknownRPCConcurrentCancelAcrossResetStress(t *testing.T) {
	srv := startFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	// Warm up HTTP/2 connection so subsequent rounds immediately exercise
	// concurrent stream handling rather than TCP/HTTP2 handshake setup.
	if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p/topics/warm"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("warmup CreateTopic: want Unimplemented, got %v", err)
	}

	for round := range 20 {
		srv.Reset()

		const workers = 8
		subCtxs := make([]context.Context, workers)
		cancels := make([]context.CancelFunc, workers)
		for i := range workers {
			subCtxs[i], cancels[i] = context.WithCancel(ctx)
		}

		errs := make([]error, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = pub.CreateTopic(subCtxs[idx], &pubsubpb.Topic{Name: "projects/p/topics/t"})
			}(i)
		}

		if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
			return len(calls) >= 1
		}); err != nil {
			for _, c := range cancels {
				c()
			}
			wg.Wait()
			t.Fatalf("round %d WaitForCalls: %v", round, err)
		}

		for i := range workers {
			if i%2 == 0 {
				cancels[i]()
			}
		}
		srv.Reset()
		wg.Wait()
		for _, c := range cancels {
			c()
		}

		for i, err := range errs {
			switch status.Code(err) {
			case codes.Unimplemented, codes.Canceled, codes.Unavailable:
			default:
				t.Fatalf("round %d worker %d unexpected status: %v", round, i, err)
			}
		}

		// Without wiping s.calls with a second Reset(), any calls captured after
		// Reset() (e.g., client loopyWriter frames that entered TagRPC in the new
		// epoch) must have valid contiguous new-epoch Seq starting at 1.
		for j, got := range srv.Calls() {
			if got.Seq != int64(j+1) || got.FullMethod != "/google.pubsub.v1.Publisher/CreateTopic" || !got.Unexpected {
				t.Fatalf("round %d invalid post-reset call[%d]: %+v", round, j, got)
			}
		}
	}

	srv.Reset()
	connPost := dialFake(t, srv)
	pubPost := pubsubpb.NewPublisherClient(connPost)
	if _, err := pubPost.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("clean-after-stress")}},
	}); err != nil {
		t.Fatalf("post-stress Publish: %v", err)
	}
	calls := srv.Calls()
	if len(calls) != 1 || calls[0].Seq != 1 || calls[0].FullMethod != fake.MethodPublish {
		t.Fatalf("expected single clean Seq=1 Publish after final Reset, got %+v", calls)
	}
}

func TestResetIsolatesAttemptCountersAcrossEpochs(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Warm up shared connection.
	if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("warm")}},
	}); err != nil {
		t.Fatalf("warmup Publish: %v", err)
	}

	// Build a large PublishRequest (~400 KiB) so proto.CloneOf takes enough CPU
	// time between unaryInterceptor WaitForPublish wakeup and handlePublish
	// s.mu.Lock() to regularly exercise cross-Reset() inner handler races.
	largeMsgs := make([]*pubsubpb.PubsubMessage, 200)
	payload := bytes.Repeat([]byte("a"), 2048)
	for i := range largeMsgs {
		largeMsgs[i] = &pubsubpb.PubsubMessage{Data: payload}
	}

	for iter := range 25 {
		srv.Reset()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pub.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: largeMsgs,
			})
		}()

		if _, err := srv.WaitForPublish(ctx, 1); err != nil {
			wg.Wait()
			t.Fatalf("Publish iter %d WaitForPublish: %v", iter, err)
		}
		srv.Reset()
		wg.Wait()

		srv.AddPublishRule(fake.PublishRule{
			Match: fake.MatchCallAttempt(1),
			Err:   status.Error(codes.Aborted, "attempt-1"),
			Times: 1,
		})
		_, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("next-epoch")}},
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("Publish iter %d: expected MatchCallAttempt(1) to fire codes.Aborted after Reset, got %v", iter, err)
		}
	}

	// Verify Ack, ModAck, and Pull attempt counters also isolate across Reset().
	largeAckIDs := make([]string, 500)
	for i := range largeAckIDs {
		largeAckIDs[i] = strings.Repeat("k", 256)
	}
	for iter := range 10 {
		// Acknowledge
		srv.Reset()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       largeAckIDs,
			})
		}()
		if _, err := srv.WaitForAck(ctx, 1); err != nil {
			wg.Wait()
			t.Fatalf("Ack iter %d WaitForAck: %v", iter, err)
		}
		srv.Reset()
		wg.Wait()
		srv.AddAckRule(fake.AckRule{
			Match: fake.MatchAckAttempt(1),
			Err:   status.Error(codes.Aborted, "ack-attempt-1"),
			Times: 1,
		})
		if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
			Subscription: "projects/p/subscriptions/s",
			AckIds:       []string{"next-ack"},
		}); status.Code(err) != codes.Aborted {
			t.Fatalf("Ack iter %d: expected MatchAckAttempt(1) codes.Aborted after Reset, got %v", iter, err)
		}

		// ModifyAckDeadline
		srv.Reset()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             largeAckIDs,
				AckDeadlineSeconds: 60,
			})
		}()
		if _, err := srv.WaitForModAck(ctx, 1); err != nil {
			wg.Wait()
			t.Fatalf("ModAck iter %d WaitForModAck: %v", iter, err)
		}
		srv.Reset()
		wg.Wait()
		srv.AddModAckRule(fake.ModAckRule{
			Match: fake.MatchModAckAttempt(1),
			Err:   status.Error(codes.Aborted, "modack-attempt-1"),
			Times: 1,
		})
		if _, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       "projects/p/subscriptions/s",
			AckIds:             []string{"next-modack"},
			AckDeadlineSeconds: 60,
		}); status.Code(err) != codes.Aborted {
			t.Fatalf("ModAck iter %d: expected MatchModAckAttempt(1) codes.Aborted after Reset, got %v", iter, err)
		}

		// Pull
		srv.Reset()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sub.Pull(ctx, &pubsubpb.PullRequest{
				Subscription: strings.Repeat("s", 1024),
				MaxMessages:  10,
			})
		}()
		if _, err := srv.WaitForPull(ctx, 1); err != nil {
			wg.Wait()
			t.Fatalf("Pull iter %d WaitForPull: %v", iter, err)
		}
		srv.Reset()
		wg.Wait()
		srv.AddPullRule(fake.PullRule{
			Match: func(mc *fake.PullMatchContext) bool { return mc.Attempt == 1 },
			Err:   status.Error(codes.Aborted, "pull-attempt-1"),
			Times: 1,
		})
		if _, err := sub.Pull(ctx, &pubsubpb.PullRequest{
			Subscription: "projects/p/subscriptions/s",
			MaxMessages:  1,
		}); status.Code(err) != codes.Aborted {
			t.Fatalf("Pull iter %d: expected Pull Attempt==1 codes.Aborted after Reset, got %v", iter, err)
		}
	}
}

// readHTTP2ServerSettingsFrame reads and validates the initial HTTP/2 SETTINGS
// frame flushed by grpc's NewServerTransport before it blocks in
// io.ReadFull(t.conn, preface). Receiving this frame proves the server accepted
// the TCP connection, passed handleRawConn's s.quit check, and entered
// NewServerTransport's preface read without relying on time.Sleep.
func readHTTP2ServerSettingsFrame(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	var hdr [9]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		t.Fatalf("read server HTTP/2 SETTINGS frame header: %v", err)
	}
	frameLen := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
	frameType := hdr[3]
	frameFlags := hdr[4]
	streamID := binary.BigEndian.Uint32(hdr[5:9]) & 0x7fffffff
	if frameType != 0x04 || frameFlags != 0 || streamID != 0 || frameLen%6 != 0 || frameLen > 1024 {
		t.Fatalf("unexpected initial HTTP/2 SETTINGS frame header: type=0x%02x flags=0x%02x stream=%d len=%d", frameType, frameFlags, streamID, frameLen)
	}
	if frameLen > 0 {
		payload := make([]byte, frameLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Fatalf("read server HTTP/2 SETTINGS payload (%d bytes): %v", frameLen, err)
		}
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetReadDeadline: %v", err)
	}
}

func TestServerCloseUnprefaceRawTCPConnNoHang(t *testing.T) {
	srv, err := fake.NewServer()
	if err != nil {
		t.Fatalf("fake.NewServer: %v", err)
	}
	// Register srv.Close() cleanup BEFORE raw socket cleanups so LIFO t.Cleanup
	// closes the raw sockets first if Close() ever fails/times out.
	t.Cleanup(func() { _ = srv.Close() })

	// 0-byte idle raw TCP socket.
	rawIdle, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("net.Dial idle: %v", err)
	}
	t.Cleanup(func() { _ = rawIdle.Close() })
	readHTTP2ServerSettingsFrame(t, rawIdle)

	// Partial HTTP/2 preface socket (missing remainder of magic preface).
	rawPartial, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("net.Dial partial: %v", err)
	}
	t.Cleanup(func() { _ = rawPartial.Close() })
	if _, err := rawPartial.Write([]byte("PRI * HTTP/2.0\r\n")); err != nil {
		t.Fatalf("write partial preface: %v", err)
	}
	readHTTP2ServerSettingsFrame(t, rawPartial)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- srv.Close()
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("srv.Close() returned error: %v", err)
		}
	case <-timer.C:
		t.Fatalf("srv.Close() hung >2s with open unpreface raw TCP connections")
	}

	// Second Close() must remain idempotent and return nil immediately.
	if err := srv.Close(); err != nil {
		t.Fatalf("second srv.Close() returned error: %v", err)
	}
}

func TestStreamingPullServerDeadlineExceededNoCodecErrorCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: p}
	t.Cleanup(tr.CloseIdleConnections)
	httpClient := &http.Client{Transport: tr}

	// Subcase 1: Pre-frame-0 server-side grpc-timeout deadline expiration.
	t.Run("pre_frame_0_timeout", func(t *testing.T) {
		srv := startFakeServer(t)
		pr, pw := io.Pipe()
		t.Cleanup(func() { _ = pw.Close(); _ = pr.Close() })

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+srv.Addr()+fake.MethodStreamingPull, pr)
		if err != nil {
			t.Fatalf("NewRequestWithContext: %v", err)
		}
		req.Header.Set("Content-Type", "application/grpc")
		req.Header.Set("TE", "trailers")
		req.Header.Set("Grpc-Timeout", "50m")

		resp, err := httpClient.Do(req)
		if err == nil {
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		} else if !strings.Contains(err.Error(), "CANCEL") {
			t.Fatalf("expected server RST_STREAM(CANCEL) on pre-frame-0 Grpc-Timeout, got err=%v", err)
		}
		_ = pw.Close()

		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.Streams) == 1 && snap.Streams[0].Closed
		}); err != nil {
			t.Fatalf("WaitFor stream close: %v", err)
		}
		snap := srv.Snapshot()
		if len(snap.StreamingPullCalls()) != 0 {
			t.Fatalf("expected 0 StreamingPull calls on pre-frame-0 server DeadlineExceeded, got %+v", snap.StreamingPullCalls())
		}
		if snap.Streams[0].CloseCode != codes.DeadlineExceeded && snap.Streams[0].CloseCode != codes.Canceled {
			t.Fatalf("expected StreamSummary.CloseCode=DeadlineExceeded or Canceled, got %s (%q)", snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
		}
	})

	// Subcase 2: Valid frame 0 followed by server-side grpc-timeout waiting for frame 1.
	t.Run("post_frame_0_timeout", func(t *testing.T) {
		srv := startFakeServer(t)
		pr, pw := io.Pipe()
		t.Cleanup(func() { _ = pw.Close(); _ = pr.Close() })

		wire, err := proto.Marshal(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		})
		if err != nil {
			t.Fatalf("proto.Marshal: %v", err)
		}
		frame := make([]byte, 5+len(wire))
		binary.BigEndian.PutUint32(frame[1:5], uint32(len(wire)))
		copy(frame[5:], wire)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+srv.Addr()+fake.MethodStreamingPull, pr)
		if err != nil {
			t.Fatalf("NewRequestWithContext: %v", err)
		}
		req.Header.Set("Content-Type", "application/grpc")
		req.Header.Set("TE", "trailers")
		req.Header.Set("Grpc-Timeout", "250m")

		type httpRes struct {
			resp *http.Response
			err  error
		}
		resCh := make(chan httpRes, 1)
		go func() {
			resp, doErr := httpClient.Do(req)
			resCh <- httpRes{resp: resp, err: doErr}
		}()

		if _, err := pw.Write(frame); err != nil {
			t.Fatalf("pw.Write frame 0: %v", err)
		}
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}

		res := <-resCh
		if res.err != nil {
			t.Fatalf("httpClient.Do: %v", res.err)
		}
		defer res.resp.Body.Close()
		_, readErr := io.ReadAll(res.resp.Body)
		if readErr == nil {
			gs := res.resp.Trailer.Get("Grpc-Status")
			if gs == "" {
				gs = res.resp.Header.Get("Grpc-Status")
			}
			if gs != "4" && gs != "1" {
				t.Fatalf("expected Grpc-Status 4 (DeadlineExceeded) or 1 (Canceled) or RST_STREAM(CANCEL), got %q", gs)
			}
		} else if !strings.Contains(readErr.Error(), "CANCEL") {
			t.Fatalf("expected server RST_STREAM(CANCEL) on post-frame-0 Grpc-Timeout, got %v", readErr)
		}
		_ = pw.Close()

		if err := h.WaitClosed(ctx); err != nil {
			t.Fatalf("WaitClosed: %v", err)
		}
		snap := srv.Snapshot()
		calls := snap.StreamingPullCalls()
		if len(calls) != 1 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
			t.Fatalf("expected strictly 1 OK StreamingPullCall without phantom CodecError, got %+v", calls)
		}
		if sum := h.Summary(); sum.CloseCode != codes.DeadlineExceeded && sum.CloseCode != codes.Canceled {
			t.Fatalf("expected StreamSummary.CloseCode=DeadlineExceeded or Canceled, got %s (%q)", sum.CloseCode, sum.CloseErr)
		}
	})
}

func TestEODStaggeredMultiRoundAndErrorInfoCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Part A: Live gRPC ErrorInfo field capture & Clone() immutability.
	t.Run("error_info_capture_and_clone", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		nonEODSt, err := status.New(codes.ResourceExhausted, "quota").WithDetails(&errdetails.ErrorInfo{
			Domain:   "googleapis.com",
			Reason:   "QUOTA_EXCEEDED",
			Metadata: map[string]string{"a1": "ignored"},
		})
		if err != nil {
			t.Fatalf("WithDetails: %v", err)
		}

		srv.AddAckRule(fake.AckRule{
			Err: fake.EODErrorStatus(codes.InvalidArgument, "ack-eod", map[string]string{
				"a1": fake.EODPermanentInvalidAckID,
				"a2": fake.EODTransientOther,
			}),
			Times: 1,
		})
		srv.AddAckRule(fake.AckRule{
			Err:   fake.EODErrorStatus(codes.InvalidArgument, "empty-eod", nil),
			Times: 1,
		})
		srv.AddAckRule(fake.AckRule{
			Err:   nonEODSt.Err(),
			Times: 1,
		})
		srv.AddAckRule(fake.AckRule{
			Err:   status.Error(codes.Unavailable, "plain-unavailable"),
			Times: 1,
		})
		srv.AddModAckRule(fake.ModAckRule{
			Err: fake.EODModAckErrorStatus(codes.InvalidArgument, "modack-eod", map[string]string{
				"m1": fake.EODTransientInvalidAckID,
			}),
			Times: 1,
		})

		for range 4 {
			_, _ = sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"a1", "a2"},
			})
		}
		_, _ = sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       "projects/p/subscriptions/s",
			AckIds:             []string{"m1"},
			AckDeadlineSeconds: 60,
		})

		snap := srv.Snapshot()
		ackCalls := snap.AckCalls()
		if len(ackCalls) != 4 {
			t.Fatalf("expected 4 AckCalls, got %d", len(ackCalls))
		}

		// 1. EOD ErrorInfo on Acknowledge.
		if ackCalls[0].ResponseErrorInfoDomain != fake.EODDomainPubSub ||
			ackCalls[0].ResponseErrorInfoReason != fake.EODReasonAckFailed ||
			diffMap(map[string]string{"a1": fake.EODPermanentInvalidAckID, "a2": fake.EODTransientOther}, ackCalls[0].ResponseEODFailures) != "" {
			t.Fatalf("unexpected Ack[0] ErrorInfo capture: %+v", ackCalls[0])
		}
		// Clone deep copy must isolate ResponseEODFailures.
		cloned0 := ackCalls[0].Clone()
		cloned0.ResponseEODFailures["a1"] = "mutated"
		if srv.Calls()[0].ResponseEODFailures["a1"] != fake.EODPermanentInvalidAckID {
			t.Fatalf("Clone() shared mutable ResponseEODFailures map")
		}

		// 2. Empty-map EOD ErrorInfo must remain non-nil empty map!
		if ackCalls[1].ResponseErrorInfoDomain != fake.EODDomainPubSub ||
			ackCalls[1].ResponseErrorInfoReason != fake.EODReasonAckFailed ||
			ackCalls[1].ResponseEODFailures == nil ||
			len(ackCalls[1].ResponseEODFailures) != 0 {
			t.Fatalf("expected non-nil empty ResponseEODFailures for empty-map EOD ErrorInfo, got %+v", ackCalls[1])
		}
		clonedEmpty := ackCalls[1].Clone()
		if clonedEmpty.ResponseEODFailures == nil || len(clonedEmpty.ResponseEODFailures) != 0 {
			t.Fatalf("Clone() collapsed non-nil empty ResponseEODFailures to nil")
		}

		// 3. Non-EOD ErrorInfo populates Domain/Reason but leaves ResponseEODFailures == nil.
		if ackCalls[2].ResponseErrorInfoDomain != "googleapis.com" ||
			ackCalls[2].ResponseErrorInfoReason != "QUOTA_EXCEEDED" ||
			ackCalls[2].ResponseEODFailures != nil {
			t.Fatalf("expected non-EOD ErrorInfo to leave ResponseEODFailures nil, got %+v", ackCalls[2])
		}

		// 4. Plain status error leaves all ErrorInfo fields empty/nil.
		if ackCalls[3].ResponseErrorInfoDomain != "" ||
			ackCalls[3].ResponseErrorInfoReason != "" ||
			ackCalls[3].ResponseEODFailures != nil {
			t.Fatalf("expected plain Unavailable to have empty ErrorInfo fields, got %+v", ackCalls[3])
		}

		// 5. EOD ErrorInfo on ModifyAckDeadline.
		modCalls := snap.ModAckCalls()
		if len(modCalls) != 1 ||
			modCalls[0].ResponseErrorInfoDomain != fake.EODDomainPubSub ||
			modCalls[0].ResponseErrorInfoReason != fake.EODReasonModAckFailed ||
			diffMap(map[string]string{"m1": fake.EODTransientInvalidAckID}, modCalls[0].ResponseEODFailures) != "" {
			t.Fatalf("unexpected ModAck ErrorInfo capture: %+v", modCalls)
		}
	})

	// Part B: Live staggered multi-round EODScript for Acknowledge and ModifyAckDeadline.
	for _, kind := range []string{"ack", "modack"} {
		t.Run("staggered_"+kind, func(t *testing.T) {
			srv := startFakeServer(t)
			conn := dialFake(t, srv)
			sub := pubsubpb.NewSubscriberClient(conn)

			behaviors := map[string]fake.EODAckBehavior{
				"perm": {PermanentReason: fake.EODPermanentInvalidAckID},
				"t1":   {TransientReason: fake.EODTransientUnorderedAckID, TransientTimes: 2},
				"t2":   {TransientReason: fake.EODTransientInvalidAckID, TransientTimes: 1},
			}
			send := func(ids []string) error {
				if kind == "ack" {
					_, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
						Subscription: "projects/p/subscriptions/s",
						AckIds:       ids,
					})
					return err
				}
				_, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
					Subscription:       "projects/p/subscriptions/s",
					AckIds:             ids,
					AckDeadlineSeconds: 60,
				})
				return err
			}

			addAlwaysEOD := func() {
				if kind == "ack" {
					srv.AddAckRule(fake.AckRule{
						Times: fake.RepeatAlways,
						EOD:   fake.NewEODScript(codes.InvalidArgument, "staggered ack", fake.EODReasonAckFailed, behaviors),
					})
				} else {
					srv.AddModAckRule(fake.ModAckRule{
						Times: fake.RepeatAlways,
						EOD:   fake.NewEODScript(codes.InvalidArgument, "staggered modack", fake.EODReasonModAckFailed, behaviors),
					})
				}
			}
			addSingleErr := func(err error) {
				if kind == "ack" {
					srv.AddAckRule(fake.AckRule{Times: 1, Err: err})
				} else {
					srv.AddModAckRule(fake.ModAckRule{Times: 1, Err: err})
				}
			}

			addAlwaysEOD()

			// Round 1: [perm, t1, t2, ok] -> ErrorInfo{perm, t1, t2} (ok succeeds).
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("round 1: want InvalidArgument, got %v", err)
			}
			// Round 2: [t1, t2] -> ErrorInfo{t1} (t2 succeeds by omission!).
			if err := send([]string{"t1", "t2"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("round 2: want InvalidArgument, got %v", err)
			}
			// Round 3: [t1] -> OK.
			if err := send([]string{"t1"}); err != nil {
				t.Fatalf("round 3: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
				t.Fatalf("staggered %s AssertEODSplitRetry failed: %v", kind, err)
			}

			// Negative 1: dropping t2 after plain Unavailable (not EOD ErrorInfo omission) MUST FAIL.
			srv.Reset()
			if kind == "ack" {
				srv.AddAckRule(fake.AckRule{Times: 1, EOD: fake.NewEODScript(codes.InvalidArgument, "r1", fake.EODReasonAckFailed, behaviors)})
				srv.AddAckRule(fake.AckRule{Times: 1, Err: status.Error(codes.Unavailable, "transient-net")})
			} else {
				srv.AddModAckRule(fake.ModAckRule{Times: 1, EOD: fake.NewEODScript(codes.InvalidArgument, "r1", fake.EODReasonModAckFailed, behaviors)})
				srv.AddModAckRule(fake.ModAckRule{Times: 1, Err: status.Error(codes.Unavailable, "transient-net")})
			}
			_ = send([]string{"perm", "t1", "t2", "ok"})
			_ = send([]string{"t1", "t2"}) // fails Unavailable
			_ = send([]string{"t1"})       // buggy drop of t2
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail when t2 dropped after plain Unavailable")
			}

			// Negative 2: redundantly re-sending t2 after round-2 EOD omission MUST FAIL.
			srv.Reset()
			addAlwaysEOD()
			_ = send([]string{"perm", "t1", "t2", "ok"})
			_ = send([]string{"t1", "t2"}) // t2 omitted -> succeeded
			_ = send([]string{"t1", "t2"}) // buggy duplicate retry of recovered t2
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail when t2 redundantly retried after round-2 EOD omission")
			}

			// Negative 3: intra-call duplicate ack_id in retry call MUST FAIL.
			srv.Reset()
			addAlwaysEOD()
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 3 round 1: want InvalidArgument, got %v", err)
			}
			if err := send([]string{"t1", "t1", "t2"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 3 round 2: want InvalidArgument, got %v", err)
			}
			if err := send([]string{"t1"}); err != nil {
				t.Fatalf("Negative 3 round 3: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail on duplicate ack_id in retry call")
			}

			// Negative 4: duplicate ack_id in initial fail call MUST consume only 1 EODScript transient attempt AND fail AssertEODSplitRetry.
			srv.Reset()
			addAlwaysEOD()
			if err := send([]string{"perm", "t1", "t1", "t2", "ok"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 4 round 1: want InvalidArgument, got %v", err)
			}
			if err := send([]string{"t1", "t2"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 4 round 2: want InvalidArgument (duplicate t1 in round 1 must count as 1 RPC), got %v", err)
			}
			if err := send([]string{"t1"}); err != nil {
				t.Fatalf("Negative 4 round 3: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail on duplicate ack_id in initial fail call")
			}

			// Negative 5: pre-EOD codes.Unavailable followed by premature drop of perm and ok MUST FAIL.
			srv.Reset()
			addSingleErr(status.Error(codes.Unavailable, "pre-eod-unavailable"))
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("Negative 5 round 1: want Unavailable, got %v", err)
			}
			if err := send([]string{"t1", "t2"}); err != nil {
				t.Fatalf("Negative 5 round 2: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail on pre-EOD Unavailable premature drop")
			}

			// Negative 6: non-EOD ErrorInfo (googleapis.com/QUOTA_EXCEEDED) followed by premature drop MUST FAIL.
			srv.Reset()
			nonEODSt, err := status.New(codes.ResourceExhausted, "quota").WithDetails(&errdetails.ErrorInfo{
				Domain:   "googleapis.com",
				Reason:   "QUOTA_EXCEEDED",
				Metadata: map[string]string{"perm": "quota"},
			})
			if err != nil {
				t.Fatalf("WithDetails: %v", err)
			}
			addSingleErr(nonEODSt.Err())
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("Negative 6 round 1: want ResourceExhausted, got %v", err)
			}
			if err := send([]string{"t1", "t2"}); err != nil {
				t.Fatalf("Negative 6 round 2: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail on non-EOD ErrorInfo premature drop")
			}

			// Negative 7: plain codes.InvalidArgument (without ErrorInfo) followed by premature drop MUST FAIL.
			srv.Reset()
			addSingleErr(status.Error(codes.InvalidArgument, "plain-invalid-argument"))
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 7 round 1: want InvalidArgument, got %v", err)
			}
			if err := send([]string{"t1", "t2"}); err != nil {
				t.Fatalf("Negative 7 round 2: want OK, got %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				t.Fatalf("expected AssertEODSplitRetry to fail on plain InvalidArgument premature drop")
			}

			// Negative 8: zero-sleep Gate-synchronized in-flight retry MUST FAIL until completed.
			srv.Reset()
			gate := fake.NewGate()
			if kind == "ack" {
				srv.AddAckRule(fake.AckRule{Times: 1, EOD: fake.NewEODScript(codes.InvalidArgument, "r1", fake.EODReasonAckFailed, behaviors)})
				srv.AddAckRule(fake.AckRule{Times: 1, Gate: gate})
			} else {
				srv.AddModAckRule(fake.ModAckRule{Times: 1, EOD: fake.NewEODScript(codes.InvalidArgument, "r1", fake.EODReasonModAckFailed, behaviors)})
				srv.AddModAckRule(fake.ModAckRule{Times: 1, Gate: gate})
			}
			if err := send([]string{"perm", "t1", "t2", "ok"}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("Negative 8 round 1: want InvalidArgument, got %v", err)
			}
			var gateWG sync.WaitGroup
			var gatedErr error
			gateWG.Add(1)
			go func() {
				defer gateWG.Done()
				gatedErr = send([]string{"t1", "t2"})
			}()
			if err := gate.WaitEntered(ctx); err != nil {
				gate.Release()
				gateWG.Wait()
				t.Fatalf("Negative 8 WaitEntered: %v", err)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err == nil {
				gate.Release()
				gateWG.Wait()
				t.Fatalf("expected AssertEODSplitRetry to fail while retry call is still in flight")
			}
			gate.Release()
			gateWG.Wait()
			if gatedErr != nil {
				t.Fatalf("Negative 8 gated retry expected OK, got %v", gatedErr)
			}
			if err := fake.AssertEODSplitRetry(srv.Snapshot(), []string{"perm"}, []string{"t1", "t2"}, []string{"ok"}); err != nil {
				t.Fatalf("expected AssertEODSplitRetry to pass after gated retry completed: %v", err)
			}
		})
	}
}

func diffMap(want, got map[string]string) string {
	return cmp.Diff(want, got)
}

func TestPublishExplicitEmptyMessageIDsAndResetRestoresOptions(t *testing.T) {
	srv := startFakeServer(t,
		fake.WithSuppressPong(true),
		fake.WithDefaultSubscriptionProperties(true, true),
	)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Explicit empty non-nil []string{} returns 0 IDs (PUB-10).
	srv.AddPublishRule(fake.PublishRule{
		MessageIDs: []string{},
		Times:      1,
	})
	resp, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m1")}, {Data: []byte("m2")}},
	})
	if err != nil {
		t.Fatalf("Publish with empty MessageIDs: %v", err)
	}
	if len(resp.GetMessageIds()) != 0 {
		t.Fatalf("expected 0 MessageIds for explicit []string{}, got %v", resp.GetMessageIds())
	}

	// Override constructor options, then Reset() and verify defaults are restored.
	srv.SetSuppressPong(false)
	srv.SetDefaultSubscriptionProperties(false, false)
	srv.Reset()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30}); err != nil {
		t.Fatalf("send frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	rProps, err := sp.Recv()
	if err != nil {
		t.Fatalf("Recv sub props: %v", err)
	}
	if !rProps.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() || !rProps.GetSubscriptionProperties().GetMessageOrderingEnabled() {
		t.Fatalf("expected Reset to restore WithDefaultSubscriptionProperties(true, true), got %+v", rProps.GetSubscriptionProperties())
	}

	// Send keepalive ping; WithSuppressPong(true) must remain restored after Reset().
	if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if err := srv.WaitForCalls(ctx, func([]fake.CapturedCall) bool {
		return h.Summary().KeepalivePingCount >= 1
	}); err != nil {
		t.Fatalf("WaitFor ping: %v", err)
	}
	if err := h.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: "after-suppressed-ping"}}); err != nil {
		t.Fatalf("PushMessages: %v", err)
	}
	rNext, err := sp.Recv()
	if err != nil {
		t.Fatalf("Recv after suppressed ping: %v", err)
	}
	if len(rNext.GetReceivedMessages()) != 1 || rNext.GetReceivedMessages()[0].GetAckId() != "after-suppressed-ping" {
		t.Fatalf("expected pushed message instead of pong when WithSuppressPong(true) restored, got %+v", rNext)
	}
}

func TestStreamingPullPipelinedFrame0AndFrame1CaptureAndInitialError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("pure_grpc_frame0_valid_frame1_oversize", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		frame0 := &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}
		frame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("a", 512)},
		}

		for iter := range 5 {
			srv.Reset()
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d StreamingPull open: %v", iter, err)
			}
			if err := sp.Send(frame0); err != nil {
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(frame1); err != nil {
				t.Fatalf("iter %d send frame 1: %v", iter, err)
			}
			if _, recvErr := sp.Recv(); status.Code(recvErr) != codes.ResourceExhausted {
				t.Fatalf("iter %d expected ResourceExhausted, got %v", iter, recvErr)
			}

			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 {
				t.Fatalf("iter %d expected 2 StreamingPull frames, got %d: %+v", iter, len(calls), calls)
			}
			if calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
				t.Fatalf("iter %d unexpected frame 0 capture: %+v", iter, calls[0])
			}
			if calls[1].StreamSeq != 1 || calls[1].StatusCode != codes.ResourceExhausted || calls[1].CodecError == "" {
				t.Fatalf("iter %d unexpected frame 1 capture: %+v", iter, calls[1])
			}
			if snap.Streams[0].CloseCode != codes.ResourceExhausted {
				t.Fatalf("iter %d expected StreamSummary.CloseCode=ResourceExhausted, got %s (%q)", iter, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
			}
		}
	})

	t.Run("raw_h2c_multiframe_invalid_utf8_frame1", func(t *testing.T) {
		srv := startFakeServer(t)
		frame0Wire, err := proto.Marshal(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		})
		if err != nil {
			t.Fatalf("proto.Marshal frame 0: %v", err)
		}
		// Field 2 (ack_ids, repeated string) with invalid UTF-8 bytes.
		frame1InvalidUTF8 := []byte{0x12, 0x03, 'a', 0xc5, 'z'}

		code, _, err := sendRawGRPCH2CFrames(
			ctx,
			srv.Addr(),
			fake.MethodStreamingPull,
			"",
			rawGRPCFrame{payload: frame0Wire},
			rawGRPCFrame{payload: frame1InvalidUTF8},
		)
		if err != nil {
			t.Fatalf("sendRawGRPCH2CFrames invalid UTF-8: %v", err)
		}
		if code != codes.Internal && code != codes.InvalidArgument {
			t.Fatalf("expected Internal/InvalidArgument over wire, got %v", code)
		}

		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.Streams) == 1 && snap.Streams[0].Closed
		}); err != nil {
			t.Fatalf("WaitFor stream closed: %v", err)
		}
		snap := srv.Snapshot()
		calls := snap.StreamingPullCalls()
		if len(calls) != 2 {
			t.Fatalf("expected 2 StreamingPull frames, got %d: %+v", len(calls), calls)
		}
		if calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
			t.Fatalf("unexpected frame 0 capture: %+v", calls[0])
		}
		if calls[1].StreamSeq != 1 || (calls[1].StatusCode != codes.Internal && calls[1].StatusCode != codes.InvalidArgument) || calls[1].CodecError == "" || !strings.Contains(calls[1].CodecError, "UTF-8") {
			t.Fatalf("unexpected frame 1 UTF-8 CodecError capture: %+v", calls[1])
		}
		if snap.Streams[0].CloseCode != calls[1].StatusCode {
			t.Fatalf("expected StreamSummary.CloseCode=%s, got %s (%q)", calls[1].StatusCode, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
		}
	})

	t.Run("raw_h2c_multiframe_raw_rfc1951_flate_frame1", func(t *testing.T) {
		srv := startFakeServer(t)
		frame0Wire, err := proto.Marshal(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		})
		if err != nil {
			t.Fatalf("proto.Marshal frame 0: %v", err)
		}
		frame1Wire, err := proto.Marshal(&pubsubpb.StreamingPullRequest{
			AckIds: []string{"ack-1"},
		})
		if err != nil {
			t.Fatalf("proto.Marshal frame 1: %v", err)
		}

		var zlibBuf bytes.Buffer
		zw := zlib.NewWriter(&zlibBuf)
		if _, err := zw.Write(frame0Wire); err != nil {
			t.Fatalf("zlib write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zlib close: %v", err)
		}

		var flateBuf bytes.Buffer
		fw, err := flate.NewWriter(&flateBuf, flate.DefaultCompression)
		if err != nil {
			t.Fatalf("flate.NewWriter: %v", err)
		}
		if _, err := fw.Write(frame1Wire); err != nil {
			t.Fatalf("flate write: %v", err)
		}
		if err := fw.Close(); err != nil {
			t.Fatalf("flate close: %v", err)
		}

		code, _, err := sendRawGRPCH2CFrames(
			ctx,
			srv.Addr(),
			fake.MethodStreamingPull,
			"deflate",
			rawGRPCFrame{payload: zlibBuf.Bytes(), compressed: true},
			rawGRPCFrame{payload: flateBuf.Bytes(), compressed: true},
		)
		if err != nil {
			t.Fatalf("sendRawGRPCH2CFrames raw flate: %v", err)
		}
		if code != codes.Internal && code != codes.InvalidArgument {
			t.Fatalf("expected Internal/InvalidArgument over wire, got %v", code)
		}

		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.Streams) == 1 && snap.Streams[0].Closed
		}); err != nil {
			t.Fatalf("WaitFor stream closed: %v", err)
		}
		snap := srv.Snapshot()
		calls := snap.StreamingPullCalls()
		if len(calls) != 2 {
			t.Fatalf("expected 2 StreamingPull frames, got %d: %+v", len(calls), calls)
		}
		if calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" || calls[0].Compression != "deflate" {
			t.Fatalf("unexpected frame 0 capture: %+v", calls[0])
		}
		if calls[1].StreamSeq != 1 || (calls[1].StatusCode != codes.Internal && calls[1].StatusCode != codes.InvalidArgument) || calls[1].CodecError == "" || calls[1].Compression != "deflate" {
			t.Fatalf("unexpected frame 1 raw flate CodecError capture: %+v", calls[1])
		}
		if snap.Streams[0].CloseCode != calls[1].StatusCode {
			t.Fatalf("expected StreamSummary.CloseCode=%s, got %s (%q)", calls[1].StatusCode, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
		}
	})

	t.Run("initial_error_preserved_with_pipelined_oversize_frame1", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		frame0 := &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}
		frame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("a", 512)},
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:        1,
				InitialError: status.Error(codes.PermissionDenied, "denied-frame0"),
			})
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d StreamingPull open: %v", iter, err)
			}
			if err := sp.Send(frame0); err != nil {
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(frame1); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("iter %d send frame 1: %v", iter, err)
			}
			if _, recvErr := sp.Recv(); status.Code(recvErr) != codes.PermissionDenied {
				t.Fatalf("iter %d expected PermissionDenied (InitialError), got %v", iter, recvErr)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 {
				t.Fatalf("iter %d expected strictly 1 frame when InitialError fires, got %d: %+v", iter, len(calls), calls)
			}
			if calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.PermissionDenied || calls[0].CodecError != "" {
				t.Fatalf("iter %d unexpected frame 0 capture: %+v", iter, calls[0])
			}
			if snap.Streams[0].CloseCode != codes.PermissionDenied {
				t.Fatalf("iter %d expected StreamSummary.CloseCode=PermissionDenied, got %s", iter, snap.Streams[0].CloseCode)
			}
		}

		// Also verify single-HTTP/2-body pipelined Frame 0 + oversize Frame 1 via raw h2c.
		srv.Reset()
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times:        1,
			InitialError: status.Error(codes.PermissionDenied, "denied-frame0-raw"),
		})
		f0Wire, err := proto.Marshal(frame0)
		if err != nil {
			t.Fatalf("proto.Marshal frame 0: %v", err)
		}
		f1Wire, err := proto.Marshal(frame1)
		if err != nil {
			t.Fatalf("proto.Marshal frame 1: %v", err)
		}
		code, _, err := sendRawGRPCH2CFrames(
			ctx,
			srv.Addr(),
			fake.MethodStreamingPull,
			"",
			rawGRPCFrame{payload: f0Wire},
			rawGRPCFrame{payload: f1Wire},
		)
		if err != nil {
			t.Fatalf("sendRawGRPCH2CFrames InitialError: %v", err)
		}
		if code != codes.PermissionDenied {
			t.Fatalf("raw h2c expected PermissionDenied (InitialError), got %v", code)
		}
		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.Streams) == 1 && snap.Streams[0].Closed
		}); err != nil {
			t.Fatalf("WaitFor raw InitialError stream closed: %v", err)
		}
		snapRaw := srv.Snapshot()
		callsRaw := snapRaw.StreamingPullCalls()
		if len(callsRaw) != 1 || callsRaw[0].StatusCode != codes.PermissionDenied || snapRaw.Streams[0].CloseCode != codes.PermissionDenied {
			t.Fatalf("unexpected raw InitialError capture: calls=%+v streams=%+v", callsRaw, snapRaw.Streams)
		}
	})
}

func TestStreamingPullPipelinedMultiFrameAndAfterInitialError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	frame0Valid := &pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}
	frame0Wire, err := proto.Marshal(frame0Valid)
	if err != nil {
		t.Fatalf("proto.Marshal frame0Valid: %v", err)
	}
	// Field 2 (ack_ids, repeated string) with invalid UTF-8 bytes.
	frameInvalidUTF8 := []byte{0x12, 0x03, 'a', 0xc5, 'z'}

	t.Run("after_initial_error_with_default_sub_props_and_initial_responses", func(t *testing.T) {
		srv := startFakeServer(t,
			fake.WithMaxRecvMsgSize(256),
			fake.WithDefaultSubscriptionProperties(true, true),
		)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		oversizeFrame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("a", 512)},
		}
		initResp := &pubsubpb.StreamingPullResponse{
			ReceivedMessages: []*pubsubpb.ReceivedMessage{
				{
					AckId:   "init-ack-1",
					Message: &pubsubpb.PubsubMessage{Data: []byte("init-payload")},
				},
			},
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				InitialResponses:  []*pubsubpb.StreamingPullResponse{initResp},
				AfterInitialError: status.Error(codes.Aborted, "after-init-subprops"),
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("pure grpc iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(oversizeFrame1); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("pure grpc iter %d send frame 1: %v", iter, err)
			}
			var gotResps []*pubsubpb.StreamingPullResponse
			var recvErr error
			for {
				r, err := sp.Recv()
				if err != nil {
					recvErr = err
					break
				}
				gotResps = append(gotResps, r)
			}
			if status.Code(recvErr) != codes.Aborted {
				t.Fatalf("pure grpc iter %d expected wire codes.Aborted, got %v (resps=%d)", iter, recvErr, len(gotResps))
			}
			if len(gotResps) != 2 {
				t.Fatalf("pure grpc iter %d expected 2 responses (SubProps + InitialResponse), got %d: %+v", iter, len(gotResps), gotResps)
			}
			if !gotResps[0].GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() || !gotResps[0].GetSubscriptionProperties().GetMessageOrderingEnabled() {
				t.Fatalf("pure grpc iter %d expected default SubscriptionProperties true/true, got %+v", iter, gotResps[0].GetSubscriptionProperties())
			}
			if len(gotResps[1].GetReceivedMessages()) != 1 || gotResps[1].GetReceivedMessages()[0].GetAckId() != "init-ack-1" {
				t.Fatalf("pure grpc iter %d expected init-ack-1, got %+v", iter, gotResps[1])
			}

			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 || calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
				t.Fatalf("pure grpc iter %d expected strictly 1 OK frame 0 call, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.Aborted {
				t.Fatalf("pure grpc iter %d expected StreamSummary.CloseCode=Aborted, got %s (%q)", iter, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
			}
			if len(snap.Streams[0].SentMessages) != 1 || snap.Streams[0].SentMessages[0].GetAckId() != "init-ack-1" {
				t.Fatalf("pure grpc iter %d expected SentMessages=[init-ack-1], got %+v", iter, snap.Streams[0].SentMessages)
			}
		}

		// Raw h2c single-DATA-body: Frame 0 + invalid UTF-8 Frame 1 with DefaultSubProps + InitialResponses + AfterInitialError.
		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				InitialResponses:  []*pubsubpb.StreamingPullResponse{initResp},
				AfterInitialError: status.Error(codes.Aborted, "after-init-raw-h2c"),
			})
			code, body, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: frame0Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("raw h2c iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.Aborted {
				t.Fatalf("raw h2c iter %d expected wire codes.Aborted, got %v", iter, code)
			}
			resps, err := decodeRawGRPCStreamingPullResponses(body)
			if err != nil {
				t.Fatalf("raw h2c iter %d decode responses: %v", iter, err)
			}
			if len(resps) != 2 {
				t.Fatalf("raw h2c iter %d expected 2 responses, got %d: %+v", iter, len(resps), resps)
			}
			if !resps[0].GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() || !resps[0].GetSubscriptionProperties().GetMessageOrderingEnabled() {
				t.Fatalf("raw h2c iter %d expected default SubscriptionProperties true/true, got %+v", iter, resps[0].GetSubscriptionProperties())
			}
			if len(resps[1].GetReceivedMessages()) != 1 || resps[1].GetReceivedMessages()[0].GetAckId() != "init-ack-1" {
				t.Fatalf("raw h2c iter %d expected init-ack-1, got %+v", iter, resps[1])
			}

			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("raw h2c iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 || calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
				t.Fatalf("raw h2c iter %d expected strictly 1 OK frame 0 call, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.Aborted {
				t.Fatalf("raw h2c iter %d expected StreamSummary.CloseCode=Aborted, got %s (%q)", iter, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
			}
		}
	})

	t.Run("after_initial_error_without_initial_responses", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		oversizeFrame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("b", 512)},
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				AfterInitialError: status.Error(codes.Aborted, "after-init-empty-grpc"),
			})
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("pure grpc iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(oversizeFrame1); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("pure grpc iter %d send frame 1: %v", iter, err)
			}
			if _, recvErr := sp.Recv(); status.Code(recvErr) != codes.Aborted {
				t.Fatalf("pure grpc iter %d expected wire codes.Aborted, got %v", iter, recvErr)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 || calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK {
				t.Fatalf("pure grpc iter %d expected strictly 1 OK frame 0 call, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.Aborted {
				t.Fatalf("pure grpc iter %d expected StreamSummary.CloseCode=Aborted, got %s", iter, snap.Streams[0].CloseCode)
			}
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				AfterInitialError: status.Error(codes.Aborted, "after-init-empty-raw"),
			})
			code, body, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: frame0Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("raw h2c iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.Aborted || len(body) != 0 {
				t.Fatalf("raw h2c iter %d expected wire codes.Aborted with empty body, got code=%v bodyLen=%d", iter, code, len(body))
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("raw h2c iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 || calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK {
				t.Fatalf("raw h2c iter %d expected strictly 1 OK frame 0 call, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.Aborted {
				t.Fatalf("raw h2c iter %d expected StreamSummary.CloseCode=Aborted, got %s", iter, snap.Streams[0].CloseCode)
			}
		}
	})

	t.Run("on_open_close_with_pipelined_failing_frame1", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		oversizeFrame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("c", 512)},
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnOpen: func(h *fake.StreamHandle) {
					_ = h.PushMessages([]*pubsubpb.ReceivedMessage{
						{AckId: "onopen-ack-1", Message: &pubsubpb.PubsubMessage{Data: []byte("onopen-msg")}},
					})
					_ = h.Close(status.Error(codes.PermissionDenied, "onopen-close-grpc"))
				},
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("pure grpc iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(oversizeFrame1); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("pure grpc iter %d send frame 1: %v", iter, err)
			}
			var gotResps []*pubsubpb.StreamingPullResponse
			var recvErr error
			for {
				r, err := sp.Recv()
				if err != nil {
					recvErr = err
					break
				}
				gotResps = append(gotResps, r)
			}
			if status.Code(recvErr) != codes.PermissionDenied {
				t.Fatalf("pure grpc iter %d expected wire codes.PermissionDenied, got %v", iter, recvErr)
			}
			if len(gotResps) != 1 || len(gotResps[0].GetReceivedMessages()) != 1 || gotResps[0].GetReceivedMessages()[0].GetAckId() != "onopen-ack-1" {
				t.Fatalf("pure grpc iter %d expected queued OnOpen message before close, got %+v", iter, gotResps)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 1 || calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK {
				t.Fatalf("pure grpc iter %d expected strictly 1 OK frame 0 call, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.PermissionDenied {
				t.Fatalf("pure grpc iter %d expected StreamSummary.CloseCode=PermissionDenied, got %s", iter, snap.Streams[0].CloseCode)
			}
		}

		// Raw h2c single-DATA-body: OnOpen h.Close(PermissionDenied) without queued responses + invalid UTF-8 Frame 1.
		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnOpen: func(h *fake.StreamHandle) {
					_ = h.Close(status.Error(codes.PermissionDenied, "onopen-close-raw"))
				},
			})
			code, _, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: frame0Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("raw h2c iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.PermissionDenied {
				t.Fatalf("raw h2c iter %d expected wire codes.PermissionDenied, got %v", iter, code)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("raw h2c iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			if len(snap.StreamingPullCalls()) != 1 || snap.Streams[0].CloseCode != codes.PermissionDenied {
				t.Fatalf("raw h2c iter %d unexpected capture: calls=%+v streams=%+v", iter, snap.StreamingPullCalls(), snap.Streams)
			}
		}
	})

	t.Run("pipelined_3frame_strict_client_id_frame1_not_clobbered_by_frame2", func(t *testing.T) {
		srv := startFakeServer(t,
			fake.WithMaxRecvMsgSize(256),
			fake.WithStrictStreamingPullValidation(true),
		)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		strictF0 := &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
			ClientId:                 "strict-client",
		}
		strictInvalidF1 := &pubsubpb.StreamingPullRequest{
			ClientId: "strict-client",
			AckIds:   []string{"ack-1"},
		}
		oversizeF2 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("z", 512)},
		}

		// Part A: Pure gRPC 3-frame burst (Frame 0 + strict ClientId invalid Frame 1 + oversize Frame 2).
		for iter := range 5 {
			srv.Reset()
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc iter %d open: %v", iter, err)
			}
			if err := sp.Send(strictF0); err != nil {
				t.Fatalf("pure grpc iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(strictInvalidF1); err != nil {
				t.Fatalf("pure grpc iter %d send frame 1: %v", iter, err)
			}
			if err := sp.Send(oversizeF2); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("pure grpc iter %d send frame 2: %v", iter, err)
			}
			if _, recvErr := sp.Recv(); status.Code(recvErr) != codes.InvalidArgument {
				t.Fatalf("pure grpc iter %d expected wire codes.InvalidArgument from Frame 1, got %v", iter, recvErr)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 {
				t.Fatalf("pure grpc iter %d expected 2 captured frames (Frame 2 must not be read), got %d: %+v", iter, len(calls), calls)
			}
			if calls[0].StreamSeq != 0 || calls[0].StatusCode != codes.OK {
				t.Fatalf("pure grpc iter %d unexpected frame 0: %+v", iter, calls[0])
			}
			if calls[1].StreamSeq != 1 || calls[1].StatusCode != codes.InvalidArgument || !strings.Contains(calls[1].StatusErr, "client_id") {
				t.Fatalf("pure grpc iter %d unexpected frame 1: %+v", iter, calls[1])
			}
			if snap.Streams[0].CloseCode != codes.InvalidArgument {
				t.Fatalf("pure grpc iter %d expected StreamSummary.CloseCode=InvalidArgument, got %s (%q)", iter, snap.Streams[0].CloseCode, snap.Streams[0].CloseErr)
			}
		}

		// Part B: Raw h2c single-DATA-body 3-frame burst (Frame 0 + strict ClientId invalid Frame 1 + invalid UTF-8 Frame 2).
		strictF0Wire, err := proto.Marshal(strictF0)
		if err != nil {
			t.Fatalf("proto.Marshal strictF0: %v", err)
		}
		strictF1Wire, err := proto.Marshal(strictInvalidF1)
		if err != nil {
			t.Fatalf("proto.Marshal strictInvalidF1: %v", err)
		}
		for iter := range 5 {
			srv.Reset()
			code, _, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: strictF0Wire},
				rawGRPCFrame{payload: strictF1Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("raw h2c iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.InvalidArgument {
				t.Fatalf("raw h2c iter %d expected wire codes.InvalidArgument from Frame 1, got %v", iter, code)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("raw h2c iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 || calls[0].StatusCode != codes.OK || calls[1].StatusCode != codes.InvalidArgument || !strings.Contains(calls[1].StatusErr, "client_id") {
				t.Fatalf("raw h2c iter %d expected Frame 0 OK + Frame 1 InvalidArgument only, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.InvalidArgument {
				t.Fatalf("raw h2c iter %d expected StreamSummary.CloseCode=InvalidArgument, got %s", iter, snap.Streams[0].CloseCode)
			}
		}
	})

	t.Run("pipelined_3frame_onrequest_close_and_abort_frame1_not_clobbered_by_frame2", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		triggerF1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{"trigger-frame1"},
		}
		triggerF1Wire, err := proto.Marshal(triggerF1)
		if err != nil {
			t.Fatalf("proto.Marshal triggerF1: %v", err)
		}
		oversizeF2 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{strings.Repeat("x", 512)},
		}

		// Part A: Raw h2c single-DATA-body Frame 0 + OnRequest h.Close(Aborted) Frame 1 + invalid UTF-8 Frame 2.
		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnRequest: func(h *fake.StreamHandle, req *pubsubpb.StreamingPullRequest) {
					if len(req.GetAckIds()) == 1 && req.GetAckIds()[0] == "trigger-frame1" {
						_ = h.PushMessages([]*pubsubpb.ReceivedMessage{
							{AckId: "onreq-ack-1", Message: &pubsubpb.PubsubMessage{Data: []byte("onreq-msg")}},
						})
						_ = h.Close(status.Error(codes.Aborted, "onrequest-close-frame1"))
					}
				},
			})

			code, body, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: frame0Wire},
				rawGRPCFrame{payload: triggerF1Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("onrequest close iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.Aborted {
				t.Fatalf("onrequest close iter %d expected wire codes.Aborted, got %v", iter, code)
			}
			resps, err := decodeRawGRPCStreamingPullResponses(body)
			if err != nil {
				t.Fatalf("onrequest close iter %d decode responses: %v", iter, err)
			}
			if len(resps) != 1 || len(resps[0].GetReceivedMessages()) != 1 || resps[0].GetReceivedMessages()[0].GetAckId() != "onreq-ack-1" {
				t.Fatalf("onrequest close iter %d expected onreq-ack-1 response before Aborted, got %+v", iter, resps)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("onrequest close iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 || calls[0].StatusCode != codes.OK || calls[1].StatusCode != codes.OK {
				t.Fatalf("onrequest close iter %d expected strictly 2 OK calls (Frame 2 unread), got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.Aborted {
				t.Fatalf("onrequest close iter %d expected StreamSummary.CloseCode=Aborted, got %s", iter, snap.Streams[0].CloseCode)
			}
		}

		// Part B: Pure gRPC + raw h2c Frame 0 + OnRequest h.Abort(PermissionDenied) Frame 1 + oversize/invalid-UTF-8 Frame 2.
		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnRequest: func(h *fake.StreamHandle, req *pubsubpb.StreamingPullRequest) {
					if len(req.GetAckIds()) == 1 && req.GetAckIds()[0] == "trigger-frame1" {
						_ = h.PushMessages([]*pubsubpb.ReceivedMessage{
							{AckId: "must-not-drain", Message: &pubsubpb.PubsubMessage{Data: []byte("dropped")}},
						})
						h.Abort(status.Error(codes.PermissionDenied, "onrequest-abort-frame1"))
					}
				},
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc abort iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("pure grpc abort iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(triggerF1); err != nil {
				t.Fatalf("pure grpc abort iter %d send frame 1: %v", iter, err)
			}
			if err := sp.Send(oversizeF2); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("pure grpc abort iter %d send frame 2: %v", iter, err)
			}
			if _, recvErr := sp.Recv(); status.Code(recvErr) != codes.PermissionDenied {
				t.Fatalf("pure grpc abort iter %d expected wire codes.PermissionDenied, got %v", iter, recvErr)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc abort iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 || calls[0].StatusCode != codes.OK || calls[1].StatusCode != codes.OK {
				t.Fatalf("pure grpc abort iter %d expected strictly 2 OK calls, got %+v", iter, calls)
			}
			if snap.Streams[0].CloseCode != codes.PermissionDenied || len(snap.Streams[0].SentMessages) != 0 {
				t.Fatalf("pure grpc abort iter %d expected CloseCode=PermissionDenied and 0 SentMessages, got %+v", iter, snap.Streams[0])
			}
		}

		for iter := range 5 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnRequest: func(h *fake.StreamHandle, req *pubsubpb.StreamingPullRequest) {
					if len(req.GetAckIds()) == 1 && req.GetAckIds()[0] == "trigger-frame1" {
						_ = h.PushMessages([]*pubsubpb.ReceivedMessage{
							{AckId: "must-not-drain-raw", Message: &pubsubpb.PubsubMessage{Data: []byte("dropped")}},
						})
						h.Abort(status.Error(codes.PermissionDenied, "onrequest-abort-raw"))
					}
				},
			})

			code, body, err := sendRawGRPCH2CFrames(
				ctx,
				srv.Addr(),
				fake.MethodStreamingPull,
				"",
				rawGRPCFrame{payload: frame0Wire},
				rawGRPCFrame{payload: triggerF1Wire},
				rawGRPCFrame{payload: frameInvalidUTF8},
			)
			if err != nil {
				t.Fatalf("raw h2c abort iter %d sendRawGRPCH2CFrames: %v", iter, err)
			}
			if code != codes.PermissionDenied || len(body) != 0 {
				t.Fatalf("raw h2c abort iter %d expected wire codes.PermissionDenied and empty body, got code=%v bodyLen=%d", iter, code, len(body))
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("raw h2c abort iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 2 || snap.Streams[0].CloseCode != codes.PermissionDenied || len(snap.Streams[0].SentMessages) != 0 {
				t.Fatalf("raw h2c abort iter %d unexpected capture: calls=%+v streams=%+v", iter, calls, snap.Streams)
			}
		}
	})

	t.Run("valid_pipelined_multiframe_burst_no_deadlock", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithStrictStreamingPullValidation(true))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		validF0 := &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
			ClientId:                 "valid-burst-client",
			ProtocolVersion:          1,
		}
		validF1Ack := &pubsubpb.StreamingPullRequest{
			AckIds: []string{"ack-burst-1", "ack-burst-2"},
		}
		validF2ModAck := &pubsubpb.StreamingPullRequest{
			ModifyDeadlineAckIds:  []string{"mod-burst-1"},
			ModifyDeadlineSeconds: []int32{60},
		}
		validF3Keepalive := &pubsubpb.StreamingPullRequest{}

		for iter := range 5 {
			srv.Reset()
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("pure grpc iter %d open: %v", iter, err)
			}
			if err := sp.Send(validF0); err != nil {
				t.Fatalf("pure grpc iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(validF1Ack); err != nil {
				t.Fatalf("pure grpc iter %d send frame 1: %v", iter, err)
			}
			if err := sp.Send(validF2ModAck); err != nil {
				t.Fatalf("pure grpc iter %d send frame 2: %v", iter, err)
			}
			if err := sp.Send(validF3Keepalive); err != nil {
				t.Fatalf("pure grpc iter %d send frame 3: %v", iter, err)
			}
			if err := sp.CloseSend(); err != nil {
				t.Fatalf("pure grpc iter %d CloseSend: %v", iter, err)
			}

			var resps []*pubsubpb.StreamingPullResponse
			var recvErr error
			for {
				r, err := sp.Recv()
				if err != nil {
					recvErr = err
					break
				}
				resps = append(resps, r)
			}
			if !errors.Is(recvErr, io.EOF) {
				t.Fatalf("pure grpc iter %d expected io.EOF after CloseSend, got %v", iter, recvErr)
			}
			if len(resps) != 1 || len(resps[0].GetReceivedMessages()) != 0 {
				t.Fatalf("pure grpc iter %d expected 1 empty keepalive pong, got %+v", iter, resps)
			}

			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("pure grpc iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			calls := snap.StreamingPullCalls()
			if len(calls) != 4 {
				t.Fatalf("pure grpc iter %d expected 4 captured frames, got %d: %+v", iter, len(calls), calls)
			}
			for i, c := range calls {
				if c.StreamSeq != int64(i) || c.StatusCode != codes.OK || c.CodecError != "" {
					t.Fatalf("pure grpc iter %d unexpected frame %d capture: %+v", iter, i, c)
				}
			}
			sum := snap.Streams[0]
			if sum.FrameCount != 4 || sum.KeepalivePingCount != 1 || sum.ModAckCount != 1 || sum.CloseCode != codes.OK {
				t.Fatalf("pure grpc iter %d unexpected StreamSummary: %+v", iter, sum)
			}
			if diff := cmp.Diff([]string{"ack-burst-1", "ack-burst-2"}, sum.AckIDsSent); diff != "" {
				t.Fatalf("pure grpc iter %d AckIDsSent diff (-want +got):\n%s", iter, diff)
			}
		}

		// Raw h2c single-DATA-body valid 3-frame burst (Frame 0 + Ack Frame 1 + ModAck Frame 2 + END_STREAM).
		f0RawWire, err := proto.Marshal(validF0)
		if err != nil {
			t.Fatalf("proto.Marshal validF0: %v", err)
		}
		f1RawWire, err := proto.Marshal(validF1Ack)
		if err != nil {
			t.Fatalf("proto.Marshal validF1Ack: %v", err)
		}
		f2RawWire, err := proto.Marshal(validF2ModAck)
		if err != nil {
			t.Fatalf("proto.Marshal validF2ModAck: %v", err)
		}
		srv.Reset()
		code, body, err := sendRawGRPCH2CFrames(
			ctx,
			srv.Addr(),
			fake.MethodStreamingPull,
			"",
			rawGRPCFrame{payload: f0RawWire},
			rawGRPCFrame{payload: f1RawWire},
			rawGRPCFrame{payload: f2RawWire},
		)
		if err != nil {
			t.Fatalf("raw h2c valid burst sendRawGRPCH2CFrames: %v", err)
		}
		if code != codes.OK || len(body) != 0 {
			t.Fatalf("raw h2c valid burst expected codes.OK with empty body, got code=%v bodyLen=%d", code, len(body))
		}
		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.Streams) == 1 && snap.Streams[0].Closed
		}); err != nil {
			t.Fatalf("raw h2c valid burst WaitFor stream closed: %v", err)
		}
		snapRaw := srv.Snapshot()
		callsRaw := snapRaw.StreamingPullCalls()
		if len(callsRaw) != 3 || snapRaw.Streams[0].FrameCount != 3 || snapRaw.Streams[0].ModAckCount != 1 || snapRaw.Streams[0].CloseCode != codes.OK {
			t.Fatalf("raw h2c valid burst unexpected capture: calls=%+v streams=%+v", callsRaw, snapRaw.Streams)
		}
	})

	t.Run("stalled_initial_responses_client_cancel_no_deadlock", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		sub := pubsubpb.NewSubscriberClient(conn)

		chunk := bytes.Repeat([]byte("i"), 32<<10)
		initResps := make([]*pubsubpb.StreamingPullResponse, 32)
		for i := range initResps {
			initResps[i] = &pubsubpb.StreamingPullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{AckId: fmt.Sprintf("stalled-init-%d", i), Message: &pubsubpb.PubsubMessage{Data: chunk}},
				},
			}
		}
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times:            1,
			InitialResponses: initResps,
		})

		spCtx, spCancel := context.WithCancel(ctx)
		defer spCancel()
		sp, err := sub.StreamingPull(spCtx)
		if err != nil {
			t.Fatalf("StreamingPull open: %v", err)
		}
		if err := sp.Send(frame0Valid); err != nil {
			t.Fatalf("send frame 0: %v", err)
		}
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}
		// Read only the first response so InitialResponses fills the 64 KiB
		// window and stalls inside drainOutbound before requestNextRecv() runs.
		first, err := sp.Recv()
		if err != nil || len(first.GetReceivedMessages()) != 1 || first.GetReceivedMessages()[0].GetAckId() != "stalled-init-0" {
			t.Fatalf("expected first InitialResponse, got %+v err=%v", first, err)
		}

		spCancel()
		waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
		defer waitCancel()
		if err := h.WaitClosed(waitCtx); err != nil {
			t.Fatalf("WaitClosed timed out after client cancel during stalled InitialResponses: %v", err)
		}
		if sum := h.Summary(); sum.CloseCode != codes.Canceled {
			t.Fatalf("expected StreamSummary.CloseCode=Canceled, got %s (%q)", sum.CloseCode, sum.CloseErr)
		}
	})
}

func TestStreamingPullCloseSendCancelDuringDrainOutbound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	frame0Valid := &pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}
	frame0Wire, err := proto.Marshal(frame0Valid)
	if err != nil {
		t.Fatalf("proto.Marshal frame0Valid: %v", err)
	}

	makeLargeResponses := func(prefix string, count int) []*pubsubpb.StreamingPullResponse {
		chunk := bytes.Repeat([]byte("d"), 32<<10)
		resps := make([]*pubsubpb.StreamingPullResponse, count)
		for i := range resps {
			resps[i] = &pubsubpb.StreamingPullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{
						AckId:   fmt.Sprintf("%s-%d", prefix, i),
						Message: &pubsubpb.PubsubMessage{Data: chunk},
					},
				},
			}
		}
		return resps
	}

	// 1. Stalled 64 KiB InitialResponses + AfterInitialError(Aborted) + CloseSend() + client spCancel() (20 iters).
	t.Run("stalled_initial_responses_after_initial_error_closesend_and_cancel", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		sub := pubsubpb.NewSubscriberClient(conn)
		initResps := makeLargeResponses("stalled-init", 32)

		for iter := range 20 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				InitialResponses:  initResps,
				AfterInitialError: status.Error(codes.Aborted, "after-init-stalled"),
			})

			spCtx, spCancel := context.WithCancel(ctx)
			sp, err := sub.StreamingPull(spCtx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				spCancel()
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("iter %d CloseSend: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}
			first, err := sp.Recv()
			if err != nil || len(first.GetReceivedMessages()) != 1 || first.GetReceivedMessages()[0].GetAckId() != "stalled-init-0" {
				spCancel()
				t.Fatalf("iter %d expected stalled-init-0, got %+v err=%v", iter, first, err)
			}

			spCancel()
			waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
			err = h.WaitClosed(waitCtx)
			waitCancel()
			if err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			if sum := h.Summary(); sum.CloseCode != codes.Canceled {
				t.Fatalf("iter %d expected CloseCode=Canceled (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}
	})

	// 2. Non-stalled InitialResponses + AfterInitialError(Aborted) + CloseSend() + immediate spCancel() (20 iters).
	t.Run("nonstalled_initial_responses_after_initial_error_closesend_and_immediate_cancel", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		initResps := make([]*pubsubpb.StreamingPullResponse, 20)
		for i := range initResps {
			initResps[i] = &pubsubpb.StreamingPullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{
						AckId:   fmt.Sprintf("fast-init-%d", i),
						Message: &pubsubpb.PubsubMessage{Data: []byte("fast-payload")},
					},
				},
			}
		}

		for iter := range 20 {
			srv.Reset()
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:             1,
				InitialResponses:  initResps,
				AfterInitialError: status.Error(codes.Aborted, "after-init-nonstalled"),
			})

			spCtx, spCancel := context.WithCancel(ctx)
			sp, err := sub.StreamingPull(spCtx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				spCancel()
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("iter %d CloseSend: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}

			spCancel()
			waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
			err = h.WaitClosed(waitCtx)
			waitCancel()
			if err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			if sum := h.Summary(); sum.CloseCode != codes.Aborted && sum.CloseCode != codes.Canceled {
				t.Fatalf("iter %d expected CloseCode in {Aborted, Canceled} (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}
	})

	// 3. Stalled 64 KiB InitialResponses + pipelined valid Frame 1 (OnRequest) + CloseSend() + spCancel() (20 iters, !recvInFlight).
	t.Run("stalled_initial_responses_pipelined_valid_frame1_closesend_and_cancel", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		sub := pubsubpb.NewSubscriberClient(conn)
		initResps := makeLargeResponses("stalled-pipelined", 32)
		validFrame1 := &pubsubpb.StreamingPullRequest{
			AckIds: []string{"pipelined-ack-1"},
		}

		for iter := range 20 {
			srv.Reset()
			var onReqCount atomic.Int32
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:            1,
				InitialResponses: initResps,
				OnRequest: func(*fake.StreamHandle, *pubsubpb.StreamingPullRequest) {
					onReqCount.Add(1)
				},
			})

			spCtx, spCancel := context.WithCancel(ctx)
			sp, err := sub.StreamingPull(spCtx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				spCancel()
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if err := sp.Send(validFrame1); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("iter %d send frame 1: %v", iter, err)
			}
			if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("iter %d CloseSend: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}
			first, err := sp.Recv()
			if err != nil || len(first.GetReceivedMessages()) != 1 || first.GetReceivedMessages()[0].GetAckId() != "stalled-pipelined-0" {
				spCancel()
				t.Fatalf("iter %d expected stalled-pipelined-0, got %+v err=%v", iter, first, err)
			}

			spCancel()
			waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
			err = h.WaitClosed(waitCtx)
			waitCancel()
			if err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			if got := onReqCount.Load(); got != 0 {
				t.Fatalf("iter %d OnRequest invoked %d times post-cancel (expected 0)", iter, got)
			}
			if calls := srv.Snapshot().StreamingPullCalls(); len(calls) != 1 || calls[0].StreamSeq != 0 {
				t.Fatalf("iter %d expected only frame 0 captured, got %+v", iter, calls)
			}
			if sum := h.Summary(); sum.CloseCode != codes.Canceled {
				t.Fatalf("iter %d expected CloseCode=Canceled (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}
	})

	// 4. Idle after Frame 0 (recvInFlight == true) + CloseSend() + stalled h.PushMessages + h.Close(Aborted) + spCancel() (20 iters).
	t.Run("idle_after_frame0_closesend_pushed_stall_and_cancel", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		sub := pubsubpb.NewSubscriberClient(conn)
		chunk := bytes.Repeat([]byte("p"), 32<<10)

		for iter := range 20 {
			srv.Reset()
			spCtx, spCancel := context.WithCancel(ctx)
			sp, err := sub.StreamingPull(spCtx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				spCancel()
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				spCancel()
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}

			// Complete one keepalive ping/pong so the server finishes draining
			// and blocks inside stream.Recv() with recvInFlight == true.
			if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
				spCancel()
				t.Fatalf("iter %d send ping: %v", iter, err)
			}
			if pong, err := sp.Recv(); err != nil || len(pong.GetReceivedMessages()) != 0 {
				spCancel()
				t.Fatalf("iter %d expected empty keepalive pong, got %+v err=%v", iter, pong, err)
			}

			for i := range 32 {
				if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
					{
						AckId:   fmt.Sprintf("pushed-stall-%d", i),
						Message: &pubsubpb.PubsubMessage{Data: chunk},
					},
				}); err != nil {
					spCancel()
					t.Fatalf("iter %d PushMessages(%d): %v", iter, i, err)
				}
			}
			if err := h.Close(status.Error(codes.Aborted, "pushed-close")); err != nil {
				spCancel()
				t.Fatalf("iter %d h.Close: %v", iter, err)
			}

			first, err := sp.Recv()
			if err != nil || len(first.GetReceivedMessages()) != 1 || first.GetReceivedMessages()[0].GetAckId() != "pushed-stall-0" {
				spCancel()
				t.Fatalf("iter %d expected pushed-stall-0, got %+v err=%v", iter, first, err)
			}
			if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("iter %d CloseSend: %v", iter, err)
			}

			spCancel()
			waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
			err = h.WaitClosed(waitCtx)
			waitCancel()
			if err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			if sum := h.Summary(); sum.CloseCode != codes.Aborted && sum.CloseCode != codes.Canceled {
				t.Fatalf("iter %d expected CloseCode in {Aborted, Canceled} (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}

		// Also cover non-stalled h.PushMessages + CloseSend() + spCancel() with recvInFlight == true.
		connFast := dialFake(t, srv)
		subFast := pubsubpb.NewSubscriberClient(connFast)
		smallPayload := bytes.Repeat([]byte("f"), 64)
		for iter := range 20 {
			srv.Reset()
			spCtx, spCancel := context.WithCancel(ctx)
			sp, err := subFast.StreamingPull(spCtx)
			if err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d send frame 0: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d WaitForStreamingPull: %v", iter, err)
			}
			if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d send ping: %v", iter, err)
			}
			if pong, err := sp.Recv(); err != nil || len(pong.GetReceivedMessages()) != 0 {
				spCancel()
				t.Fatalf("nonstalled iter %d expected empty keepalive pong, got %+v err=%v", iter, pong, err)
			}

			for i := range 400 {
				if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
					{
						AckId:   fmt.Sprintf("pushed-fast-%d", i),
						Message: &pubsubpb.PubsubMessage{Data: smallPayload},
					},
				}); err != nil {
					spCancel()
					t.Fatalf("nonstalled iter %d PushMessages(%d): %v", iter, i, err)
				}
			}
			if err := h.Close(status.Error(codes.Aborted, "pushed-fast-close")); err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d h.Close: %v", iter, err)
			}

			if _, err := sp.Recv(); err != nil {
				spCancel()
				t.Fatalf("nonstalled iter %d Recv 0: %v", iter, err)
			}
			if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
				spCancel()
				t.Fatalf("nonstalled iter %d CloseSend: %v", iter, err)
			}
			// Receive 2 more pushed messages so END_STREAM reaches recvCh before cancel.
			for range 2 {
				_, _ = sp.Recv()
			}

			spCancel()
			waitCtx, waitCancel := context.WithTimeout(ctx, 2*time.Second)
			err = h.WaitClosed(waitCtx)
			waitCancel()
			if err != nil {
				t.Fatalf("nonstalled iter %d WaitClosed: %v", iter, err)
			}
			if sum := h.Summary(); sum.CloseCode != codes.Aborted && sum.CloseCode != codes.Canceled {
				t.Fatalf("nonstalled iter %d expected CloseCode in {Aborted, Canceled} (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}
	})

	// 5. Raw h2c Grpc-Timeout expiration during OnOpen before drainOutbound with END_STREAM (CloseSend(), 10 iters).
	t.Run("raw_h2c_grpc_timeout_during_on_open_with_end_stream", func(t *testing.T) {
		srv := startFakeServer(t)
		p := new(http.Protocols)
		p.SetUnencryptedHTTP2(true)
		tr := &http.Transport{Protocols: p}
		t.Cleanup(tr.CloseIdleConnections)
		httpClient := &http.Client{Transport: tr}

		// Warm up httpClient's HTTP/2 connection before timing Grpc-Timeout iterations.
		warmWire, err := proto.Marshal(&pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/warm",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("warm")}},
		})
		if err != nil {
			t.Fatalf("proto.Marshal warmup: %v", err)
		}
		warmBody := make([]byte, 5+len(warmWire))
		binary.BigEndian.PutUint32(warmBody[1:5], uint32(len(warmWire)))
		copy(warmBody[5:], warmWire)
		warmReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+srv.Addr()+fake.MethodPublish, bytes.NewReader(warmBody))
		if err != nil {
			t.Fatalf("warmup NewRequestWithContext: %v", err)
		}
		warmReq.Header.Set("Content-Type", "application/grpc")
		warmReq.Header.Set("TE", "trailers")
		warmResp, err := httpClient.Do(warmReq)
		if err != nil {
			t.Fatalf("warmup httpClient.Do: %v", err)
		}
		_, _ = io.ReadAll(warmResp.Body)
		_ = warmResp.Body.Close()

		frame0Body := make([]byte, 5+len(frame0Wire))
		binary.BigEndian.PutUint32(frame0Body[1:5], uint32(len(frame0Wire)))
		copy(frame0Body[5:], frame0Wire)

		for iter := range 10 {
			srv.Reset()
			enteredOnOpen := make(chan struct{})
			releaseOnOpen := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() { close(releaseOnOpen) })
			}

			func() {
				defer release()
				srv.AddStreamingPullScript(fake.StreamingPullScript{
					Times: 1,
					InitialResponses: []*pubsubpb.StreamingPullResponse{
						{ReceivedMessages: []*pubsubpb.ReceivedMessage{{AckId: "never-sent-after-timeout"}}},
					},
					AfterInitialError: status.Error(codes.Aborted, "never-reached"),
					OnOpen: func(*fake.StreamHandle) {
						close(enteredOnOpen)
						select {
						case <-releaseOnOpen:
						case <-ctx.Done():
						}
					},
				})

				req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+srv.Addr()+fake.MethodStreamingPull, bytes.NewReader(frame0Body))
				if err != nil {
					t.Fatalf("iter %d NewRequestWithContext: %v", iter, err)
				}
				req.Header.Set("Content-Type", "application/grpc")
				req.Header.Set("TE", "trailers")
				req.Header.Set("Grpc-Timeout", "35m")

				resp, err := httpClient.Do(req)
				if err == nil {
					_, _ = io.ReadAll(resp.Body)
					_ = resp.Body.Close()
				}
				release()

				if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
					return len(snap.Streams) == 1 && snap.Streams[0].Closed
				}); err != nil {
					t.Fatalf("iter %d WaitFor stream closed: %v", iter, err)
				}
				select {
				case <-enteredOnOpen:
				default:
					t.Fatalf("iter %d Grpc-Timeout expired before OnOpen entered", iter)
				}
				snap := srv.Snapshot()
				sum := snap.Streams[0]
				if sum.CloseCode != codes.DeadlineExceeded && sum.CloseCode != codes.Canceled {
					t.Fatalf("iter %d expected CloseCode in {DeadlineExceeded, Canceled} (never OK), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
				}
				if len(sum.SentMessages) != 0 {
					t.Fatalf("iter %d expected 0 SentMessages after OnOpen timeout, got %+v", iter, sum.SentMessages)
				}
			}()
		}
	})
}

func TestStreamingPullSendErrorsAndWaitForSentMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	frame0Valid := &pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}

	// 1. WithMaxSendMsgSize (codes.ResourceExhausted) via InitialResponses (with and without CloseSend).
	t.Run("max_send_msg_size_initial_responses_with_and_without_closesend", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxSendMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		oversizeResp := &pubsubpb.StreamingPullResponse{
			ReceivedMessages: []*pubsubpb.ReceivedMessage{
				{
					AckId:   "oversize-init-ack",
					Message: &pubsubpb.PubsubMessage{Data: bytes.Repeat([]byte("m"), 1024)},
				},
			},
		}

		for _, withCloseSend := range []bool{false, true} {
			name := "without_closesend"
			if withCloseSend {
				name = "with_closesend"
			}
			t.Run(name, func(t *testing.T) {
				for iter := range 5 {
					srv.Reset()
					onCloseCh := make(chan error, 1)
					srv.AddStreamingPullScript(fake.StreamingPullScript{
						Times:            1,
						InitialResponses: []*pubsubpb.StreamingPullResponse{oversizeResp},
						OnClose: func(_ *fake.StreamHandle, err error) {
							onCloseCh <- err
						},
					})

					sp, err := sub.StreamingPull(ctx)
					if err != nil {
						t.Fatalf("iter %d open: %v", iter, err)
					}
					if err := sp.Send(frame0Valid); err != nil {
						t.Fatalf("iter %d send frame 0: %v", iter, err)
					}
					if withCloseSend {
						if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
							t.Fatalf("iter %d CloseSend: %v", iter, err)
						}
					}
					_, recvErr := sp.Recv()
					if status.Code(recvErr) != codes.ResourceExhausted {
						t.Fatalf("iter %d expected wire ResourceExhausted, got %v", iter, recvErr)
					}

					var onCloseErr error
					select {
					case onCloseErr = <-onCloseCh:
					case <-ctx.Done():
						t.Fatalf("iter %d timed out waiting for OnClose: %v", iter, ctx.Err())
					}
					if status.Code(onCloseErr) != codes.ResourceExhausted {
						t.Fatalf("iter %d expected OnClose ResourceExhausted, got %v", iter, onCloseErr)
					}
					if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
						return len(snap.Streams) == 1 && snap.Streams[0].Closed
					}); err != nil {
						t.Fatalf("iter %d WaitFor stream closed: %v", iter, err)
					}
					snap := srv.Snapshot()
					sum := snap.Streams[0]
					if sum.CloseCode != codes.ResourceExhausted {
						t.Fatalf("iter %d expected StreamSummary.CloseCode=ResourceExhausted (never Canceled), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
					}
					if len(sum.SentMessages) != 0 {
						t.Fatalf("iter %d expected 0 SentMessages on failed send, got %d", iter, len(sum.SentMessages))
					}
					if calls := snap.StreamingPullCalls(); len(calls) != 1 || calls[0].CodecError != "" {
						t.Fatalf("iter %d expected only Frame 0 without CodecError, got %+v", iter, calls)
					}
				}
			})
		}
	})

	// 2. WithMaxSendMsgSize (codes.ResourceExhausted) via h.PushMessages with recvInFlight == true.
	t.Run("max_send_msg_size_push_messages_recv_in_flight", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxSendMsgSize(256))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		for iter := range 5 {
			srv.Reset()
			onCloseCh := make(chan error, 1)
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnClose: func(_ *fake.StreamHandle, err error) {
					onCloseCh <- err
				},
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}

			// Complete one keepalive ping/pong so requestNextRecv() is blocked in stream.Recv().
			if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
				t.Fatalf("iter %d send ping: %v", iter, err)
			}
			if pong, err := sp.Recv(); err != nil || len(pong.GetReceivedMessages()) != 0 {
				t.Fatalf("iter %d expected empty keepalive pong, got %+v err=%v", iter, pong, err)
			}

			if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
				{
					AckId:   "oversize-push-ack",
					Message: &pubsubpb.PubsubMessage{Data: bytes.Repeat([]byte("p"), 1024)},
				},
			}); err != nil {
				t.Fatalf("iter %d PushMessages: %v", iter, err)
			}

			_, recvErr := sp.Recv()
			if status.Code(recvErr) != codes.ResourceExhausted {
				t.Fatalf("iter %d expected wire ResourceExhausted, got %v", iter, recvErr)
			}
			if err := h.WaitClosed(ctx); err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			var onCloseErr error
			select {
			case onCloseErr = <-onCloseCh:
			case <-ctx.Done():
				t.Fatalf("iter %d timed out waiting for OnClose: %v", iter, ctx.Err())
			}
			if status.Code(onCloseErr) != codes.ResourceExhausted {
				t.Fatalf("iter %d expected OnClose ResourceExhausted, got %v", iter, onCloseErr)
			}
			sum := h.Summary()
			if sum.CloseCode != codes.ResourceExhausted {
				t.Fatalf("iter %d expected StreamSummary.CloseCode=ResourceExhausted (never Canceled), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
			if len(sum.SentMessages) != 0 {
				t.Fatalf("iter %d expected 0 SentMessages on failed push, got %d", iter, len(sum.SentMessages))
			}
			if calls := srv.Snapshot().StreamingPullCalls(); len(calls) != 2 || calls[0].CodecError != "" || calls[1].CodecError != "" {
				t.Fatalf("iter %d expected only Frame 0 and ping without CodecError, got %+v", iter, calls)
			}
		}
	})

	// 3. Invalid UTF-8 in InitialResponses (proto.Marshal -> codes.Internal).
	t.Run("invalid_utf8_initial_responses_marshal_error", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		invalidUTF8Resp := &pubsubpb.StreamingPullResponse{
			ReceivedMessages: []*pubsubpb.ReceivedMessage{
				{
					AckId:   string([]byte{0xff, 0xfe, 0xfd}),
					Message: &pubsubpb.PubsubMessage{Data: []byte("bad-utf8-ack-id")},
				},
			},
		}

		for iter := range 6 {
			withCloseSend := iter%2 == 1
			srv.Reset()
			onCloseCh := make(chan error, 1)
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times:            1,
				InitialResponses: []*pubsubpb.StreamingPullResponse{invalidUTF8Resp},
				OnClose: func(_ *fake.StreamHandle, err error) {
					onCloseCh <- err
				},
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			if withCloseSend {
				if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("iter %d CloseSend: %v", iter, err)
				}
			}

			_, recvErr := sp.Recv()
			if status.Code(recvErr) != codes.Internal {
				t.Fatalf("iter %d (withCloseSend=%v) expected wire Internal, got %v", iter, withCloseSend, recvErr)
			}
			var onCloseErr error
			select {
			case onCloseErr = <-onCloseCh:
			case <-ctx.Done():
				t.Fatalf("iter %d timed out waiting for OnClose: %v", iter, ctx.Err())
			}
			if status.Code(onCloseErr) != codes.Internal {
				t.Fatalf("iter %d expected OnClose Internal, got %v", iter, onCloseErr)
			}
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && snap.Streams[0].Closed
			}); err != nil {
				t.Fatalf("iter %d WaitFor stream closed: %v", iter, err)
			}
			snap := srv.Snapshot()
			sum := snap.Streams[0]
			if sum.CloseCode != codes.Internal {
				t.Fatalf("iter %d expected StreamSummary.CloseCode=Internal (never Canceled), got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
			if sum.CloseErr == "" || len(sum.SentMessages) != 0 {
				t.Fatalf("iter %d unexpected StreamSummary state: %+v", iter, sum)
			}
		}
	})

	// 4. Concurrent stalled stream.Send + oversize Frame 1 RecvMsg CodecError (ResourceExhausted) preserved.
	t.Run("concurrent_stalled_send_and_oversize_frame1_codec_error", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
		conn := dialFake(t, srv,
			grpc.WithInitialWindowSize(64<<10),
			grpc.WithInitialConnWindowSize(64<<10),
		)
		sub := pubsubpb.NewSubscriberClient(conn)
		chunk := bytes.Repeat([]byte("s"), 20<<10)

		for iter := range 5 {
			srv.Reset()
			onCloseCh := make(chan error, 1)
			srv.AddStreamingPullScript(fake.StreamingPullScript{
				Times: 1,
				OnClose: func(_ *fake.StreamHandle, err error) {
					onCloseCh <- err
				},
			})

			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open: %v", iter, err)
			}
			if err := sp.Send(frame0Valid); err != nil {
				t.Fatalf("iter %d send frame 0: %v", iter, err)
			}
			h, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}

			// Enter stream.Recv() before queueing outbound messages.
			if err := sp.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
				t.Fatalf("iter %d send ping: %v", iter, err)
			}
			if pong, err := sp.Recv(); err != nil || len(pong.GetReceivedMessages()) != 0 {
				t.Fatalf("iter %d expected empty keepalive pong, got %+v err=%v", iter, pong, err)
			}

			for i := range 32 {
				if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
					{
						AckId:   fmt.Sprintf("stall-codec-%d", i),
						Message: &pubsubpb.PubsubMessage{Data: chunk},
					},
				}); err != nil {
					t.Fatalf("iter %d PushMessages(%d): %v", iter, i, err)
				}
			}

			// Under a 64 KiB window and 20 KiB messages, writeQuota.get lets 4 messages
			// through before Message 4 stalls inside stream.Send.
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1 && len(snap.Streams[0].SentMessages) >= 4
			}); err != nil {
				t.Fatalf("iter %d WaitFor initial SentMessages>=4: %v", iter, err)
			}

			// Trigger oversize Frame 1 CodecError while stream.Send is stalled in drainOutbound.
			_ = sp.Send(&pubsubpb.StreamingPullRequest{
				AckIds: []string{strings.Repeat("x", 512)},
			})

			var recvErr error
			for range 64 {
				_, recvErr = sp.Recv()
				if recvErr != nil {
					break
				}
			}
			if status.Code(recvErr) != codes.ResourceExhausted {
				t.Fatalf("iter %d expected wire ResourceExhausted, got %v", iter, recvErr)
			}
			if err := h.WaitClosed(ctx); err != nil {
				t.Fatalf("iter %d WaitClosed: %v", iter, err)
			}
			var onCloseErr error
			select {
			case onCloseErr = <-onCloseCh:
			case <-ctx.Done():
				t.Fatalf("iter %d timed out waiting for OnClose: %v", iter, ctx.Err())
			}
			if status.Code(onCloseErr) != codes.ResourceExhausted {
				t.Fatalf("iter %d expected OnClose ResourceExhausted, got %v", iter, onCloseErr)
			}
			sum := h.Summary()
			if sum.CloseCode != codes.ResourceExhausted {
				t.Fatalf("iter %d expected StreamSummary.CloseCode=ResourceExhausted, got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
			calls := srv.Snapshot().StreamingPullCalls()
			if len(calls) != 3 || calls[2].StatusCode != codes.ResourceExhausted || calls[2].CodecError == "" {
				t.Fatalf("iter %d expected Frame 2 CodecError=ResourceExhausted captured, got %+v", iter, calls)
			}
		}
	})

	// 5. Server.WaitFor wakes promptly on SentMessages updates after markReady() (h.PushMessages & InitialResponses).
	t.Run("wait_for_sent_messages_after_mark_ready", func(t *testing.T) {
		t.Run("push_messages", func(t *testing.T) {
			srv := startFakeServer(t)
			conn := dialFake(t, srv)
			sub := pubsubpb.NewSubscriberClient(conn)

			for iter := range 5 {
				srv.Reset()
				sp, err := sub.StreamingPull(ctx)
				if err != nil {
					t.Fatalf("iter %d open: %v", iter, err)
				}
				if err := sp.Send(frame0Valid); err != nil {
					t.Fatalf("iter %d send frame 0: %v", iter, err)
				}
				h, err := srv.WaitForStreamingPull(ctx)
				if err != nil {
					t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
				}

				waitCtx, waitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				var enteredPred atomic.Bool
				enteredPredCh := make(chan struct{})
				waitErrCh := make(chan error, 1)
				go func() {
					waitErrCh <- srv.WaitFor(waitCtx, func(snap fake.Snapshot) bool {
						ok := len(snap.Streams) == 1 && len(snap.Streams[0].SentMessages) >= 2
						if !ok && !enteredPred.Swap(true) {
							close(enteredPredCh)
						}
						return ok
					})
				}()

				select {
				case <-enteredPredCh:
				case <-ctx.Done():
					waitCancel()
					t.Fatalf("iter %d timed out waiting for WaitFor predicate entry: %v", iter, ctx.Err())
				}

				for _, ackID := range []string{"wake-push-1", "wake-push-2"} {
					if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
						{AckId: ackID, Message: &pubsubpb.PubsubMessage{Data: []byte(ackID)}},
					}); err != nil {
						waitCancel()
						t.Fatalf("iter %d PushMessages(%s): %v", iter, ackID, err)
					}
				}

				var waitErr error
				select {
				case waitErr = <-waitErrCh:
				case <-ctx.Done():
					waitCancel()
					t.Fatalf("iter %d timed out awaiting WaitFor result: %v", iter, ctx.Err())
				}
				waitCancel()
				if waitErr != nil {
					t.Fatalf("iter %d WaitFor(SentMessages>=2) failed to wake after PushMessages: %v", iter, waitErr)
				}

				for range 2 {
					if _, err := sp.Recv(); err != nil {
						t.Fatalf("iter %d sp.Recv: %v", iter, err)
					}
				}
				if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("iter %d CloseSend: %v", iter, err)
				}
				if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("iter %d expected io.EOF after CloseSend, got %v", iter, err)
				}
			}
		})

		t.Run("initial_responses", func(t *testing.T) {
			srv := startFakeServer(t)
			conn := dialFake(t, srv,
				grpc.WithInitialWindowSize(64<<10),
				grpc.WithInitialConnWindowSize(64<<10),
			)
			sub := pubsubpb.NewSubscriberClient(conn)

			chunk := bytes.Repeat([]byte("w"), 32<<10)
			initResps := make([]*pubsubpb.StreamingPullResponse, 32)
			for i := range initResps {
				initResps[i] = &pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{
							AckId:   fmt.Sprintf("wake-init-%d", i),
							Message: &pubsubpb.PubsubMessage{Data: chunk},
						},
					},
				}
			}

			for iter := range 5 {
				srv.Reset()
				srv.AddStreamingPullScript(fake.StreamingPullScript{
					Times:            1,
					InitialResponses: initResps,
				})

				sp, err := sub.StreamingPull(ctx)
				if err != nil {
					t.Fatalf("iter %d open: %v", iter, err)
				}
				if err := sp.Send(frame0Valid); err != nil {
					t.Fatalf("iter %d send frame 0: %v", iter, err)
				}
				if _, err := srv.WaitForStreamingPull(ctx); err != nil {
					t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
				}

				waitCtx, waitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				var enteredPred atomic.Bool
				enteredPredCh := make(chan struct{})
				waitErrCh := make(chan error, 1)
				go func() {
					waitErrCh <- srv.WaitFor(waitCtx, func(snap fake.Snapshot) bool {
						ok := len(snap.Streams) == 1 && len(snap.Streams[0].SentMessages) >= 10
						if !ok && !enteredPred.Swap(true) {
							close(enteredPredCh)
						}
						return ok
					})
				}()

				select {
				case <-enteredPredCh:
				case <-ctx.Done():
					waitCancel()
					t.Fatalf("iter %d timed out waiting for WaitFor predicate entry: %v", iter, ctx.Err())
				}

				for i := range 12 {
					if _, err := sp.Recv(); err != nil {
						waitCancel()
						t.Fatalf("iter %d sp.Recv(%d): %v", iter, i, err)
					}
				}

				var waitErr error
				select {
				case waitErr = <-waitErrCh:
				case <-ctx.Done():
					waitCancel()
					t.Fatalf("iter %d timed out awaiting WaitFor result: %v", iter, ctx.Err())
				}
				waitCancel()
				if waitErr != nil {
					t.Fatalf("iter %d WaitFor(SentMessages>=10) failed to wake as InitialResponses drained: %v", iter, waitErr)
				}

				if err := sp.CloseSend(); err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("iter %d CloseSend: %v", iter, err)
				}
				for i := 12; i < len(initResps); i++ {
					if _, err := sp.Recv(); err != nil {
						t.Fatalf("iter %d drain sp.Recv(%d): %v", iter, i, err)
					}
				}
				if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("iter %d expected io.EOF after draining InitialResponses, got %v", iter, err)
				}
			}
		})
	})
}

func TestStreamingPullWaitForOutOfOrderReadyAndMultiStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("out_of_order_ready_returns_distinct_handles", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		if _, err := srv.WaitForStreamingPullN(ctx, 0); err == nil {
			t.Fatalf("expected error for WaitForStreamingPullN(0), got nil")
		}
		if _, err := srv.WaitForStreamingPullN(ctx, -1); err == nil {
			t.Fatalf("expected error for WaitForStreamingPullN(-1), got nil")
		}

		for iter := range 10 {
			srv.Reset()

			sp1, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open sp1: %v", iter, err)
			}
			// Wait until stream-1 is registered in s.streamOrder before opening sp2.
			if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
				return len(snap.Streams) == 1
			}); err != nil {
				t.Fatalf("iter %d WaitFor stream-1 entry: %v", iter, err)
			}

			sp2, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open sp2: %v", iter, err)
			}
			// Deliver Frame 0 on sp2 first so stream-2 becomes ready before stream-1.
			if err := sp2.Send(&pubsubpb.StreamingPullRequest{
				Subscription:             "projects/p/subscriptions/sub-2",
				StreamAckDeadlineSeconds: 30,
				ClientId:                 "cid-2",
			}); err != nil {
				t.Fatalf("iter %d sp2 send frame 0: %v", iter, err)
			}

			h1, err := srv.WaitForStreamingPullN(ctx, 1)
			if err != nil {
				t.Fatalf("iter %d WaitForStreamingPullN(1): %v", iter, err)
			}
			if h1.ID() != "stream-2" {
				t.Fatalf("iter %d expected WaitForStreamingPullN(1)=stream-2, got %q", iter, h1.ID())
			}
			hFirst, err := srv.WaitForStreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d WaitForStreamingPull: %v", iter, err)
			}
			if hFirst.ID() != h1.ID() {
				t.Fatalf("iter %d WaitForStreamingPull=%q != WaitForStreamingPullN(1)=%q", iter, hFirst.ID(), h1.ID())
			}

			// Now deliver Frame 0 on sp1; stream-1 becomes ready second.
			if err := sp1.Send(&pubsubpb.StreamingPullRequest{
				Subscription:             "projects/p/subscriptions/sub-1",
				StreamAckDeadlineSeconds: 30,
				ClientId:                 "cid-1",
			}); err != nil {
				t.Fatalf("iter %d sp1 send frame 0: %v", iter, err)
			}

			h2, err := srv.WaitForStreamingPullN(ctx, 2)
			if err != nil {
				t.Fatalf("iter %d WaitForStreamingPullN(2): %v", iter, err)
			}
			if h2.ID() != "stream-1" || h1.ID() == h2.ID() {
				t.Fatalf("iter %d expected WaitForStreamingPullN(2)=stream-1 distinct from h1=%q, got h2=%q", iter, h1.ID(), h2.ID())
			}

			if err := h1.PushMessages([]*pubsubpb.ReceivedMessage{
				{AckId: "ack-h1", Message: &pubsubpb.PubsubMessage{Data: []byte("msg-for-sp2")}},
			}); err != nil {
				t.Fatalf("iter %d h1.PushMessages: %v", iter, err)
			}
			if err := h2.PushMessages([]*pubsubpb.ReceivedMessage{
				{AckId: "ack-h2", Message: &pubsubpb.PubsubMessage{Data: []byte("msg-for-sp1")}},
			}); err != nil {
				t.Fatalf("iter %d h2.PushMessages: %v", iter, err)
			}
			if err := h1.Close(nil); err != nil {
				t.Fatalf("iter %d h1.Close: %v", iter, err)
			}
			if err := h2.Close(nil); err != nil {
				t.Fatalf("iter %d h2.Close: %v", iter, err)
			}

			r2, err := sp2.Recv()
			if err != nil || len(r2.GetReceivedMessages()) != 1 || string(r2.GetReceivedMessages()[0].GetMessage().GetData()) != "msg-for-sp2" {
				t.Fatalf("iter %d sp2 expected msg-for-sp2, got %+v err=%v", iter, r2, err)
			}
			if _, err := sp2.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("iter %d sp2 expected io.EOF, got %v", iter, err)
			}
			_ = sp2.CloseSend()

			r1, err := sp1.Recv()
			if err != nil || len(r1.GetReceivedMessages()) != 1 || string(r1.GetReceivedMessages()[0].GetMessage().GetData()) != "msg-for-sp1" {
				t.Fatalf("iter %d sp1 expected msg-for-sp1, got %+v err=%v", iter, r1, err)
			}
			if _, err := sp1.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("iter %d sp1 expected io.EOF, got %v", iter, err)
			}
			_ = sp1.CloseSend()

			if err := h1.WaitClosed(ctx); err != nil {
				t.Fatalf("iter %d h1.WaitClosed: %v", iter, err)
			}
			if err := h2.WaitClosed(ctx); err != nil {
				t.Fatalf("iter %d h2.WaitClosed: %v", iter, err)
			}
			if sum := h1.Summary(); sum.CloseCode != codes.OK {
				t.Fatalf("iter %d h1 expected CloseCode=OK, got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
			if sum := h2.Summary(); sum.CloseCode != codes.OK {
				t.Fatalf("iter %d h2 expected CloseCode=OK, got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}

			// Verify srv.Reset() clears readyStreams so the next epoch starts cleanly at stream-1.
			srv.Reset()
			spPost, err := sub.StreamingPull(ctx)
			if err != nil {
				t.Fatalf("iter %d open post-reset stream: %v", iter, err)
			}
			if err := spPost.Send(&pubsubpb.StreamingPullRequest{
				Subscription:             "projects/p/subscriptions/post-reset",
				StreamAckDeadlineSeconds: 30,
			}); err != nil {
				t.Fatalf("iter %d post-reset send frame 0: %v", iter, err)
			}
			hPost, err := srv.WaitForStreamingPullN(ctx, 1)
			if err != nil {
				t.Fatalf("iter %d post-reset WaitForStreamingPullN(1): %v", iter, err)
			}
			if hPost.ID() != "stream-1" || hPost.InitialRequest().GetSubscription() != "projects/p/subscriptions/post-reset" {
				t.Fatalf("iter %d expected post-reset stream-1/post-reset, got id=%q sub=%q",
					iter, hPost.ID(), hPost.InitialRequest().GetSubscription())
			}
			if err := hPost.Close(nil); err != nil {
				t.Fatalf("iter %d hPost.Close: %v", iter, err)
			}
			if _, err := spPost.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("iter %d spPost expected io.EOF, got %v", iter, err)
			}
			_ = spPost.CloseSend()
			if err := hPost.WaitClosed(ctx); err != nil {
				t.Fatalf("iter %d hPost.WaitClosed: %v", iter, err)
			}
			if sum := hPost.Summary(); sum.CloseCode != codes.OK {
				t.Fatalf("iter %d hPost expected CloseCode=OK, got %s (%q)", iter, sum.CloseCode, sum.CloseErr)
			}
		}
	})

	t.Run("concurrent_16_streams_waitfor_n_unique_delivery", func(t *testing.T) {
		srv := startFakeServer(t)
		const numStreams = 16
		const numConns = 4
		subs := make([]pubsubpb.SubscriberClient, numConns)
		for i := range numConns {
			subs[i] = pubsubpb.NewSubscriberClient(dialFake(t, srv))
		}

		for round := range 5 {
			srv.Reset()
			roundCtx, roundCancel := context.WithTimeout(ctx, 10*time.Second)

			startCh := make(chan struct{})
			clientErrs := make([]error, numStreams)
			var wg sync.WaitGroup
			for c := range numStreams {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					subName := fmt.Sprintf("projects/p/subscriptions/r%d-s%d", round, idx)
					sub := subs[idx%numConns]
					<-startCh

					sp, err := sub.StreamingPull(roundCtx)
					if err != nil {
						clientErrs[idx] = fmt.Errorf("client %d open: %w", idx, err)
						return
					}
					if err := sp.Send(&pubsubpb.StreamingPullRequest{
						Subscription:             subName,
						StreamAckDeadlineSeconds: 30,
						ClientId:                 fmt.Sprintf("cid-%d-%d", round, idx),
					}); err != nil {
						clientErrs[idx] = fmt.Errorf("client %d send frame 0: %w", idx, err)
						return
					}

					var gotMsgs []*pubsubpb.ReceivedMessage
					for {
						resp, err := sp.Recv()
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							clientErrs[idx] = fmt.Errorf("client %d recv: %w", idx, err)
							return
						}
						gotMsgs = append(gotMsgs, resp.GetReceivedMessages()...)
					}
					_ = sp.CloseSend()

					if len(gotMsgs) != 1 {
						clientErrs[idx] = fmt.Errorf("client %d expected 1 message, got %d", idx, len(gotMsgs))
						return
					}
					wantPrefix := "payload:" + subName + ":"
					if got := string(gotMsgs[0].GetMessage().GetData()); !strings.HasPrefix(got, wantPrefix) {
						clientErrs[idx] = fmt.Errorf("client %d payload=%q want prefix %q", idx, got, wantPrefix)
						return
					}
				}(c)
			}

			close(startCh)

			seen := make(map[string]bool, numStreams)
			handles := make([]*fake.StreamHandle, 0, numStreams)
			var coordErr error
			for i := 1; i <= numStreams; i++ {
				h, err := srv.WaitForStreamingPullN(roundCtx, i)
				if err != nil {
					coordErr = fmt.Errorf("round %d WaitForStreamingPullN(%d): %w", round, i, err)
					break
				}
				if seen[h.ID()] {
					coordErr = fmt.Errorf("round %d WaitForStreamingPullN(%d) returned duplicate stream ID %q", round, i, h.ID())
					break
				}
				seen[h.ID()] = true
				handles = append(handles, h)

				initReq := h.InitialRequest()
				if initReq == nil {
					coordErr = fmt.Errorf("round %d handle %s missing InitialRequest", round, h.ID())
					break
				}
				payload := "payload:" + initReq.GetSubscription() + ":" + h.ID()
				if err := h.PushMessages([]*pubsubpb.ReceivedMessage{
					{
						AckId:   "ack-" + h.ID(),
						Message: &pubsubpb.PubsubMessage{Data: []byte(payload)},
					},
				}); err != nil {
					coordErr = fmt.Errorf("round %d handle %s PushMessages: %w", round, h.ID(), err)
					break
				}
				if err := h.Close(nil); err != nil {
					coordErr = fmt.Errorf("round %d handle %s Close: %w", round, h.ID(), err)
					break
				}
			}

			if coordErr != nil {
				roundCancel()
				wg.Wait()
				t.Fatal(coordErr)
			}

			wg.Wait()
			roundCancel()

			if len(seen) != numStreams {
				t.Fatalf("round %d expected %d unique stream IDs, got %d", round, numStreams, len(seen))
			}
			for _, cErr := range clientErrs {
				if cErr != nil {
					t.Fatalf("round %d %v", round, cErr)
				}
			}
			for _, h := range handles {
				if err := h.WaitClosed(ctx); err != nil {
					t.Fatalf("round %d handle %s WaitClosed: %v", round, h.ID(), err)
				}
				sum := h.Summary()
				if sum.CloseCode != codes.OK || len(sum.SentMessages) != 1 {
					t.Fatalf("round %d handle %s unexpected summary: %+v", round, h.ID(), sum)
				}
			}
		}
	})
}

func TestStrictStreamingPullFrameBoundsAndDisallowedFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sendFrame0 := func(t *testing.T, srv *fake.Server, sub pubsubpb.SubscriberClient, req0 *pubsubpb.StreamingPullRequest) (pubsubpb.Subscriber_StreamingPullClient, error) {
		t.Helper()
		sp, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull open: %v", err)
		}
		if err := sp.Send(req0); err != nil {
			return sp, err
		}
		return sp, nil
	}

	t.Run("strict_initial_and_subsequent_rejections", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithStrictStreamingPullValidation(true))
		sub := pubsubpb.NewSubscriberClient(dialFake(t, srv))
		validInit := &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}

		for _, tc := range []struct {
			name    string
			req     *pubsubpb.StreamingPullRequest
			wantSub string
		}{
			{"init_deadline_5", &pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 5}, "initial stream_ack_deadline_seconds"},
			{"init_deadline_601", &pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 601}, "initial stream_ack_deadline_seconds"},
			{"init_modack_5", &pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30, ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{5}}, "modify_deadline_seconds"},
			{"len_mismatch", &pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30, ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{}}, "length"},
			{"neg_modack", &pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30, ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{-1}}, "must be >= 0"},
		} {
			t.Run("init_"+tc.name, func(t *testing.T) {
				srv.Reset()
				sp, _ := sendFrame0(t, srv, sub, tc.req)
				_, err := sp.Recv()
				if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), tc.wantSub) {
					t.Fatalf("init %s expected InvalidArgument(%q), got %v", tc.name, tc.wantSub, err)
				}
			})
		}

		for _, tc := range []struct {
			name    string
			req     *pubsubpb.StreamingPullRequest
			wantSub string
		}{
			{"sub_stream_deadline_5", &pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 5}, "subsequent stream_ack_deadline_seconds"},
			{"sub_stream_deadline_601", &pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 601}, "subsequent stream_ack_deadline_seconds"},
			{"sub_modack_5", &pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{5}}, "modify_deadline_seconds"},
			{"sub_modack_601", &pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{601}}, "modify_deadline_seconds"},
			{"sub_len_mismatch", &pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{10, 20}}, "length"},
			{"sub_neg_modack", &pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"a1"}, ModifyDeadlineSeconds: []int32{-5}}, "must be >= 0"},
			{"sub_max_outstanding_msgs", &pubsubpb.StreamingPullRequest{MaxOutstandingMessages: 10}, "max_outstanding_messages"},
			{"sub_max_outstanding_bytes", &pubsubpb.StreamingPullRequest{MaxOutstandingBytes: 1024}, "max_outstanding_bytes"},
			{"sub_protocol_version", &pubsubpb.StreamingPullRequest{ProtocolVersion: 1}, "protocol_version"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv.Reset()
				sp, _ := sendFrame0(t, srv, sub, validInit)
				if _, err := srv.WaitForStreamingPull(ctx); err != nil {
					t.Fatalf("WaitForStreamingPull: %v", err)
				}
				if err := sp.Send(tc.req); err != nil && !errors.Is(err, io.EOF) {
					t.Fatalf("send subsequent %s: %v", tc.name, err)
				}
				_, err := sp.Recv()
				if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), tc.wantSub) {
					t.Fatalf("%s expected InvalidArgument(%q), got %v", tc.name, tc.wantSub, err)
				}
			})
		}

		// Valid nack (0) and valid [10, 600] deadline pass under strict mode.
		srv.Reset()
		spOK, _ := sendFrame0(t, srv, sub, validInit)
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull valid: %v", err)
		}
		if err := spOK.Send(&pubsubpb.StreamingPullRequest{
			StreamAckDeadlineSeconds: 60,
			ModifyDeadlineAckIds:     []string{"n1", "e1"},
			ModifyDeadlineSeconds:    []int32{0, 120},
		}); err != nil {
			t.Fatalf("send valid frame 1: %v", err)
		}
		if err := h.Close(nil); err != nil {
			t.Fatalf("h.Close: %v", err)
		}
		if _, err := spOK.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected clean EOF, got %v", err)
		}
	})

	t.Run("non_strict_allows_out_of_bounds_deadlines_for_verifier", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithStrictStreamingPullValidation(false))
		sub := pubsubpb.NewSubscriberClient(dialFake(t, srv))
		sp, _ := sendFrame0(t, srv, sub, &pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		})
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{
			StreamAckDeadlineSeconds: 5,
			ModifyDeadlineAckIds:     []string{"a1"},
			ModifyDeadlineSeconds:    []int32{999},
		}); err != nil {
			t.Fatalf("send frame 1: %v", err)
		}
		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			return len(snap.StreamingPullCalls()) >= 2
		}); err != nil {
			t.Fatalf("WaitFor frame 1: %v", err)
		}
		_ = h.Close(nil)
		_, _ = sp.Recv()
		if err := fake.AssertAckAndModAckWithinBounds(srv.Snapshot()); err == nil {
			t.Fatalf("expected AssertAckAndModAckWithinBounds to fail on non-strict frame 1")
		}
	})
}

func TestStreamAckDeadlineUpdatePongAndProtoVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv := startFakeServer(t)
	sub := pubsubpb.NewSubscriberClient(dialFake(t, srv))

	// ProtocolVersion=1: stream_ack_deadline_seconds-only frame triggers empty pong.
	sp1, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("open sp1: %v", err)
	}
	if err := sp1.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/sub-v1",
		StreamAckDeadlineSeconds: 30,
		ProtocolVersion:          1,
	}); err != nil {
		t.Fatalf("send frame 0 v1: %v", err)
	}
	h1, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull v1: %v", err)
	}
	if err := sp1.Send(&pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 20}); err != nil {
		t.Fatalf("send stream_ack_deadline update: %v", err)
	}
	pong, err := sp1.Recv()
	if err != nil || len(pong.GetReceivedMessages()) != 0 {
		t.Fatalf("expected empty pong for protocol_version=1 stream_ack_deadline update, got %+v err=%v", pong, err)
	}
	if err := h1.Close(nil); err != nil {
		t.Fatalf("h1.Close: %v", err)
	}
	_, _ = sp1.Recv()
	_ = sp1.CloseSend()
	if err := h1.WaitClosed(ctx); err != nil {
		t.Fatalf("h1.WaitClosed: %v", err)
	}
	snap1 := srv.Snapshot()
	spCalls := snap1.StreamingPullCalls()
	if len(spCalls) != 2 || spCalls[0].IsStreamAckDeadlineUpdate() || !spCalls[1].IsStreamAckDeadlineUpdate() {
		t.Fatalf("IsStreamAckDeadlineUpdate mismatch: %+v", spCalls)
	}
	if sum := h1.Summary(); sum.StreamAckDeadlineSeconds != 20 || sum.ProtocolVersion != 1 {
		t.Fatalf("unexpected StreamSummary after deadline update: %+v", sum)
	}
	if err := fake.VerifySequence(snap1, fake.VerifyOptions{IgnoreStreamAckDeadlineUpdates: true}, fake.ExpectStreamingPullInitial("init", fake.StreamingPullInitialExpectation{Subscription: "projects/p/subscriptions/sub-v1"})); err != nil {
		t.Fatalf("VerifySequence IgnoreStreamAckDeadlineUpdates=true failed: %v", err)
	}

	// ProtocolVersion=0: stream_ack_deadline_seconds update does NOT send pong.
	srv.Reset()
	sp0, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("open sp0: %v", err)
	}
	if err := sp0.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/sub-v0",
		StreamAckDeadlineSeconds: 30,
		ProtocolVersion:          0,
	}); err != nil {
		t.Fatalf("send frame 0 v0: %v", err)
	}
	h0, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull v0: %v", err)
	}
	if err := sp0.Send(&pubsubpb.StreamingPullRequest{StreamAckDeadlineSeconds: 25}); err != nil {
		t.Fatalf("send v0 update: %v", err)
	}
	if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
		return len(snap.StreamingPullCalls()) == 2
	}); err != nil {
		t.Fatalf("WaitFor v0 frame 1: %v", err)
	}
	if err := h0.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: "real-msg", Message: &pubsubpb.PubsubMessage{Data: []byte("hi")}}}); err != nil {
		t.Fatalf("h0.PushMessages: %v", err)
	}
	msgResp, err := sp0.Recv()
	if err != nil || len(msgResp.GetReceivedMessages()) != 1 || msgResp.GetReceivedMessages()[0].GetAckId() != "real-msg" {
		t.Fatalf("expected real-msg directly without pong on protocol_version=0, got %+v err=%v", msgResp, err)
	}
	_ = h0.Close(nil)
	_, _ = sp0.Recv()
	_ = sp0.CloseSend()
}

func TestFaultMatchersGateCountEODDefaultAndPublicServerAPIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// MatchOrderingKey("") and MatchPublishAny
	if fake.MatchOrderingKey("")(&fake.PublishMatchContext{}) {
		t.Fatalf("MatchOrderingKey('') must return false on nil Request")
	}
	allEmpty := &fake.PublishMatchContext{Request: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a")}, {Data: []byte("b")}}}}
	mixedEmpty := &fake.PublishMatchContext{Request: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a")}, {Data: []byte("b"), OrderingKey: "k1"}}}}
	onlyK1 := &fake.PublishMatchContext{Request: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("a"), OrderingKey: "k1"}}}}
	if !fake.MatchOrderingKey("")(allEmpty) || fake.MatchOrderingKey("")(mixedEmpty) || fake.MatchOrderingKey("")(onlyK1) {
		t.Fatalf("MatchOrderingKey('') matching failed")
	}
	anyMatch := fake.MatchPublishAny(fake.MatchTopic("projects/p/topics/missing"), fake.MatchOrderingKey("k1"))
	if !anyMatch(onlyK1) || anyMatch(allEmpty) {
		t.Fatalf("MatchPublishAny matching failed")
	}

	// MatchHedgedAttempt, MatchHedgedAttemptGTE, MatchHasTelemetry
	noTel := &fake.PublishMatchContext{}
	att0 := &fake.PublishMatchContext{Telemetry: &pubsubpb.PubsubClientTelemetry{Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: 0}}}}
	att2 := &fake.PublishMatchContext{Telemetry: &pubsubpb.PubsubClientTelemetry{Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: 2}}}}
	if !fake.MatchHedgedAttempt(0)(noTel) || !fake.MatchHedgedAttempt(0)(att0) || fake.MatchHedgedAttempt(1)(noTel) || !fake.MatchHedgedAttempt(2)(att2) || fake.MatchHedgedAttempt(1)(att2) {
		t.Fatalf("MatchHedgedAttempt failed")
	}
	if !fake.MatchHedgedAttemptGTE(0)(noTel) || fake.MatchHedgedAttemptGTE(1)(noTel) || !fake.MatchHedgedAttemptGTE(1)(att2) || fake.MatchHedgedAttemptGTE(3)(att2) {
		t.Fatalf("MatchHedgedAttemptGTE failed")
	}
	if fake.MatchHasTelemetry()(noTel) || !fake.MatchHasTelemetry()(att0) {
		t.Fatalf("MatchHasTelemetry failed")
	}

	// MatchAckIDsContaining, MatchModAckID, MatchModAckIDsContaining
	ackNil := &fake.AckMatchContext{}
	ackSome := &fake.AckMatchContext{Request: &pubsubpb.AcknowledgeRequest{AckIds: []string{"a1", "a2", "a3"}}}
	if fake.MatchAckIDsContaining("a1")(ackNil) || !fake.MatchAckIDsContaining("a1", "a3")(ackSome) || fake.MatchAckIDsContaining("a1", "missing")(ackSome) {
		t.Fatalf("MatchAckIDsContaining failed")
	}
	modNil := &fake.ModAckMatchContext{}
	modSome := &fake.ModAckMatchContext{Request: &pubsubpb.ModifyAckDeadlineRequest{AckIds: []string{"m1", "m2", "m3"}}}
	if fake.MatchModAckID("m1")(modNil) || !fake.MatchModAckID("m2")(modSome) || fake.MatchModAckID("nope")(modSome) {
		t.Fatalf("MatchModAckID failed")
	}
	if fake.MatchModAckIDsContaining("m1")(modNil) || !fake.MatchModAckIDsContaining("m1", "m3")(modSome) || fake.MatchModAckIDsContaining("m1", "nope")(modSome) {
		t.Fatalf("MatchModAckIDsContaining failed")
	}

	// EODErrorStatusWithReason(codes.OK, ..., "", ...) defaults
	eodSt := fake.EODErrorStatusWithReason(codes.OK, "eod msg", "", map[string]string{"a1": fake.EODPermanentInvalidAckID})
	if status.Code(eodSt) != codes.InvalidArgument {
		t.Fatalf("expected codes.InvalidArgument fallback, got %v", eodSt)
	}
	stObj := status.Convert(eodSt)
	if details := stObj.Details(); len(details) != 1 || details[0].(*errdetails.ErrorInfo).GetReason() != fake.EODReasonAckFailed {
		t.Fatalf("expected default reason %q, got %+v", fake.EODReasonAckFailed, details)
	}

	// Gate.EnteredCount + WithListenAddr + WithAttributes + With*Confirmation + Server.*Stream + ProtoSize/PullCalls/StreamsForSubscription
	srv := startFakeServer(t, fake.WithListenAddr("127.0.0.1:0"))
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	sub := pubsubpb.NewSubscriberClient(conn)

	g := fake.NewGate()
	if g.EnteredCount() != 0 {
		t.Fatalf("initial EnteredCount = %d, want 0", g.EnteredCount())
	}
	srv.AddPublishRule(fake.PublishRule{Gate: g, Times: 1})
	pubDone := make(chan error, 1)
	go func() {
		_, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("gated")}},
		})
		pubDone <- err
	}()
	if err := g.WaitEntered(ctx); err != nil {
		t.Fatalf("WaitEntered: %v", err)
	}
	if g.EnteredCount() != 1 {
		t.Fatalf("EnteredCount while gated = %d, want 1", g.EnteredCount())
	}
	g.Release()
	if err := <-pubDone; err != nil {
		t.Fatalf("gated publish: %v", err)
	}

	// Exercise Pull + Snapshot.PullCalls + ProtoSize across methods.
	if _, err := sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: "projects/p/subscriptions/sub-pull", MaxMessages: 1}); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if _, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: "projects/p/subscriptions/sub-pull", AckIds: []string{"ack-x"}}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if _, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: "projects/p/subscriptions/sub-pull", AckIds: []string{"ack-y"}, AckDeadlineSeconds: 30}); err != nil {
		t.Fatalf("ModifyAckDeadline: %v", err)
	}

	// Unknown stream errors for Server.*Stream methods.
	for name, err := range map[string]error{
		"PushResponse":          srv.PushResponse("missing", nil),
		"PushMessages":          srv.PushMessages("missing", nil),
		"CloseStream":           srv.CloseStream("missing", nil),
		"AbortStream":           srv.AbortStream("missing", nil),
		"SetStreamSuppressPong": srv.SetStreamSuppressPong("missing", true),
	} {
		if err == nil {
			t.Errorf("Server.%s(missing) expected error, got nil", name)
		}
	}

	// StreamingPull using Server.PushMessages (WithAttributes/Confirmations), Server.PushResponse(nil), Server.SetStreamSuppressPong, Server.CloseStream, and Server.AbortStream.
	spA, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("open spA: %v", err)
	}
	if err := spA.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/sub-match", StreamAckDeadlineSeconds: 30, ProtocolVersion: 1}); err != nil {
		t.Fatalf("spA frame 0: %v", err)
	}
	hA, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull spA: %v", err)
	}
	if err := srv.SetStreamSuppressPong(hA.ID(), false); err != nil {
		t.Fatalf("SetStreamSuppressPong: %v", err)
	}
	if err := hA.PushResponse(nil); err != nil {
		t.Fatalf("hA.PushResponse(nil): %v", err)
	}
	if err := srv.PushResponse(hA.ID(), &pubsubpb.StreamingPullResponse{}); err != nil {
		t.Fatalf("srv.PushResponse: %v", err)
	}
	if err := srv.PushMessages(
		hA.ID(),
		[]*pubsubpb.ReceivedMessage{{AckId: "a-opt", Message: &pubsubpb.PubsubMessage{Data: []byte("opt")}}},
		fake.WithAttributes(map[string]string{"k1": "v1"}),
		fake.WithAttributes(nil),
		fake.WithAcknowledgeConfirmation(&pubsubpb.StreamingPullResponse_AcknowledgeConfirmation{AckIds: []string{"c1"}}),
		fake.WithModifyAckDeadlineConfirmation(&pubsubpb.StreamingPullResponse_ModifyAckDeadlineConfirmation{AckIds: []string{"m1"}}),
	); err != nil {
		t.Fatalf("srv.PushMessages: %v", err)
	}
	if err := srv.CloseStream(hA.ID(), nil); err != nil {
		t.Fatalf("srv.CloseStream: %v", err)
	}
	// Receive 2 empty responses then the message with merged attributes & confirmations, then EOF.
	for range 2 {
		rEmpty, rErr := spA.Recv()
		if rErr != nil || len(rEmpty.GetReceivedMessages()) != 0 {
			t.Fatalf("expected empty PushResponse, got %+v err=%v", rEmpty, rErr)
		}
	}
	rMsg, rErr := spA.Recv()
	if rErr != nil || len(rMsg.GetReceivedMessages()) != 1 || rMsg.GetReceivedMessages()[0].GetMessage().GetAttributes()["k1"] != "v1" ||
		len(rMsg.GetAcknowledgeConfirmation().GetAckIds()) != 1 || len(rMsg.GetModifyAckDeadlineConfirmation().GetAckIds()) != 1 {
		t.Fatalf("pushed message options mismatch: %+v err=%v", rMsg, rErr)
	}
	if _, err := spA.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after CloseStream, got %v", err)
	}
	_ = spA.CloseSend()
	if err := hA.WaitClosed(ctx); err != nil {
		t.Fatalf("hA.WaitClosed: %v", err)
	}

	// Server.AbortStream test.
	spB, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("open spB: %v", err)
	}
	if err := spB.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/sub-other", StreamAckDeadlineSeconds: 30}); err != nil {
		t.Fatalf("spB frame 0: %v", err)
	}
	hB, err := srv.WaitForStreamingPullN(ctx, 2)
	if err != nil {
		t.Fatalf("WaitForStreamingPullN(2): %v", err)
	}
	if err := srv.AbortStream(hB.ID(), status.Error(codes.Aborted, "aborted-by-server")); err != nil {
		t.Fatalf("srv.AbortStream: %v", err)
	}
	if _, err := spB.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("expected Aborted after AbortStream, got %v", err)
	}
	_ = hB.WaitClosed(ctx)

	snap := srv.Snapshot()
	if len(snap.PullCalls()) != 1 {
		t.Fatalf("PullCalls = %d, want 1", len(snap.PullCalls()))
	}
	if got := snap.StreamsForSubscription("projects/p/subscriptions/sub-match"); len(got) != 1 || got[0].StreamID != hA.ID() {
		t.Fatalf("StreamsForSubscription mismatch: %+v", got)
	}
	for _, c := range snap.Calls {
		if c.ProtoSize() <= 0 {
			t.Errorf("expected positive ProtoSize for %s, got %d", c.FullMethod, c.ProtoSize())
		}
	}
	if (fake.CapturedCall{}).ProtoSize() != 0 {
		t.Errorf("empty CapturedCall.ProtoSize expected 0")
	}
	// Also cover RawReq branch in ProtoSize.
	rawCall := fake.CapturedCall{RawReq: &pubsubpb.PublishRequest{Topic: "projects/p/topics/t"}}
	if rawCall.ProtoSize() <= 0 {
		t.Errorf("expected positive ProtoSize for RawReq")
	}
}

func TestTier5_FakeAdversarialEdgePaths(t *testing.T) {
	t.Run("IsValidTraceparent_edge_vectors", func(t *testing.T) {
		valid := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
		if !fake.IsValidTraceparent(valid) {
			t.Fatalf("expected valid traceparent %q", valid)
		}
		bad := []string{
			"",
			"01-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
			"00-0123456789ABCDEF0123456789abcdef-0123456789abcdef-01",
			"00-0123456789abcdef0123456789abcdeg-0123456789abcdef-01",
			"00-0123456789abcdef0123456789abcdef-0123456789abcdef-1",
			"00-0123456789abcdef0123456789abcdef0-0123456789abcdef-01",
			"00-00000000000000000000000000000000-0123456789abcdef-01",
			"00-0123456789abcdef0123456789abcdef-0000000000000000-01",
		}
		for _, v := range bad {
			if fake.IsValidTraceparent(v) {
				t.Errorf("IsValidTraceparent(%q) = true, want false", v)
			}
		}
	})

	t.Run("deflate_pool_reuse_corrupt_recovery_and_grpc", func(t *testing.T) {
		comp := encoding.GetCompressor("deflate")
		if comp == nil {
			t.Fatalf("deflate compressor not registered")
		}
		makeZlib := func(msg string) []byte {
			var buf bytes.Buffer
			w, err := comp.Compress(&buf)
			if err != nil {
				t.Fatalf("Compress(%q): %v", msg, err)
			}
			if _, err := w.Write([]byte(msg)); err != nil {
				t.Fatalf("Write(%q): %v", msg, err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close(%q): %v", msg, err)
			}
			return buf.Bytes()
		}

		// Warm pool and sequentially reuse pooled reader across multiple calls.
		for i, payload := range []string{"first-call", "second-call-reuse", "third-call-reuse"} {
			r, err := comp.Decompress(bytes.NewReader(makeZlib(payload)))
			if err != nil {
				t.Fatalf("Decompress[%d]: %v", i, err)
			}
			got, err := io.ReadAll(r)
			if cl, ok := r.(io.Closer); ok {
				_ = cl.Close()
			}
			if err != nil || string(got) != payload {
				t.Fatalf("Decompress[%d] read = %q err=%v, want %q", i, string(got), err, payload)
			}
		}

		// Corrupt RFC 1950 header exercises pooled Reset error path, then subsequent valid call still recovers.
		if _, err := comp.Decompress(bytes.NewReader([]byte{0x00, 0x01, 0x02})); err == nil {
			t.Fatalf("expected error decompressing corrupt RFC 1950 bytes")
		}
		rOK, err := comp.Decompress(bytes.NewReader(makeZlib("recovered-after-corrupt")))
		if err != nil {
			t.Fatalf("Decompress after corrupt failed: %v", err)
		}
		gotOK, err := io.ReadAll(rOK)
		if cl, ok := rOK.(io.Closer); ok {
			_ = cl.Close()
		}
		if err != nil || string(gotOK) != "recovered-after-corrupt" {
			t.Fatalf("recovered payload = %q err=%v", string(gotOK), err)
		}

		// Sequential multi-call RPCs over gRPC using deflate compressor.
		srv := startFakeServer(t)
		conn := dialFake(t, srv, grpc.WithDefaultCallOptions(grpc.UseCompressor("deflate")))
		pub := pubsubpb.NewPublisherClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 3 {
			if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte(fmt.Sprintf("deflate-msg-%d", i))}},
			}); err != nil {
				t.Fatalf("deflate Publish[%d]: %v", i, err)
			}
		}
	})

	t.Run("matchers_publish_response_pull_gate_delay_responsefunc_and_reset", func(t *testing.T) {
		if fake.MatchPayloadContains([]byte("needle"))(&fake.PublishMatchContext{Request: nil}) {
			t.Errorf("MatchPayloadContains with nil request should return false")
		}
		if fake.MatchPayloadContains([]byte("needle"))(&fake.PublishMatchContext{
			Request: &pubsubpb.PublishRequest{Messages: []*pubsubpb.PubsubMessage{{Data: []byte("haystack")}}},
		}) {
			t.Errorf("MatchPayloadContains mismatch should return false")
		}
		if fake.MatchPublishAll(
			func(*fake.PublishMatchContext) bool { return true },
			func(*fake.PublishMatchContext) bool { return false },
		)(&fake.PublishMatchContext{}) {
			t.Errorf("MatchPublishAll with trailing false matcher should return false")
		}

		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		pub := pubsubpb.NewPublisherClient(conn)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// PublishRule.Response (cloned) + waitRuleDelay normal expiry + ResponseFunc on Pull.
		wantPubResp := &pubsubpb.PublishResponse{MessageIds: []string{"custom-pub-id-1"}}
		srv.AddPublishRule(fake.PublishRule{
			Match:    fake.MatchPublishAll(fake.MatchTopic("projects/p/topics/t"), fake.MatchPayloadContains([]byte("hit"))),
			Delay:    15 * time.Millisecond,
			Response: wantPubResp,
		})
		gotPub, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hit-me")}},
		})
		if err != nil || len(gotPub.GetMessageIds()) != 1 || gotPub.GetMessageIds()[0] != "custom-pub-id-1" {
			t.Fatalf("PublishRule.Response got %v err=%v", gotPub, err)
		}

		// PullRule.Gate + PullRule.Delay + PullRule.ResponseFunc.
		gate := fake.NewGate()
		srv.AddPullRule(fake.PullRule{
			Gate:  gate,
			Delay: 15 * time.Millisecond,
			ResponseFunc: func(_ context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
				return &pubsubpb.PullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{{
						AckId:   "dyn-" + req.GetSubscription(),
						Message: &pubsubpb.PubsubMessage{Data: []byte("from-func")},
					}},
				}, nil
			},
		})
		pullDone := make(chan error, 1)
		var pullResp *pubsubpb.PullResponse
		go func() {
			var pErr error
			pullResp, pErr = sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: "projects/p/subscriptions/s", MaxMessages: 1})
			pullDone <- pErr
		}()
		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		gate.Release()
		if err := <-pullDone; err != nil {
			t.Fatalf("gated Pull: %v", err)
		}
		if len(pullResp.GetReceivedMessages()) != 1 || pullResp.GetReceivedMessages()[0].GetAckId() != "dyn-projects/p/subscriptions/s" {
			t.Fatalf("unexpected ResponseFunc PullResponse: %+v", pullResp)
		}

		// waitRuleDelay abort via srv.Reset() returns Unavailable.
		delayStarted := make(chan struct{})
		srv.AddPullRule(fake.PullRule{
			Match: func(*fake.PullMatchContext) bool {
				close(delayStarted)
				return true
			},
			Delay: 2 * time.Second,
		})
		abortCh := make(chan error, 1)
		go func() {
			_, aErr := sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: "projects/p/subscriptions/s", MaxMessages: 1})
			abortCh <- aErr
		}()
		<-delayStarted
		time.Sleep(15 * time.Millisecond)
		srv.Reset()
		if err := <-abortCh; status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
			t.Fatalf("expected Unavailable from Reset during Delay, got %v", err)
		}
	})

	t.Run("streaming_pull_fail_before_recv_suppress_pong_and_sub_props", func(t *testing.T) {
		srv := startFakeServer(t, fake.WithDefaultSubscriptionProperties(false, false))
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		suppressPong := true
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times: 1,
			MatchConnect: func(attempt int, md metadata.MD) bool {
				return attempt == 1 && len(md) >= 0
			},
			FailBeforeRecvErr: status.Error(codes.Unavailable, "pre-recv-connect"),
		})
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times:        1,
			SuppressPong: &suppressPong,
			SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
				ExactlyOnceDeliveryEnabled: true,
				MessageOrderingEnabled:     true,
			},
		})

		// Attempt 1 hits FailBeforeRecvErr via MatchConnect before sending frame 0.
		sp1, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull attempt 1 dial: %v", err)
		}
		if _, err := sp1.Recv(); status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "pre-recv-connect") {
			t.Fatalf("attempt 1 expected pre-recv Unavailable, got %v", err)
		}

		// Attempt 2 connects, receives overridden SubscriptionProperties, and suppresses ping pong.
		sp2, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull attempt 2 dial: %v", err)
		}
		if err := sp2.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 20}); err != nil {
			t.Fatalf("sp2 frame 0: %v", err)
		}
		h2, err := srv.WaitForStreamingPullN(ctx, 1)
		if err != nil {
			t.Fatalf("WaitForStreamingPullN(1): %v", err)
		}
		propResp, err := sp2.Recv()
		if err != nil || !propResp.GetSubscriptionProperties().GetExactlyOnceDeliveryEnabled() || !propResp.GetSubscriptionProperties().GetMessageOrderingEnabled() {
			t.Fatalf("SubscriptionProperties override mismatch: %+v err=%v", propResp, err)
		}
		if err := sp2.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
			t.Fatalf("sp2 empty ping frame: %v", err)
		}
		// Give server a brief window to react; SuppressPong must prevent any pong response before CloseStream.
		time.Sleep(30 * time.Millisecond)
		if err := srv.CloseStream(h2.ID(), nil); err != nil {
			t.Fatalf("CloseStream(h2): %v", err)
		}
		if r, err := sp2.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected clean EOF with SuppressPong=true, got resp=%+v err=%v", r, err)
		}
		_ = h2.WaitClosed(ctx)
	})
}

func TestStreamingPullScript_PredicateNegativeMatch(t *testing.T) {
	t.Run("MatchConnect_false_skipped", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var connectChecks atomic.Int64
		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times: 1,
			MatchConnect: func(attempt int, _ metadata.MD) bool {
				connectChecks.Add(1)
				return false
			},
			InitialError: status.Error(codes.PermissionDenied, "should-skip-matchconnect"),
		})

		sp, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull open: %v", err)
		}
		if err := sp.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/sub-1",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("StreamingPull frame 0: %v", err)
		}
		h, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}
		if err := h.Close(nil); err != nil {
			t.Fatalf("h.Close(nil): %v", err)
		}
		if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected clean io.EOF when MatchConnect returns false, got %v", err)
		}
		_ = sp.CloseSend()
		if err := h.WaitClosed(ctx); err != nil {
			t.Fatalf("h.WaitClosed: %v", err)
		}
		if connectChecks.Load() != 1 {
			t.Fatalf("expected MatchConnect evaluated once, got %d", connectChecks.Load())
		}
	})

	t.Run("Match_false_skipped", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		srv.AddStreamingPullScript(fake.StreamingPullScript{
			Times: 1,
			Match: func(_ int, req *pubsubpb.StreamingPullRequest, _ metadata.MD) bool {
				return req.GetSubscription() == "projects/p/subscriptions/wanted"
			},
			InitialError: status.Error(codes.PermissionDenied, "matched-wanted-only"),
		})

		spWrong, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull(other) open: %v", err)
		}
		if err := spWrong.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/other",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("StreamingPull(other) frame 0: %v", err)
		}
		hWrong, err := srv.WaitForStreamingPull(ctx)
		if err != nil {
			t.Fatalf("WaitForStreamingPull(other): %v", err)
		}
		if err := hWrong.Close(nil); err != nil {
			t.Fatalf("hWrong.Close(nil): %v", err)
		}
		if _, err := spWrong.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected clean io.EOF on non-matching subscription, got %v", err)
		}
		_ = spWrong.CloseSend()
		if err := hWrong.WaitClosed(ctx); err != nil {
			t.Fatalf("hWrong.WaitClosed: %v", err)
		}

		// Verify unconsumed script still fires on matching subscription.
		spWant, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("StreamingPull(wanted) open: %v", err)
		}
		if err := spWant.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/wanted",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("StreamingPull(wanted) frame 0: %v", err)
		}
		if _, err := spWant.Recv(); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected PermissionDenied on matching subscription, got %v", err)
		}
		_ = spWant.CloseSend()
	})
}

func TestStreamingPull_CloseSendBeforeFrame0CleanEOF(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after immediate CloseSend before frame 0, got %v", err)
	}
	if calls := srv.Snapshot().Calls; len(calls) != 0 {
		t.Fatalf("expected 0 captured calls on clean EOF before frame 0, got %+v", calls)
	}
}

func TestStreamHandle_CloseIOEOFReturnsCleanEOF(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("StreamingPull frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	if err := h.Close(io.EOF); err != nil {
		t.Fatalf("h.Close(io.EOF): %v", err)
	}
	if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF from StreamHandle.Close(io.EOF), got %v", err)
	}
	_ = sp.CloseSend()
	if err := h.WaitClosed(ctx); err != nil {
		t.Fatalf("h.WaitClosed: %v", err)
	}
}

func TestStreamHandle_WithPublishTimeOverwritesExisting(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("StreamingPull frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}

	initialTS := time.Unix(1700000100, 0).UTC()
	overrideTS := time.Unix(1700000999, 0).UTC()
	if err := h.PushMessages(
		[]*pubsubpb.ReceivedMessage{{
			AckId: "ack-pt-1",
			Message: &pubsubpb.PubsubMessage{
				MessageId:   "msg-pt-1",
				Data:        []byte("payload"),
				PublishTime: timestamppb.New(initialTS),
			},
		}},
		fake.WithPublishTime(overrideTS),
	); err != nil {
		t.Fatalf("PushMessages: %v", err)
	}
	resp, err := sp.Recv()
	if err != nil || len(resp.GetReceivedMessages()) != 1 {
		t.Fatalf("sp.Recv = (%+v, %v)", resp, err)
	}
	gotTS := resp.GetReceivedMessages()[0].GetMessage().GetPublishTime().AsTime().UTC()
	if !gotTS.Equal(overrideTS) {
		t.Fatalf("PublishTime = %v, want %v (initial was %v)", gotTS, overrideTS, initialTS)
	}
	_ = sp.CloseSend()
	_ = h.Close(nil)
	_ = h.WaitClosed(ctx)
}

func TestEODScript_EvaluateDefaultsViaModAck(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.AddModAckRule(fake.ModAckRule{
		Times: fake.RepeatAlways,
		EOD: &fake.EODScript{
			Code: codes.InvalidArgument,
			Behaviors: map[string]fake.EODAckBehavior{
				"t1": {TransientTimes: 1},
			},
		},
	})

	_, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       "projects/p/subscriptions/s",
		AckIds:             []string{"t1"},
		AckDeadlineSeconds: 60,
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument status, got %v", err)
	}
	if st.Message() != "exactly-once delivery failure" {
		t.Fatalf("default EOD message = %q, want %q", st.Message(), "exactly-once delivery failure")
	}
	var ei *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if info, isInfo := d.(*errdetails.ErrorInfo); isInfo {
			ei = info
			break
		}
	}
	if ei == nil {
		t.Fatalf("expected ErrorInfo detail on EOD error, got %+v", st.Details())
	}
	if ei.GetDomain() != fake.EODDomainPubSub || ei.GetReason() != fake.EODReasonModAckFailed || ei.GetMetadata()["t1"] != fake.EODTransientOther {
		t.Fatalf("unexpected default ErrorInfo: domain=%q reason=%q metadata=%v", ei.GetDomain(), ei.GetReason(), ei.GetMetadata())
	}

	if _, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       "projects/p/subscriptions/s",
		AckIds:             []string{"t1"},
		AckDeadlineSeconds: 60,
	}); err != nil {
		t.Fatalf("expected retry after TransientTimes=1 to succeed, got %v", err)
	}
}

func TestServer_AckModAckPullRuleDelayAndGateAbortOnReset(t *testing.T) {
	t.Run("Acknowledge_Delay_Reset_ReturnsUnavailable", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		matched := make(chan struct{})
		var matchOnce sync.Once
		srv.AddAckRule(fake.AckRule{
			Match: func(*fake.AckMatchContext) bool {
				matchOnce.Do(func() { close(matched) })
				return true
			},
			Delay: 5 * time.Second,
		})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"ack-delay-1"},
			})
			errCh <- err
		}()

		select {
		case <-matched:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for AckRule match: %v", ctx.Err())
		}
		srv.Reset()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
				t.Fatalf("expected Unavailable(fake server reset) from AckRule Delay on Reset, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Acknowledge result: %v", ctx.Err())
		}
	})

	t.Run("ModifyAckDeadline_Delay_Reset_ReturnsUnavailable", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		matched := make(chan struct{})
		var matchOnce sync.Once
		srv.AddModAckRule(fake.ModAckRule{
			Match: func(*fake.ModAckMatchContext) bool {
				matchOnce.Do(func() { close(matched) })
				return true
			},
			Delay: 5 * time.Second,
		})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"modack-delay-1"},
				AckDeadlineSeconds: 60,
			})
			errCh <- err
		}()

		select {
		case <-matched:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ModAckRule match: %v", ctx.Err())
		}
		srv.Reset()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
				t.Fatalf("expected Unavailable(fake server reset) from ModAckRule Delay on Reset, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ModifyAckDeadline result: %v", ctx.Err())
		}
	})

	t.Run("Pull_Gate_Reset_ReturnsUnavailable", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		gate := fake.NewGate()
		srv.AddPullRule(fake.PullRule{Gate: gate})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.Pull(ctx, &pubsubpb.PullRequest{
				Subscription: "projects/p/subscriptions/s",
				MaxMessages:  1,
			})
			errCh <- err
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		srv.Reset()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
				t.Fatalf("expected Unavailable(fake server reset) from PullRule Gate on Reset, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Pull result: %v", ctx.Err())
		}
	})

	t.Run("Acknowledge_Gate_Reset_ReturnsUnavailable", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		gate := fake.NewGate()
		srv.AddAckRule(fake.AckRule{Gate: gate})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"ack-gate-reset-1"},
			})
			errCh <- err
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		srv.Reset()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
				t.Fatalf("expected Unavailable(fake server reset) from AckRule Gate on Reset, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Acknowledge result: %v", ctx.Err())
		}
	})

	t.Run("Acknowledge_Gate_Cancel_ReturnsCanceled", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rpcCtx, rpcCancel := context.WithCancel(ctx)
		defer rpcCancel()

		gate := fake.NewGate()
		srv.AddAckRule(fake.AckRule{Gate: gate})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.Acknowledge(rpcCtx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"ack-gate-cancel-1"},
			})
			errCh <- err
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		rpcCancel()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("expected Canceled from AckRule Gate on client cancel, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Acknowledge result: %v", ctx.Err())
		}

		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			calls := snap.AckCalls()
			return len(calls) == 1 && calls[0].Completed
		}); err != nil {
			t.Fatalf("WaitFor Acknowledge completion: %v", err)
		}
		ackCalls := srv.Snapshot().AckCalls()
		if len(ackCalls) != 1 || ackCalls[0].StatusCode != codes.Canceled || !ackCalls[0].CanceledByClient {
			t.Fatalf("expected captured Ack call with StatusCode=Canceled and CanceledByClient=true, got %+v", ackCalls)
		}
	})

	t.Run("ModifyAckDeadline_Gate_Reset_ReturnsUnavailable", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		gate := fake.NewGate()
		srv.AddModAckRule(fake.ModAckRule{Gate: gate})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"modack-gate-reset-1"},
				AckDeadlineSeconds: 60,
			})
			errCh <- err
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		srv.Reset()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "fake server reset") {
				t.Fatalf("expected Unavailable(fake server reset) from ModAckRule Gate on Reset, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ModifyAckDeadline result: %v", ctx.Err())
		}
	})

	t.Run("ModifyAckDeadline_Gate_Cancel_ReturnsCanceled", func(t *testing.T) {
		srv := startFakeServer(t)
		conn := dialFake(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		rpcCtx, rpcCancel := context.WithCancel(ctx)
		defer rpcCancel()

		gate := fake.NewGate()
		srv.AddModAckRule(fake.ModAckRule{Gate: gate})

		errCh := make(chan error, 1)
		go func() {
			_, err := sub.ModifyAckDeadline(rpcCtx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"modack-gate-cancel-1"},
				AckDeadlineSeconds: 60,
			})
			errCh <- err
		}()

		if err := gate.WaitEntered(ctx); err != nil {
			t.Fatalf("gate.WaitEntered: %v", err)
		}
		rpcCancel()

		select {
		case err := <-errCh:
			if status.Code(err) != codes.Canceled {
				t.Fatalf("expected Canceled from ModAckRule Gate on client cancel, got %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for ModifyAckDeadline result: %v", ctx.Err())
		}

		if err := srv.WaitFor(ctx, func(snap fake.Snapshot) bool {
			calls := snap.ModAckCalls()
			return len(calls) == 1 && calls[0].Completed
		}); err != nil {
			t.Fatalf("WaitFor ModifyAckDeadline completion: %v", err)
		}
		modAckCalls := srv.Snapshot().ModAckCalls()
		if len(modAckCalls) != 1 || modAckCalls[0].StatusCode != codes.Canceled || !modAckCalls[0].CanceledByClient {
			t.Fatalf("expected captured ModAck call with StatusCode=Canceled and CanceledByClient=true, got %+v", modAckCalls)
		}
	})
}

func TestStreamHandle_ClosedRejectsPushAndClose(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("StreamingPull frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}
	if err := h.Close(nil); err != nil {
		t.Fatalf("initial h.Close(nil): %v", err)
	}
	if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF from sp.Recv after Close, got %v", err)
	}
	_ = sp.CloseSend()
	if err := h.WaitClosed(ctx); err != nil {
		t.Fatalf("h.WaitClosed: %v", err)
	}

	if err := h.PushResponse(&pubsubpb.StreamingPullResponse{}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected PushResponse on closed stream to return 'closed' error, got %v", err)
	}
	if err := h.PushMessages([]*pubsubpb.ReceivedMessage{{Message: &pubsubpb.PubsubMessage{Data: []byte("after-close")}}}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected PushMessages on closed stream to return 'closed' error, got %v", err)
	}
	if err := h.SetSubscriptionProperties(true, false); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected SetSubscriptionProperties on closed stream to return 'closed' error, got %v", err)
	}
	if err := h.Close(nil); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("expected Close on closed stream to return 'already closed' error, got %v", err)
	}
	if err := srv.PushResponse(h.ID(), nil); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected srv.PushResponse on closed stream to return 'closed' error, got %v", err)
	}
	if err := srv.CloseStream(h.ID(), errors.New("late close")); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("expected srv.CloseStream on closed stream to return 'already closed' error, got %v", err)
	}
}

func TestServer_WaitForTimeoutReturnsContextError(t *testing.T) {
	srv := startFakeServer(t)

	t.Run("DeadlineExceeded", func(t *testing.T) {
		expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		if err := srv.WaitFor(expiredCtx, func(fake.Snapshot) bool { return false }); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitFor(expiredCtx) = %v, want DeadlineExceeded", err)
		}
		if err := srv.WaitForCalls(expiredCtx, func([]fake.CapturedCall) bool { return false }); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitForCalls(expiredCtx) = %v, want DeadlineExceeded", err)
		}
		if calls, err := srv.WaitForPublish(expiredCtx, 1); !errors.Is(err, context.DeadlineExceeded) || len(calls) != 0 {
			t.Fatalf("WaitForPublish(expiredCtx) = (%d calls, %v), want (0, DeadlineExceeded)", len(calls), err)
		}
		if calls, err := srv.WaitForAck(expiredCtx, 1); !errors.Is(err, context.DeadlineExceeded) || len(calls) != 0 {
			t.Fatalf("WaitForAck(expiredCtx) = (%d calls, %v), want (0, DeadlineExceeded)", len(calls), err)
		}
		if calls, err := srv.WaitForModAck(expiredCtx, 1); !errors.Is(err, context.DeadlineExceeded) || len(calls) != 0 {
			t.Fatalf("WaitForModAck(expiredCtx) = (%d calls, %v), want (0, DeadlineExceeded)", len(calls), err)
		}
		if calls, err := srv.WaitForPull(expiredCtx, 1); !errors.Is(err, context.DeadlineExceeded) || len(calls) != 0 {
			t.Fatalf("WaitForPull(expiredCtx) = (%d calls, %v), want (0, DeadlineExceeded)", len(calls), err)
		}
		if h, err := srv.WaitForStreamingPull(expiredCtx); !errors.Is(err, context.DeadlineExceeded) || h != nil {
			t.Fatalf("WaitForStreamingPull(expiredCtx) = (%v, %v), want (nil, DeadlineExceeded)", h, err)
		}
		if h, err := srv.WaitForStreamingPullN(expiredCtx, 1); !errors.Is(err, context.DeadlineExceeded) || h != nil {
			t.Fatalf("WaitForStreamingPullN(expiredCtx, 1) = (%v, %v), want (nil, DeadlineExceeded)", h, err)
		}
	})

	t.Run("Canceled", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := srv.WaitFor(canceledCtx, func(fake.Snapshot) bool { return false }); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitFor(canceledCtx) = %v, want Canceled", err)
		}
		if calls, err := srv.WaitForPublish(canceledCtx, 1); !errors.Is(err, context.Canceled) || len(calls) != 0 {
			t.Fatalf("WaitForPublish(canceledCtx) = (%d calls, %v), want (0, Canceled)", len(calls), err)
		}
		if h, err := srv.WaitForStreamingPullN(canceledCtx, 1); !errors.Is(err, context.Canceled) || h != nil {
			t.Fatalf("WaitForStreamingPullN(canceledCtx, 1) = (%v, %v), want (nil, Canceled)", h, err)
		}
	})
}

func TestServer_ResetClosesTrackedUnaryConnOnHang(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer rpcCancel()

	entered := make(chan struct{})
	var enterOnce sync.Once
	srv.AddPublishRule(fake.PublishRule{
		ResponseFunc: func(hCtx context.Context, _ *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
			enterOnce.Do(func() { close(entered) })
			<-hCtx.Done()
			return nil, status.FromContextError(hCtx.Err()).Err()
		},
	})

	rpcDone := make(chan error, 1)
	go func() {
		_, err := pub.Publish(rpcCtx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hang")}},
		})
		rpcDone <- err
	}()

	select {
	case <-entered:
	case <-rpcCtx.Done():
		t.Fatalf("timed out waiting for ResponseFunc entry: %v", rpcCtx.Err())
	}

	resetDone := make(chan struct{})
	go func() {
		srv.Reset()
		close(resetDone)
	}()

	select {
	case <-resetDone:
	case <-time.After(500 * time.Millisecond):
		rpcCancel()
		<-resetDone
		t.Fatal("srv.Reset() exceeded 500ms waiting on hanging unary handler; tracked connection not closed")
	}

	select {
	case err := <-rpcDone:
		if err == nil {
			t.Fatal("expected hanging Publish RPC to fail after Reset closed tracked connection")
		}
	case <-rpcCtx.Done():
		t.Fatalf("timed out waiting for client Publish completion after Reset: %v", rpcCtx.Err())
	}
}

func TestStreamHandle_WaitClosedTimeoutReturnsCtxErr(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sp, err := sub.StreamingPull(ctx)
	if err != nil {
		t.Fatalf("StreamingPull open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("StreamingPull frame 0: %v", err)
	}
	h, err := srv.WaitForStreamingPull(ctx)
	if err != nil {
		t.Fatalf("WaitForStreamingPull: %v", err)
	}

	expiredCtx, cancelExp := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExp()
	if err := h.WaitClosed(expiredCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitClosed(expiredCtx) = %v, want context.DeadlineExceeded", err)
	}

	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := h.WaitClosed(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitClosed(canceledCtx) = %v, want context.Canceled", err)
	}

	if err := h.Close(nil); err != nil {
		t.Fatalf("h.Close(nil): %v", err)
	}
	if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("sp.Recv after Close = %v, want io.EOF", err)
	}
	_ = sp.CloseSend()
	if err := h.WaitClosed(ctx); err != nil {
		t.Fatalf("WaitClosed after Close: %v", err)
	}
}

func TestGate_WaitEnteredTimeoutReturnsCtxErr(t *testing.T) {
	g := fake.NewGate()
	defer g.Release()

	expiredCtx, cancelExp := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExp()
	if err := g.WaitEntered(expiredCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitEntered(expiredCtx) = %v, want context.DeadlineExceeded", err)
	}
	if err := g.WaitEnteredN(expiredCtx, 0); err != nil {
		t.Fatalf("WaitEnteredN(expiredCtx, 0) = %v, want nil", err)
	}

	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := g.WaitEnteredN(canceledCtx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitEnteredN(canceledCtx, 1) = %v, want context.Canceled", err)
	}
}

func TestCapturedCall_OrderingKeyOnUnorderedAndNil(t *testing.T) {
	if key, mixed := (fake.CapturedCall{}).OrderingKey(); key != "" || mixed {
		t.Fatalf("zero CapturedCall.OrderingKey() = (%q, %v), want (\"\", false)", key, mixed)
	}
	emptyReqCall := fake.CapturedCall{PublishReq: &pubsubpb.PublishRequest{}}
	if key, mixed := emptyReqCall.OrderingKey(); key != "" || mixed {
		t.Fatalf("empty PublishReq.OrderingKey() = (%q, %v), want (\"\", false)", key, mixed)
	}
	unordCall := fake.CapturedCall{
		PublishReq: &pubsubpb.PublishRequest{
			Topic: "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{
				{Data: []byte("u1")},
				{Data: []byte("u2"), OrderingKey: ""},
			},
		},
	}
	if key, mixed := unordCall.OrderingKey(); key != "" || mixed {
		t.Fatalf("unordered PublishReq.OrderingKey() = (%q, %v), want (\"\", false)", key, mixed)
	}
	ordCall := fake.CapturedCall{
		PublishReq: &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("o1"), OrderingKey: "k1"}},
		},
	}
	if key, mixed := ordCall.OrderingKey(); key != "k1" || mixed {
		t.Fatalf("ordered PublishReq.OrderingKey() = (%q, %v), want (\"k1\", false)", key, mixed)
	}
	mixedCall := fake.CapturedCall{
		PublishReq: &pubsubpb.PublishRequest{
			Topic: "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{
				{Data: []byte("o1"), OrderingKey: "k1"},
				{Data: []byte("u1")},
			},
		},
	}
	if key, mixed := mixedCall.OrderingKey(); key != "k1" || !mixed {
		t.Fatalf("mixed PublishReq.OrderingKey() = (%q, %v), want (\"k1\", true)", key, mixed)
	}

	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, msg := range []*pubsubpb.PubsubMessage{
		{Data: []byte("u1")},
		{Data: []byte("o1"), OrderingKey: "k1"},
		{Data: []byte("u2")},
		{Data: []byte("o2"), OrderingKey: "k1"},
	} {
		if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{msg},
		}); err != nil {
			t.Fatalf("Publish(%q): %v", msg.GetData(), err)
		}
	}

	snap := srv.Snapshot()
	if err := fake.AssertOrderedPublishFIFO(snap, "k1", [][]byte{[]byte("o1"), []byte("o2")}); err != nil {
		t.Fatalf("AssertOrderedPublishFIFO over mixed unordered+ordered snapshot failed: %v", err)
	}
	if err := fake.AssertOrderedPublishFIFO(snap, "k1", [][]byte{[]byte("o2"), []byte("o1")}); err == nil {
		t.Fatal("expected AssertOrderedPublishFIFO to fail on wrong FIFO order")
	}
}

func TestNewServer_InvalidListenAddrReturnsError(t *testing.T) {
	srv, err := fake.NewServer(fake.WithListenAddr("invalid-listen-addr-without-port"))
	if srv != nil {
		_ = srv.Close()
	}
	if err == nil || srv != nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("NewServer(invalid-listen-addr-without-port) = (%v, %v), want (nil, listen error)", srv, err)
	}
}

func TestStreamingPull_TelemetryDecodeErrAllPaths(t *testing.T) {
	srv := startFakeServer(t, fake.WithMaxRecvMsgSize(256))
	conn := dialFake(t, srv)
	sub := pubsubpb.NewSubscriberClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	badCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(fake.TelemetryHeaderName, "!!!not-base64!!!"))

	// 1. Pre-recv failure (FailBeforeRecvErr).
	srv.AddStreamingPullScript(fake.StreamingPullScript{
		Times:             1,
		FailBeforeRecvErr: status.Error(codes.Unavailable, "pre-recv fail"),
	})
	spPre, err := sub.StreamingPull(badCtx)
	if err != nil {
		t.Fatalf("StreamingPull pre-recv open: %v", err)
	}
	if _, err := spPre.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable from FailBeforeRecvErr, got %v", err)
	}

	// 2 & 3. Valid Frame 0 + Oversized Frame 1 recv error.
	sp, err := sub.StreamingPull(badCtx)
	if err != nil {
		t.Fatalf("StreamingPull frame test open: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}); err != nil {
		t.Fatalf("send frame 0: %v", err)
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{
		AckIds: []string{strings.Repeat("a", 512)},
	}); err != nil {
		t.Fatalf("send oversized frame 1: %v", err)
	}
	if _, err := sp.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted on oversized frame 1, got %v", err)
	}

	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) == 3 && calls[2].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls: %v", err)
	}

	spCalls := srv.Snapshot().StreamingPullCalls()
	if len(spCalls) != 3 {
		t.Fatalf("expected 3 StreamingPullCalls, got %d", len(spCalls))
	}
	for i, c := range spCalls {
		if c.TelemetryDecodeErr == "" {
			t.Fatalf("StreamingPullCall[%d] (seq=%d streamSeq=%d status=%s) expected non-empty TelemetryDecodeErr", i, c.Seq, c.StreamSeq, c.StatusCode)
		}
	}
	if spCalls[0].StatusCode != codes.Unavailable || spCalls[1].StreamSeq != 0 || spCalls[2].StreamSeq != 1 || spCalls[2].StatusCode != codes.ResourceExhausted {
		t.Fatalf("unexpected StreamingPullCall sequence: %+v", spCalls)
	}
}

func TestServer_TransportAndUnknownRPCTelemetryDecodeErr(t *testing.T) {
	srv := startFakeServer(t, fake.WithMaxRecvMsgSize(32))
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	badCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(fake.TelemetryHeaderName, "!!!not-base64!!!"))

	// 1. Oversized unary transport error -> recordTransportError.
	if _, err := pub.Publish(badCtx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: bytes.Repeat([]byte("a"), 128)}},
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted on oversized Publish, got %v", err)
	}
	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) == 1 && calls[0].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls 1: %v", err)
	}

	// 2. Unknown gRPC RPC -> handleUnknownRPC.
	if err := conn.Invoke(badCtx, "/google.pubsub.v1.Subscriber/UnknownMethod", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected Unimplemented on unknown RPC, got %v", err)
	}
	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) == 2 && calls[1].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls 2: %v", err)
	}

	calls := srv.Calls()
	if calls[0].TelemetryDecodeErr == "" || calls[0].CodecError == "" {
		t.Fatalf("transport error call missing TelemetryDecodeErr or CodecError: %+v", calls[0])
	}
	if calls[1].TelemetryDecodeErr == "" || !calls[1].Unexpected {
		t.Fatalf("unknown RPC call missing TelemetryDecodeErr or Unexpected: %+v", calls[1])
	}
}

func TestPublishRule_WaitRuleDelayCtxCancelShortCircuitsRuleErr(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	matchedCh := make(chan struct{})
	var matchOnce sync.Once
	srv.AddPublishRule(fake.PublishRule{
		Match: func(*fake.PublishMatchContext) bool {
			matchOnce.Do(func() { close(matchedCh) })
			return true
		},
		Times: 1,
		Delay: 5 * time.Second,
		Err:   status.Error(codes.PermissionDenied, "must short-circuit on ctx cancel"),
	})

	callCtx, callCancel := context.WithCancel(ctx)
	defer callCancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := pub.Publish(callCtx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("delay-cancel")}},
		})
		errCh <- err
	}()

	select {
	case <-matchedCh:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Publish rule match: %v", ctx.Err())
	}
	callCancel()

	var pubErr error
	select {
	case pubErr = <-errCh:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for canceled Publish return: %v", ctx.Err())
	}
	if status.Code(pubErr) != codes.Canceled {
		t.Fatalf("expected client Publish Canceled, got %v", pubErr)
	}

	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) == 1 && calls[0].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls: %v", err)
	}

	calls := srv.Calls()
	if calls[0].StatusCode != codes.Canceled {
		t.Fatalf("expected CapturedCall.StatusCode=Canceled (short-circuiting before matched.Err), got %s (err=%q)", calls[0].StatusCode, calls[0].StatusErr)
	}
}

func TestGate_ReleaseWithErrorPropagatesErrAndStatusCode(t *testing.T) {
	srv := startFakeServer(t)
	conn := dialFake(t, srv)
	pub := pubsubpb.NewPublisherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gate := fake.NewGate()
	srv.AddPublishRule(fake.PublishRule{
		Times: 1,
		Gate:  gate,
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/t",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("gate-err")}},
		})
		errCh <- err
	}()

	if err := gate.WaitEntered(ctx); err != nil {
		t.Fatalf("gate.WaitEntered: %v", err)
	}

	gate.ReleaseWithError(status.Error(codes.Unavailable, "gate injected boom"))
	// Second release must be a no-op (first release wins).
	gate.ReleaseWithError(status.Error(codes.Internal, "ignored second release"))

	var pubErr error
	select {
	case pubErr = <-errCh:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for gated Publish return: %v", ctx.Err())
	}
	if status.Code(pubErr) != codes.Unavailable || !strings.Contains(pubErr.Error(), "gate injected boom") {
		t.Fatalf("expected Unavailable 'gate injected boom', got %v", pubErr)
	}

	if err := srv.WaitForCalls(ctx, func(calls []fake.CapturedCall) bool {
		return len(calls) == 1 && calls[0].Completed
	}); err != nil {
		t.Fatalf("WaitForCalls: %v", err)
	}
	calls := srv.Calls()
	if calls[0].StatusCode != codes.Unavailable || !strings.Contains(calls[0].StatusErr, "gate injected boom") {
		t.Fatalf("expected CapturedCall.StatusCode=Unavailable with 'gate injected boom', got status=%s err=%q", calls[0].StatusCode, calls[0].StatusErr)
	}
}
