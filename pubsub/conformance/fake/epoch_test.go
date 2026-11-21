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

package fake

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func dialFakeInternal(t *testing.T, srv *Server) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%q): %v", srv.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func waitSignal(ctx context.Context, t *testing.T, ch <-chan struct{}, desc string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", desc, ctx.Err())
	}
}

func TestEpochIsolationTagRPCBeforeResetAcrossAllEntrypoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runEntrypoint := func(t *testing.T, name string, opts []Option, invoke func(ctx context.Context, conn *grpc.ClientConn) error, verifyPost func(t *testing.T, srv *Server, conn *grpc.ClientConn)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			var armed atomic.Bool
			enteredCh := make(chan struct{}, 1)
			releaseCh := make(chan struct{})
			rpcEndCh := make(chan struct{}, 4)

			hooks := &serverTestHooks{
				afterTagRPC: func(_ string, _ *epochState) {
					if armed.CompareAndSwap(true, false) {
						enteredCh <- struct{}{}
						select {
						case <-releaseCh:
						case <-ctx.Done():
						}
					}
				},
				onRPCEnd: func(_ string, _ *epochState) {
					rpcEndCh <- struct{}{}
				},
			}
			t.Cleanup(func() {
				select {
				case <-releaseCh:
				default:
					close(releaseCh)
				}
			})

			srv, err := NewServer(append(opts, withTestHooks(hooks))...)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			conn := dialFakeInternal(t, srv)

			// Warm up HTTP/2 transport before arming TagRPC gate.
			pub := pubsubpb.NewPublisherClient(conn)
			if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/warm",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("warm")}},
			}); err != nil {
				t.Fatalf("warmup Publish: %v", err)
			}
			waitSignal(ctx, t, rpcEndCh, "warmup onRPCEnd")
			srv.Reset()

			armed.Store(true)
			errCh := make(chan error, 1)
			go func() {
				errCh <- invoke(ctx, conn)
			}()

			waitSignal(ctx, t, enteredCh, "afterTagRPC")
			srv.Reset()
			close(releaseCh)

			gotErr := <-errCh
			waitSignal(ctx, t, rpcEndCh, "stale RPC onRPCEnd")

			if name == "recordTransportError" {
				if status.Code(gotErr) != codes.ResourceExhausted {
					t.Fatalf("expected ResourceExhausted for oversize payload, got %v", gotErr)
				}
			} else if status.Code(gotErr) != codes.Unavailable {
				t.Fatalf("expected Unavailable on stale-epoch %s, got %v", name, gotErr)
			}

			snap := srv.Snapshot()
			if len(snap.Calls) != 0 || len(snap.Streams) != 0 {
				t.Fatalf("expected 0 leaked calls/streams after single Reset, got calls=%+v streams=%+v", snap.Calls, snap.Streams)
			}
			if verifyPost != nil {
				verifyPost(t, srv, conn)
			}
		})
	}

	runEntrypoint(t, "handleUnknownRPC_CreateTopic", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewPublisherClient(conn).CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p/topics/t"})
			return err
		},
		nil,
	)

	runEntrypoint(t, "handleUnknownRPC_GetSchema", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSchemaServiceClient(conn).GetSchema(ctx, &pubsubpb.GetSchemaRequest{Name: "projects/p/schemas/s"})
			return err
		},
		nil,
	)

	runEntrypoint(t, "unaryInterceptor_Publish", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("stale")}},
			})
			return err
		},
		func(t *testing.T, srv *Server, conn *grpc.ClientConn) {
			srv.AddPublishRule(PublishRule{Match: MatchCallAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
			_, err := pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("fresh")}},
			})
			if status.Code(err) != codes.AlreadyExists {
				t.Fatalf("expected ep1 Attempt=1 AlreadyExists, got %v", err)
			}
		},
	)

	runEntrypoint(t, "unaryInterceptor_Pull", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{
				Subscription: "projects/p/subscriptions/s",
				MaxMessages:  1,
			})
			return err
		},
		func(t *testing.T, srv *Server, conn *grpc.ClientConn) {
			srv.AddPullRule(PullRule{
				Match: func(mc *PullMatchContext) bool { return mc.Attempt == 1 },
				Err:   status.Error(codes.AlreadyExists, "ep1"),
				Times: 1,
			})
			_, err := pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{
				Subscription: "projects/p/subscriptions/s",
				MaxMessages:  1,
			})
			if status.Code(err) != codes.AlreadyExists {
				t.Fatalf("expected ep1 Pull Attempt=1 AlreadyExists, got %v", err)
			}
		},
	)

	runEntrypoint(t, "unaryInterceptor_Acknowledge", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"a1"},
			})
			return err
		},
		func(t *testing.T, srv *Server, conn *grpc.ClientConn) {
			srv.AddAckRule(AckRule{Match: MatchAckAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
			_, err := pubsubpb.NewSubscriberClient(conn).Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"a1"},
			})
			if status.Code(err) != codes.AlreadyExists {
				t.Fatalf("expected ep1 Ack Attempt=1 AlreadyExists, got %v", err)
			}
		},
	)

	runEntrypoint(t, "unaryInterceptor_ModifyAckDeadline", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"m1"},
				AckDeadlineSeconds: 60,
			})
			return err
		},
		func(t *testing.T, srv *Server, conn *grpc.ClientConn) {
			srv.AddModAckRule(ModAckRule{Match: MatchModAckAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
			_, err := pubsubpb.NewSubscriberClient(conn).ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"m1"},
				AckDeadlineSeconds: 60,
			})
			if status.Code(err) != codes.AlreadyExists {
				t.Fatalf("expected ep1 ModAck Attempt=1 AlreadyExists, got %v", err)
			}
		},
	)

	runEntrypoint(t, "handleStreamingPull", nil,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			sp, err := pubsubpb.NewSubscriberClient(conn).StreamingPull(ctx)
			if err != nil {
				return err
			}
			_ = sp.Send(&pubsubpb.StreamingPullRequest{
				Subscription:             "projects/p/subscriptions/s",
				StreamAckDeadlineSeconds: 30,
			})
			_, err = sp.Recv()
			return err
		},
		nil,
	)

	runEntrypoint(t, "recordTransportError", []Option{WithMaxRecvMsgSize(1024)},
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: make([]byte, 2048)}},
			})
			return err
		},
		nil,
	)
}

func TestUnknownRPCConcurrentCancelAcrossResetNoEpochLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const workers = 10
	for round := range 5 {
		var mu sync.Mutex
		pausedCount := 0
		var pauseActive bool
		enteredCh := make(chan struct{}, workers)
		releaseCh := make(chan struct{})
		rpcEndCh := make(chan struct{}, workers+1)

		hooks := &serverTestHooks{
			afterTagRPC: func(fullMethod string, _ *epochState) {
				if fullMethod == MethodPublish {
					return
				}
				mu.Lock()
				shouldPause := pauseActive && pausedCount < workers
				if shouldPause {
					pausedCount++
				}
				mu.Unlock()
				if shouldPause {
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			onRPCEnd: func(_ string, _ *epochState) {
				rpcEndCh <- struct{}{}
			},
		}

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("round %d NewServer: %v", round, err)
		}
		conn := dialFakeInternal(t, srv)
		pub := pubsubpb.NewPublisherClient(conn)
		schema := pubsubpb.NewSchemaServiceClient(conn)

		// Warm up transport.
		if _, err := pub.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    "projects/p/topics/warm",
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte("warm")}},
		}); err != nil {
			_ = srv.Close()
			t.Fatalf("round %d warmup: %v", round, err)
		}
		waitSignal(ctx, t, rpcEndCh, "warmup onRPCEnd")
		srv.Reset()

		mu.Lock()
		pauseActive = true
		mu.Unlock()

		subCtxs := make([]context.Context, workers)
		subCancels := make([]context.CancelFunc, workers)
		for i := range workers {
			subCtxs[i], subCancels[i] = context.WithCancel(ctx)
		}

		errs := make([]error, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if idx%2 == 0 {
					_, errs[idx] = pub.CreateTopic(subCtxs[idx], &pubsubpb.Topic{Name: "projects/p/topics/t"})
				} else {
					_, errs[idx] = schema.GetSchema(subCtxs[idx], &pubsubpb.GetSchemaRequest{Name: "projects/p/schemas/s"})
				}
			}(i)
		}

		for i := range workers {
			select {
			case <-enteredCh:
			case <-ctx.Done():
				close(releaseCh)
				for _, c := range subCancels {
					c()
				}
				wg.Wait()
				_ = srv.Close()
				t.Fatalf("round %d timed out waiting for worker %d TagRPC: %v", round, i, ctx.Err())
			}
		}

		// Cancel half of the workers right after TagRPC tagged ep0, then Reset ONCE.
		for i := range workers {
			if i%2 == 0 {
				subCancels[i]()
			}
		}
		srv.Reset()
		close(releaseCh)
		wg.Wait()
		for _, c := range subCancels {
			c()
		}
		for i := range workers {
			select {
			case <-rpcEndCh:
			case <-ctx.Done():
				_ = srv.Close()
				t.Fatalf("round %d timed out waiting for onRPCEnd %d: %v", round, i, ctx.Err())
			}
		}

		for i, err := range errs {
			code := status.Code(err)
			if i%2 == 0 {
				if code != codes.Canceled && code != codes.Unavailable {
					_ = srv.Close()
					t.Fatalf("round %d canceled worker %d unexpected status %v", round, i, err)
				}
			} else if code != codes.Unavailable {
				_ = srv.Close()
				t.Fatalf("round %d uncanceled worker %d expected Unavailable, got %v", round, i, err)
			}
		}

		// Assert ZERO leaked calls WITHOUT any second srv.Reset() or sleep!
		if got := srv.Calls(); len(got) != 0 {
			_ = srv.Close()
			t.Fatalf("round %d expected 0 leaked calls after single Reset, got %+v", round, got)
		}
		_ = srv.Close()
	}
}

func TestEpochIsolationInFlightHandlersAcrossReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("handleUnknownRPC_wg_wait", func(t *testing.T) {
		var armed atomic.Bool
		enteredCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		graceExpiredCh := make(chan struct{})

		hooks := &serverTestHooks{
			afterUnknownUnlock: func(_ *epochState) {
				if armed.CompareAndSwap(true, false) {
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			onResetGraceExpired: func() {
				close(graceExpiredCh)
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn := dialFakeInternal(t, srv)
		pub := pubsubpb.NewPublisherClient(conn)

		armed.Store(true)
		errCh := make(chan error, 1)
		go func() {
			_, callErr := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p/topics/t"})
			errCh <- callErr
		}()
		waitSignal(ctx, t, enteredCh, "afterUnknownUnlock")

		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()

		select {
		case <-resetDoneCh:
			t.Fatalf("Reset returned before handleUnknownRPC finished while ep.wg was held")
		case <-graceExpiredCh:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for graceExpiredCh: %v", ctx.Err())
		}

		close(releaseCh)
		waitSignal(ctx, t, resetDoneCh, "Reset completion")
		if gotErr := <-errCh; status.Code(gotErr) != codes.Unimplemented {
			t.Fatalf("expected Unimplemented, got %v", gotErr)
		}
		if got := srv.Calls(); len(got) != 0 {
			t.Fatalf("expected 0 leaked calls after Reset, got %+v", got)
		}
	})

	runInnerAttemptTest := func(t *testing.T, name, targetMethod string, invoke func(ctx context.Context, conn *grpc.ClientConn) error, addSentinel func(srv *Server)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			var armed atomic.Bool
			enteredCh := make(chan struct{}, 1)
			releaseCh := make(chan struct{})
			graceExpiredCh := make(chan struct{})

			hooks := &serverTestHooks{
				beforeInnerAttempt: func(fullMethod string, _ *epochState) {
					if fullMethod == targetMethod && armed.CompareAndSwap(true, false) {
						enteredCh <- struct{}{}
						select {
						case <-releaseCh:
						case <-ctx.Done():
						}
					}
				},
				onResetGraceExpired: func() {
					close(graceExpiredCh)
				},
			}
			t.Cleanup(func() {
				select {
				case <-releaseCh:
				default:
					close(releaseCh)
				}
			})

			srv, err := NewServer(withTestHooks(hooks))
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			t.Cleanup(func() { _ = srv.Close() })
			conn1 := dialFakeInternal(t, srv)
			conn2 := dialFakeInternal(t, srv)

			armed.Store(true)
			ep0ErrCh := make(chan error, 1)
			go func() {
				ep0ErrCh <- invoke(ctx, conn1)
			}()
			waitSignal(ctx, t, enteredCh, "beforeInnerAttempt")

			resetDoneCh := make(chan struct{})
			go func() {
				srv.Reset()
				close(resetDoneCh)
			}()

			select {
			case <-resetDoneCh:
				t.Fatalf("Reset returned while %s handler still held ep.wg", name)
			case <-graceExpiredCh:
			case <-ctx.Done():
				t.Fatalf("timed out waiting for graceExpiredCh: %v", ctx.Err())
			}

			// Register Attempt == 1 sentinel in ep1 BEFORE releasing stale ep0 handler.
			addSentinel(srv)
			close(releaseCh)

			waitSignal(ctx, t, resetDoneCh, "Reset completion")
			if ep0Err := <-ep0ErrCh; status.Code(ep0Err) != codes.Unavailable {
				t.Fatalf("expected stale ep0 %s to return Unavailable, got %v", name, ep0Err)
			}
			if got := srv.Calls(); len(got) != 0 {
				t.Fatalf("expected 0 calls after Reset, got %+v", got)
			}

			if ep1Err := invoke(ctx, conn2); status.Code(ep1Err) != codes.AlreadyExists {
				t.Fatalf("expected fresh ep1 Attempt=1 sentinel AlreadyExists, got %v", ep1Err)
			}
		})
	}

	runInnerAttemptTest(t, "Publish_inner_attempt_and_RuleLeak", MethodPublish,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m")}},
			})
			return err
		},
		func(srv *Server) {
			srv.AddPublishRule(PublishRule{Match: MatchCallAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
		},
	)

	runInnerAttemptTest(t, "Acknowledge_inner_attempt", MethodAcknowledge,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: "projects/p/subscriptions/s",
				AckIds:       []string{"a1"},
			})
			return err
		},
		func(srv *Server) {
			srv.AddAckRule(AckRule{Match: MatchAckAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
		},
	)

	runInnerAttemptTest(t, "ModifyAckDeadline_inner_attempt", MethodModifyAckDeadline,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       "projects/p/subscriptions/s",
				AckIds:             []string{"m1"},
				AckDeadlineSeconds: 60,
			})
			return err
		},
		func(srv *Server) {
			srv.AddModAckRule(ModAckRule{Match: MatchModAckAttempt(1), Err: status.Error(codes.AlreadyExists, "ep1"), Times: 1})
		},
	)

	runInnerAttemptTest(t, "Pull_inner_attempt", MethodPull,
		func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{
				Subscription: "projects/p/subscriptions/s",
				MaxMessages:  1,
			})
			return err
		},
		func(srv *Server) {
			srv.AddPullRule(PullRule{
				Match: func(mc *PullMatchContext) bool { return mc.Attempt == 1 },
				Err:   status.Error(codes.AlreadyExists, "ep1"),
				Times: 1,
			})
		},
	)

	t.Run("Publish_unaryInterceptor_defer_ep_guard", func(t *testing.T) {
		var armed atomic.Bool
		enteredCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		beforeResetWaitCh := make(chan struct{}, 1)
		ep0RPCEndCh := make(chan struct{}, 1)
		var ep0Ep atomic.Pointer[epochState]

		hooks := &serverTestHooks{
			beforeInnerAttempt: func(fullMethod string, ep *epochState) {
				if fullMethod == MethodPublish && armed.CompareAndSwap(true, false) {
					ep0Ep.Store(ep)
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			beforeResetWait: func() {
				select {
				case beforeResetWaitCh <- struct{}{}:
				default:
				}
			},
			onRPCEnd: func(_ string, ep *epochState) {
				if target := ep0Ep.Load(); target != nil && ep == target {
					select {
					case ep0RPCEndCh <- struct{}{}:
					default:
					}
				}
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn1 := dialFakeInternal(t, srv)
		conn2 := dialFakeInternal(t, srv)
		pub1 := pubsubpb.NewPublisherClient(conn1)
		pub2 := pubsubpb.NewPublisherClient(conn2)

		// Warm up both connections, then clean epoch.
		if _, err := pub1.Publish(ctx, &pubsubpb.PublishRequest{Topic: "projects/p/topics/w", Messages: []*pubsubpb.PubsubMessage{{Data: []byte("w1")}}}); err != nil {
			t.Fatalf("warmup conn1: %v", err)
		}
		if _, err := pub2.Publish(ctx, &pubsubpb.PublishRequest{Topic: "projects/p/topics/w", Messages: []*pubsubpb.PubsubMessage{{Data: []byte("w2")}}}); err != nil {
			t.Fatalf("warmup conn2: %v", err)
		}
		srv.Reset()
		// Drain buffered beforeResetWait notification from warmup Reset.
		select {
		case <-beforeResetWaitCh:
		default:
		}

		// Launch ep0 Publish with ordering key "k-shared", paused inside handlePublish.
		armed.Store(true)
		ep0ErrCh := make(chan error, 1)
		go func() {
			_, callErr := pub1.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("ep0"), OrderingKey: "k-shared"}},
			})
			ep0ErrCh <- callErr
		}()
		waitSignal(ctx, t, enteredCh, "ep0 beforeInnerAttempt")

		// Start Reset; once beforeResetWaitCh signals, s.epoch is already ep1 with activePublish=0.
		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()
		waitSignal(ctx, t, beforeResetWaitCh, "beforeResetWait")

		ep1Gate := NewGate()
		srv.AddPublishRule(PublishRule{
			Match: MatchOrderingKey("k-shared"),
			Gate:  ep1Gate,
			Times: 2,
		})

		var ep1WG sync.WaitGroup
		ep1WG.Add(1)
		go func() {
			defer ep1WG.Done()
			_, _ = pub2.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("ep1-1"), OrderingKey: "k-shared"}},
			})
		}()
		if err := ep1Gate.WaitEntered(ctx); err != nil {
			t.Fatalf("ep1Gate.WaitEntered: %v", err)
		}

		// Release stale ep0 call while ep1Pub1 is actively holding ep1 activePublishTotal=1!
		close(releaseCh)
		waitSignal(ctx, t, ep0RPCEndCh, "ep0RPCEndCh")
		waitSignal(ctx, t, resetDoneCh, "resetDoneCh")
		if ep0Err := <-ep0ErrCh; status.Code(ep0Err) != codes.Unavailable {
			t.Fatalf("expected ep0 Publish Unavailable, got %v", ep0Err)
		}

		// Start ep1Pub2 concurrently with ep1Pub1. Both must overlap at concurrency == 2.
		ep1WG.Add(1)
		go func() {
			defer ep1WG.Done()
			_, _ = pub2.Publish(ctx, &pubsubpb.PublishRequest{
				Topic:    "projects/p/topics/t",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("ep1-2"), OrderingKey: "k-shared"}},
			})
		}()
		if err := ep1Gate.WaitEnteredN(ctx, 2); err != nil {
			t.Fatalf("ep1Gate.WaitEnteredN(2): %v", err)
		}
		ep1Gate.Release()
		ep1WG.Wait()

		snap := srv.Snapshot()
		if snap.MaxConcurrentPublishTotal != 2 || snap.MaxConcurrentPublishByKey["k-shared"] != 2 {
			t.Fatalf("expected MaxConcurrentPublishTotal=2 and ByKey[k-shared]=2 (no ep0 defer clobber), got total=%d byKey=%v",
				snap.MaxConcurrentPublishTotal, snap.MaxConcurrentPublishByKey)
		}
	})

	t.Run("StreamingPull_preRecv_wg_wait", func(t *testing.T) {
		var armed atomic.Bool
		enteredCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		graceExpiredCh := make(chan struct{})

		hooks := &serverTestHooks{
			beforeStreamPreRecv: func(_ *epochState) {
				if armed.CompareAndSwap(true, false) {
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			onResetGraceExpired: func() {
				close(graceExpiredCh)
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn := dialFakeInternal(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		armed.Store(true)
		errCh := make(chan error, 1)
		go func() {
			sp, err := sub.StreamingPull(ctx)
			if err != nil {
				errCh <- err
				return
			}
			_, err = sp.Recv()
			errCh <- err
		}()
		waitSignal(ctx, t, enteredCh, "beforeStreamPreRecv")

		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()
		select {
		case <-resetDoneCh:
			t.Fatalf("Reset returned before StreamingPull pre-recv handler exited")
		case <-graceExpiredCh:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for graceExpiredCh: %v", ctx.Err())
		}

		close(releaseCh)
		waitSignal(ctx, t, resetDoneCh, "Reset completion")
		if gotErr := <-errCh; status.Code(gotErr) != codes.Unavailable {
			t.Fatalf("expected Unavailable, got %v", gotErr)
		}
		snap := srv.Snapshot()
		if len(snap.Calls) != 0 || len(snap.Streams) != 0 {
			t.Fatalf("expected 0 calls/streams after Reset, got %+v", snap)
		}
	})

	t.Run("StreamingPull_recvGoroutine_wg_wait", func(t *testing.T) {
		var armed atomic.Bool
		rpcEndCh := make(chan struct{}, 1)
		recvExitingCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		graceExpiredCh := make(chan struct{})

		hooks := &serverTestHooks{
			onRPCEnd: func(fullMethod string, _ *epochState) {
				if fullMethod == MethodStreamingPull {
					select {
					case rpcEndCh <- struct{}{}:
					default:
					}
				}
			},
			onStreamRecvExit: func(_ *epochState) {
				if armed.CompareAndSwap(true, false) {
					recvExitingCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			onResetGraceExpired: func() {
				close(graceExpiredCh)
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn := dialFakeInternal(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		armed.Store(true)
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
		if _, err := srv.WaitForStreamingPull(ctx); err != nil {
			t.Fatalf("WaitForStreamingPull: %v", err)
		}
		if err := sp.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		if _, err := sp.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF after CloseSend, got %v", err)
		}

		// Ensure BOTH the main handler has exited (calling its ep.wg.Done()) AND
		// the recv goroutine is paused right before its ep.wg.Done().
		waitSignal(ctx, t, rpcEndCh, "StreamingPull handler onRPCEnd")
		waitSignal(ctx, t, recvExitingCh, "onStreamRecvExit")

		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()
		select {
		case <-resetDoneCh:
			t.Fatalf("Reset returned while StreamingPull recv goroutine still held ep.wg")
		case <-graceExpiredCh:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for graceExpiredCh: %v", ctx.Err())
		}

		close(releaseCh)
		waitSignal(ctx, t, resetDoneCh, "Reset completion")
	})

	t.Run("StreamingPull_inner_script_match", func(t *testing.T) {
		var armed atomic.Bool
		enteredCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		beforeResetWaitCh := make(chan struct{}, 1)

		hooks := &serverTestHooks{
			beforeStreamScript: func(_ *epochState) {
				if armed.CompareAndSwap(true, false) {
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			beforeResetWait: func() {
				select {
				case beforeResetWaitCh <- struct{}{}:
				default:
				}
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn1 := dialFakeInternal(t, srv)
		conn2 := dialFakeInternal(t, srv)
		sub1 := pubsubpb.NewSubscriberClient(conn1)
		sub2 := pubsubpb.NewSubscriberClient(conn2)

		armed.Store(true)
		sp0, err := sub1.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("sp0 open: %v", err)
		}
		if err := sp0.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("sp0 send: %v", err)
		}
		waitSignal(ctx, t, enteredCh, "beforeStreamScript")

		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()
		waitSignal(ctx, t, beforeResetWaitCh, "beforeResetWait")

		var onOpenCount atomic.Int32
		srv.AddStreamingPullScript(StreamingPullScript{
			Times:        1,
			InitialError: status.Error(codes.PermissionDenied, "ep1-script"),
			OnOpen: func(*StreamHandle) {
				onOpenCount.Add(1)
			},
		})
		close(releaseCh)

		waitSignal(ctx, t, resetDoneCh, "Reset completion")
		if _, err := sp0.Recv(); status.Code(err) != codes.Unavailable {
			t.Fatalf("expected stale ep0 stream to return Unavailable, got %v", err)
		}
		if onOpenCount.Load() != 0 {
			t.Fatalf("ep1 script OnOpen must not run on stale ep0")
		}
		snap := srv.Snapshot()
		if len(snap.Calls) != 0 || len(snap.Streams) != 0 {
			t.Fatalf("expected 0 leaked calls/streams after Reset, got %+v", snap)
		}

		// Verify ep1 script remained unconsumed and fires on fresh ep1 stream.
		sp1, err := sub2.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("sp1 open: %v", err)
		}
		_ = sp1.Send(&pubsubpb.StreamingPullRequest{
			Subscription:             "projects/p/subscriptions/s",
			StreamAckDeadlineSeconds: 30,
		})
		_ = sp1.CloseSend()
		if _, err := sp1.Recv(); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected ep1 StreamingPullScript PermissionDenied, got %v", err)
		}
	})

	t.Run("StreamingPull_inner_script_match_invalid_frame0", func(t *testing.T) {
		var armed atomic.Bool
		enteredCh := make(chan struct{}, 1)
		releaseCh := make(chan struct{})
		beforeResetWaitCh := make(chan struct{}, 1)
		rpcEndCh := make(chan struct{}, 1)

		hooks := &serverTestHooks{
			beforeStreamScript: func(_ *epochState) {
				if armed.CompareAndSwap(true, false) {
					enteredCh <- struct{}{}
					select {
					case <-releaseCh:
					case <-ctx.Done():
					}
				}
			},
			beforeResetWait: func() {
				select {
				case beforeResetWaitCh <- struct{}{}:
				default:
				}
			},
			onRPCEnd: func(_ string, _ *epochState) {
				select {
				case rpcEndCh <- struct{}{}:
				default:
				}
			},
		}
		t.Cleanup(func() {
			select {
			case <-releaseCh:
			default:
				close(releaseCh)
			}
		})

		srv, err := NewServer(WithStrictStreamingPullValidation(true), withTestHooks(hooks))
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		conn := dialFakeInternal(t, srv)
		sub := pubsubpb.NewSubscriberClient(conn)

		armed.Store(true)
		sp0, err := sub.StreamingPull(ctx)
		if err != nil {
			t.Fatalf("sp0 open: %v", err)
		}
		// Empty subscription fails strict frame-0 validation -> bypasses script match
		// and directly exercises recordStreamingPullFrame's s.epoch != ep guard.
		if err := sp0.Send(&pubsubpb.StreamingPullRequest{
			StreamAckDeadlineSeconds: 30,
		}); err != nil {
			t.Fatalf("sp0 send invalid frame 0: %v", err)
		}
		waitSignal(ctx, t, enteredCh, "beforeStreamScript")

		resetDoneCh := make(chan struct{})
		go func() {
			srv.Reset()
			close(resetDoneCh)
		}()
		waitSignal(ctx, t, beforeResetWaitCh, "beforeResetWait")
		close(releaseCh)

		waitSignal(ctx, t, resetDoneCh, "Reset completion")
		_, _ = sp0.Recv()
		waitSignal(ctx, t, rpcEndCh, "onRPCEnd")

		snap := srv.Snapshot()
		if len(snap.Calls) != 0 || len(snap.Streams) != 0 {
			t.Fatalf("expected 0 leaked calls/streams for invalid frame 0 across Reset, got %+v", snap)
		}
	})
}

// White-box direct unit tests (merged from Explorer 2's blueprint to stay at 10 .go files total).

type fakeDirectServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeDirectServerStream) Context() context.Context {
	return s.ctx
}

type fakeRecvDeadlineStream struct {
	pubsubpb.Subscriber_StreamingPullServer
	ctx  context.Context
	reqs []*pubsubpb.StreamingPullRequest
	idx  int
}

func (s *fakeRecvDeadlineStream) Context() context.Context     { return s.ctx }
func (s *fakeRecvDeadlineStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeRecvDeadlineStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeRecvDeadlineStream) SetTrailer(metadata.MD)       {}
func (s *fakeRecvDeadlineStream) SendMsg(any) error            { return nil }
func (s *fakeRecvDeadlineStream) RecvMsg(any) error            { return nil }
func (s *fakeRecvDeadlineStream) Send(*pubsubpb.StreamingPullResponse) error {
	return nil
}
func (s *fakeRecvDeadlineStream) Recv() (*pubsubpb.StreamingPullRequest, error) {
	if s.idx < len(s.reqs) {
		req := s.reqs[s.idx]
		s.idx++
		return req, nil
	}
	return nil, status.Error(codes.DeadlineExceeded, "context deadline exceeded")
}

func TestShouldRecordStreamingPullRecvErrorDeterministic(t *testing.T) {
	t.Run("direct_helper_cases", func(t *testing.T) {
		bg := context.Background()
		dlCtx, dlCancel := context.WithDeadline(bg, time.Now().Add(-time.Second))
		defer dlCancel()
		cCtx, cCancel := context.WithCancel(bg)
		cCancel()

		if shouldRecordStreamingPullRecvError(bg, status.Error(codes.Canceled, "canceled")) {
			t.Fatalf("codes.Canceled must not be recorded")
		}
		if shouldRecordStreamingPullRecvError(dlCtx, status.Error(codes.DeadlineExceeded, "context deadline exceeded")) {
			t.Fatalf("DeadlineExceeded matching ctx.Err()==DeadlineExceeded must not be recorded")
		}
		if !shouldRecordStreamingPullRecvError(bg, status.Error(codes.DeadlineExceeded, "unrelated")) {
			t.Fatalf("DeadlineExceeded with non-expired ctx must still be recorded")
		}
		if !shouldRecordStreamingPullRecvError(cCtx, status.Error(codes.DeadlineExceeded, "canceled-ctx")) {
			t.Fatalf("DeadlineExceeded with Canceled ctx must still be recorded")
		}
		if !shouldRecordStreamingPullRecvError(dlCtx, status.Error(codes.ResourceExhausted, "oversize")) {
			t.Fatalf("ResourceExhausted must always be recorded")
		}
	})

	t.Run("handleStreamingPull_pre_frame_0_deadline", func(t *testing.T) {
		srv, err := NewServer()
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		defer func() { _ = srv.Close() }()

		dlCtx, dlCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer dlCancel()
		stream := &fakeRecvDeadlineStream{ctx: dlCtx}

		gotErr := srv.handleStreamingPull(stream)
		if status.Code(gotErr) != codes.DeadlineExceeded {
			t.Fatalf("expected DeadlineExceeded, got %v", gotErr)
		}
		snap := srv.Snapshot()
		if len(snap.StreamingPullCalls()) != 0 {
			t.Fatalf("expected 0 StreamingPull calls on DeadlineExceeded Recv(), got %+v", snap.StreamingPullCalls())
		}
		if len(snap.Streams) != 1 || snap.Streams[0].CloseCode != codes.DeadlineExceeded {
			t.Fatalf("expected 1 stream with CloseCode=DeadlineExceeded, got %+v", snap.Streams)
		}
	})

	t.Run("handleStreamingPull_post_frame_0_deadline", func(t *testing.T) {
		srv, err := NewServer()
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		defer func() { _ = srv.Close() }()

		dlCtx, dlCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer dlCancel()
		stream := &fakeRecvDeadlineStream{
			ctx: dlCtx,
			reqs: []*pubsubpb.StreamingPullRequest{
				{Subscription: "projects/p/subscriptions/s", StreamAckDeadlineSeconds: 30},
			},
		}

		gotErr := srv.handleStreamingPull(stream)
		if status.Code(gotErr) != codes.DeadlineExceeded {
			t.Fatalf("expected DeadlineExceeded, got %v", gotErr)
		}
		snap := srv.Snapshot()
		calls := snap.StreamingPullCalls()
		if len(calls) != 1 || calls[0].StatusCode != codes.OK || calls[0].CodecError != "" {
			t.Fatalf("expected strictly 1 frame-0 OK call without CodecError, got %+v", calls)
		}
		if len(snap.Streams) != 1 || snap.Streams[0].CloseCode != codes.DeadlineExceeded {
			t.Fatalf("expected CloseCode=DeadlineExceeded, got %+v", snap.Streams)
		}
	})
}

func TestEpochIsolationEntryPointsRejectStaleEpoch(t *testing.T) {
	srv, err := NewServer()
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	srv.mu.Lock()
	oldEp := srv.epoch
	srv.mu.Unlock()

	srv.Reset()

	staleCtx := context.WithValue(context.Background(), rpcMetaKey{}, &rpcMeta{
		fullMethod: MethodPublish,
		ep:         oldEp,
	})

	var handlerCalled atomic.Bool
	_, err = srv.unaryInterceptor(staleCtx, &pubsubpb.PublishRequest{}, &grpc.UnaryServerInfo{FullMethod: MethodPublish}, func(context.Context, any) (any, error) {
		handlerCalled.Store(true)
		return &pubsubpb.PublishResponse{}, nil
	})
	if status.Code(err) != codes.Unavailable || handlerCalled.Load() {
		t.Fatalf("expected unaryInterceptor to reject stale epoch with Unavailable without invoking handler, err=%v called=%v", err, handlerCalled.Load())
	}

	unknownCtx := context.WithValue(context.Background(), rpcMetaKey{}, &rpcMeta{
		fullMethod: "/google.pubsub.v1.Publisher/CreateTopic",
		ep:         oldEp,
	})
	err = srv.handleUnknownRPC(nil, &fakeDirectServerStream{ctx: unknownCtx})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected handleUnknownRPC stale epoch Unavailable, got %v", err)
	}

	spCtx := context.WithValue(context.Background(), rpcMetaKey{}, &rpcMeta{
		fullMethod: MethodStreamingPull,
		ep:         oldEp,
	})
	err = srv.handleStreamingPull(&fakeRecvDeadlineStream{ctx: spCtx})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected handleStreamingPull stale epoch Unavailable, got %v", err)
	}

	snap := srv.Snapshot()
	if len(snap.Calls) != 0 || len(snap.Streams) != 0 {
		t.Fatalf("expected 0 leaked calls/streams, got %+v", snap)
	}
	srv.mu.Lock()
	streamAttempts := srv.streamAttempts
	streamIDCounter := srv.streamIDCounter
	srv.mu.Unlock()
	if streamAttempts != 0 || streamIDCounter != 0 {
		t.Fatalf("expected streamAttempts=0 and streamIDCounter=0, got %d, %d", streamAttempts, streamIDCounter)
	}

	// Ensure TagRPC in new epoch tags s.epoch and recordTransportError ignores stale oldEp.
	sh := &serverStatsHandler{
		tagEpoch: func() any {
			srv.mu.Lock()
			ep := srv.epoch
			srv.mu.Unlock()
			return ep
		},
		onTransportErr: srv.recordTransportError,
	}
	sh.HandleRPC(context.Background(), &stats.End{Error: status.Error(codes.Internal, "ignored")}) // nil meta safe
	srv.recordTransportError(context.Background(), &rpcMeta{fullMethod: MethodPublish, ep: oldEp}, &stats.End{Error: status.Error(codes.ResourceExhausted, "stale")})
	if len(srv.Calls()) != 0 {
		t.Fatalf("recordTransportError leaked stale call")
	}
}

func TestEpochIsolationInnerHandlersAndStreamScript(t *testing.T) {
	srv, err := NewServer()
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	srv.mu.Lock()
	oldEp := srv.epoch
	srv.mu.Unlock()

	srv.Reset()

	// Register ep1 Attempt == 1 rules and StreamingPullScript.
	srv.AddPublishRule(PublishRule{Match: MatchCallAttempt(1), Err: status.Error(codes.AlreadyExists, "pub-1"), Times: 1})
	srv.AddAckRule(AckRule{Match: MatchAckAttempt(1), Err: status.Error(codes.AlreadyExists, "ack-1"), Times: 1})
	srv.AddModAckRule(ModAckRule{Match: MatchModAckAttempt(1), Err: status.Error(codes.AlreadyExists, "modack-1"), Times: 1})
	srv.AddPullRule(PullRule{
		Match: func(mc *PullMatchContext) bool { return mc.Attempt == 1 },
		Err:   status.Error(codes.AlreadyExists, "pull-1"),
		Times: 1,
	})
	srv.AddStreamingPullScript(StreamingPullScript{
		Times:        1,
		InitialError: status.Error(codes.AlreadyExists, "sp-1"),
	})

	staleCtx := context.WithValue(context.Background(), activeCallKey{}, &activeCallContext{ep: oldEp})

	if _, err := srv.handlePublish(staleCtx, &pubsubpb.PublishRequest{
		Topic:    "projects/p/topics/t",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("m")}},
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale handlePublish: want Unavailable, got %v", err)
	}
	if _, err := srv.handleAcknowledge(staleCtx, &pubsubpb.AcknowledgeRequest{
		Subscription: "projects/p/subscriptions/s",
		AckIds:       []string{"a1"},
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale handleAcknowledge: want Unavailable, got %v", err)
	}
	if _, err := srv.handleModifyAckDeadline(staleCtx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription:       "projects/p/subscriptions/s",
		AckIds:             []string{"m1"},
		AckDeadlineSeconds: 60,
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale handleModifyAckDeadline: want Unavailable, got %v", err)
	}
	if _, err := srv.handlePull(staleCtx, &pubsubpb.PullRequest{
		Subscription: "projects/p/subscriptions/s",
		MaxMessages:  1,
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale handlePull: want Unavailable, got %v", err)
	}

	if _, _, err := srv.matchStreamingPullScript(oldEp, oldEp.ctx, 1, nil, &pubsubpb.StreamingPullRequest{
		Subscription:             "projects/p/subscriptions/s",
		StreamAckDeadlineSeconds: 30,
	}, metadata.MD{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stale matchStreamingPullScript: want Unavailable, got %v", err)
	}

	srv.mu.Lock()
	if srv.publishAttempts != 0 || srv.ackAttempts != 0 || srv.modAckAttempts != 0 || srv.pullAttempts != 0 {
		srv.mu.Unlock()
		t.Fatalf("stale inner handlers incremented ep1 attempt counters: pub=%d ack=%d mod=%d pull=%d",
			srv.publishAttempts, srv.ackAttempts, srv.modAckAttempts, srv.pullAttempts)
	}
	srv.mu.Unlock()

	// Verify all ep1 Attempt == 1 rules and StreamingPullScript remain unconsumed.
	if _, err := srv.handlePublish(context.Background(), &pubsubpb.PublishRequest{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ep1 Publish Attempt=1 rule lost: %v", err)
	}
	if _, err := srv.handleAcknowledge(context.Background(), &pubsubpb.AcknowledgeRequest{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ep1 Ack Attempt=1 rule lost: %v", err)
	}
	if _, err := srv.handleModifyAckDeadline(context.Background(), &pubsubpb.ModifyAckDeadlineRequest{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ep1 ModAck Attempt=1 rule lost: %v", err)
	}
	if _, err := srv.handlePull(context.Background(), &pubsubpb.PullRequest{}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ep1 Pull Attempt=1 rule lost: %v", err)
	}
	srv.mu.Lock()
	ep1 := srv.epoch
	srv.mu.Unlock()
	matched, _, err := srv.matchStreamingPullScript(ep1, ep1.ctx, 1, nil, &pubsubpb.StreamingPullRequest{}, metadata.MD{})
	if err != nil || matched == nil || status.Code(matched.InitialError) != codes.AlreadyExists {
		t.Fatalf("ep1 StreamingPullScript lost: matched=%+v err=%v", matched, err)
	}
}

type spyNetConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *spyNetConn) Close() error {
	c.closed.Store(true)
	return nil
}

func TestScheduleForceCloseClosedChPriority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range 200 {
		doneCh := make(chan struct{})
		srv := &Server{
			testHooks: &serverTestHooks{
				onForceCloseExit: func() {
					close(doneCh)
				},
			},
		}
		closedCh := make(chan struct{})
		close(closedCh)
		spy := &spyNetConn{}
		st := &streamSession{
			srv:      srv,
			conn:     spy,
			closedCh: closedCh,
		}
		st.scheduleForceClose(0)
		waitSignal(ctx, t, doneCh, "onForceCloseExit")
		if spy.closed.Load() {
			t.Fatalf("iter %d: scheduleForceClose closed connection even though closedCh was already closed", i)
		}
	}
}

func TestValidateStreamingPullFrameNil(t *testing.T) {
	if err := validateStreamingPullFrame(nil, true, true); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument on nil StreamingPullRequest, got %v", err)
	}
}
