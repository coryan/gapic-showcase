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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxAckBatchCount = 1000
	maxAckBatchBytes = 512 * 1024

	defaultKeepalivePingInterval = 30 * time.Second
	defaultKeepalivePongTimeout  = 15 * time.Second
	leaseGracePeriod             = 5 * time.Second
)

type subFlowController struct {
	mu       sync.Mutex
	cond     *sync.Cond
	maxMsgs  int64
	maxBytes int64
	curMsgs  int64
	curBytes int64
	maxConc  int32
	curConc  int32
}

func newSubFlowController(maxMsgs, maxBytes int64, maxConc int32) *subFlowController {
	fc := &subFlowController{
		maxMsgs:  maxMsgs,
		maxBytes: maxBytes,
		maxConc:  maxConc,
	}
	fc.cond = sync.NewCond(&fc.mu)
	return fc
}

func (fc *subFlowController) acquire(ctx context.Context, msgBytes int64) (int64, bool) {
	bounded := msgBytes
	if fc.maxBytes > 0 && bounded > fc.maxBytes {
		bounded = fc.maxBytes
	}
	stopWatch := context.AfterFunc(ctx, func() {
		fc.mu.Lock()
		fc.cond.Broadcast()
		fc.mu.Unlock()
	})
	defer stopWatch()

	fc.mu.Lock()
	defer fc.mu.Unlock()
	for {
		if ctx.Err() != nil {
			return 0, false
		}
		msgsOK := fc.maxMsgs <= 0 || fc.curMsgs+1 <= fc.maxMsgs
		bytesOK := fc.maxBytes <= 0 || fc.curBytes+bounded <= fc.maxBytes
		concOK := fc.maxConc <= 0 || fc.curConc+1 <= fc.maxConc
		if msgsOK && bytesOK && concOK {
			fc.curMsgs++
			fc.curBytes += bounded
			fc.curConc++
			return bounded, true
		}
		fc.cond.Wait()
	}
}

func (fc *subFlowController) release(bounded int64) {
	fc.mu.Lock()
	fc.curMsgs--
	if fc.curMsgs < 0 {
		fc.curMsgs = 0
	}
	fc.curBytes -= bounded
	if fc.curBytes < 0 {
		fc.curBytes = 0
	}
	fc.curConc--
	if fc.curConc < 0 {
		fc.curConc = 0
	}
	fc.cond.Broadcast()
	fc.mu.Unlock()
}

type settleReq struct {
	action proxypb.SettleMessageCommand_SettleAction
}

type settlementWaiter struct {
	ackID      string
	entry      *deliveryEntry
	isNack     bool
	wantResult bool
	skipTrace  bool
	doneCh     chan struct{}
}

type receivedBatch struct {
	rms     []*pubsubpb.ReceivedMessage
	eod     bool
	ordered bool
}

type deliveryEntry struct {
	token           string
	ackID           string
	msg             *pubsubpb.PubsubMessage
	deliveryAttempt int32
	eod             bool
	boundedFC       int64
	fc              *subFlowController
	trace           *subscribeMessageTrace
	settleCh        chan settleReq
	expiredCh       chan struct{}
	maxExtAt        time.Time
	hasMaxExt       bool
	expired         atomic.Bool
	settled         atomic.Bool
	fcReleased      atomic.Bool
	returned        atomic.Bool
	ccDone          bool
	callbackExited  bool
	drainingSettles bool
	deferredSettles []proxypb.SettleMessageCommand_SettleAction
}

func (d *deliveryEntry) releaseFC() {
	if d.fcReleased.CompareAndSwap(false, true) && d.fc != nil {
		d.fc.release(d.boundedFC)
	}
}

// Caller must hold s.mu. Sets callbackExited=true and drains all queued settles
// in strict FIFO order (settleCh precedes deferredSettles).
func (d *deliveryEntry) takePendingSettlesLocked() []proxypb.SettleMessageCommand_SettleAction {
	d.callbackExited = true
	var actions []proxypb.SettleMessageCommand_SettleAction
	for {
		select {
		case req := <-d.settleCh:
			actions = append(actions, req.action)
		default:
			if len(d.deferredSettles) > 0 {
				actions = append(actions, d.deferredSettles...)
				d.deferredSettles = nil
			}
			return actions
		}
	}
}

func (s *subscriberSession) isPastMaxExt(entry *deliveryEntry, now time.Time) bool {
	return entry.hasMaxExt && !now.Before(entry.maxExtAt)
}

func (s *subscriberSession) expireEntryIfPastDeadline(entry *deliveryEntry, now time.Time, preDelivery bool) bool {
	if entry.expired.Load() {
		return true
	}
	if !s.isPastMaxExt(entry, now) {
		return false
	}
	s.mu.Lock()
	if entry.settled.Load() {
		s.mu.Unlock()
		return false
	}
	won := entry.expired.CompareAndSwap(false, true)
	if won {
		delete(s.activeLeases, entry.token)
		if preDelivery {
			delete(s.pendingDeliveries, entry.token)
		}
		entry.trace.markExpired()
	}
	s.mu.Unlock()
	if won {
		close(entry.expiredCh)
		entry.trace.endScheduler()
		entry.trace.endSubscribe("expired", "")
	}
	return entry.expired.Load()
}

type subscriberSession struct {
	stream proxypb.PubSubProxy_SubscriberSessionServer
	srv    *ProxyServer
	cfg    *ProfileConfig
	traces *TraceStore
	start  *proxypb.StartSubscriberCommand

	conn   *grpc.ClientConn
	client pubsubpb.SubscriberClient

	fullSub   string
	subID     string
	projectID string
	clientID  string

	enableMonitoring bool
	isEOD            atomic.Bool
	msgOrdering      atomic.Bool

	maxExtension    time.Duration // <0 unlimited
	minAckExt       time.Duration
	maxAckExt       time.Duration
	streamDeadlineS int32

	numStreams         int
	perStreamFC        bool
	maxOutstandingMsgs int64
	maxOutstandingByte int64
	maxConcurrency     int32
	sharedFC           *subFlowController

	retryCodes        map[codes.Code]bool
	useCustomRetry    bool
	initialRetryDelay time.Duration
	maxRetryDelay     time.Duration
	retryMultiplier   float64

	shutdownBehavior proxypb.ShutdownBehavior
	shutdownTimeout  time.Duration

	customHeaders map[string]string

	sendCh     chan *proxypb.SubscriberEvent
	senderDone chan struct{}

	sessionCtx    context.Context
	sessionCancel context.CancelFunc

	pullCtx    context.Context
	pullCancel context.CancelFunc

	abortCtx    context.Context
	abortCancel context.CancelFunc

	callbackAbortCh   chan struct{}
	callbackAbortOnce sync.Once

	mu                sync.Mutex
	pendingDeliveries map[string]*deliveryEntry // delivery_token -> entry
	activeLeases      map[string]*deliveryEntry // delivery_token -> entry
	pendingAcks       []*settlementWaiter
	pendingNacks      []*settlementWaiter
	keyChains         map[string]chan struct{}
	deliverChain      chan struct{}
	stopping          bool
	settlementStopped bool
	leaseStopped      bool
	stopBehavior      proxypb.ShutdownBehavior
	stopTimeout       time.Duration
	stopTriggerCh     chan struct{}
	stopTriggerOnce   sync.Once
	flushTriggerCh    chan struct{}
	armFlushCh        chan struct{}
	extResetCh        chan struct{}
	fatalErrCode      int32
	fatalErrMsg       string

	deliverySeq     atomic.Int64
	activeCallbacks atomic.Int32
	totalDelivered  atomic.Int64

	callbackWG sync.WaitGroup
	detachedWG sync.WaitGroup
	streamWG   sync.WaitGroup
	leaseWG    sync.WaitGroup
}

func runSubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer, srv *ProxyServer) error {
	firstCmd, err := stream.Recv()
	if err != nil {
		return err
	}
	start := firstCmd.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first SubscriberCommand must be StartSubscriberCommand")
	}

	cfg := srv.cfg
	if !cfg.SupportsPerStreamFlowControl && start.EnablePerStreamFlowControl != nil && *start.EnablePerStreamFlowControl {
		return status.Error(codes.Unimplemented, "per-stream flow control not supported")
	}
	if !cfg.SupportsExactlyOnceDelivery && start.UseExactlyOnce {
		return status.Error(codes.Unimplemented, "exactly-once delivery not supported")
	}
	if !cfg.SupportsShutdownBehavior && start.ShutdownBehavior != proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_UNSPECIFIED {
		return status.Error(codes.Unimplemented, "shutdown behavior not supported")
	}
	if !cfg.SupportsShutdownTimeout && start.ShutdownTimeoutMs != nil {
		return status.Error(codes.Unimplemented, "shutdown timeout not supported")
	}
	if !cfg.SupportsAckWithResult && (start.AutoSettleMode == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT ||
		start.AutoSettleMode == proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT) {
		return status.Error(codes.Unimplemented, "ack/nack with result not supported")
	}
	if !cfg.SupportsCustomRetryCodes && len(start.RetryCodes) > 0 {
		return status.Error(codes.Unimplemented, "custom retry codes not supported")
	}

	projID, subID, fullSub, err := extractResourceNames(start.Subscription, "subscriptions", start.ProjectId)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if cfg.HasFault("hang-subscribe") {
		select {
		case <-srv.stopCh:
			return status.Error(codes.Unavailable, "proxy shutting down")
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}

	if cfg.Delay > 0 {
		select {
		case <-time.After(cfg.Delay):
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}

	if cfg.HasFault("missing-rpc") {
		return runMissingRPCSubscriberSession(stream, start)
	}

	conn, err := dialPubSubEndpoint(start.Endpoint, start.UseEmulatorEnv)
	if err != nil {
		return err
	}
	defer conn.Close()

	clientID := strings.TrimSpace(start.ClientId)
	if clientID == "" {
		clientID = uuid.NewString()
	}

	maxExt := time.Duration(cfg.Defaults.SubscriberMaxExtensionMs) * time.Millisecond
	if cfg.Defaults.SubscriberMaxExtensionMs <= 0 {
		maxExt = -1
	}
	if start.MaxExtensionMs != nil {
		if *start.MaxExtensionMs < 0 {
			maxExt = -1
		} else {
			maxExt = time.Duration(*start.MaxExtensionMs) * time.Millisecond
		}
	}

	minAckExt := time.Duration(cfg.Defaults.SubscriberMinAckExtensionMs) * time.Millisecond
	if minAckExt <= 0 {
		minAckExt = 10 * time.Second
	}
	if start.MinDurationPerAckExtensionMs != nil && *start.MinDurationPerAckExtensionMs > 0 {
		minAckExt = time.Duration(*start.MinDurationPerAckExtensionMs) * time.Millisecond
	}

	maxAckExt := time.Duration(cfg.Defaults.SubscriberMaxAckExtensionMs) * time.Millisecond
	if maxAckExt <= 0 {
		maxAckExt = 600 * time.Second
	}
	if start.MaxDurationPerAckExtensionMs != nil && *start.MaxDurationPerAckExtensionMs > 0 {
		maxAckExt = time.Duration(*start.MaxDurationPerAckExtensionMs) * time.Millisecond
	}

	streamDL := cfg.Defaults.SubscriberDefaultStreamAckDeadlineSeconds
	if streamDL <= 0 {
		streamDL = 60
	}
	if start.MaxDurationPerAckExtensionMs != nil && *start.MaxDurationPerAckExtensionMs > 0 {
		dlSec := int32(*start.MaxDurationPerAckExtensionMs / 1000)
		if dlSec < 10 {
			dlSec = 10
		}
		if dlSec > 600 {
			dlSec = 600
		}
		streamDL = dlSec
	}

	numStreams := 1
	if start.NumStreams != nil && *start.NumStreams > 0 {
		numStreams = int(*start.NumStreams)
		if numStreams > 16 {
			numStreams = 16
		}
	}
	perStreamFC := cfg.Defaults.SubscriberPerStreamFlowControl
	if start.EnablePerStreamFlowControl != nil {
		perStreamFC = *start.EnablePerStreamFlowControl
	}
	maxOutMsgs := cfg.Defaults.SubscriberMaxOutstandingMessages
	if start.MaxOutstandingMessages != nil {
		maxOutMsgs = *start.MaxOutstandingMessages
	}
	maxOutBytes := cfg.Defaults.SubscriberMaxOutstandingBytes
	if start.MaxOutstandingBytes != nil {
		maxOutBytes = *start.MaxOutstandingBytes
	}
	var maxConc int32
	if start.MaxConcurrency != nil && *start.MaxConcurrency > 0 {
		maxConc = *start.MaxConcurrency
	}

	initRetry := time.Duration(cfg.Defaults.RetryInitialDelayMs) * time.Millisecond
	if initRetry <= 0 {
		initRetry = 100 * time.Millisecond
	}
	if start.InitialRetryDelayMs != nil && *start.InitialRetryDelayMs > 0 {
		initRetry = time.Duration(*start.InitialRetryDelayMs) * time.Millisecond
	}
	maxRetry := time.Duration(cfg.Defaults.RetryMaxDelayMs) * time.Millisecond
	if maxRetry <= 0 {
		maxRetry = 60 * time.Second
	}
	if start.MaxRetryDelayMs != nil && *start.MaxRetryDelayMs > 0 {
		maxRetry = time.Duration(*start.MaxRetryDelayMs) * time.Millisecond
	}
	mult := cfg.Defaults.RetryDelayMultiplier
	if mult <= 1.0 {
		mult = 2.0
	}
	if start.RetryDelayMultiplier != nil && *start.RetryDelayMultiplier > 1.0 {
		mult = *start.RetryDelayMultiplier
	}

	var retryCodes map[codes.Code]bool
	var useCustomRetry bool
	if len(start.RetryCodes) > 0 {
		useCustomRetry = true
		retryCodes = make(map[codes.Code]bool, len(start.RetryCodes))
		for _, c := range start.RetryCodes {
			retryCodes[codes.Code(c)] = true
		}
	}

	shutdownBehavior := start.ShutdownBehavior
	if shutdownBehavior == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_UNSPECIFIED {
		shutdownBehavior = proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING
	}
	shutdownTimeout := 30 * time.Second
	if start.ShutdownTimeoutMs != nil {
		if *start.ShutdownTimeoutMs < 0 {
			shutdownTimeout = -1
		} else {
			shutdownTimeout = time.Duration(*start.ShutdownTimeoutMs) * time.Millisecond
		}
	}

	sessCtx, sessCancel := context.WithCancel(context.Background())
	pullCtx, pullCancel := context.WithCancel(sessCtx)
	abortCtx, abortCancel := context.WithCancel(sessCtx)

	sess := &subscriberSession{
		stream:             stream,
		srv:                srv,
		cfg:                cfg,
		traces:             srv.traces,
		start:              start,
		conn:               conn,
		client:             pubsubpb.NewSubscriberClient(conn),
		fullSub:            fullSub,
		subID:              subID,
		projectID:          projID,
		clientID:           clientID,
		enableMonitoring:   start.EnableMonitoring,
		maxExtension:       maxExt,
		minAckExt:          minAckExt,
		maxAckExt:          maxAckExt,
		streamDeadlineS:    streamDL,
		numStreams:         numStreams,
		perStreamFC:        perStreamFC,
		maxOutstandingMsgs: maxOutMsgs,
		maxOutstandingByte: maxOutBytes,
		maxConcurrency:     maxConc,
		retryCodes:         retryCodes,
		useCustomRetry:     useCustomRetry,
		initialRetryDelay:  initRetry,
		maxRetryDelay:      maxRetry,
		retryMultiplier:    mult,
		shutdownBehavior:   shutdownBehavior,
		shutdownTimeout:    shutdownTimeout,
		customHeaders:      maps.Clone(start.CustomHeaders),
		sendCh:             make(chan *proxypb.SubscriberEvent, 512),
		senderDone:         make(chan struct{}),
		sessionCtx:         sessCtx,
		sessionCancel:      sessCancel,
		pullCtx:            pullCtx,
		pullCancel:         pullCancel,
		abortCtx:           abortCtx,
		abortCancel:        abortCancel,
		callbackAbortCh:    make(chan struct{}),
		pendingDeliveries:  make(map[string]*deliveryEntry),
		activeLeases:       make(map[string]*deliveryEntry),
		keyChains:          make(map[string]chan struct{}),
		stopTriggerCh:      make(chan struct{}),
		flushTriggerCh:     make(chan struct{}, 16),
		armFlushCh:         make(chan struct{}, 1),
		extResetCh:         make(chan struct{}, 1),
	}
	sess.isEOD.Store(start.UseExactlyOnce)
	sess.msgOrdering.Store(false)
	if !perStreamFC {
		sess.sharedFC = newSubFlowController(maxOutMsgs, maxOutBytes, maxConc)
	}

	return sess.run()
}

func runMissingRPCSubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer, start *proxypb.StartSubscriberCommand) error {
	cid := start.ClientId
	if cid == "" {
		cid = uuid.NewString()
	}
	if err := stream.Send(&proxypb.SubscriberEvent{
		Event: &proxypb.SubscriberEvent_Started{
			Started: &proxypb.StartedEvent{ClientId: cid},
		},
	}); err != nil {
		return err
	}
	for {
		cmd, err := stream.Recv()
		if err != nil {
			return nil
		}
		if cmd.GetStop() != nil {
			return stream.Send(&proxypb.SubscriberEvent{
				Event: &proxypb.SubscriberEvent_Stopped{
					Stopped: &proxypb.StoppedEvent{TotalDelivered: 0},
				},
			})
		}
	}
}

func (s *subscriberSession) run() error {
	go func() {
		select {
		case <-s.srv.stopCh:
			s.sessionCancel()
		case <-s.sessionCtx.Done():
		}
	}()

	// Dedicated event sender goroutine serializing stream.Send calls.
	go func() {
		defer close(s.senderDone)
		for ev := range s.sendCh {
			if err := s.stream.Send(ev); err != nil {
				return
			}
		}
	}()

	// Emit StartedEvent once stream is ready.
	s.emit(&proxypb.SubscriberEvent{
		Event: &proxypb.SubscriberEvent_Started{
			Started: &proxypb.StartedEvent{ClientId: s.clientID},
		},
	})

	// Continuous command reader goroutine (must keep running Settle commands even after Stop).
	cmdReaderDone := make(chan struct{})
	go func() {
		defer close(cmdReaderDone)
		for {
			cmd, err := s.stream.Recv()
			if err != nil {
				s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
				return
			}
			switch c := cmd.Command.(type) {
			case *proxypb.SubscriberCommand_Settle:
				if c.Settle != nil {
					s.handleSettleCommand(c.Settle)
				}
			case *proxypb.SubscriberCommand_Stop:
				var bh proxypb.ShutdownBehavior
				var tm *int64
				if c.Stop != nil {
					bh = c.Stop.ShutdownBehavior
					tm = c.Stop.ShutdownTimeoutMs
				}
				s.triggerStop(bh, tm)
			}
		}
	}()

	// Start lease extension/expiry and settlement batch loops separately so
	// settlement retries never block lease expiration or extension ticks.
	leaseStopCh := make(chan struct{})
	s.leaseWG.Add(2)
	go func() {
		defer s.leaseWG.Done()
		s.runLeaseLoop(leaseStopCh)
	}()
	go func() {
		defer s.leaseWG.Done()
		s.runSettlementLoop(leaseStopCh)
	}()

	// Start StreamingPull workers.
	for i := 0; i < s.numStreams; i++ {
		var fc *subFlowController
		if s.perStreamFC {
			fc = newSubFlowController(s.maxOutstandingMsgs, s.maxOutstandingByte, s.maxConcurrency)
		} else {
			fc = s.sharedFC
		}
		s.streamWG.Add(1)
		go func() {
			defer s.streamWG.Done()
			s.runStreamingPullLoop(fc)
		}()
	}

	// Wait for StopSubscriberCommand or fatal stream error.
	select {
	case <-s.stopTriggerCh:
	case <-s.srv.stopCh:
		s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
	}

	s.mu.Lock()
	behavior := s.stopBehavior
	timeout := s.stopTimeout
	s.mu.Unlock()

	// Stop pulling new messages.
	s.pullCancel()
	s.streamWG.Wait()

	if behavior == proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY {
		s.abortCallbacks()
		s.bulkNackAllUnsettled()
		s.callbackWG.Wait()
	} else {
		// WaitForProcessing: wait up to shutdownTimeout while active callbacks & settlements drain.
		cbDone := make(chan struct{})
		go func() {
			s.callbackWG.Wait()
			close(cbDone)
		}()

		if timeout == 0 {
			s.abortCallbacks()
			s.bulkNackAllUnsettled()
			<-cbDone
		} else if timeout < 0 {
			<-cbDone
		} else {
			timer := time.NewTimer(timeout)
			select {
			case <-cbDone:
				timer.Stop()
			case <-timer.C:
				s.abortCallbacks()
				s.bulkNackAllUnsettled()
				<-cbDone
			}
		}
	}

	s.mu.Lock()
	s.settlementStopped = true
	s.mu.Unlock()
	s.detachedWG.Wait()

	s.mu.Lock()
	s.leaseStopped = true
	s.mu.Unlock()

	// Flush pending ack/nack batches and stop lease manager.
	close(leaseStopCh)
	s.leaseWG.Wait()

	s.mu.Lock()
	errCode := s.fatalErrCode
	errMsg := s.fatalErrMsg
	s.mu.Unlock()

	s.emit(&proxypb.SubscriberEvent{
		Event: &proxypb.SubscriberEvent_Stopped{
			Stopped: &proxypb.StoppedEvent{
				ErrorCode:      errCode,
				ErrorMessage:   errMsg,
				TotalDelivered: s.totalDelivered.Load(),
			},
		},
	})
	close(s.sendCh)
	<-s.senderDone
	s.abortCancel()
	s.sessionCancel()
	return nil
}

func (s *subscriberSession) emit(ev *proxypb.SubscriberEvent) {
	select {
	case s.sendCh <- ev:
	case <-s.senderDone:
	case <-s.sessionCtx.Done():
	}
}

func (s *subscriberSession) triggerStop(bh proxypb.ShutdownBehavior, tmMs *int64) {
	s.stopTriggerOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		if bh != proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_UNSPECIFIED {
			s.stopBehavior = bh
		} else {
			s.stopBehavior = s.shutdownBehavior
		}
		if tmMs != nil {
			if *tmMs < 0 {
				s.stopTimeout = -1
			} else {
				s.stopTimeout = time.Duration(*tmMs) * time.Millisecond
			}
		} else {
			s.stopTimeout = s.shutdownTimeout
		}
		s.mu.Unlock()
		close(s.stopTriggerCh)
	})
}

func (s *subscriberSession) abortCallbacks() {
	s.callbackAbortOnce.Do(func() {
		if s.abortCancel != nil {
			s.abortCancel()
		}
		close(s.callbackAbortCh)
	})
}

func (s *subscriberSession) subOutgoingContext(ctx context.Context) context.Context {
	pairs := make([]string, 0, 2+len(s.customHeaders)*2)
	if !s.cfg.HasFault("missing-routing-header") {
		subVal := url.QueryEscape(s.fullSub)
		if s.cfg.UnescapedRoutingHeader {
			subVal = s.fullSub
		}
		pairs = append(pairs, "x-goog-request-params", "subscription="+subVal)
	}
	if !s.cfg.HasFault("missing-custom-headers") {
		for k, v := range s.customHeaders {
			pairs = append(pairs, k, v)
		}
	}
	if len(pairs) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func (s *subscriberSession) isStreamErrorRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	code := st.Code()
	lang := strings.ToLower(s.cfg.Language)
	if code == codes.Unavailable && strings.Contains(st.Message(), "Server shutdownNow invoked") && (lang == "go" || lang == "java") {
		return false
	}
	if s.useCustomRetry {
		return s.retryCodes[code]
	}
	switch code {
	case codes.Aborted, codes.Internal, codes.Unavailable, codes.DeadlineExceeded, codes.Unknown:
		return true
	case codes.ResourceExhausted:
		return s.cfg.RetryStreamingPullResExhausted
	default:
		return false
	}
}

func (s *subscriberSession) applySubscriptionProperties(props *pubsubpb.StreamingPullResponse_SubscriptionProperties) (eod, ordered bool) {
	if props != nil {
		prevEOD := s.isEOD.Swap(props.ExactlyOnceDeliveryEnabled)
		if prevEOD != props.ExactlyOnceDeliveryEnabled {
			select {
			case s.extResetCh <- struct{}{}:
			default:
			}
		}
		if s.cfg.SupportsSubscriberOrdering {
			s.msgOrdering.Store(props.MessageOrderingEnabled)
		}
		return props.ExactlyOnceDeliveryEnabled, props.MessageOrderingEnabled && s.cfg.SupportsSubscriberOrdering
	}
	return s.isEOD.Load(), s.cfg.SupportsSubscriberOrdering && s.msgOrdering.Load()
}

func (s *subscriberSession) runStreamingPullLoop(fc *subFlowController) {
	if s.cfg.HasFault("wrong-rpc-method") {
		// Faulty client calls unary Pull instead of StreamingPull.
		callCtx := s.subOutgoingContext(s.pullCtx)
		resp, err := s.client.Pull(callCtx, &pubsubpb.PullRequest{
			Subscription:      s.fullSub,
			MaxMessages:       10,
			ReturnImmediately: true,
		})
		if err == nil && resp != nil {
			batchEOD, batchOrdered := s.applySubscriptionProperties(nil)
			s.handleReceivedBatch(receivedBatch{
				rms:     resp.ReceivedMessages,
				eod:     batchEOD,
				ordered: batchOrdered,
			}, fc)
		}
		return
	}

	delay := s.initialRetryDelay
	for {
		if s.pullCtx.Err() != nil {
			return
		}
		err := s.runOneStream(fc)
		if s.pullCtx.Err() != nil {
			return
		}
		if err == nil {
			delay = s.initialRetryDelay
			continue
		}
		if !s.isStreamErrorRetryable(err) {
			st, _ := status.FromError(err)
			code := st.Code()
			if code == codes.OK {
				code = codes.Unknown
			}
			s.mu.Lock()
			if s.fatalErrCode == 0 {
				s.fatalErrCode = int32(code)
				s.fatalErrMsg = st.Message()
			}
			s.mu.Unlock()
			s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
			return
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-s.pullCtx.Done():
			timer.Stop()
			return
		}
		next := time.Duration(float64(delay) * s.retryMultiplier)
		if next > s.maxRetryDelay {
			next = s.maxRetryDelay
		}
		delay = next
	}
}

func (s *subscriberSession) runOneStream(fc *subFlowController) error {
	streamCtx, cancelStream := context.WithCancel(s.pullCtx)
	defer cancelStream()

	callCtx := s.subOutgoingContext(streamCtx)
	spStream, err := s.client.StreamingPull(callCtx)
	if err != nil {
		return err
	}

	protoVer := int64(0)
	if s.cfg.SupportsKeepalive {
		protoVer = 1
	}

	subName := s.fullSub
	if s.cfg.HasFault("wrong-rpc-method") {
		subName = s.fullSub + "-wrong"
	}

	initReq := &pubsubpb.StreamingPullRequest{
		Subscription:             subName,
		StreamAckDeadlineSeconds: s.streamDeadlineS,
		ClientId:                 s.clientID,
		MaxOutstandingMessages:   s.maxOutstandingMsgs,
		MaxOutstandingBytes:      s.maxOutstandingByte,
		ProtocolVersion:          protoVer,
	}
	if err := spStream.Send(initReq); err != nil {
		if errors.Is(err, io.EOF) {
			for {
				if _, rerr := spStream.Recv(); rerr != nil {
					return rerr
				}
			}
		}
		return err
	}

	var recvBlockedOnBatch atomic.Bool
	var keepaliveTimedOut atomic.Bool
	batchCh := make(chan receivedBatch, 16)
	var batchWG sync.WaitGroup
	batchWG.Add(1)
	go func() {
		defer batchWG.Done()
		for batch := range batchCh {
			s.handleReceivedBatch(batch, fc)
		}
	}()
	closeBatchOnce := sync.OnceFunc(func() {
		close(batchCh)
	})

	var pingMu sync.Mutex
	lastResp := time.Now()
	respSignalCh := make(chan struct{}, 1)

	kaDone := make(chan struct{})
	if s.cfg.SupportsKeepalive {
		pingInt := s.cfg.KeepalivePingOverride
		if pingInt <= 0 {
			pingInt = time.Duration(s.cfg.Defaults.KeepalivePingIntervalMs) * time.Millisecond
		}
		if pingInt <= 0 {
			pingInt = defaultKeepalivePingInterval
		}
		pongTO := s.cfg.KeepalivePongOverride
		if pongTO <= 0 {
			pongTO = time.Duration(s.cfg.Defaults.KeepalivePongTimeoutMs) * time.Millisecond
		}
		if pongTO <= 0 {
			pongTO = defaultKeepalivePongTimeout
		}
		go func() {
			defer close(kaDone)
			pingTicker := time.NewTicker(pingInt)
			defer pingTicker.Stop()

			var pongTimer *time.Timer
			var pongC <-chan time.Time
			var pingArmedAt time.Time
			stopPong := func() {
				if pongTimer != nil {
					pongTimer.Stop()
					pongTimer = nil
					pongC = nil
				}
			}
			defer stopPong()

			for {
				select {
				case <-streamCtx.Done():
					return
				case <-respSignalCh:
					pingMu.Lock()
					lr := lastResp
					pingMu.Unlock()
					if pongTimer != nil && !lr.Before(pingArmedAt) {
						stopPong()
					}
				case <-pingTicker.C:
					now := time.Now()
					if pongTimer == nil {
						pingMu.Lock()
						pingArmedAt = now
						pingMu.Unlock()
						pongTimer = time.NewTimer(pongTO)
						pongC = pongTimer.C
					}
					if err := spStream.Send(&pubsubpb.StreamingPullRequest{}); err != nil {
						return
					}
				case <-pongC:
					pingMu.Lock()
					lr := lastResp
					armedAt := pingArmedAt
					pingMu.Unlock()
					if !lr.Before(armedAt) {
						stopPong()
						continue
					}
					if recvBlockedOnBatch.Load() {
						pongTimer = time.NewTimer(pongTO)
						pongC = pongTimer.C
						continue
					}
					keepaliveTimedOut.Store(true)
					cancelStream()
					return
				}
			}
		}()
	} else {
		close(kaDone)
	}

	defer func() {
		cancelStream()
		<-kaDone
		closeBatchOnce()
		batchWG.Wait()
	}()

	for {
		resp, err := spStream.Recv()
		if err != nil {
			if keepaliveTimedOut.Load() && s.pullCtx.Err() == nil {
				return nil
			}
			return err
		}
		pingMu.Lock()
		lastResp = time.Now()
		pingMu.Unlock()
		select {
		case respSignalCh <- struct{}{}:
		default:
		}

		batchEOD, batchOrdered := s.applySubscriptionProperties(resp.SubscriptionProperties)

		if len(resp.ReceivedMessages) == 0 {
			continue
		}

		batch := receivedBatch{
			rms:     resp.ReceivedMessages,
			eod:     batchEOD,
			ordered: batchOrdered,
		}
		select {
		case batchCh <- batch:
		default:
			recvBlockedOnBatch.Store(true)
			select {
			case batchCh <- batch:
				recvBlockedOnBatch.Store(false)
			case <-streamCtx.Done():
				recvBlockedOnBatch.Store(false)
				closeBatchOnce()
				batchWG.Wait()
				s.handleReceivedBatch(batch, fc)
				if keepaliveTimedOut.Load() && s.pullCtx.Err() == nil {
					return nil
				}
				return streamCtx.Err()
			}
		}
	}
}

func (s *subscriberSession) ackDeadlineSecFor(eod bool) int32 {
	dl := s.minAckExt
	if eod && s.start.MinDurationPerAckExtensionMs == nil && dl < 60*time.Second {
		dl = 60 * time.Second
	}
	if s.maxAckExt > 0 && dl > s.maxAckExt {
		dl = s.maxAckExt
	}
	sec := int32(dl.Seconds())
	if sec < 10 {
		sec = 10
	}
	if sec > 600 {
		sec = 600
	}
	return sec
}

func (s *subscriberSession) currentAckDeadlineSec() int32 {
	return s.ackDeadlineSecFor(s.isEOD.Load())
}

func (s *subscriberSession) handleReceivedBatch(batch receivedBatch, fc *subFlowController) {
	rms := batch.rms
	if s.cfg.HasFault("out-of-order-ack") && len(rms) > 1 {
		rev := make([]*pubsubpb.ReceivedMessage, len(rms))
		for i := range rms {
			rev[i] = rms[len(rms)-1-i]
		}
		rms = rev
	}

	eod := batch.eod
	ordered := batch.ordered
	entries := make([]*deliveryEntry, 0, len(rms))
	tracesByAck := make(map[string][]*subscribeMessageTrace, len(rms))
	var batchMaxExtAt time.Time
	if s.maxExtension >= 0 {
		batchMaxExtAt = time.Now().Add(s.maxExtension)
	}

	for _, rm := range rms {
		if rm == nil || rm.Message == nil {
			continue
		}
		tr := newSubscribeMessageTrace(
			s.traces,
			s.enableMonitoring,
			s.projectID,
			s.fullSub,
			s.subID,
			rm.AckId,
			rm.Message.MessageId,
			rm.Message.Data,
			rm.Message.Attributes,
			rm.Message.OrderingKey,
			rm.DeliveryAttempt,
			len(rms),
			eod,
		)
		entry := &deliveryEntry{
			token:           fmt.Sprintf("deliv-%d", s.deliverySeq.Add(1)),
			ackID:           rm.AckId,
			msg:             rm.Message,
			deliveryAttempt: rm.DeliveryAttempt,
			eod:             eod,
			fc:              fc,
			trace:           tr,
			settleCh:        make(chan settleReq, 1),
			expiredCh:       make(chan struct{}),
		}
		if s.maxExtension >= 0 {
			entry.hasMaxExt = true
			entry.maxExtAt = batchMaxExtAt
		}
		entries = append(entries, entry)
		tracesByAck[rm.AckId] = append(tracesByAck[rm.AckId], tr)
	}

	dedupeEntryAckIDs := func(list []*deliveryEntry) []string {
		ackIDs := make([]string, 0, len(list))
		seen := make(map[string]struct{}, len(list))
		for _, e := range list {
			if _, ok := seen[e.ackID]; !ok {
				seen[e.ackID] = struct{}{}
				ackIDs = append(ackIDs, e.ackID)
			}
		}
		return ackIDs
	}

	nackAndCloseRemaining := func(rem []*deliveryEntry) {
		if len(rem) == 0 {
			return
		}
		now := time.Now()
		toNack := make([]*deliveryEntry, 0, len(rem))
		for _, e := range rem {
			if s.isPastMaxExt(e, now) {
				if e.expired.CompareAndSwap(false, true) {
					e.trace.markExpired()
					close(e.expiredCh)
				}
				if !e.ccDone {
					e.trace.startConcurrencyControl()
					e.trace.endConcurrencyControl()
					e.ccDone = true
				}
				e.trace.startScheduler()
				e.trace.endScheduler()
				e.trace.endSubscribe("expired", "")
				continue
			}
			toNack = append(toNack, e)
		}
		if len(toNack) == 0 {
			return
		}
		ackIDs := dedupeEntryAckIDs(toNack)
		trMap := make(map[string][]*subscribeMessageTrace, len(toNack))
		for _, e := range toNack {
			trMap[e.ackID] = append(trMap[e.ackID], e.trace)
		}
		nackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		nackErr := s.sendModAckRaw(nackCtx, ackIDs, 0, false, trMap)
		cancel()
		for _, e := range toNack {
			if nackErr != nil {
				if s.cfg.CppSpans {
					e.trace.endSubscribe("nacked", nackErr.Error())
				} else if e.eod {
					e.trace.endSubscribe("", nackErr.Error())
				} else {
					e.trace.endSubscribe("nacked", "")
				}
			} else {
				e.trace.endSubscribe("nacked", "")
			}
		}
	}

	if s.pullCtx.Err() != nil {
		nackAndCloseRemaining(entries)
		return
	}

	// EOD receipt modack gating (EOD-04).
	if eod && s.cfg.SupportsEodReceiptModack && len(entries) > 0 {
		dlSec := s.ackDeadlineSecFor(eod)
		if dlSec < 60 && s.start.MaxDurationPerAckExtensionMs == nil && s.start.MinDurationPerAckExtensionMs == nil {
			dlSec = 60
		}
		ackIDs := dedupeEntryAckIDs(entries)
		results := s.sendModAckWithRetryDetailed(s.pullCtx, ackIDs, dlSec, true, eod, tracesByAck)
		if s.pullCtx.Err() != nil {
			var toNack []*deliveryEntry
			for _, e := range entries {
				res := results[e.ackID]
				isPermPreCancel := res.status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS &&
					res.status != proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED &&
					res.grpcCode != int32(codes.Canceled) &&
					res.grpcCode != int32(codes.DeadlineExceeded)
				if isPermPreCancel {
					e.trace.endSubscribe("", "receipt modack failed")
				} else {
					toNack = append(toNack, e)
				}
			}
			nackAndCloseRemaining(toNack)
			return
		}
		filtered := make([]*deliveryEntry, 0, len(entries))
		for _, e := range entries {
			st := results[e.ackID].status
			if st == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS || st == proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED {
				filtered = append(filtered, e)
			} else {
				e.trace.endSubscribe("", "receipt modack failed")
			}
		}
		entries = filtered
	}

	for i, entry := range entries {
		if s.pullCtx.Err() != nil {
			nackAndCloseRemaining(entries[i:])
			return
		}
		entry.trace.startConcurrencyControl()
		bounded, ok := fc.acquire(s.pullCtx, int64(len(entry.msg.Data)))
		entry.trace.endConcurrencyControl()
		entry.ccDone = true
		if !ok {
			nackAndCloseRemaining(entries[i:])
			return
		}
		entry.boundedFC = bounded

		s.mu.Lock()
		if s.isPastMaxExt(entry, time.Now()) {
			wonExpire := entry.expired.CompareAndSwap(false, true)
			if wonExpire {
				entry.trace.markExpired()
			}
			s.mu.Unlock()
			entry.releaseFC()
			if wonExpire {
				close(entry.expiredCh)
			}
			entry.trace.startScheduler()
			entry.trace.endScheduler()
			entry.trace.endSubscribe("expired", "")
			continue
		}
		if s.stopping {
			s.mu.Unlock()
			entry.releaseFC()
			nackAndCloseRemaining(entries[i:])
			return
		}
		s.pendingDeliveries[entry.token] = entry
		s.activeLeases[entry.token] = entry
		entry.trace.startScheduler()

		var prevCh, nextCh chan struct{}
		var deliverPrevCh, deliverNextCh chan struct{}
		key := entry.msg.OrderingKey
		if key != "" && ordered {
			prevCh = s.keyChains[key]
			nextCh = make(chan struct{})
			s.keyChains[key] = nextCh
		}
		if prevCh == nil {
			deliverPrevCh = s.deliverChain
			deliverNextCh = make(chan struct{})
			s.deliverChain = deliverNextCh
		}
		s.callbackWG.Add(1)
		s.mu.Unlock()

		go s.dispatchDelivery(entry, prevCh, nextCh, deliverPrevCh, deliverNextCh)
	}
}

func (s *subscriberSession) dispatchDelivery(entry *deliveryEntry, prevCh, nextCh, deliverPrevCh, deliverNextCh chan struct{}) {
	defer s.callbackWG.Done()
	if nextCh != nil {
		defer close(nextCh)
	}
	var deliverOnce sync.Once
	finishDeliver := func() {
		if deliverNextCh != nil {
			deliverOnce.Do(func() { close(deliverNextCh) })
		}
	}
	defer func() {
		s.mu.Lock()
		if entry.drainingSettles {
			s.mu.Unlock()
			return
		}
		trailing := entry.takePendingSettlesLocked()
		if len(trailing) == 0 {
			s.mu.Unlock()
			return
		}
		entry.drainingSettles = true
		s.mu.Unlock()
		s.drainDetachedSettles(entry, trailing)
	}()
	defer finishDeliver()
	// Go defers execute LIFO: registering releaseFC before activeCallbacks.Add(-1)
	// guarantees activeCallbacks decrements before the FC permit is freed.
	defer entry.releaseFC()

	if prevCh != nil {
		select {
		case <-prevCh:
		case <-entry.expiredCh:
			entry.trace.endScheduler()
			return
		case <-s.callbackAbortCh:
			entry.trace.endScheduler()
			return
		}
	}
	if deliverPrevCh != nil {
		select {
		case <-deliverPrevCh:
		case <-entry.expiredCh:
			entry.trace.endScheduler()
			return
		case <-s.callbackAbortCh:
			entry.trace.endScheduler()
			return
		}
	}
	if s.expireEntryIfPastDeadline(entry, time.Now(), true) {
		entry.trace.endScheduler()
		return
	}
	select {
	case <-s.callbackAbortCh:
		entry.trace.endScheduler()
		return
	default:
	}

	entry.trace.endScheduler()

	if s.cfg.HasFault("out-of-order-ack") {
		// Deliberately send premature Acknowledge BEFORE emitting MessageDeliveredEvent,
		// while respecting callbackAbortCh so NACK_IMMEDIATELY never stalls.
		ackCtx, ackCancel := context.WithTimeout(s.sessionCtx, 5*time.Second)
		ackDone := make(chan struct{})
		go func() {
			select {
			case <-s.callbackAbortCh:
				ackCancel()
			case <-ackDone:
			}
		}()
		_, _ = s.client.Acknowledge(s.subOutgoingContext(ackCtx), &pubsubpb.AcknowledgeRequest{
			Subscription: s.fullSub,
			AckIds:       []string{entry.ackID},
		})
		close(ackDone)
		ackCancel()
		if s.expireEntryIfPastDeadline(entry, time.Now(), true) {
			return
		}
		select {
		case <-s.callbackAbortCh:
			return
		default:
		}
	}

	if s.expireEntryIfPastDeadline(entry, time.Now(), true) {
		return
	}
	select {
	case <-s.callbackAbortCh:
		return
	default:
	}

	tp, ts := entry.trace.startProcess(
		entry.msg.MessageId,
		len(entry.msg.Data),
		entry.msg.OrderingKey,
		entry.deliveryAttempt,
		entry.msg.Attributes,
	)

	active := s.activeCallbacks.Add(1)
	defer s.activeCallbacks.Add(-1)

	s.totalDelivered.Add(1)
	autoMode := s.start.AutoSettleMode
	autoSettled := autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_UNSPECIFIED &&
		autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE &&
		autoMode != proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE

	var pubTime *timestamppb.Timestamp
	if entry.msg.PublishTime != nil {
		pubTime = entry.msg.PublishTime
	}

	ev := &proxypb.MessageDeliveredEvent{
		DeliveryToken:       entry.token,
		MessageId:           entry.msg.MessageId,
		Data:                entry.msg.Data,
		Attributes:          maps.Clone(entry.msg.Attributes),
		OrderingKey:         entry.msg.OrderingKey,
		PublishTime:         pubTime,
		DeliveryAttempt:     entry.deliveryAttempt,
		HasDeliveryAttempt:  entry.deliveryAttempt > 0,
		ActiveCallbacks:     active,
		CallbackTraceparent: tp,
		CallbackTracestate:  ts,
		IsExactlyOnce:       entry.eod,
		AutoSettled:         autoSettled,
	}
	s.emit(&proxypb.SubscriberEvent{
		Event: &proxypb.SubscriberEvent_MessageDelivered{
			MessageDelivered: ev,
		},
	})
	finishDeliver()

	switch autoMode {
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK:
		s.executeSettle(entry, proxypb.SettleMessageCommand_SETTLE_ACTION_ACK)
		return
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK:
		s.executeSettle(entry, proxypb.SettleMessageCommand_SETTLE_ACTION_NACK)
		return
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT:
		s.executeSettle(entry, proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT)
		return
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT:
		s.executeSettle(entry, proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT)
		return
	case proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE:
		s.executeSettle(entry, proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE)
		return
	}

	// Interactive wait for SettleMessageCommand, lease expiration, or abort.
	select {
	case req := <-entry.settleCh:
		s.executeSettle(entry, req.action)
	case <-entry.expiredCh:
		s.siphonOrEndProcess(entry)
	case <-s.callbackAbortCh:
		s.siphonOrEndProcess(entry)
	}
}

func (s *subscriberSession) siphonOrEndProcess(entry *deliveryEntry) {
	s.mu.Lock()
	trailing := entry.takePendingSettlesLocked()
	entry.drainingSettles = true
	s.mu.Unlock()

	handoffDrain := false
	defer func() {
		if !handoffDrain {
			s.mu.Lock()
			entry.drainingSettles = false
			s.mu.Unlock()
		}
	}()

	if len(trailing) == 0 {
		entry.trace.endProcess("")
		handoffDrain = true
		s.drainDetachedSettles(entry, nil)
		return
	}
	doneCh, wantWait := s.settleEntrySingle(entry, trailing[0])
	handoffDrain = true
	s.drainDetachedSettles(entry, trailing[1:])
	if wantWait && doneCh != nil {
		select {
		case <-doneCh:
		case <-s.callbackAbortCh:
		case <-s.sessionCtx.Done():
		}
	}
}

func (s *subscriberSession) handleSettleCommand(cmd *proxypb.SettleMessageCommand) {
	if !s.cfg.SupportsAckWithResult && (cmd.Action == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT ||
		cmd.Action == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT) {
		s.mu.Lock()
		if s.fatalErrCode == 0 {
			s.fatalErrCode = int32(codes.Unimplemented)
			s.fatalErrMsg = "ack/nack with result not supported"
		}
		s.mu.Unlock()
		s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
		return
	}
	s.mu.Lock()
	if s.settlementStopped {
		s.mu.Unlock()
		return
	}
	entry, ok := s.pendingDeliveries[cmd.DeliveryToken]
	if !ok {
		s.mu.Unlock()
		return
	}
	if entry.callbackExited || entry.returned.Load() {
		if entry.drainingSettles {
			entry.deferredSettles = append(entry.deferredSettles, cmd.Action)
			s.mu.Unlock()
			return
		}
		entry.drainingSettles = true
		s.detachedWG.Add(1)
		s.mu.Unlock()
		defer s.detachedWG.Done()
		s.drainDetachedSettles(entry, []proxypb.SettleMessageCommand_SettleAction{cmd.Action})
		return
	}
	if len(entry.deferredSettles) > 0 {
		entry.deferredSettles = append(entry.deferredSettles, cmd.Action)
	} else {
		select {
		case entry.settleCh <- settleReq{action: cmd.Action}:
		default:
			entry.deferredSettles = append(entry.deferredSettles, cmd.Action)
		}
	}
	s.mu.Unlock()
}

func (s *subscriberSession) executeSettle(entry *deliveryEntry, action proxypb.SettleMessageCommand_SettleAction) {
	s.mu.Lock()
	trailing := entry.takePendingSettlesLocked()
	entry.drainingSettles = true
	s.mu.Unlock()

	handoffDrain := false
	defer func() {
		if !handoffDrain {
			s.mu.Lock()
			entry.drainingSettles = false
			s.mu.Unlock()
		}
	}()

	doneCh, wantWait := s.settleEntrySingle(entry, action)
	handoffDrain = true
	s.drainDetachedSettles(entry, trailing)
	if wantWait && doneCh != nil {
		select {
		case <-doneCh:
		case <-s.callbackAbortCh:
		case <-s.sessionCtx.Done():
		}
	}
}

func (s *subscriberSession) drainDetachedSettles(entry *deliveryEntry, initial []proxypb.SettleMessageCommand_SettleAction) {
	cleared := false
	defer func() {
		if !cleared {
			s.mu.Lock()
			entry.drainingSettles = false
			s.mu.Unlock()
		}
	}()
	pending := initial
	for {
		for _, act := range pending {
			s.settleEntrySingle(entry, act)
		}
		s.mu.Lock()
		next := entry.takePendingSettlesLocked()
		if len(next) == 0 {
			entry.drainingSettles = false
			cleared = true
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		pending = next
	}
}

func (s *subscriberSession) settleEntrySingle(entry *deliveryEntry, action proxypb.SettleMessageCommand_SettleAction) (chan struct{}, bool) {
	if !s.cfg.SupportsAckWithResult && (action == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT ||
		action == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT) {
		s.mu.Lock()
		if s.fatalErrCode == 0 {
			s.fatalErrCode = int32(codes.Unimplemented)
			s.fatalErrMsg = "ack/nack with result not supported"
		}
		s.mu.Unlock()
		entry.trace.endProcess("")
		s.triggerStop(proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY, nil)
		return nil, false
	}

	s.mu.Lock()
	if action == proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE {
		now := time.Now()
		if entry.settled.Load() {
			s.mu.Unlock()
			entry.trace.endProcess("")
			return nil, false
		}
		if entry.expired.Load() || s.isPastMaxExt(entry, now) {
			delete(s.activeLeases, entry.token)
			if s.cfg.SupportsAutoNackOnDrop {
				delete(s.pendingDeliveries, entry.token)
			} else {
				entry.returned.Store(true)
			}
			wonExpire := entry.expired.CompareAndSwap(false, true)
			if wonExpire {
				entry.trace.markExpired()
			}
			s.mu.Unlock()
			entry.trace.endProcess("")
			if wonExpire {
				close(entry.expiredCh)
				entry.trace.endScheduler()
				entry.trace.endSubscribe("expired", "")
			}
			return nil, false
		}
		if !s.cfg.SupportsAutoNackOnDrop {
			entry.returned.Store(true)
			s.mu.Unlock()
			entry.trace.endProcess("")
			return nil, false
		}
		if !entry.settled.CompareAndSwap(false, true) {
			s.mu.Unlock()
			entry.trace.endProcess("")
			return nil, false
		}
		delete(s.pendingDeliveries, entry.token)
		delete(s.activeLeases, entry.token)
		s.mu.Unlock()
		entry.trace.endProcess("nack")
		s.queueSettlement(entry, true, false, false)
		return nil, false
	}

	if !entry.settled.CompareAndSwap(false, true) {
		s.mu.Unlock()
		entry.trace.endProcess("")
		return nil, false
	}
	wasExpired := entry.expired.Load()
	delete(s.pendingDeliveries, entry.token)
	delete(s.activeLeases, entry.token)
	s.mu.Unlock()

	isNack := action == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK ||
		action == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
	wantResult := action == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT ||
		action == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT

	if wasExpired {
		entry.trace.endProcess("")
	} else if isNack {
		entry.trace.endProcess("nack")
	} else {
		entry.trace.endProcess("ack")
	}

	doneCh := s.queueSettlement(entry, isNack, wantResult, wasExpired)
	return doneCh, wantResult
}

func (s *subscriberSession) queueSettlement(entry *deliveryEntry, isNack, wantResult, skipTrace bool) chan struct{} {
	w := &settlementWaiter{
		ackID:      entry.ackID,
		entry:      entry,
		isNack:     isNack,
		wantResult: wantResult,
		skipTrace:  skipTrace,
		doneCh:     make(chan struct{}),
	}
	s.mu.Lock()
	if s.leaseStopped {
		s.mu.Unlock()
		if !skipTrace && entry != nil {
			entry.trace.endSubscribe("", "")
		}
		close(w.doneCh)
		return w.doneCh
	}
	wasEmpty := len(s.pendingAcks) == 0 && len(s.pendingNacks) == 0
	if isNack {
		s.pendingNacks = append(s.pendingNacks, w)
	} else {
		s.pendingAcks = append(s.pendingAcks, w)
	}
	shouldFlush := len(s.pendingAcks) >= maxAckBatchCount || len(s.pendingNacks) >= maxAckBatchCount
	s.mu.Unlock()
	if shouldFlush {
		select {
		case s.flushTriggerCh <- struct{}{}:
		default:
		}
	} else if wasEmpty {
		select {
		case s.armFlushCh <- struct{}{}:
		default:
		}
	}
	return w.doneCh
}

func (s *subscriberSession) bulkNackAllUnsettled() {
	now := time.Now()
	s.mu.Lock()
	entries := make([]*deliveryEntry, 0, len(s.activeLeases))
	var expired []*deliveryEntry
	for tok, e := range s.activeLeases {
		if e.expired.Load() {
			delete(s.activeLeases, tok)
			delete(s.pendingDeliveries, tok)
			continue
		}
		if !e.settled.Load() && s.isPastMaxExt(e, now) {
			if e.expired.CompareAndSwap(false, true) {
				delete(s.activeLeases, tok)
				delete(s.pendingDeliveries, tok)
				e.trace.markExpired()
				expired = append(expired, e)
			}
			continue
		}
		if e.settled.CompareAndSwap(false, true) {
			delete(s.activeLeases, tok)
			delete(s.pendingDeliveries, tok)
			entries = append(entries, e)
		}
	}
	s.mu.Unlock()
	for _, e := range expired {
		close(e.expiredCh)
		e.trace.endScheduler()
		e.trace.endSubscribe("expired", "")
	}

	if len(entries) == 0 {
		return
	}
	ackIDs := make([]string, 0, len(entries))
	seenAck := make(map[string]struct{}, len(entries))
	traces := make(map[string][]*subscribeMessageTrace, len(entries))
	for _, e := range entries {
		e.releaseFC()
		if _, ok := seenAck[e.ackID]; !ok {
			seenAck[e.ackID] = struct{}{}
			ackIDs = append(ackIDs, e.ackID)
		}
		traces[e.ackID] = append(traces[e.ackID], e.trace)
	}
	nackCtx, nackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	nackErr := s.sendModAckRaw(nackCtx, ackIDs, 0, false, traces)
	nackCancel()
	for _, e := range entries {
		if nackErr != nil {
			if s.cfg.CppSpans {
				e.trace.endSubscribe("nacked", nackErr.Error())
			} else if e.eod {
				e.trace.endSubscribe("", nackErr.Error())
			} else {
				e.trace.endSubscribe("nacked", "")
			}
		} else {
			e.trace.endSubscribe("nacked", "")
		}
	}
}

func (s *subscriberSession) runLeaseLoop(stopCh chan struct{}) {
	expInterval := 50 * time.Millisecond
	if s.cfg.FastTimings {
		expInterval = 15 * time.Millisecond
	}
	if s.maxExtension > 0 && s.maxExtension < 400*time.Millisecond {
		scaled := s.maxExtension / 8
		if scaled < 10*time.Millisecond {
			scaled = 10 * time.Millisecond
		}
		if scaled < expInterval {
			expInterval = scaled
		}
	}
	expiryTick := time.NewTicker(expInterval)
	defer expiryTick.Stop()

	calcExtInterval := func() time.Duration {
		dl := time.Duration(s.currentAckDeadlineSec()) * time.Second
		if s.start.MaxDurationPerAckExtensionMs != nil && *s.start.MaxDurationPerAckExtensionMs > 0 {
			reqDl := time.Duration(*s.start.MaxDurationPerAckExtensionMs) * time.Millisecond
			if reqDl < dl {
				dl = reqDl
			}
		}
		iv := dl - leaseGracePeriod
		if iv <= 0 {
			iv = dl / 2
		}
		minIv := 100 * time.Millisecond
		if s.cfg.FastTimings {
			minIv = 15 * time.Millisecond
		}
		if s.maxExtension > 0 {
			extCap := s.maxExtension / 3
			if extCap < iv {
				iv = extCap
			}
			if extCap < minIv {
				minIv = extCap
			}
			if minIv < 10*time.Millisecond {
				minIv = 10 * time.Millisecond
			}
		}
		if s.cfg.FastTimings && iv > 35*time.Millisecond {
			iv = 35 * time.Millisecond
		}
		if iv < minIv {
			iv = minIv
		}
		return iv
	}
	extTimer := time.NewTimer(calcExtInterval())
	defer extTimer.Stop()

	var extInFlight atomic.Bool

	for {
		select {
		case <-stopCh:
			now := time.Now()
			s.mu.Lock()
			var remaining []*deliveryEntry
			var expiredOnStop []*deliveryEntry
			for tok, e := range s.activeLeases {
				delete(s.activeLeases, tok)
				delete(s.pendingDeliveries, tok)
				if e.expired.Load() {
					continue
				}
				if !e.settled.Load() && s.isPastMaxExt(e, now) && e.expired.CompareAndSwap(false, true) {
					e.trace.markExpired()
					expiredOnStop = append(expiredOnStop, e)
					continue
				}
				if e.settled.CompareAndSwap(false, true) {
					remaining = append(remaining, e)
				}
			}
			s.mu.Unlock()
			for _, e := range expiredOnStop {
				close(e.expiredCh)
				e.trace.endScheduler()
				e.trace.endSubscribe("expired", "")
			}
			for _, e := range remaining {
				e.trace.endSubscribe("", "")
			}
			return
		case <-s.extResetCh:
			if !extTimer.Stop() {
				select {
				case <-extTimer.C:
				default:
				}
			}
			extTimer.Reset(calcExtInterval())
		case <-expiryTick.C:
			now := time.Now()
			s.mu.Lock()
			var expired []*deliveryEntry
			for tok, e := range s.activeLeases {
				if s.isPastMaxExt(e, now) && !e.settled.Load() && e.expired.CompareAndSwap(false, true) {
					delete(s.activeLeases, tok)
					e.trace.markExpired()
					expired = append(expired, e)
				}
			}
			s.mu.Unlock()
			for _, e := range expired {
				close(e.expiredCh)
				e.trace.endScheduler()
				e.trace.endSubscribe("expired", "")
			}
		case <-extTimer.C:
			now := time.Now()
			var extGuard time.Duration
			if s.maxExtension > 0 {
				extGuard = s.maxExtension / 10
				if extGuard > 50*time.Millisecond {
					extGuard = 50 * time.Millisecond
				}
				if s.cfg.FastTimings && extGuard < 20*time.Millisecond {
					extGuard = 20 * time.Millisecond
				}
				if extGuard > s.maxExtension/2 {
					extGuard = s.maxExtension / 2
				}
			}
			s.mu.Lock()
			if s.leaseStopped || s.abortCtx.Err() != nil {
				stopped := s.leaseStopped
				s.mu.Unlock()
				if !stopped {
					extTimer.Reset(calcExtInterval())
				}
				continue
			}
			var toExtend []string
			seenAck := make(map[string]struct{})
			traces := make(map[string][]*subscribeMessageTrace)
			for _, e := range s.activeLeases {
				if e.expired.Load() || e.settled.Load() {
					continue
				}
				if e.hasMaxExt && (!now.Before(e.maxExtAt) || (extGuard > 0 && !now.Before(e.maxExtAt.Add(-extGuard)))) {
					continue
				}
				if _, ok := seenAck[e.ackID]; !ok {
					seenAck[e.ackID] = struct{}{}
					toExtend = append(toExtend, e.ackID)
				}
				traces[e.ackID] = append(traces[e.ackID], e.trace)
			}
			s.mu.Unlock()
			if len(toExtend) > 0 && extInFlight.CompareAndSwap(false, true) {
				isEOD := s.isEOD.Load()
				dlSec := s.ackDeadlineSecFor(isEOD)
				s.leaseWG.Add(1)
				go func(ackIDs []string, dl int32, eod bool, tr map[string][]*subscribeMessageTrace) {
					defer s.leaseWG.Done()
					defer extInFlight.Store(false)
					if s.abortCtx.Err() != nil {
						return
					}
					modCtx, modCancel := context.WithTimeout(s.abortCtx, 60*time.Second)
					defer modCancel()
					if eod {
						s.sendModAckWithRetry(modCtx, ackIDs, dl, false, eod, tr)
					} else {
						_ = s.sendModAckRaw(modCtx, ackIDs, dl, false, tr)
					}
				}(toExtend, dlSec, isEOD, traces)
			}
			extTimer.Reset(calcExtInterval())
		}
	}
}

func (s *subscriberSession) runSettlementLoop(stopCh chan struct{}) {
	coalesceWindow := 50 * time.Millisecond
	if s.cfg.FastTimings {
		coalesceWindow = 20 * time.Millisecond
	}
	var flushTimer *time.Timer
	var flushTimerC <-chan time.Time

	stopFlushTimer := func() {
		if flushTimer == nil {
			return
		}
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer = nil
		flushTimerC = nil
	}
	defer stopFlushTimer()

	hasPending := func() bool {
		s.mu.Lock()
		pending := len(s.pendingAcks) > 0 || len(s.pendingNacks) > 0
		s.mu.Unlock()
		return pending
	}
	ensureArmed := func() {
		if flushTimer == nil && hasPending() {
			flushTimer = time.NewTimer(coalesceWindow)
			flushTimerC = flushTimer.C
		}
	}

	flushAll := func() {
		s.mu.Lock()
		acks := s.pendingAcks
		s.pendingAcks = nil
		nacks := s.pendingNacks
		s.pendingNacks = nil
		s.mu.Unlock()

		if len(acks) > 0 {
			s.sendSettlementBatch(acks, false)
		}
		if len(nacks) > 0 {
			s.sendSettlementBatch(nacks, true)
		}
	}

	for {
		select {
		case <-stopCh:
			stopFlushTimer()
			flushAll()
			return
		case <-s.flushTriggerCh:
			stopFlushTimer()
			flushAll()
			ensureArmed()
		case <-s.armFlushCh:
			ensureArmed()
		case <-flushTimerC:
			flushTimer = nil
			flushTimerC = nil
			flushAll()
			ensureArmed()
		}
	}
}

func splitAckIDBatches(ackIDs []string, maxBytes int, measure func([]string) int) [][]string {
	if len(ackIDs) == 0 {
		return nil
	}
	var out [][]string
	var cur []string
	for _, id := range ackIDs {
		cur = append(cur, id)
		if len(cur) >= maxAckBatchCount || (len(cur) > 1 && measure(cur) > maxBytes) {
			if len(cur) > 1 && measure(cur) > maxBytes {
				last := cur[len(cur)-1]
				out = append(out, cur[:len(cur)-1])
				cur = []string{last}
			} else {
				out = append(out, cur)
				cur = nil
			}
		}
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

func (s *subscriberSession) sendSettlementBatch(items []*settlementWaiter, isNack bool) {
	if len(items) == 0 {
		return
	}
	if s.cfg.HasFault("out-of-order-ack") && len(items) > 1 {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}

	resName := "acked"
	if isNack {
		resName = "nacked"
	}

	settleSubBatch := func(subItems []*settlementWaiter, batchEOD bool) {
		ackIDs := make([]string, 0, len(subItems))
		seenAck := make(map[string]struct{}, len(subItems))
		traces := make(map[string][]*subscribeMessageTrace, len(subItems))
		for _, w := range subItems {
			if _, ok := seenAck[w.ackID]; !ok {
				seenAck[w.ackID] = struct{}{}
				ackIDs = append(ackIDs, w.ackID)
			}
			if !w.skipTrace && w.entry != nil {
				traces[w.ackID] = append(traces[w.ackID], w.entry.trace)
			}
		}

		settleCtx, settleCancel := context.WithTimeout(s.sessionCtx, 30*time.Second)
		abortDone := make(chan struct{})
		go func() {
			select {
			case <-s.callbackAbortCh:
				settleCancel()
			case <-abortDone:
			}
		}()

		var results map[string]ackResultInfo
		if isNack {
			if s.cfg.HasFault("wrong-rpc-method") {
				// Faulty nack sends deadline 10 instead of 0.
				results = s.sendModAckWithRetryDetailed(settleCtx, ackIDs, 10, false, batchEOD, traces)
			} else {
				results = s.sendModAckWithRetryDetailed(settleCtx, ackIDs, 0, false, batchEOD, traces)
			}
		} else {
			results = s.sendAckWithRetryDetailed(settleCtx, ackIDs, batchEOD, traces)
		}

		if s.cfg.HasFault("extra-unexpected-rpc") && !isNack {
			extraCtx, extraCancel := context.WithTimeout(settleCtx, 5*time.Second)
			_, _ = s.client.Acknowledge(s.subOutgoingContext(extraCtx), &pubsubpb.AcknowledgeRequest{
				Subscription: s.fullSub,
				AckIds:       ackIDs,
			})
			extraCancel()
		}

		close(abortDone)
		settleCancel()

		for _, w := range subItems {
			res := results[w.ackID]
			if !w.skipTrace {
				if res.status == proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS || res.status == proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED {
					w.entry.trace.endSubscribe(resName, "")
				} else if batchEOD {
					errMsg := res.errMsg
					if errMsg == "" {
						errMsg = codes.Code(res.grpcCode).String()
					}
					if s.cfg.CppSpans {
						w.entry.trace.endSubscribe(resName, errMsg)
					} else {
						w.entry.trace.endSubscribe("", errMsg)
					}
				} else {
					w.entry.trace.endSubscribe(resName, "")
				}
			}
			if w.wantResult {
				st := res.status
				grpcCode := res.grpcCode
				errMsg := res.errMsg
				if !batchEOD && !s.cfg.HasFault("eod02-noneod-raw-status") {
					st = proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS
					grpcCode = 0
					errMsg = ""
				} else if st == proxypb.SettlementResultEvent_ACK_STATUS_UNSPECIFIED {
					st = proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS
				}
				s.emit(&proxypb.SubscriberEvent{
					Event: &proxypb.SubscriberEvent_SettlementResult{
						SettlementResult: &proxypb.SettlementResultEvent{
							DeliveryToken: w.entry.token,
							Status:        st,
							GrpcErrorCode: grpcCode,
							ErrorMessage:  errMsg,
							IsNack:        isNack,
						},
					},
				})
			}
			close(w.doneCh)
		}
	}

	var aloItems, eodItems []*settlementWaiter
	for _, w := range items {
		if w.entry.eod {
			eodItems = append(eodItems, w)
		} else {
			aloItems = append(aloItems, w)
		}
	}
	if len(aloItems) > 0 && len(eodItems) > 0 {
		if items[0].entry.eod {
			settleSubBatch(eodItems, true)
			settleSubBatch(aloItems, false)
		} else {
			settleSubBatch(aloItems, false)
			settleSubBatch(eodItems, true)
		}
		return
	}
	settleSubBatch(items, items[0].entry.eod)
}

type ackResultInfo struct {
	status   proxypb.SettlementResultEvent_AckStatus
	grpcCode int32
	errMsg   string
}

func extractErrorInfoMetadata(err error) map[string]string {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok && len(ei.Metadata) > 0 {
			return ei.Metadata
		}
	}
	return nil
}

func classifyPermanentReason(reason string) proxypb.SettlementResultEvent_AckStatus {
	if reason == "PERMANENT_FAILURE_INVALID_ACK_ID" {
		return proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID
	}
	return proxypb.SettlementResultEvent_ACK_STATUS_OTHER
}

func isTransientReason(reason string) bool {
	return strings.HasPrefix(reason, "TRANSIENT_")
}

func mapGRPCCodeToAckStatus(code codes.Code) proxypb.SettlementResultEvent_AckStatus {
	switch code {
	case codes.OK:
		return proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS
	case codes.PermissionDenied:
		return proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED
	case codes.FailedPrecondition:
		return proxypb.SettlementResultEvent_ACK_STATUS_FAILED_PRECONDITION
	case codes.Unavailable:
		return proxypb.SettlementResultEvent_ACK_STATUS_UNAVAILABLE
	default:
		return proxypb.SettlementResultEvent_ACK_STATUS_OTHER
	}
}

func isTransientAckCode(code codes.Code) bool {
	switch code {
	case codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unavailable:
		return true
	default:
		return false
	}
}

func (s *subscriberSession) sendAckWithRetryDetailed(ctx context.Context, ackIDs []string, isEOD bool, traces map[string][]*subscribeMessageTrace) map[string]ackResultInfo {
	chunks := splitAckIDBatches(ackIDs, maxAckBatchBytes, func(ids []string) int {
		return proto.Size(&pubsubpb.AcknowledgeRequest{Subscription: s.fullSub, AckIds: ids})
	})
	type ackOutcomeItem struct {
		id     string
		code   codes.Code
		errMsg string
	}
	endGoAckSub := func(tr *subscribeMessageTrace, code codes.Code, errMsg string) {
		if code == codes.OK {
			tr.endSubscribe("acked", "")
			return
		}
		if isEOD {
			msg := errMsg
			if msg == "" {
				msg = code.String()
			}
			tr.endSubscribe("", msg)
			return
		}
		tr.endSubscribe("acked", "")
	}
	out := make(map[string]ackResultInfo, len(ackIDs))
	for _, chunk := range chunks {
		pending := append([]string(nil), chunk...)
		delay := 100 * time.Millisecond
		if s.cfg.FastTimings {
			delay = 15 * time.Millisecond
		}
		deadline := time.Now().Add(600 * time.Second)
		var attached0 []*subscribeMessageTrace
		var activeSet map[*subscribeMessageTrace]bool
		var cppSpans map[*subscribeMessageTrace]*proxypb.TraceSpan

		emitAckResolution := func(items []ackOutcomeItem, attempt int) {
			if len(items) == 0 {
				return
			}
			var seen map[*subscribeMessageTrace]struct{}
			for _, it := range items {
				for _, tr := range traces[it.id] {
					if tr == nil {
						continue
					}
					if seen == nil {
						seen = make(map[*subscribeMessageTrace]struct{}, len(items))
					}
					if _, dup := seen[tr]; dup {
						continue
					}
					seen[tr] = struct{}{}
					if s.cfg.CppSpans {
						if sp := cppSpans[tr]; sp != nil {
							delete(cppSpans, tr)
							finishCppExactlyOnceSettleSpan(s.traces, s.enableMonitoring, sp, it.code, it.errMsg)
						}
						if activeSet[tr] {
							tr.addSubscribeEvent("ack end", 0)
						}
						continue
					}
					if activeSet[tr] {
						if attempt > 0 {
							tr.addSubscribeEvent("ack end", 0)
						}
						endGoAckSub(tr, it.code, it.errMsg)
					}
				}
			}
		}

		for attempt := 0; len(pending) > 0; attempt++ {
			var rpcSpan *proxypb.TraceSpan
			var attached int
			if attempt == 0 {
				unique := collectUniqueTraces(pending, traces)
				rpcSpan, attached0, attached = attachSettlementBatch(
					s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID,
					"ack", 0, false, !s.cfg.CppSpans, true, unique,
				)
				if len(attached0) > 0 {
					activeSet = make(map[*subscribeMessageTrace]bool, len(attached0))
					for _, tr := range attached0 {
						activeSet[tr] = true
					}
					if s.cfg.CppSpans {
						cppSpans = make(map[*subscribeMessageTrace]*proxypb.TraceSpan, len(attached0))
						for _, id := range pending {
							for _, tr := range traces[id] {
								if tr != nil && activeSet[tr] && cppSpans[tr] == nil {
									cppSpans[tr] = startCppExactlyOnceSettleSpan(s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID, "ack", id, tr)
								}
							}
						}
					}
				}
			}
			callCtx := s.subOutgoingContext(ctx)
			_, err := s.client.Acknowledge(callCtx, &pubsubpb.AcknowledgeRequest{
				Subscription: s.fullSub,
				AckIds:       pending,
			})

			var resolved []ackOutcomeItem
			var nextPending []string
			anyPermFail := false
			if err == nil {
				resolved = make([]ackOutcomeItem, 0, len(pending))
				for _, id := range pending {
					out[id] = ackResultInfo{status: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
					resolved = append(resolved, ackOutcomeItem{id: id, code: codes.OK})
				}
			} else {
				md := extractErrorInfoMetadata(err)
				st, _ := status.FromError(err)
				errMsgFallback := st.Message()
				if errMsgFallback == "" {
					errMsgFallback = err.Error()
				}
				if len(md) > 0 {
					for _, id := range pending {
						reason, found := md[id]
						if !found {
							out[id] = ackResultInfo{status: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
							resolved = append(resolved, ackOutcomeItem{id: id, code: codes.OK})
							continue
						}
						if isTransientReason(reason) && isEOD && time.Now().Before(deadline) && ctx.Err() == nil {
							nextPending = append(nextPending, id)
							continue
						}
						anyPermFail = true
						ackSt := classifyPermanentReason(reason)
						if isTransientReason(reason) {
							ackSt = mapGRPCCodeToAckStatus(st.Code())
						}
						out[id] = ackResultInfo{
							status:   ackSt,
							grpcCode: int32(st.Code()),
							errMsg:   reason,
						}
						resolved = append(resolved, ackOutcomeItem{id: id, code: st.Code(), errMsg: reason})
					}
				} else if isEOD && isTransientAckCode(st.Code()) && time.Now().Before(deadline) && ctx.Err() == nil {
					nextPending = append([]string(nil), pending...)
				} else {
					anyPermFail = true
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   mapGRPCCodeToAckStatus(st.Code()),
							grpcCode: int32(st.Code()),
							errMsg:   errMsgFallback,
						}
						resolved = append(resolved, ackOutcomeItem{id: id, code: st.Code(), errMsg: errMsgFallback})
					}
				}
			}

			if attempt == 0 && !s.cfg.CppSpans {
				for _, tr := range attached0 {
					tr.addSubscribeEvent("ack end", 0)
				}
				if rpcSpan != nil && attached > 0 {
					if err != nil || len(nextPending) > 0 || anyPermFail {
						spanErr := err
						if spanErr == nil {
							spanErr = errors.New("some messages in the batch failed or require retry")
						}
						recordSpanError(rpcSpan, spanErr.Error())
					}
					rpcSpan.EndTime = timestamppb.Now()
					s.traces.Record(s.enableMonitoring, rpcSpan)
				}
			}
			emitAckResolution(resolved, attempt)
			pending = nextPending

			if len(pending) > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-s.callbackAbortCh:
					t.Stop()
					aborted := make([]ackOutcomeItem, 0, len(pending))
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   proxypb.SettlementResultEvent_ACK_STATUS_OTHER,
							grpcCode: int32(codes.Canceled),
							errMsg:   "subscriber aborted during ack retry",
						}
						aborted = append(aborted, ackOutcomeItem{id: id, code: codes.Canceled, errMsg: "subscriber aborted during ack retry"})
					}
					emitAckResolution(aborted, attempt+1)
					pending = nil
				case <-ctx.Done():
					t.Stop()
					ctxCode := status.FromContextError(ctx.Err()).Code()
					if ctxCode == codes.OK {
						ctxCode = codes.DeadlineExceeded
					}
					canceled := make([]ackOutcomeItem, 0, len(pending))
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   proxypb.SettlementResultEvent_ACK_STATUS_OTHER,
							grpcCode: int32(ctxCode),
							errMsg:   ctx.Err().Error(),
						}
						canceled = append(canceled, ackOutcomeItem{id: id, code: ctxCode, errMsg: ctx.Err().Error()})
					}
					emitAckResolution(canceled, attempt+1)
					pending = nil
				}
				delay *= 2
				if delay > 64*time.Second {
					delay = 64 * time.Second
				}
			}
		}
	}
	return out
}

func (s *subscriberSession) sendModAckWithRetry(ctx context.Context, ackIDs []string, deadlineSec int32, isReceipt bool, isEOD bool, traces map[string][]*subscribeMessageTrace) map[string]proxypb.SettlementResultEvent_AckStatus {
	detailed := s.sendModAckWithRetryDetailed(ctx, ackIDs, deadlineSec, isReceipt, isEOD, traces)
	out := make(map[string]proxypb.SettlementResultEvent_AckStatus, len(detailed))
	for k, v := range detailed {
		out[k] = v.status
	}
	return out
}

func (s *subscriberSession) sendModAckRaw(ctx context.Context, ackIDs []string, deadlineSec int32, isReceipt bool, traces map[string][]*subscribeMessageTrace) error {
	chunks := splitAckIDBatches(ackIDs, maxAckBatchBytes, func(ids []string) int {
		return proto.Size(&pubsubpb.ModifyAckDeadlineRequest{Subscription: s.fullSub, AckIds: ids, AckDeadlineSeconds: deadlineSec})
	})
	op := "modack"
	if deadlineSec == 0 {
		op = "nack"
	}
	var firstErr error
	for _, chunk := range chunks {
		unique := collectUniqueTraces(chunk, traces)
		rpcSpan, active, attached := attachSettlementBatch(
			s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID,
			op, deadlineSec, isReceipt, !s.cfg.CppSpans || op == "modack", true, unique,
		)
		_, err := s.client.ModifyAckDeadline(s.subOutgoingContext(ctx), &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       s.fullSub,
			AckIds:             chunk,
			AckDeadlineSeconds: deadlineSec,
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if rpcSpan != nil && attached > 0 {
			if err != nil && (!s.cfg.CppSpans || op != "modack") {
				recordSpanError(rpcSpan, err.Error())
			}
			rpcSpan.EndTime = timestamppb.Now()
			s.traces.Record(s.enableMonitoring, rpcSpan)
		}
		if s.cfg.CppSpans && op == "nack" {
			st, _ := status.FromError(err)
			errMsg := ""
			if err != nil {
				errMsg = st.Message()
				if errMsg == "" {
					errMsg = err.Error()
				}
			}
			activeSet := make(map[*subscribeMessageTrace]bool, len(active))
			for _, tr := range active {
				activeSet[tr] = true
			}
			var seen map[*subscribeMessageTrace]struct{}
			for _, id := range chunk {
				for _, tr := range traces[id] {
					if tr == nil || !activeSet[tr] {
						continue
					}
					if seen == nil {
						seen = make(map[*subscribeMessageTrace]struct{}, len(active))
					}
					if _, dup := seen[tr]; dup {
						continue
					}
					seen[tr] = struct{}{}
					sp := newCppExactlyOnceSettleSpan(s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID, "nack", id, tr, st.Code(), errMsg)
					s.traces.Record(s.enableMonitoring, sp)
					tr.addSubscribeEvent("nack end", 0)
				}
			}
		} else {
			for _, tr := range active {
				tr.addSubscribeEvent(op+" end", 0)
			}
		}
	}
	return firstErr
}

func (s *subscriberSession) sendModAckWithRetryDetailed(ctx context.Context, ackIDs []string, deadlineSec int32, isReceipt bool, isEOD bool, traces map[string][]*subscribeMessageTrace) map[string]ackResultInfo {
	chunks := splitAckIDBatches(ackIDs, maxAckBatchBytes, func(ids []string) int {
		return proto.Size(&pubsubpb.ModifyAckDeadlineRequest{Subscription: s.fullSub, AckIds: ids, AckDeadlineSeconds: deadlineSec})
	})
	op := "modack"
	if deadlineSec == 0 {
		op = "nack"
	}
	type modOutcomeItem struct {
		id     string
		code   codes.Code
		errMsg string
	}
	endGoNackSub := func(tr *subscribeMessageTrace, code codes.Code, errMsg string) {
		if op != "nack" {
			return
		}
		if code == codes.OK {
			tr.endSubscribe("nacked", "")
			return
		}
		if isEOD {
			msg := errMsg
			if msg == "" {
				msg = code.String()
			}
			tr.endSubscribe("", msg)
			return
		}
		tr.endSubscribe("nacked", "")
	}
	out := make(map[string]ackResultInfo, len(ackIDs))
	for _, chunk := range chunks {
		pending := append([]string(nil), chunk...)
		delay := 100 * time.Millisecond
		if s.cfg.FastTimings {
			delay = 15 * time.Millisecond
		}
		maxRetries := 10
		if deadlineSec > 0 && !isReceipt {
			maxRetries = 3
		}
		var activeSet map[*subscribeMessageTrace]bool
		var cppSpans map[*subscribeMessageTrace]*proxypb.TraceSpan

		emitModAckResolution := func(items []modOutcomeItem, attempt int) {
			if len(items) == 0 || (s.cfg.CppSpans && op == "modack") {
				return
			}
			var seen map[*subscribeMessageTrace]struct{}
			for _, it := range items {
				for _, tr := range traces[it.id] {
					if tr == nil {
						continue
					}
					if seen == nil {
						seen = make(map[*subscribeMessageTrace]struct{}, len(items))
					}
					if _, dup := seen[tr]; dup {
						continue
					}
					seen[tr] = struct{}{}
					if s.cfg.CppSpans {
						if op == "nack" {
							if sp := cppSpans[tr]; sp != nil {
								delete(cppSpans, tr)
								finishCppExactlyOnceSettleSpan(s.traces, s.enableMonitoring, sp, it.code, it.errMsg)
							}
							if activeSet[tr] {
								tr.addSubscribeEvent("nack end", 0)
							}
						}
						continue
					}
					if activeSet[tr] {
						if attempt > 0 {
							tr.addSubscribeEvent(op+" end", 0)
						}
						endGoNackSub(tr, it.code, it.errMsg)
					}
				}
			}
		}

		for attempt := 0; len(pending) > 0; attempt++ {
			var rpcSpan *proxypb.TraceSpan
			var attachedAttempt []*subscribeMessageTrace
			var attached int
			if attempt == 0 || (s.cfg.CppSpans && op == "modack") {
				unique := collectUniqueTraces(pending, traces)
				rpcSpan, attachedAttempt, attached = attachSettlementBatch(
					s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID,
					op, deadlineSec, isReceipt, !s.cfg.CppSpans || op == "modack", attempt == 0, unique,
				)
				if attempt == 0 && len(attachedAttempt) > 0 {
					activeSet = make(map[*subscribeMessageTrace]bool, len(attachedAttempt))
					for _, tr := range attachedAttempt {
						activeSet[tr] = true
					}
					if s.cfg.CppSpans && op == "nack" {
						cppSpans = make(map[*subscribeMessageTrace]*proxypb.TraceSpan, len(attachedAttempt))
						for _, id := range pending {
							for _, tr := range traces[id] {
								if tr != nil && activeSet[tr] && cppSpans[tr] == nil {
									cppSpans[tr] = startCppExactlyOnceSettleSpan(s.traces, s.enableMonitoring, s.projectID, s.fullSub, s.subID, "nack", id, tr)
								}
							}
						}
					}
				}
			}
			_, err := s.client.ModifyAckDeadline(s.subOutgoingContext(ctx), &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       s.fullSub,
				AckIds:             pending,
				AckDeadlineSeconds: deadlineSec,
			})

			var resolved []modOutcomeItem
			var nextPending []string
			anyPermFail := false
			if err == nil {
				resolved = make([]modOutcomeItem, 0, len(pending))
				for _, id := range pending {
					out[id] = ackResultInfo{status: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
					resolved = append(resolved, modOutcomeItem{id: id, code: codes.OK})
				}
			} else {
				md := extractErrorInfoMetadata(err)
				st, _ := status.FromError(err)
				errMsgFallback := st.Message()
				if errMsgFallback == "" {
					errMsgFallback = err.Error()
				}
				if len(md) > 0 {
					for _, id := range pending {
						reason, found := md[id]
						if !found {
							out[id] = ackResultInfo{status: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS}
							resolved = append(resolved, modOutcomeItem{id: id, code: codes.OK})
							continue
						}
						if isTransientReason(reason) && isEOD && attempt < maxRetries && ctx.Err() == nil {
							nextPending = append(nextPending, id)
							continue
						}
						anyPermFail = true
						ackSt := classifyPermanentReason(reason)
						if isTransientReason(reason) {
							ackSt = mapGRPCCodeToAckStatus(st.Code())
						}
						out[id] = ackResultInfo{
							status:   ackSt,
							grpcCode: int32(st.Code()),
							errMsg:   reason,
						}
						resolved = append(resolved, modOutcomeItem{id: id, code: st.Code(), errMsg: reason})
					}
				} else if isEOD && isTransientAckCode(st.Code()) && attempt < maxRetries && ctx.Err() == nil {
					nextPending = append([]string(nil), pending...)
				} else {
					anyPermFail = true
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   mapGRPCCodeToAckStatus(st.Code()),
							grpcCode: int32(st.Code()),
							errMsg:   errMsgFallback,
						}
						resolved = append(resolved, modOutcomeItem{id: id, code: st.Code(), errMsg: errMsgFallback})
					}
				}
			}

			if rpcSpan != nil && attached > 0 {
				if (!s.cfg.CppSpans || op != "modack") && (err != nil || (!s.cfg.CppSpans && (len(nextPending) > 0 || anyPermFail))) {
					spanErr := err
					if spanErr == nil {
						spanErr = errors.New("some messages in the batch failed or require retry")
					}
					recordSpanError(rpcSpan, spanErr.Error())
				}
				rpcSpan.EndTime = timestamppb.Now()
				s.traces.Record(s.enableMonitoring, rpcSpan)
			}
			if s.cfg.CppSpans && op == "modack" {
				for _, tr := range attachedAttempt {
					tr.addSubscribeEvent("modack end", 0)
				}
			} else if !s.cfg.CppSpans && attempt == 0 {
				for _, tr := range attachedAttempt {
					tr.addSubscribeEvent(op+" end", 0)
				}
			}
			emitModAckResolution(resolved, attempt)
			pending = nextPending

			if len(pending) > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-s.callbackAbortCh:
					t.Stop()
					aborted := make([]modOutcomeItem, 0, len(pending))
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   proxypb.SettlementResultEvent_ACK_STATUS_OTHER,
							grpcCode: int32(codes.Canceled),
							errMsg:   "subscriber aborted during modack retry",
						}
						aborted = append(aborted, modOutcomeItem{id: id, code: codes.Canceled, errMsg: "subscriber aborted during modack retry"})
					}
					emitModAckResolution(aborted, attempt+1)
					pending = nil
				case <-ctx.Done():
					t.Stop()
					ctxCode := status.FromContextError(ctx.Err()).Code()
					if ctxCode == codes.OK {
						ctxCode = codes.DeadlineExceeded
					}
					canceled := make([]modOutcomeItem, 0, len(pending))
					for _, id := range pending {
						out[id] = ackResultInfo{
							status:   proxypb.SettlementResultEvent_ACK_STATUS_OTHER,
							grpcCode: int32(ctxCode),
							errMsg:   ctx.Err().Error(),
						}
						canceled = append(canceled, modOutcomeItem{id: id, code: ctxCode, errMsg: ctx.Err().Error()})
					}
					emitModAckResolution(canceled, attempt+1)
					pending = nil
				}
				delay *= 2
				if delay > 64*time.Second {
					delay = 64 * time.Second
				}
			}
		}
	}
	return out
}

// handleUnaryPull implements the synchronous UnaryPull RPC.
func handleUnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest, srv *ProxyServer) (*proxypb.UnaryPullResponse, error) {
	cfg := srv.cfg
	if !cfg.SupportsUnaryPull {
		return nil, status.Error(codes.Unimplemented, "unary pull not supported")
	}
	if !cfg.SupportsAckWithResult && (req.SettleAction == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT ||
		req.SettleAction == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT) {
		return nil, status.Error(codes.Unimplemented, "ack/nack with result not supported")
	}
	if !cfg.SupportsCustomRetryCodes && len(req.RetryCodes) > 0 {
		return nil, status.Error(codes.Unimplemented, "custom retry codes not supported")
	}

	projID, subID, fullSub, err := extractResourceNames(req.Subscription, "subscriptions", req.ProjectId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if cfg.HasFault("hang-pull") {
		select {
		case <-srv.stopCh:
			return nil, status.Error(codes.Unavailable, "proxy shutting down")
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if cfg.Delay > 0 {
		select {
		case <-time.After(cfg.Delay):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if cfg.HasFault("missing-rpc") {
		return &proxypb.UnaryPullResponse{}, nil
	}

	conn, err := dialPubSubEndpoint(req.Endpoint, req.UseEmulatorEnv)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pubsubpb.NewSubscriberClient(conn)

	outCtx := ctx
	pairs := make([]string, 0, 2+len(req.CustomHeaders)*2)
	if !cfg.HasFault("missing-routing-header") {
		subVal := url.QueryEscape(fullSub)
		if cfg.UnescapedRoutingHeader {
			subVal = fullSub
		}
		pairs = append(pairs, "x-goog-request-params", "subscription="+subVal)
	}
	if !cfg.HasFault("missing-custom-headers") {
		for k, v := range req.CustomHeaders {
			pairs = append(pairs, k, v)
		}
	}
	if len(pairs) > 0 {
		outCtx = metadata.AppendToOutgoingContext(ctx, pairs...)
	}

	if req.TotalTimeoutMs != nil && *req.TotalTimeoutMs > 0 {
		var cancel context.CancelFunc
		outCtx, cancel = context.WithTimeout(outCtx, time.Duration(*req.TotalTimeoutMs)*time.Millisecond)
		defer cancel()
	}

	maxMsgs := req.MaxMessages
	if maxMsgs <= 0 {
		maxMsgs = 1
	}

	pullTopicOrSub := fullSub
	if cfg.HasFault("wrong-rpc-method") {
		pullTopicOrSub = fullSub + "-wrong"
	}

	retryDelay := time.Duration(cfg.Defaults.RetryInitialDelayMs) * time.Millisecond
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}
	if req.InitialRetryDelayMs != nil && *req.InitialRetryDelayMs > 0 {
		retryDelay = time.Duration(*req.InitialRetryDelayMs) * time.Millisecond
	}
	maxRetryDelay := time.Duration(cfg.Defaults.RetryMaxDelayMs) * time.Millisecond
	if maxRetryDelay <= 0 {
		maxRetryDelay = 60 * time.Second
	}
	if req.MaxRetryDelayMs != nil && *req.MaxRetryDelayMs > 0 {
		maxRetryDelay = time.Duration(*req.MaxRetryDelayMs) * time.Millisecond
	}
	retryMult := cfg.Defaults.RetryDelayMultiplier
	if retryMult <= 1.0 {
		retryMult = 2.0
	}
	if req.RetryDelayMultiplier != nil && *req.RetryDelayMultiplier > 1.0 {
		retryMult = *req.RetryDelayMultiplier
	}
	maxAttempts := 3
	if req.TotalTimeoutMs != nil && *req.TotalTimeoutMs > 0 && req.MaxAttempts == nil {
		maxAttempts = 100
	}
	if req.MaxAttempts != nil && *req.MaxAttempts > 0 {
		maxAttempts = int(*req.MaxAttempts)
	}

	var customRetryCodes map[codes.Code]bool
	if len(req.RetryCodes) > 0 {
		customRetryCodes = make(map[codes.Code]bool, len(req.RetryCodes))
		for _, c := range req.RetryCodes {
			customRetryCodes[codes.Code(c)] = true
		}
	}
	shouldRetryPull := func(code codes.Code) bool {
		if len(customRetryCodes) > 0 {
			return customRetryCodes[code]
		}
		return isTransientAckCode(code)
	}

	var resp *pubsubpb.PullResponse
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err = client.Pull(outCtx, &pubsubpb.PullRequest{
			Subscription:      pullTopicOrSub,
			MaxMessages:       maxMsgs,
			ReturnImmediately: req.ReturnImmediately,
		})
		if err == nil {
			break
		}
		st, _ := status.FromError(err)
		if attempt+1 >= maxAttempts || !shouldRetryPull(st.Code()) || outCtx.Err() != nil {
			break
		}
		t := time.NewTimer(retryDelay)
		select {
		case <-t.C:
		case <-outCtx.Done():
			t.Stop()
			err = outCtx.Err()
			st = status.FromContextError(err)
			if st == nil || st.Code() == codes.OK {
				st = status.New(codes.DeadlineExceeded, err.Error())
			}
			if cfg.CppSpans && req.EnableMonitoring {
				sp := newCppUnaryPullReceiveSpan(srv.traces, req.EnableMonitoring, projID, subID, "", nil, nil, "", st.Code(), st.Message())
				srv.traces.Record(req.EnableMonitoring, sp)
			}
			return &proxypb.UnaryPullResponse{
				ErrorCode:    int32(st.Code()),
				ErrorMessage: st.Message(),
			}, nil
		}
		nextDelay := time.Duration(float64(retryDelay) * retryMult)
		if nextDelay > maxRetryDelay {
			nextDelay = maxRetryDelay
		}
		retryDelay = nextDelay
	}
	if err != nil {
		st, _ := status.FromError(err)
		if cfg.CppSpans && req.EnableMonitoring {
			sp := newCppUnaryPullReceiveSpan(srv.traces, req.EnableMonitoring, projID, subID, "", nil, nil, "", st.Code(), st.Message())
			srv.traces.Record(req.EnableMonitoring, sp)
		}
		return &proxypb.UnaryPullResponse{
			ErrorCode:    int32(st.Code()),
			ErrorMessage: st.Message(),
		}, nil
	}

	if len(resp.ReceivedMessages) == 0 {
		return &proxypb.UnaryPullResponse{}, nil
	}

	n := len(resp.ReceivedMessages)
	ackIDs := make([]string, n)
	recvSpans := make([]*proxypb.TraceSpan, n)
	subTraces := make([]*subscribeMessageTrace, n)
	for i, rm := range resp.ReceivedMessages {
		ackIDs[i] = rm.AckId
		if cfg.CppSpans {
			sp := newCppUnaryPullReceiveSpan(
				srv.traces,
				req.EnableMonitoring,
				projID,
				subID,
				rm.Message.GetMessageId(),
				rm.Message.GetData(),
				rm.Message.GetAttributes(),
				rm.Message.GetOrderingKey(),
				codes.OK,
				"",
			)
			srv.traces.Record(req.EnableMonitoring, sp)
			recvSpans[i] = sp
		} else {
			tr := newSubscribeMessageTrace(
				srv.traces,
				req.EnableMonitoring,
				projID,
				fullSub,
				subID,
				rm.AckId,
				rm.Message.GetMessageId(),
				rm.Message.GetData(),
				rm.Message.GetAttributes(),
				rm.Message.GetOrderingKey(),
				rm.DeliveryAttempt,
				n,
				false,
			)
			_, _ = tr.startProcess(rm.Message.GetMessageId(), len(rm.Message.GetData()), rm.Message.GetOrderingKey(), rm.DeliveryAttempt, rm.Message.GetAttributes())
			subTraces[i] = tr
		}
	}

	// Optional lease hold with extension.
	if req.LeaseExtensionMs != nil && *req.LeaseExtensionMs > 0 {
		hold := time.Duration(*req.LeaseExtensionMs) * time.Millisecond
		deadlineSec := int32(10)
		if req.MinDurationPerAckExtensionMs != nil && *req.MinDurationPerAckExtensionMs > 0 {
			sec := int32(*req.MinDurationPerAckExtensionMs / 1000)
			if sec >= 10 && sec <= 600 {
				deadlineSec = sec
			}
		}
		if req.MaxDurationPerAckExtensionMs != nil && *req.MaxDurationPerAckExtensionMs > 0 {
			sec := int32(*req.MaxDurationPerAckExtensionMs / 1000)
			if sec >= 10 && sec < deadlineSec {
				deadlineSec = sec
			}
		}
		var modSpan *proxypb.TraceSpan
		if !cfg.CppSpans {
			modSpan = newSubscribeRPCSpan(srv.traces, req.EnableMonitoring, projID, fullSub, subID, "modack", len(ackIDs), deadlineSec, false)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("modack start", len(ackIDs))
					tr.linkSubAndRPCSpan(modSpan, "modack")
				}
			}
		}
		_, modErr := client.ModifyAckDeadline(outCtx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       fullSub,
			AckIds:             ackIDs,
			AckDeadlineSeconds: deadlineSec,
		})
		if modSpan != nil {
			if modErr != nil {
				recordSpanError(modSpan, modErr.Error())
			}
			modSpan.EndTime = timestamppb.Now()
			srv.traces.Record(req.EnableMonitoring, modSpan)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("modack end", 0)
				}
			}
		} else if cfg.CppSpans && req.EnableMonitoring {
			st, _ := status.FromError(modErr)
			errMsg := ""
			if modErr != nil {
				errMsg = st.Message()
				if errMsg == "" {
					errMsg = modErr.Error()
				}
			}
			for i, ackID := range ackIDs {
				sp := newCppUnaryPullSettleSpan(srv.traces, req.EnableMonitoring, projID, subID, "modack", ackID, resp.ReceivedMessages[i].DeliveryAttempt, deadlineSec, recvSpans[i], st.Code(), errMsg)
				srv.traces.Record(req.EnableMonitoring, sp)
			}
		}
		timer := time.NewTimer(hold)
		select {
		case <-timer.C:
		case <-outCtx.Done():
			timer.Stop()
		}
	}

	perStatus := make([]proxypb.SettlementResultEvent_AckStatus, n)
	perCode := make([]int32, n)
	perErr := make([]string, n)

	recordSettlementResult := func(op, procAction, subResult string, err error) {
		md := extractErrorInfoMetadata(err)
		st, _ := status.FromError(err)
		batchMsg := ""
		if err != nil {
			batchMsg = st.Message()
			if batchMsg == "" {
				batchMsg = err.Error()
			}
		}
		for i, ackID := range ackIDs {
			msgCode := codes.OK
			msgErr := ""
			msgStatus := proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS
			if err != nil {
				if len(md) > 0 {
					if reason, found := md[ackID]; found {
						msgCode = st.Code()
						msgErr = reason
						if isTransientReason(reason) {
							msgStatus = mapGRPCCodeToAckStatus(st.Code())
						} else {
							msgStatus = classifyPermanentReason(reason)
						}
					}
				} else {
					msgCode = st.Code()
					msgErr = batchMsg
					msgStatus = mapGRPCCodeToAckStatus(st.Code())
				}
			}
			perStatus[i] = msgStatus
			perCode[i] = int32(msgCode)
			perErr[i] = msgErr
			if cfg.CppSpans && req.EnableMonitoring {
				sp := newCppUnaryPullSettleSpan(srv.traces, req.EnableMonitoring, projID, subID, op, ackID, resp.ReceivedMessages[i].DeliveryAttempt, 0, recvSpans[i], msgCode, msgErr)
				srv.traces.Record(req.EnableMonitoring, sp)
			} else if subTraces[i] != nil {
				subTraces[i].endProcess(procAction)
				subTraces[i].endSubscribe(subResult, "")
			}
		}
	}

	switch req.SettleAction {
	case proxypb.SettleMessageCommand_SETTLE_ACTION_ACK, proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT:
		var rpcSpan *proxypb.TraceSpan
		if !cfg.CppSpans {
			rpcSpan = newSubscribeRPCSpan(srv.traces, req.EnableMonitoring, projID, fullSub, subID, "ack", len(ackIDs), 0, false)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("ack start", len(ackIDs))
					tr.linkSubAndRPCSpan(rpcSpan, "ack")
				}
			}
		}
		_, settleErr := client.Acknowledge(outCtx, &pubsubpb.AcknowledgeRequest{
			Subscription: fullSub,
			AckIds:       ackIDs,
		})
		if rpcSpan != nil {
			if settleErr != nil {
				recordSpanError(rpcSpan, settleErr.Error())
			}
			rpcSpan.EndTime = timestamppb.Now()
			srv.traces.Record(req.EnableMonitoring, rpcSpan)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("ack end", 0)
				}
			}
		}
		recordSettlementResult("ack", "ack", "acked", settleErr)
	case proxypb.SettleMessageCommand_SETTLE_ACTION_NACK, proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT:
		var rpcSpan *proxypb.TraceSpan
		if !cfg.CppSpans {
			rpcSpan = newSubscribeRPCSpan(srv.traces, req.EnableMonitoring, projID, fullSub, subID, "nack", len(ackIDs), 0, false)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("nack start", len(ackIDs))
					tr.linkSubAndRPCSpan(rpcSpan, "nack")
				}
			}
		}
		_, settleErr := client.ModifyAckDeadline(outCtx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription:       fullSub,
			AckIds:             ackIDs,
			AckDeadlineSeconds: 0,
		})
		if rpcSpan != nil {
			if settleErr != nil {
				recordSpanError(rpcSpan, settleErr.Error())
			}
			rpcSpan.EndTime = timestamppb.Now()
			srv.traces.Record(req.EnableMonitoring, rpcSpan)
			for _, tr := range subTraces {
				if tr != nil {
					tr.addSubscribeEvent("nack end", 0)
				}
			}
		}
		recordSettlementResult("nack", "nack", "nacked", settleErr)
	case proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE:
		if cfg.SupportsAutoNackOnDrop {
			var rpcSpan *proxypb.TraceSpan
			if !cfg.CppSpans {
				rpcSpan = newSubscribeRPCSpan(srv.traces, req.EnableMonitoring, projID, fullSub, subID, "nack", len(ackIDs), 0, false)
				for _, tr := range subTraces {
					if tr != nil {
						tr.addSubscribeEvent("nack start", len(ackIDs))
						tr.linkSubAndRPCSpan(rpcSpan, "nack")
					}
				}
			}
			_, settleErr := client.ModifyAckDeadline(outCtx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       fullSub,
				AckIds:             ackIDs,
				AckDeadlineSeconds: 0,
			})
			if rpcSpan != nil {
				if settleErr != nil {
					recordSpanError(rpcSpan, settleErr.Error())
				}
				rpcSpan.EndTime = timestamppb.Now()
				srv.traces.Record(req.EnableMonitoring, rpcSpan)
				for _, tr := range subTraces {
					if tr != nil {
						tr.addSubscribeEvent("nack end", 0)
					}
				}
			}
			recordSettlementResult("nack", "nack", "nacked", settleErr)
		} else {
			for _, tr := range subTraces {
				if tr != nil {
					tr.endProcess("")
					tr.endSubscribe("", "")
				}
			}
		}
	default:
		for _, tr := range subTraces {
			if tr != nil {
				tr.endProcess("")
				tr.endSubscribe("", "")
			}
		}
	}

	pulled := make([]*proxypb.UnaryPullResponse_PulledMessage, 0, n)
	for i, rm := range resp.ReceivedMessages {
		pm := &proxypb.UnaryPullResponse_PulledMessage{
			MessageId:          rm.Message.GetMessageId(),
			Data:               rm.Message.GetData(),
			Attributes:         maps.Clone(rm.Message.GetAttributes()),
			OrderingKey:        rm.Message.GetOrderingKey(),
			PublishTime:        rm.Message.GetPublishTime(),
			DeliveryAttempt:    rm.DeliveryAttempt,
			HasDeliveryAttempt: rm.DeliveryAttempt > 0,
			SettleStatus:       perStatus[i],
			SettleErrorCode:    perCode[i],
			SettleErrorMessage: perErr[i],
		}
		pulled = append(pulled, pm)
	}
	return &proxypb.UnaryPullResponse{Messages: pulled}, nil
}
