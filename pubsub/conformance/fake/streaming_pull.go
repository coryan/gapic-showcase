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
	"fmt"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pushConfig struct {
	subProps     *pubsubpb.StreamingPullResponse_SubscriptionProperties
	ackConf      *pubsubpb.StreamingPullResponse_AcknowledgeConfirmation
	modAckConf   *pubsubpb.StreamingPullResponse_ModifyAckDeadlineConfirmation
	hasDelivAtt  bool
	delivAttempt int32
	hasOrderKey  bool
	orderingKey  string
	hasPubTime   bool
	publishTime  time.Time
	attrs        map[string]string
}

// PushOption customizes messages and metadata pushed on a StreamingPull stream.
type PushOption func(*pushConfig)

// WithSubscriptionProperties attaches SubscriptionProperties (EOD and message
// ordering flags) to the StreamingPullResponse.
func WithSubscriptionProperties(exactlyOnceDelivery, messageOrdering bool) PushOption {
	return func(c *pushConfig) {
		c.subProps = &pubsubpb.StreamingPullResponse_SubscriptionProperties{
			ExactlyOnceDeliveryEnabled: exactlyOnceDelivery,
			MessageOrderingEnabled:     messageOrdering,
		}
	}
}

// WithDeliveryAttempt sets DeliveryAttempt on every ReceivedMessage in the push.
func WithDeliveryAttempt(attempt int32) PushOption {
	return func(c *pushConfig) {
		c.hasDelivAtt = true
		c.delivAttempt = attempt
	}
}

// WithOrderingKey sets PubsubMessage.OrderingKey on every pushed message.
func WithOrderingKey(key string) PushOption {
	return func(c *pushConfig) {
		c.hasOrderKey = true
		c.orderingKey = key
	}
}

// WithPublishTime sets PubsubMessage.PublishTime on every pushed message.
func WithPublishTime(t time.Time) PushOption {
	return func(c *pushConfig) {
		c.hasPubTime = true
		c.publishTime = t
	}
}

// WithAttributes merges attrs into PubsubMessage.Attributes on pushed messages.
func WithAttributes(attrs map[string]string) PushOption {
	copied := maps.Clone(attrs)
	return func(c *pushConfig) {
		if len(copied) == 0 {
			return
		}
		if c.attrs == nil {
			c.attrs = make(map[string]string, len(copied))
		}
		maps.Copy(c.attrs, copied)
	}
}

// WithAcknowledgeConfirmation attaches an AcknowledgeConfirmation to the response.
func WithAcknowledgeConfirmation(conf *pubsubpb.StreamingPullResponse_AcknowledgeConfirmation) PushOption {
	cloned := proto.CloneOf(conf)
	return func(c *pushConfig) {
		c.ackConf = cloned
	}
}

// WithModifyAckDeadlineConfirmation attaches a ModifyAckDeadlineConfirmation to the response.
func WithModifyAckDeadlineConfirmation(conf *pubsubpb.StreamingPullResponse_ModifyAckDeadlineConfirmation) PushOption {
	cloned := proto.CloneOf(conf)
	return func(c *pushConfig) {
		c.modAckConf = cloned
	}
}

type streamOutboundItem struct {
	resp    *pubsubpb.StreamingPullResponse
	isClose bool
	err     error
}

type streamSession struct {
	srv    *Server
	ep     *epochState
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	conn   net.Conn

	wakeCh         chan struct{}
	closedCh       chan struct{}
	forceCloseOnce sync.Once

	mu           sync.Mutex
	queue        []streamOutboundItem
	closed       bool
	ready        bool
	abortErr     error
	suppressPong *bool

	// Summary fields protected by mu (lock order: srv.mu -> st.mu ONLY).
	hasInitial               bool
	subscription             string
	clientID                 string
	protocolVersion          int64
	streamAckDeadlineSeconds int32
	maxOutstandingMessages   int64
	maxOutstandingBytes      int64
	openedAt                 time.Time
	closedAt                 time.Time
	frameCount               int
	keepalivePingCount       int
	ackIDsSent               []string
	modAckCount              int
	initialRequest           *pubsubpb.StreamingPullRequest
	initialMetadata          metadata.MD
	requestParams            []string
	closeCode                codes.Code
	closeErr                 string
	sentMessages             []*pubsubpb.ReceivedMessage
}

func newStreamSession(srv *Server, ep *epochState, id string, ctx context.Context, cancel context.CancelFunc, conn net.Conn, md metadata.MD) *streamSession {
	return &streamSession{
		srv:             srv,
		ep:              ep,
		id:              id,
		ctx:             ctx,
		cancel:          cancel,
		conn:            conn,
		wakeCh:          make(chan struct{}, 1),
		closedCh:        make(chan struct{}),
		openedAt:        time.Now(),
		initialMetadata: md.Copy(),
		requestParams:   ExtractRequestParams(md),
	}
}

func (st *streamSession) signalWake() {
	select {
	case st.wakeCh <- struct{}{}:
	default:
	}
}

func (st *streamSession) hasInitialFrame() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.hasInitial
}

func (st *streamSession) isReady() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.ready
}

func (st *streamSession) markReady() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ready {
		return false
	}
	st.ready = true
	return true
}

func (st *streamSession) scheduleForceClose(delay time.Duration) {
	if st.conn == nil || st.isClosed() {
		return
	}
	st.forceCloseOnce.Do(func() {
		go func() {
			if st.srv != nil && st.srv.testHooks != nil && st.srv.testHooks.onForceCloseExit != nil {
				defer st.srv.testHooks.onForceCloseExit()
			}
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-st.closedCh:
				return
			case <-timer.C:
				select {
				case <-st.closedCh:
					return
				default:
				}
				_ = st.conn.Close()
			}
		}()
	})
}

func (st *streamSession) isClosed() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

func (st *streamSession) summary() StreamSummary {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := StreamSummary{
		StreamID:                 st.id,
		Subscription:             st.subscription,
		ClientID:                 st.clientID,
		ProtocolVersion:          st.protocolVersion,
		StreamAckDeadlineSeconds: st.streamAckDeadlineSeconds,
		MaxOutstandingMessages:   st.maxOutstandingMessages,
		MaxOutstandingBytes:      st.maxOutstandingBytes,
		OpenedAt:                 st.openedAt,
		ClosedAt:                 st.closedAt,
		Closed:                   st.closed,
		HasInitial:               st.hasInitial,
		FrameCount:               st.frameCount,
		KeepalivePingCount:       st.keepalivePingCount,
		AckIDsSent:               slices.Clone(st.ackIDsSent),
		ModAckCount:              st.modAckCount,
		InitialRequest:           proto.CloneOf(st.initialRequest),
		RequestParams:            slices.Clone(st.requestParams),
		CloseCode:                st.closeCode,
		CloseErr:                 st.closeErr,
	}
	if st.initialMetadata != nil {
		out.InitialMetadata = st.initialMetadata.Copy()
	}
	if len(st.sentMessages) > 0 {
		out.SentMessages = make([]*pubsubpb.ReceivedMessage, len(st.sentMessages))
		for i, m := range st.sentMessages {
			out.SentMessages[i] = proto.CloneOf(m)
		}
	}
	return out
}

func (st *streamSession) enqueueResponse(resp *pubsubpb.StreamingPullResponse) error {
	cloned := proto.CloneOf(resp)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return fmt.Errorf("stream %q is closed", st.id)
	}
	st.queue = append(st.queue, streamOutboundItem{resp: cloned})
	st.signalWake()
	return nil
}

func (st *streamSession) enqueueClose(err error) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return fmt.Errorf("stream %q is already closed", st.id)
	}
	st.queue = append(st.queue, streamOutboundItem{isClose: true, err: err})
	st.signalWake()
	return nil
}

func (st *streamSession) popOutbound() (streamOutboundItem, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.queue) == 0 {
		return streamOutboundItem{}, false
	}
	item := st.queue[0]
	st.queue[0] = streamOutboundItem{}
	st.queue = st.queue[1:]
	return item, true
}

func (st *streamSession) abort(err error) {
	st.mu.Lock()
	if st.abortErr == nil && err != nil {
		st.abortErr = err
	}
	st.mu.Unlock()
	st.cancel()
}

func (st *streamSession) abortErrOrDefault() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.abortErr != nil {
		return st.abortErr
	}
	return status.Error(codes.Unavailable, "fake stream aborted")
}

func (st *streamSession) markClosed(err error) {
	st.mu.Lock()
	if !st.closed {
		st.closed = true
		st.closedAt = time.Now()
		st.closeCode = status.Code(err)
		if err != nil {
			st.closeErr = err.Error()
		}
		close(st.closedCh)
	}
	st.mu.Unlock()
	st.cancel()
}

func (st *streamSession) setSuppressPong(suppress bool) {
	v := suppress
	st.mu.Lock()
	st.suppressPong = &v
	st.mu.Unlock()
}

func (st *streamSession) protoVersion() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.protocolVersion
}

func (st *streamSession) effectiveSuppressPong(serverDefault bool) bool {
	st.mu.Lock()
	override := st.suppressPong
	st.mu.Unlock()
	if override != nil {
		return *override
	}
	return serverDefault
}

func (st *streamSession) recordSentResponse(resp *pubsubpb.StreamingPullResponse) bool {
	if resp == nil || len(resp.GetReceivedMessages()) == 0 {
		return false
	}
	st.mu.Lock()
	for _, rm := range resp.GetReceivedMessages() {
		st.sentMessages = append(st.sentMessages, proto.CloneOf(rm))
	}
	st.mu.Unlock()
	return true
}

func (st *streamSession) handle() *StreamHandle {
	return &StreamHandle{st: st}
}

// StreamHandle provides live control over a StreamingPull connection.
type StreamHandle struct {
	st *streamSession
}

// ID returns the unique StreamID assigned to this stream in the current epoch.
func (h *StreamHandle) ID() string {
	return h.st.id
}

// InitialRequest returns a deep clone of frame 0 (initial StreamingPullRequest),
// or nil if frame 0 has not arrived yet.
func (h *StreamHandle) InitialRequest() *pubsubpb.StreamingPullRequest {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	return proto.CloneOf(h.st.initialRequest)
}

// Summary returns an immutable snapshot of this stream's state and exchanged messages.
func (h *StreamHandle) Summary() StreamSummary {
	return h.st.summary()
}

// PushResponse queues a raw StreamingPullResponse onto this stream in FIFO order.
func (h *StreamHandle) PushResponse(resp *pubsubpb.StreamingPullResponse) error {
	if resp == nil {
		resp = &pubsubpb.StreamingPullResponse{}
	}
	return h.st.enqueueResponse(resp)
}

// PushMessages clones msgs, populates missing AckId/MessageId/PublishTime values,
// applies opts, and queues a StreamingPullResponse on this stream.
func (h *StreamHandle) PushMessages(msgs []*pubsubpb.ReceivedMessage, opts ...PushOption) error {
	resp := h.buildResponse(msgs, opts...)
	return h.st.enqueueResponse(resp)
}

// SetSubscriptionProperties queues a StreamingPullResponse carrying only
// SubscriptionProperties on this active stream.
func (h *StreamHandle) SetSubscriptionProperties(eod, ordering bool) error {
	return h.PushMessages(nil, WithSubscriptionProperties(eod, ordering))
}

// SetSuppressPong overrides keepalive pong suppression for this stream.
func (h *StreamHandle) SetSuppressPong(suppress bool) {
	h.st.setSuppressPong(suppress)
}

// Close queues an orderly stream close after draining all previously queued
// responses. Passing nil or io.EOF closes cleanly with OK status; passing a gRPC
// status error terminates the stream with that error.
func (h *StreamHandle) Close(err error) error {
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return h.st.enqueueClose(err)
}

// Abort immediately cancels and terminates the stream with err without waiting
// for outbound queue drainage. If the stream is stalled in HTTP/2 flow-control
// backpressure, the underlying transport connection is closed after a brief grace
// window so Abort and WaitClosed never deadlock.
func (h *StreamHandle) Abort(err error) {
	if err == nil {
		err = status.Error(codes.Unavailable, "stream aborted")
	}
	h.st.abort(err)
	h.st.scheduleForceClose(25 * time.Millisecond)
}

// WaitClosed blocks until this stream finishes closing or ctx expires.
func (h *StreamHandle) WaitClosed(ctx context.Context) error {
	select {
	case <-h.st.closedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *StreamHandle) buildResponse(msgs []*pubsubpb.ReceivedMessage, opts ...PushOption) *pubsubpb.StreamingPullResponse {
	var cfg pushConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	cloned := make([]*pubsubpb.ReceivedMessage, len(msgs))
	for i, m := range msgs {
		rm := proto.CloneOf(m)
		if rm == nil {
			rm = &pubsubpb.ReceivedMessage{}
		}
		if rm.AckId == "" {
			rm.AckId = fmt.Sprintf("ack-%d", h.st.ep.ackIDSeq.Add(1))
		}
		if cfg.hasDelivAtt {
			rm.DeliveryAttempt = cfg.delivAttempt
		}
		if rm.Message == nil {
			rm.Message = &pubsubpb.PubsubMessage{}
		}
		if rm.Message.MessageId == "" {
			rm.Message.MessageId = fmt.Sprintf("msg-%d", h.st.ep.msgIDSeq.Add(1))
		}
		if rm.Message.PublishTime == nil {
			if cfg.hasPubTime {
				rm.Message.PublishTime = timestamppb.New(cfg.publishTime)
			} else {
				rm.Message.PublishTime = timestamppb.Now()
			}
		} else if cfg.hasPubTime {
			rm.Message.PublishTime = timestamppb.New(cfg.publishTime)
		}
		if cfg.hasOrderKey {
			rm.Message.OrderingKey = cfg.orderingKey
		}
		if len(cfg.attrs) > 0 {
			if rm.Message.Attributes == nil {
				rm.Message.Attributes = make(map[string]string, len(cfg.attrs))
			}
			maps.Copy(rm.Message.Attributes, cfg.attrs)
		}
		cloned[i] = rm
	}
	return &pubsubpb.StreamingPullResponse{
		ReceivedMessages:              cloned,
		AcknowledgeConfirmation:       cfg.ackConf,
		ModifyAckDeadlineConfirmation: cfg.modAckConf,
		SubscriptionProperties:        cfg.subProps,
	}
}

// ActiveStreams returns handles for all currently open StreamingPull sessions in
// the current epoch.
func (s *Server) ActiveStreams() []*StreamHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*StreamHandle
	for _, st := range s.streamOrder {
		if !st.isClosed() {
			out = append(out, st.handle())
		}
	}
	return out
}

// StreamByID looks up a StreamingPull session in the current epoch by ID.
func (s *Server) StreamByID(streamID string) (*StreamHandle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamID]
	if !ok {
		return nil, false
	}
	return st.handle(), true
}

// PushResponse queues resp onto the specified streamID.
func (s *Server) PushResponse(streamID string, resp *pubsubpb.StreamingPullResponse) error {
	h, ok := s.StreamByID(streamID)
	if !ok {
		return fmt.Errorf("stream %q not found", streamID)
	}
	return h.PushResponse(resp)
}

// PushMessages sends messages on the specified streamID with optional PushOptions.
func (s *Server) PushMessages(streamID string, msgs []*pubsubpb.ReceivedMessage, opts ...PushOption) error {
	h, ok := s.StreamByID(streamID)
	if !ok {
		return fmt.Errorf("stream %q not found", streamID)
	}
	return h.PushMessages(msgs, opts...)
}

// CloseStream drains queued responses on streamID and closes it with err.
func (s *Server) CloseStream(streamID string, err error) error {
	h, ok := s.StreamByID(streamID)
	if !ok {
		return fmt.Errorf("stream %q not found", streamID)
	}
	return h.Close(err)
}

// AbortStream immediately cancels and terminates streamID with err.
func (s *Server) AbortStream(streamID string, err error) error {
	h, ok := s.StreamByID(streamID)
	if !ok {
		return fmt.Errorf("stream %q not found", streamID)
	}
	h.Abort(err)
	return nil
}

// SetStreamSuppressPong overrides keepalive pong suppression on streamID.
func (s *Server) SetStreamSuppressPong(streamID string, suppress bool) error {
	h, ok := s.StreamByID(streamID)
	if !ok {
		return fmt.Errorf("stream %q not found", streamID)
	}
	h.SetSuppressPong(suppress)
	return nil
}

type streamRecvEvent struct {
	req   *pubsubpb.StreamingPullRequest
	sizes PayloadSizes
	err   error
}

func (s *Server) handleStreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	ctx := stream.Context()
	meta := rpcMetaFromContext(ctx)
	compression, headerWireLen, _ := meta.snapshotHeader()
	md := cloneMD(ctx, meta)
	var conn net.Conn
	if meta != nil {
		conn = meta.conn
	}

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server closed")
	}
	if meta != nil && meta.ep != nil && meta.ep != s.epoch {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "fake server reset")
	}
	ep := s.epoch
	ep.wg.Add(1)

	s.streamIDCounter++
	s.streamAttempts++
	streamAttempt := s.streamAttempts
	streamID := fmt.Sprintf("stream-%d", s.streamIDCounter)

	streamCtx, streamCancel := context.WithCancel(ep.ctx)
	st := newStreamSession(s, ep, streamID, streamCtx, streamCancel, conn, md)
	s.streams[streamID] = st
	s.streamOrder = append(s.streamOrder, st)

	var preRecvFail error
	for _, entry := range s.streamScripts {
		if !entry.active() || entry.rule.FailBeforeRecvErr == nil {
			continue
		}
		if entry.rule.MatchConnect != nil {
			if entry.rule.MatchConnect(streamAttempt, md.Copy()) {
				entry.consume()
				preRecvFail = entry.rule.FailBeforeRecvErr
				break
			}
		} else if entry.rule.Match == nil {
			entry.consume()
			preRecvFail = entry.rule.FailBeforeRecvErr
			break
		}
	}
	s.notifyLocked()
	s.mu.Unlock()

	var matchedScript *StreamingPullScript
	var finalErr error
	handlerDone := make(chan struct{})
	defer func() {
		close(handlerDone)
		st.markClosed(finalErr)
		if matchedScript != nil && matchedScript.OnClose != nil {
			matchedScript.OnClose(st.handle(), finalErr)
		}
		s.mu.Lock()
		if s.epoch == ep {
			s.notifyLocked()
		}
		s.mu.Unlock()
		ep.wg.Done()
	}()

	if preRecvFail != nil {
		s.recordStreamingPullPreRecvFailure(ep, st, md, compression, headerWireLen, preRecvFail)
		finalErr = preRecvFail
		return preRecvFail
	}

	if s.testHooks != nil && s.testHooks.beforeStreamPreRecv != nil {
		s.testHooks.beforeStreamPreRecv(ep)
	}

	// Dedicated single-receiver child goroutine tracked on ep.wg. StreamingPull
	// MUST return without waiting for this goroutine because grpc-go cancels
	// stream.Context() (unblocking stream.Recv) only after the handler returns.
	// Strict 1-frame turn gating via recvPermitCh + recvCh (each cap=1) ensures
	// frame N+1 Recv() never runs concurrently with frame N validation,
	// initial/outbound drainage, or OnOpen/OnRequest close/abort handling.
	recvCh := make(chan streamRecvEvent, 1)
	recvPermitCh := make(chan struct{}, 1)
	recvPermitCh <- struct{}{}
	recvInFlight := true
	requestNextRecv := func() {
		if recvInFlight {
			return
		}
		recvPermitCh <- struct{}{}
		recvInFlight = true
	}
	ep.wg.Add(1)
	go func() {
		defer ep.wg.Done()
		defer close(recvCh)
		defer func() {
			if s.testHooks != nil && s.testHooks.onStreamRecvExit != nil {
				s.testHooks.onStreamRecvExit(ep)
			}
		}()
		var idx int
		for {
			select {
			case <-handlerDone:
				return
			default:
			}
			select {
			case <-recvPermitCh:
			case <-handlerDone:
				return
			}
			select {
			case <-handlerDone:
				return
			default:
			}
			req, err := stream.Recv()
			var sizes PayloadSizes
			if err == nil && meta != nil {
				sizes, _ = meta.payloadAt(idx)
				idx++
			}
			select {
			case <-handlerDone:
				return
			default:
			}
			select {
			case recvCh <- streamRecvEvent{req: req, sizes: sizes, err: err}:
			case <-handlerDone:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// Wait for frame 0 (initial StreamingPullRequest), prioritizing streamCtx
	// abort and recvCh over ctx.Done() so RecvMsg failures (which cancel ctx
	// before returning err) are captured with their true error code.
	var initialEv streamRecvEvent
	var gotInitial bool
	if streamCtx.Err() != nil {
		finalErr = st.abortErrOrDefault()
		return finalErr
	}
	select {
	case ev, ok := <-recvCh:
		recvInFlight = false
		initialEv, gotInitial = ev, ok
	default:
		select {
		case ev, ok := <-recvCh:
			recvInFlight = false
			initialEv, gotInitial = ev, ok
		case <-streamCtx.Done():
			finalErr = st.abortErrOrDefault()
			return finalErr
		case <-ctx.Done():
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			select {
			case ev, ok := <-recvCh:
				recvInFlight = false
				initialEv, gotInitial = ev, ok
			case <-streamCtx.Done():
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
		}
	}
	if streamCtx.Err() != nil {
		finalErr = st.abortErrOrDefault()
		return finalErr
	}
	if !gotInitial {
		finalErr = status.FromContextError(ctx.Err()).Err()
		return finalErr
	}

	if initialEv.err != nil {
		if errors.Is(initialEv.err, io.EOF) {
			if ctx.Err() != nil {
				finalErr = status.FromContextError(ctx.Err()).Err()
				return finalErr
			}
			finalErr = nil
			return nil
		}
		if shouldRecordStreamingPullRecvError(ctx, initialEv.err) {
			s.recordStreamingPullRecvError(ep, st, 0, md, compression, headerWireLen, initialEv.err)
		}
		finalErr = initialEv.err
		return finalErr
	}

	initialReq := proto.CloneOf(initialEv.req)
	valErr := validateStreamingPullFrame(initialReq, true, s.strictStreamingPull)

	if s.testHooks != nil && s.testHooks.beforeStreamScript != nil {
		s.testHooks.beforeStreamScript(ep)
	}

	// Match StreamingPullScript under s.mu using frame 0 before recording frame 0
	// so InitialError is accurately reflected in frame 0's CapturedCall.
	var defaultSubProps *pubsubpb.StreamingPullResponse_SubscriptionProperties
	if valErr == nil {
		var matchErr error
		matchedScript, defaultSubProps, matchErr = s.matchStreamingPullScript(ep, streamCtx, streamAttempt, nil, initialReq, md)
		if matchErr != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}
	}

	frame0Err := valErr
	if frame0Err == nil && matchedScript != nil && matchedScript.InitialError != nil {
		frame0Err = matchedScript.InitialError
	}
	if matchedScript != nil && matchedScript.SuppressPong != nil {
		st.setSuppressPong(*matchedScript.SuppressPong)
	}
	if frame0Err == nil {
		if streamCtx.Err() != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}
		// Send initial HTTP/2 response headers immediately after frame 0 so
		// clients like Rust tonic do not block awaiting headers, without sending
		// spurious empty DATA frames (preserving LIFE-03 cancel-before-first-response).
		if err := stream.SendHeader(metadata.MD{}); err != nil {
			frame0Err = err
		}
	}

	s.recordStreamingPullFrame(ep, st, 0, initialReq, md, compression, headerWireLen, initialEv.sizes, frame0Err)
	if frame0Err != nil {
		if streamCtx.Err() != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}
		finalErr = frame0Err
		return frame0Err
	}
	if streamCtx.Err() != nil {
		finalErr = st.abortErrOrDefault()
		return finalErr
	}

	if matchedScript != nil && matchedScript.SubscriptionProperties != nil {
		_ = st.enqueueResponse(&pubsubpb.StreamingPullResponse{
			SubscriptionProperties: proto.CloneOf(matchedScript.SubscriptionProperties),
		})
	} else if defaultSubProps != nil {
		_ = st.enqueueResponse(&pubsubpb.StreamingPullResponse{
			SubscriptionProperties: defaultSubProps,
		})
	}
	if matchedScript != nil {
		for _, resp := range matchedScript.InitialResponses {
			if resp != nil {
				_ = st.enqueueResponse(resp)
			}
		}
		if matchedScript.OnOpen != nil {
			matchedScript.OnOpen(st.handle())
		}
		if matchedScript.AfterInitialError != nil {
			_ = st.enqueueClose(matchedScript.AfterInitialError)
		}
	}

	// Mark ready, record readiness order, and wake WaitForStreamingPull waiters
	// ONLY after initial headers, SubscriptionProperties, InitialResponses,
	// OnOpen, and AfterInitialError are completely queued/invoked.
	s.mu.Lock()
	if s.epoch == ep && !s.closed.Load() {
		if st.markReady() {
			s.readyStreams = append(s.readyStreams, st)
		}
		s.notifyLocked()
	}
	s.mu.Unlock()
	if streamCtx.Err() != nil {
		finalErr = st.abortErrOrDefault()
		return finalErr
	}

	var frameSeq int64 = 1
	processRecvEvent := func(ev streamRecvEvent, allowOutbound bool) (done bool, retErr error) {
		if streamCtx.Err() != nil {
			return true, st.abortErrOrDefault()
		}
		if ev.err != nil {
			if errors.Is(ev.err, io.EOF) {
				if ctx.Err() != nil {
					return true, status.FromContextError(ctx.Err()).Err()
				}
				return true, nil
			}
			if shouldRecordStreamingPullRecvError(ctx, ev.err) {
				s.recordStreamingPullRecvError(ep, st, frameSeq, md, compression, headerWireLen, ev.err)
			}
			return true, ev.err
		}
		req := proto.CloneOf(ev.req)
		valErr := validateStreamingPullFrame(req, false, s.strictStreamingPull)
		s.recordStreamingPullFrame(ep, st, frameSeq, req, md, compression, headerWireLen, ev.sizes, valErr)
		frameSeq++
		if valErr != nil {
			return true, valErr
		}
		if streamCtx.Err() != nil {
			return true, st.abortErrOrDefault()
		}
		if matchedScript != nil && matchedScript.OnRequest != nil {
			matchedScript.OnRequest(st.handle(), proto.CloneOf(req))
		}
		if streamCtx.Err() != nil {
			return true, st.abortErrOrDefault()
		}
		if allowOutbound {
			s.mu.Lock()
			if s.closed.Load() || s.epoch != ep || streamCtx.Err() != nil {
				s.mu.Unlock()
				return true, st.abortErrOrDefault()
			}
			srvSuppress := s.suppressPong
			s.mu.Unlock()
			suppress := st.effectiveSuppressPong(srvSuppress)

			if isKeepalivePingReq(req) {
				if !suppress {
					_ = st.enqueueResponse(&pubsubpb.StreamingPullResponse{})
				}
			} else if isStreamAckDeadlineUpdateReq(req) {
				if st.protoVersion() >= 1 && !suppress {
					_ = st.enqueueResponse(&pubsubpb.StreamingPullResponse{})
				}
			}
		}
		return false, nil
	}

	drainRecvAfterCtxDone := func() error {
		if streamCtx.Err() != nil {
			return st.abortErrOrDefault()
		}
		if !recvInFlight {
			return status.FromContextError(ctx.Err()).Err()
		}
		var ev streamRecvEvent
		var ok bool
		select {
		case ev, ok = <-recvCh:
			recvInFlight = false
		case <-streamCtx.Done():
			return st.abortErrOrDefault()
		}
		if streamCtx.Err() != nil {
			return st.abortErrOrDefault()
		}
		if !ok {
			return status.FromContextError(ctx.Err()).Err()
		}
		if ev.err != nil && !errors.Is(ev.err, io.EOF) {
			if shouldRecordStreamingPullRecvError(ctx, ev.err) {
				s.recordStreamingPullRecvError(ep, st, frameSeq, md, compression, headerWireLen, ev.err)
			}
			return ev.err
		}
		return status.FromContextError(ctx.Err()).Err()
	}

	// Sole sender loop for the StreamingPull stream.
	for {
		if streamCtx.Err() != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}

		ctxCanceledInDrain := false
	drainOutbound:
		for {
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			if ctx.Err() != nil {
				ctxCanceledInDrain = true
				break drainOutbound
			}
			item, ok := st.popOutbound()
			if !ok {
				break
			}
			if item.isClose {
				finalErr = item.err
				return finalErr
			}
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			if ctx.Err() != nil {
				ctxCanceledInDrain = true
				break drainOutbound
			}
			if err := stream.Send(item.resp); err != nil {
				if streamCtx.Err() != nil {
					finalErr = st.abortErrOrDefault()
					return finalErr
				}
				sendCode := status.Code(err)
				isTeardownErr := errors.Is(err, io.EOF) ||
					sendCode == codes.Canceled ||
					sendCode == codes.DeadlineExceeded ||
					sendCode == codes.Unavailable
				if isTeardownErr && ctx.Err() != nil {
					if dErr := drainRecvAfterCtxDone(); dErr != nil {
						finalErr = dErr
					} else {
						finalErr = err
					}
					return finalErr
				}
				if errors.Is(err, io.EOF) {
					finalErr = status.Error(codes.Unavailable, err.Error())
					return finalErr
				}
				finalErr = err
				return finalErr
			}
			if st.recordSentResponse(item.resp) {
				s.mu.Lock()
				if s.epoch == ep && !s.closed.Load() {
					s.notifyLocked()
				}
				s.mu.Unlock()
			}
		}

		if streamCtx.Err() != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}
		if ctxCanceledInDrain || ctx.Err() != nil {
			finalErr = drainRecvAfterCtxDone()
			return finalErr
		}

		// Queue drained cleanly without closing/returning and streamCtx is still
		// active: issue a receive permit for the next frame if none is in flight.
		requestNextRecv()
		if streamCtx.Err() != nil {
			finalErr = st.abortErrOrDefault()
			return finalErr
		}

		select {
		case ev, ok := <-recvCh:
			recvInFlight = false
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			if !ok {
				finalErr = status.FromContextError(ctx.Err()).Err()
				return finalErr
			}
			if done, err := processRecvEvent(ev, ctx.Err() == nil); done {
				finalErr = err
				return finalErr
			}
			continue
		default:
		}
		if ctx.Err() != nil {
			finalErr = drainRecvAfterCtxDone()
			return finalErr
		}

		select {
		case <-streamCtx.Done():
			finalErr = st.abortErrOrDefault()
			return finalErr
		case <-st.wakeCh:
			// Drain queued outbound items at top of loop.
		case ev, ok := <-recvCh:
			recvInFlight = false
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			if !ok {
				finalErr = status.FromContextError(ctx.Err()).Err()
				return finalErr
			}
			if done, err := processRecvEvent(ev, ctx.Err() == nil); done {
				finalErr = err
				return finalErr
			}
		case <-ctx.Done():
			if streamCtx.Err() != nil {
				finalErr = st.abortErrOrDefault()
				return finalErr
			}
			finalErr = drainRecvAfterCtxDone()
			return finalErr
		}
	}
}

func shouldRecordStreamingPullRecvError(ctx context.Context, err error) bool {
	code := status.Code(err)
	if code == codes.Canceled {
		return false
	}
	if code == codes.DeadlineExceeded && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return false
	}
	return true
}

func (s *Server) matchStreamingPullScript(
	ep *epochState,
	streamCtx context.Context,
	streamAttempt int,
	valErr error,
	initialReq *pubsubpb.StreamingPullRequest,
	md metadata.MD,
) (*StreamingPullScript, *pubsubpb.StreamingPullResponse_SubscriptionProperties, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, nil, status.Error(codes.Unavailable, "fake server closed")
	}
	if s.epoch != ep || streamCtx.Err() != nil {
		return nil, nil, status.Error(codes.Unavailable, "fake server reset")
	}
	defaultSubProps := proto.CloneOf(s.defaultSubProps)
	if valErr != nil {
		return nil, defaultSubProps, nil
	}
	for _, entry := range s.streamScripts {
		if !entry.active() || entry.rule.FailBeforeRecvErr != nil {
			continue
		}
		match := true
		if entry.rule.MatchConnect != nil && !entry.rule.MatchConnect(streamAttempt, md.Copy()) {
			match = false
		}
		if match && entry.rule.Match != nil && !entry.rule.Match(streamAttempt, proto.CloneOf(initialReq), md.Copy()) {
			match = false
		}
		if match {
			entry.consume()
			r := entry.rule
			return &r, defaultSubProps, nil
		}
	}
	return nil, defaultSubProps, nil
}

func validateStreamingPullFrame(req *pubsubpb.StreamingPullRequest, isInitial, strict bool) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "nil StreamingPullRequest")
	}
	if len(req.GetModifyDeadlineSeconds()) != len(req.GetModifyDeadlineAckIds()) {
		return status.Errorf(
			codes.InvalidArgument,
			"modify_deadline_seconds length (%d) != modify_deadline_ack_ids length (%d)",
			len(req.GetModifyDeadlineSeconds()),
			len(req.GetModifyDeadlineAckIds()),
		)
	}
	for i, d := range req.GetModifyDeadlineSeconds() {
		if d < 0 {
			return status.Errorf(codes.InvalidArgument, "modify_deadline_seconds[%d] must be >= 0, got %d", i, d)
		}
	}
	if !strict {
		return nil
	}
	for i, d := range req.GetModifyDeadlineSeconds() {
		if d != 0 && (d < MinLeaseExtensionSeconds || d > MaxLeaseExtensionSeconds) {
			return status.Errorf(
				codes.InvalidArgument,
				"modify_deadline_seconds[%d] must be 0 or in [%d, %d], got %d",
				i,
				MinLeaseExtensionSeconds,
				MaxLeaseExtensionSeconds,
				d,
			)
		}
	}
	if isInitial {
		if req.GetSubscription() == "" {
			return status.Error(codes.InvalidArgument, "initial StreamingPullRequest must specify subscription")
		}
		if dl := req.GetStreamAckDeadlineSeconds(); dl < MinLeaseExtensionSeconds || dl > MaxLeaseExtensionSeconds {
			return status.Errorf(codes.InvalidArgument, "initial stream_ack_deadline_seconds must be in [%d, %d], got %d", MinLeaseExtensionSeconds, MaxLeaseExtensionSeconds, dl)
		}
		return nil
	}
	if req.GetSubscription() != "" {
		return status.Errorf(codes.InvalidArgument, "subsequent StreamingPullRequest must not set subscription (%q)", req.GetSubscription())
	}
	if req.GetClientId() != "" {
		return status.Error(codes.InvalidArgument, "client_id may only be set on initial StreamingPullRequest")
	}
	if req.GetMaxOutstandingMessages() != 0 {
		return status.Error(codes.InvalidArgument, "max_outstanding_messages may only be set on initial StreamingPullRequest")
	}
	if req.GetMaxOutstandingBytes() != 0 {
		return status.Error(codes.InvalidArgument, "max_outstanding_bytes may only be set on initial StreamingPullRequest")
	}
	if req.GetProtocolVersion() != 0 {
		return status.Error(codes.InvalidArgument, "protocol_version may only be set on initial StreamingPullRequest")
	}
	if sdl := req.GetStreamAckDeadlineSeconds(); sdl != 0 && (sdl < MinLeaseExtensionSeconds || sdl > MaxLeaseExtensionSeconds) {
		return status.Errorf(
			codes.InvalidArgument,
			"subsequent stream_ack_deadline_seconds must be 0 or in [%d, %d], got %d",
			MinLeaseExtensionSeconds,
			MaxLeaseExtensionSeconds,
			sdl,
		)
	}
	return nil
}

func (s *Server) recordStreamingPullPreRecvFailure(
	ep *epochState,
	st *streamSession,
	md metadata.MD,
	compression string,
	headerWireLen int,
	err error,
) {
	now := time.Now()
	svc, method := splitFullMethod(MethodStreamingPull)
	rawTel, tel, telErr := DecodeTelemetryHeader(md)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != ep || s.closed.Load() {
		return
	}
	s.seqCounter++
	rec := &callRecord{
		c: CapturedCall{
			Seq:                s.seqCounter,
			Timestamp:          now,
			EndTime:            now,
			Completed:          true,
			Service:            svc,
			Method:             method,
			FullMethod:         MethodStreamingPull,
			StreamID:           st.id,
			StreamSeq:          0,
			Metadata:           md.Copy(),
			RequestParams:      ExtractRequestParams(md),
			RawTelemetryHeader: rawTel,
			Telemetry:          tel,
			TelemetryDecodeErr: telErrStr,
			Compression:        compression,
			HeaderWireBytes:    headerWireLen,
			StatusCode:         status.Code(err),
			StatusErr:          err.Error(),
		},
	}
	s.calls = append(s.calls, rec)
	s.notifyLocked()
}

func (s *Server) recordStreamingPullRecvError(
	ep *epochState,
	st *streamSession,
	streamSeq int64,
	md metadata.MD,
	compression string,
	headerWireLen int,
	err error,
) {
	now := time.Now()
	svc, method := splitFullMethod(MethodStreamingPull)
	rawTel, tel, telErr := DecodeTelemetryHeader(md)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != ep || s.closed.Load() {
		return
	}

	st.mu.Lock()
	st.frameCount++
	st.mu.Unlock()

	s.seqCounter++
	rec := &callRecord{
		c: CapturedCall{
			Seq:                s.seqCounter,
			Timestamp:          now,
			EndTime:            now,
			Completed:          true,
			Service:            svc,
			Method:             method,
			FullMethod:         MethodStreamingPull,
			StreamID:           st.id,
			StreamSeq:          streamSeq,
			Metadata:           md.Copy(),
			RequestParams:      ExtractRequestParams(md),
			RawTelemetryHeader: rawTel,
			Telemetry:          tel,
			TelemetryDecodeErr: telErrStr,
			Compression:        compression,
			HeaderWireBytes:    headerWireLen,
			StatusCode:         status.Code(err),
			StatusErr:          errStr,
			CodecError:         errStr,
		},
	}
	s.calls = append(s.calls, rec)
	s.notifyLocked()
}

func (s *Server) recordStreamingPullFrame(
	ep *epochState,
	st *streamSession,
	streamSeq int64,
	req *pubsubpb.StreamingPullRequest,
	md metadata.MD,
	compression string,
	headerWireLen int,
	sizes PayloadSizes,
	valErr error,
) {
	now := time.Now()
	svc, method := splitFullMethod(MethodStreamingPull)
	rawTel, tel, telErr := DecodeTelemetryHeader(md)
	var telErrStr string
	if telErr != nil {
		telErrStr = telErr.Error()
	}
	errStr := ""
	if valErr != nil {
		errStr = valErr.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != ep || s.closed.Load() {
		return
	}

	st.mu.Lock()
	st.frameCount++
	if streamSeq == 0 {
		st.hasInitial = true
		st.subscription = req.GetSubscription()
		st.clientID = req.GetClientId()
		st.protocolVersion = req.GetProtocolVersion()
		st.streamAckDeadlineSeconds = req.GetStreamAckDeadlineSeconds()
		st.maxOutstandingMessages = req.GetMaxOutstandingMessages()
		st.maxOutstandingBytes = req.GetMaxOutstandingBytes()
		st.initialRequest = proto.CloneOf(req)
	} else {
		if req.GetStreamAckDeadlineSeconds() > 0 {
			st.streamAckDeadlineSeconds = req.GetStreamAckDeadlineSeconds()
		}
		if isKeepalivePingReq(req) {
			st.keepalivePingCount++
		}
		if len(req.GetAckIds()) > 0 {
			st.ackIDsSent = append(st.ackIDsSent, req.GetAckIds()...)
		}
		if len(req.GetModifyDeadlineAckIds()) > 0 {
			st.modAckCount += len(req.GetModifyDeadlineAckIds())
		}
	}
	st.mu.Unlock()

	s.seqCounter++
	rec := &callRecord{
		c: CapturedCall{
			Seq:                s.seqCounter,
			Timestamp:          now,
			EndTime:            now,
			Completed:          true,
			Service:            svc,
			Method:             method,
			FullMethod:         MethodStreamingPull,
			StreamID:           st.id,
			StreamSeq:          streamSeq,
			Metadata:           md.Copy(),
			RequestParams:      ExtractRequestParams(md),
			RawTelemetryHeader: rawTel,
			Telemetry:          tel,
			TelemetryDecodeErr: telErrStr,
			Compression:        compression,
			HeaderWireBytes:    headerWireLen,
			UncompressedBytes:  sizes.UncompressedBytes,
			CompressedBytes:    sizes.CompressedBytes,
			WireBytes:          sizes.WireBytes,
			StatusCode:         status.Code(valErr),
			StatusErr:          errStr,
			StreamingPullReq:   proto.CloneOf(req),
		},
	}
	s.calls = append(s.calls, rec)
	s.notifyLocked()
}
