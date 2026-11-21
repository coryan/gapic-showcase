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
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxPublishRequestCount = 1000
	maxPublishRequestBytes = 10_000_000

	minHedgingDelayMs     int64   = 100
	maxHedgingDelayMs     int64   = 10_000
	defaultHedgingDelayMs int64   = 1000
	maxHedgingTokensLimit int64   = 250
	defaultMaxHedgeTokens int64   = 50
	minHedgingRefillRatio float64 = 0.001
	maxHedgingRefillRatio float64 = 0.2
	defaultHedgeRefill    float64 = 0.1
	tokenScaleFactor      int64   = 1000
)

var emulatorEnvMu sync.Mutex

func dialPubSubEndpoint(endpoint string, useEmulatorEnv bool) (*grpc.ClientConn, error) {
	targetEndpoint := strings.TrimSpace(endpoint)
	if useEmulatorEnv {
		emulatorEnvMu.Lock()
		prev, hadPrev := os.LookupEnv("PUBSUB_EMULATOR_HOST")
		if targetEndpoint != "" {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", targetEndpoint)
		} else {
			targetEndpoint = os.Getenv("PUBSUB_EMULATOR_HOST")
		}
		if hadPrev {
			_ = os.Setenv("PUBSUB_EMULATOR_HOST", prev)
		} else {
			_ = os.Unsetenv("PUBSUB_EMULATOR_HOST")
		}
		emulatorEnvMu.Unlock()
	}
	if targetEndpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "missing endpoint")
	}
	return grpc.NewClient(
		"passthrough:///"+targetEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxProxyGRPCMsgBytes),
			grpc.MaxCallSendMsgSize(maxProxyGRPCMsgBytes),
		),
	)
}

func defaultPublishRetryCodes() map[codes.Code]bool {
	return map[codes.Code]bool{
		codes.Aborted:           true,
		codes.Canceled:          true,
		codes.Internal:          true,
		codes.ResourceExhausted: true,
		codes.Unknown:           true,
		codes.Unavailable:       true,
		codes.DeadlineExceeded:  true,
	}
}

func isPublishErrorRetryable(err error, retryCodes map[codes.Code]bool) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	code := st.Code()
	msg := st.Message()
	if code == codes.Internal && strings.Contains(msg, "string field contains invalid UTF-8") {
		return false
	}
	if code == codes.Unavailable && strings.Contains(msg, "Server shutdownNow invoked") {
		return false
	}
	return retryCodes[code]
}

type pendingPublishItem struct {
	pbMsg         *pubsubpb.PubsubMessage
	msgSize       int64
	fcBoundedSize int64
	orderingKey   string
	useBlocking   bool
	trace         *publishMessageTrace
	resultCh      chan *proxypb.PublishResponse
	settledCh     chan struct{}
	mutated       bool
}

type keyBatchQueue struct {
	key            string
	items          []*pendingPublishItem
	totalBytes     int64
	timer          *time.Timer
	inFlight       bool
	pendingBatches [][]*pendingPublishItem
}

// PublisherInstance implements a single publisher handle created via CreatePublisher.
type PublisherInstance struct {
	id     string
	cfg    *ProfileConfig
	traces *TraceStore

	conn   *grpc.ClientConn
	client pubsubpb.PublisherClient

	fullTopic string
	topicID   string
	projectID string

	enableMonitoring bool
	enableOrdering   bool

	countThreshold int
	byteThreshold  int64
	delayThreshold time.Duration

	fcMaxMessages int64
	fcMaxBytes    int64
	fcBehavior    proxypb.FlowControlLimitBehavior

	enableCompression         bool
	compressionBytesThreshold int64
	compressionAlgorithm      string

	hedgingEnabled     bool
	hedgingDelay       time.Duration
	hedgingMaxTokens   int64
	hedgingRefillRatio float64
	hedgingTokensMilli atomic.Int64
	hedgingSettingsErr error

	totalTimeout      time.Duration
	retryCodes        map[codes.Code]bool
	initialRetryDelay time.Duration
	maxRetryDelay     time.Duration
	retryMultiplier   float64
	maxAttempts       int32

	customHeaders map[string]string

	mu                 sync.Mutex
	fcWaiters          []*fcWaiter
	stopCh             chan struct{}
	fcOutstandingMsgs  int64
	fcOutstandingBytes int64
	stopped            bool
	closed             bool
	pausedKeys         map[string]bool
	keyQueues          map[string]*keyBatchQueue
	allPending         map[*pendingPublishItem]struct{}
	asyncTokens        map[string]chan *proxypb.PublishResponse
	tokenSeq           atomic.Int64
	wg                 sync.WaitGroup
}

type fcWaiter struct {
	notify chan struct{}
}

// NewPublisherInstance validates config, dials the Pub/Sub endpoint (unless missing-rpc),
// and constructs a PublisherInstance.
func NewPublisherInstance(id string, req *proxypb.CreatePublisherRequest, cfg *ProfileConfig, traces *TraceStore) (*PublisherInstance, error) {
	if !cfg.SupportsPublishCompression {
		if (req.EnableCompression != nil && *req.EnableCompression) ||
			req.CompressionAlgorithm != proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_UNSPECIFIED ||
			req.CompressionBytesThreshold != nil {
			return nil, status.Error(codes.Unimplemented, "publish compression not supported")
		}
	}
	if !cfg.SupportsPublishFlowControl {
		if req.MaxOutstandingMessages != nil ||
			req.MaxOutstandingBytes != nil ||
			req.LimitExceededBehavior != proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_UNSPECIFIED {
			return nil, status.Error(codes.Unimplemented, "publish flow control not supported")
		}
	}
	if !cfg.SupportsPublishHedging && req.Hedging != nil && req.Hedging.Enabled {
		return nil, status.Error(codes.Unimplemented, "publish hedging not supported")
	}
	if !cfg.SupportsCustomRetryCodes && len(req.RetryCodes) > 0 {
		return nil, status.Error(codes.Unimplemented, "custom retry codes not supported")
	}

	projID, topicID, fullTopic, err := extractResourceNames(req.Topic, "topics", req.ProjectId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var conn *grpc.ClientConn
	var client pubsubpb.PublisherClient
	if !cfg.HasFault("missing-rpc") {
		var dialErr error
		conn, dialErr = dialPubSubEndpoint(req.Endpoint, req.UseEmulatorEnv)
		if dialErr != nil {
			return nil, dialErr
		}
		client = pubsubpb.NewPublisherClient(conn)
	}

	enableOrdering := false
	if req.EnableMessageOrdering != nil {
		enableOrdering = *req.EnableMessageOrdering
	}

	countThreshold := int(cfg.Defaults.PublishBatchCountThreshold)
	if countThreshold <= 0 {
		countThreshold = 100
	}
	if req.CountThreshold != nil {
		if *req.CountThreshold > 0 {
			countThreshold = int(*req.CountThreshold)
		}
	}
	if countThreshold > maxPublishRequestCount {
		countThreshold = maxPublishRequestCount
	}

	byteThreshold := cfg.Defaults.PublishBatchByteThreshold
	if byteThreshold <= 0 {
		byteThreshold = 1_000_000
	}
	if req.ByteThreshold != nil && *req.ByteThreshold > 0 {
		byteThreshold = *req.ByteThreshold
	}

	delayMs := cfg.Defaults.PublishBatchDelayMs
	if delayMs <= 0 {
		delayMs = 10
	}
	if req.DelayThresholdMs != nil && *req.DelayThresholdMs >= 0 {
		delayMs = *req.DelayThresholdMs
	}

	fcBehavior := req.LimitExceededBehavior
	if fcBehavior == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_UNSPECIFIED {
		fcBehavior = cfg.Defaults.PublishFlowControlBehavior
	}
	fcMaxMsgs := cfg.Defaults.PublishFlowControlMaxMessages
	if req.MaxOutstandingMessages != nil {
		fcMaxMsgs = *req.MaxOutstandingMessages
	}
	fcMaxBytes := cfg.Defaults.PublishFlowControlMaxBytes
	if req.MaxOutstandingBytes != nil {
		fcMaxBytes = *req.MaxOutstandingBytes
	}

	enableComp := cfg.Defaults.PublishCompressionEnabledByDefault
	if req.EnableCompression != nil {
		enableComp = *req.EnableCompression
	}
	compThresh := cfg.Defaults.PublishCompressionBytesThreshold
	if !cfg.DefaultCompressionThreshAtZero && compThresh <= 0 {
		compThresh = 240
	}
	if req.CompressionBytesThreshold != nil && *req.CompressionBytesThreshold >= 0 {
		compThresh = *req.CompressionBytesThreshold
	}
	compAlg := "gzip"
	alg := req.CompressionAlgorithm
	if alg == proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_UNSPECIFIED {
		alg = cfg.DefaultCompressionAlg
	}
	if alg == proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE {
		compAlg = "deflate"
	}

	var hedgeEnabled bool
	hedgeDelay := time.Duration(defaultHedgingDelayMs) * time.Millisecond
	hedgeMaxTokens := defaultMaxHedgeTokens
	hedgeRefill := defaultHedgeRefill
	var hedgeErr error
	if req.Hedging != nil && req.Hedging.Enabled {
		hedgeEnabled = true
		if strings.EqualFold(cfg.Language, "rust") {
			if req.Hedging.DelayMs != nil {
				d := *req.Hedging.DelayMs
				if d < minHedgingDelayMs {
					d = minHedgingDelayMs
				} else if d > maxHedgingDelayMs {
					d = maxHedgingDelayMs
				}
				hedgeDelay = time.Duration(d) * time.Millisecond
			}
			if req.Hedging.MaxTokens != nil {
				mt := *req.Hedging.MaxTokens
				if mt < 1 {
					mt = 1
				} else if mt > maxHedgingTokensLimit {
					mt = maxHedgingTokensLimit
				}
				hedgeMaxTokens = mt
			}
			if req.Hedging.RefillRatio != nil {
				rr := *req.Hedging.RefillRatio
				if math.IsNaN(rr) {
					rr = 0.1
				} else if rr < minHedgingRefillRatio {
					rr = minHedgingRefillRatio
				} else if rr > maxHedgingRefillRatio {
					rr = maxHedgingRefillRatio
				}
				hedgeRefill = rr
			}
		} else {
			if req.Hedging.DelayMs != nil && *req.Hedging.DelayMs != 0 {
				if *req.Hedging.DelayMs < minHedgingDelayMs || *req.Hedging.DelayMs > maxHedgingDelayMs {
					hedgeErr = fmt.Errorf("hedging delay %dms must be between %d and %d", *req.Hedging.DelayMs, minHedgingDelayMs, maxHedgingDelayMs)
				} else {
					hedgeDelay = time.Duration(*req.Hedging.DelayMs) * time.Millisecond
				}
			}
			if req.Hedging.MaxTokens != nil && *req.Hedging.MaxTokens != 0 {
				if *req.Hedging.MaxTokens <= 0 || *req.Hedging.MaxTokens > maxHedgingTokensLimit {
					hedgeErr = fmt.Errorf("hedging max_tokens %d must be between 1 and %d", *req.Hedging.MaxTokens, maxHedgingTokensLimit)
				} else {
					hedgeMaxTokens = *req.Hedging.MaxTokens
				}
			}
			if req.Hedging.RefillRatio != nil && *req.Hedging.RefillRatio != 0 {
				if math.IsNaN(*req.Hedging.RefillRatio) || *req.Hedging.RefillRatio < minHedgingRefillRatio || *req.Hedging.RefillRatio > maxHedgingRefillRatio {
					hedgeErr = fmt.Errorf("hedging refill_ratio %v must be between %v and %v", *req.Hedging.RefillRatio, minHedgingRefillRatio, maxHedgingRefillRatio)
				} else {
					hedgeRefill = *req.Hedging.RefillRatio
				}
			}
		}
	}

	totalTimeout := time.Duration(cfg.Defaults.PublishTotalTimeoutMs) * time.Millisecond
	if totalTimeout <= 0 {
		totalTimeout = 60 * time.Second
	}
	if req.TotalTimeoutMs != nil {
		if *req.TotalTimeoutMs <= 0 {
			totalTimeout = 0
		} else {
			totalTimeout = time.Duration(*req.TotalTimeoutMs) * time.Millisecond
		}
	}

	retryCodes := defaultPublishRetryCodes()
	if len(req.RetryCodes) > 0 {
		retryCodes = make(map[codes.Code]bool, len(req.RetryCodes))
		for _, c := range req.RetryCodes {
			retryCodes[codes.Code(c)] = true
		}
	}

	initRetry := time.Duration(cfg.Defaults.RetryInitialDelayMs) * time.Millisecond
	if initRetry <= 0 {
		initRetry = 100 * time.Millisecond
	}
	if req.InitialRetryDelayMs != nil && *req.InitialRetryDelayMs > 0 {
		initRetry = time.Duration(*req.InitialRetryDelayMs) * time.Millisecond
	}
	maxRetry := time.Duration(cfg.Defaults.RetryMaxDelayMs) * time.Millisecond
	if maxRetry <= 0 {
		maxRetry = 60 * time.Second
	}
	if req.MaxRetryDelayMs != nil && *req.MaxRetryDelayMs > 0 {
		maxRetry = time.Duration(*req.MaxRetryDelayMs) * time.Millisecond
	}
	mult := cfg.Defaults.RetryDelayMultiplier
	if mult <= 1.0 {
		mult = 4.0
	}
	if req.RetryDelayMultiplier != nil && *req.RetryDelayMultiplier > 1.0 {
		mult = *req.RetryDelayMultiplier
	}
	var maxAttempts int32
	if req.MaxAttempts != nil && *req.MaxAttempts > 0 {
		maxAttempts = *req.MaxAttempts
	}

	p := &PublisherInstance{
		id:                        id,
		cfg:                       cfg,
		traces:                    traces,
		conn:                      conn,
		client:                    client,
		fullTopic:                 fullTopic,
		topicID:                   topicID,
		projectID:                 projID,
		enableMonitoring:          req.EnableMonitoring,
		enableOrdering:            enableOrdering,
		countThreshold:            countThreshold,
		byteThreshold:             byteThreshold,
		delayThreshold:            time.Duration(delayMs) * time.Millisecond,
		fcMaxMessages:             fcMaxMsgs,
		fcMaxBytes:                fcMaxBytes,
		fcBehavior:                fcBehavior,
		enableCompression:         enableComp,
		compressionBytesThreshold: compThresh,
		compressionAlgorithm:      compAlg,
		hedgingEnabled:            hedgeEnabled,
		hedgingDelay:              hedgeDelay,
		hedgingMaxTokens:          hedgeMaxTokens,
		hedgingRefillRatio:        hedgeRefill,
		hedgingSettingsErr:        hedgeErr,
		totalTimeout:              totalTimeout,
		retryCodes:                retryCodes,
		initialRetryDelay:         initRetry,
		maxRetryDelay:             maxRetry,
		retryMultiplier:           mult,
		maxAttempts:               maxAttempts,
		customHeaders:             maps.Clone(req.CustomHeaders),
		pausedKeys:                make(map[string]bool),
		keyQueues:                 make(map[string]*keyBatchQueue),
		allPending:                make(map[*pendingPublishItem]struct{}),
		asyncTokens:               make(map[string]chan *proxypb.PublishResponse),
		stopCh:                    make(chan struct{}),
	}
	return p, nil
}

// cppMessageProtoSize mirrors google-cloud-cpp MessageProtoSize
// (20-byte base overhead + data + message_id + ordering_key + attributes).
func cppMessageProtoSize(m *pubsubpb.PubsubMessage) int64 {
	if m == nil {
		return 0
	}
	sz := int64(20 + len(m.Data) + len(m.MessageId) + len(m.OrderingKey))
	for k, v := range m.Attributes {
		sz += int64(len(k) + len(v))
	}
	return sz
}

func (p *PublisherInstance) bundleByteLimit() int64 {
	// Account for outer PublishRequest topic overhead matching Go bundler.
	overhead := int64(len(p.fullTopic) + 5)
	if overhead >= maxPublishRequestBytes {
		return maxPublishRequestBytes
	}
	return maxPublishRequestBytes - overhead
}

// Publish enqueues (and optionally awaits) a single message publish.
func (p *PublisherInstance) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	if req.UseBlockingPublisher && !p.cfg.SupportsBlockingPublish {
		return nil, status.Error(codes.Unimplemented, "blocking publish not supported")
	}

	var origAttrs map[string]string
	if req.VerifyAttributesImmutability {
		origAttrs = maps.Clone(req.Attributes)
	}

	trace := newPublishMessageTrace(
		p.traces,
		p.enableMonitoring,
		p.projectID,
		p.fullTopic,
		p.topicID,
		req.Data,
		req.Attributes,
		req.OrderingKey,
		req.ParentTraceparent,
		req.ParentTracestate,
		req.UseBlockingPublisher,
	)

	makeMutated := func() bool {
		if !req.VerifyAttributesImmutability {
			return false
		}
		return !maps.Equal(origAttrs, req.Attributes)
	}

	failWithoutCreate := func(code codes.Code, msg string) *proxypb.PublishResponse {
		return &proxypb.PublishResponse{
			ErrorCode:               int32(code),
			ErrorMessage:            msg,
			CallerAttributesMutated: makeMutated(),
		}
	}

	isCppBlocking := p.cfg.CppSpans && req.UseBlockingPublisher
	emitFlowControl := !isCppBlocking && (!p.cfg.CppSpans || p.fcBehavior != proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE)

	failNow := func(code codes.Code, msg string) *proxypb.PublishResponse {
		trace.endCreate("", code, msg)
		return failWithoutCreate(code, msg)
	}

	failLocal := func(code codes.Code, msg string) *proxypb.PublishResponse {
		if p.cfg.CppSpans && !isCppBlocking {
			if emitFlowControl {
				trace.startFlowControl()
				trace.endFlowControl("")
			}
			trace.startBatching()
			trace.endBatching()
		}
		return failNow(code, msg)
	}

	// Non-UTF-8 attribute validation (PUB-02).
	for _, raw := range req.RawAttributes {
		if !utf8.Valid(raw.Key) || !utf8.Valid(raw.Value) {
			return failLocal(codes.Internal, "string field contains invalid UTF-8"), nil
		}
	}
	for k, v := range req.Attributes {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return failLocal(codes.Internal, "string field contains invalid UTF-8"), nil
		}
	}

	if req.OrderingKey != "" && !p.enableOrdering && p.cfg.SupportsOrderingOptIn && !p.cfg.AllowUnorderedKey {
		return failLocal(codes.InvalidArgument, "message ordering is not enabled on publisher"), nil
	}

	if p.enableOrdering && p.hedgingEnabled && p.cfg.OrderingHedgingConflictError {
		return failLocal(codes.InvalidArgument, "hedging and message ordering cannot both be enabled"), nil
	}

	if p.hedgingSettingsErr != nil {
		return failLocal(codes.InvalidArgument, p.hedgingSettingsErr.Error()), nil
	}

	clonedAttrs := make(map[string]string, len(req.Attributes)+len(req.RawAttributes)+2)
	for k, v := range req.Attributes {
		clonedAttrs[k] = v
	}
	for _, raw := range req.RawAttributes {
		clonedAttrs[string(raw.Key)] = string(raw.Value)
	}
	if !isCppBlocking {
		if tp, ts := trace.traceparentHeader(); tp != "" {
			clonedAttrs["googclient_traceparent"] = tp
			if ts != "" {
				clonedAttrs["googclient_tracestate"] = ts
			}
		}
	}

	orderingKey := req.OrderingKey
	if p.cfg.HasFault("wrong-ordering-key") {
		orderingKey = ""
	}
	if p.cfg.HasFault("wrong-attrs") {
		clonedAttrs["__e2e_corrupted_attr__"] = "mutated"
	}

	data := append([]byte(nil), req.Data...)
	if p.cfg.HasFault("wrong-payload") {
		data = append(data, []byte("-wrong")...)
	}

	pbMsg := &pubsubpb.PubsubMessage{
		Data:        data,
		Attributes:  clonedAttrs,
		OrderingKey: orderingKey,
	}
	msgSize := int64(proto.Size(pbMsg))

	if msgSize > p.bundleByteLimit() {
		if emitFlowControl {
			trace.startFlowControl()
			trace.endFlowControl("")
		}
		return failNow(codes.InvalidArgument, fmt.Sprintf("message size %d exceeds MaxPublishRequestBytes %d", msgSize, maxPublishRequestBytes)), nil
	}

	p.mu.Lock()
	if p.stopped || p.closed {
		p.mu.Unlock()
		return failLocal(codes.FailedPrecondition, "publisher stopped"), nil
	}
	if req.OrderingKey != "" && p.pausedKeys[req.OrderingKey] {
		p.mu.Unlock()
		return failLocal(codes.FailedPrecondition, fmt.Sprintf("publishing paused for ordering key %q", req.OrderingKey)), nil
	}

	// Flow control acquisition.
	if emitFlowControl {
		trace.startFlowControl()
	}
	fcBoundedSize := msgSize
	if p.fcMaxBytes > 0 && fcBoundedSize > p.fcMaxBytes {
		fcBoundedSize = p.fcMaxBytes
	}

	failFC := func(code codes.Code, errMsg string) *proxypb.PublishResponse {
		if !emitFlowControl {
			return failNow(code, errMsg)
		}
		if p.cfg.CppSpans {
			trace.endFlowControl("")
			return failNow(code, errMsg)
		}
		// Canonical Go pubsub/v2 exports ONLY "publisher flow control" on FC rejection.
		trace.endFlowControl(errMsg)
		return failWithoutCreate(code, errMsg)
	}

	if p.fcBehavior == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR {
		exceedsMsgs := p.fcMaxMessages > 0 && p.fcOutstandingMsgs+1 > p.fcMaxMessages
		exceedsBytes := p.fcMaxBytes > 0 && p.fcOutstandingBytes+msgSize > p.fcMaxBytes
		if p.cfg.Language == "cpp" && p.fcOutstandingMsgs == 0 {
			exceedsMsgs = false
			exceedsBytes = false
		}
		if exceedsMsgs || exceedsBytes {
			if req.OrderingKey != "" && p.cfg.Language != "cpp" && p.cfg.Language != "python" {
				p.pausedKeys[req.OrderingKey] = true
			}
			msg := "MaxOutstandingBytes flow controller limit exceeded"
			if exceedsMsgs {
				msg = "MaxOutstandingMessages flow controller limit exceeded"
			}
			p.mu.Unlock()
			return failFC(codes.ResourceExhausted, msg), nil
		}
		p.fcOutstandingMsgs++
		p.fcOutstandingBytes += msgSize
		fcBoundedSize = msgSize
	} else if p.fcBehavior == proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK {
		canAcquireLocked := func(sz int64) bool {
			msgsOK := p.fcMaxMessages <= 0 || p.fcOutstandingMsgs+1 <= p.fcMaxMessages
			bytesOK := p.fcMaxBytes <= 0 || p.fcOutstandingBytes+sz <= p.fcMaxBytes
			return msgsOK && bytesOK
		}
		if !p.stopped && !p.closed && ctx.Err() == nil && len(p.fcWaiters) == 0 && canAcquireLocked(fcBoundedSize) {
			p.fcOutstandingMsgs++
			p.fcOutstandingBytes += fcBoundedSize
		} else {
			if p.stopped || p.closed {
				p.mu.Unlock()
				return failFC(codes.FailedPrecondition, "publisher stopped while waiting for flow control"), nil
			}
			if ctx.Err() != nil {
				p.mu.Unlock()
				return failFC(codes.DeadlineExceeded, ctx.Err().Error()), nil
			}
			w := &fcWaiter{notify: make(chan struct{}, 1)}
			p.fcWaiters = append(p.fcWaiters, w)
			for {
				if p.stopped || p.closed {
					p.removeFCWaiterLocked(w)
					p.wakeHeadFCWaiterLocked()
					p.mu.Unlock()
					return failFC(codes.FailedPrecondition, "publisher stopped while waiting for flow control"), nil
				}
				if ctx.Err() != nil {
					p.removeFCWaiterLocked(w)
					p.wakeHeadFCWaiterLocked()
					p.mu.Unlock()
					return failFC(codes.DeadlineExceeded, ctx.Err().Error()), nil
				}
				if len(p.fcWaiters) > 0 && p.fcWaiters[0] == w && canAcquireLocked(fcBoundedSize) {
					p.removeFCWaiterLocked(w)
					p.fcOutstandingMsgs++
					p.fcOutstandingBytes += fcBoundedSize
					p.wakeHeadFCWaiterLocked()
					break
				}
				stopCh := p.stopCh
				p.mu.Unlock()
				select {
				case <-w.notify:
				case <-ctx.Done():
				case <-stopCh:
				}
				p.mu.Lock()
			}
		}
	} else {
		p.fcOutstandingMsgs++
		p.fcOutstandingBytes += msgSize
		fcBoundedSize = msgSize
	}
	if emitFlowControl {
		trace.endFlowControl("")
	}

	// Re-check pause/stopped after potential block.
	if p.stopped || p.closed {
		p.releaseFlowControlLocked(fcBoundedSize)
		p.mu.Unlock()
		if p.cfg.CppSpans && !isCppBlocking {
			trace.startBatching()
			trace.endBatching()
		}
		return failNow(codes.FailedPrecondition, "publisher stopped"), nil
	}
	if req.OrderingKey != "" && p.pausedKeys[req.OrderingKey] {
		p.releaseFlowControlLocked(fcBoundedSize)
		p.mu.Unlock()
		if p.cfg.CppSpans && !isCppBlocking {
			trace.startBatching()
			trace.endBatching()
		}
		return failNow(codes.FailedPrecondition, fmt.Sprintf("publishing paused for ordering key %q", req.OrderingKey)), nil
	}

	resCh := make(chan *proxypb.PublishResponse, 1)
	item := &pendingPublishItem{
		pbMsg:         pbMsg,
		msgSize:       msgSize,
		fcBoundedSize: fcBoundedSize,
		orderingKey:   req.OrderingKey,
		useBlocking:   req.UseBlockingPublisher,
		trace:         trace,
		resultCh:      resCh,
		settledCh:     make(chan struct{}),
		mutated:       makeMutated(),
	}

	var token string
	if req.AsyncFire {
		token = fmt.Sprintf("pub-tok-%d", p.tokenSeq.Add(1))
		p.asyncTokens[token] = resCh
	}

	if !isCppBlocking {
		trace.startBatching()
	}
	p.enqueueItemLocked(item)
	p.mu.Unlock()

	if req.AsyncFire {
		return &proxypb.PublishResponse{
			PublishToken:            token,
			CallerAttributesMutated: item.mutated,
		}, nil
	}

	select {
	case resp := <-resCh:
		return resp, nil
	case <-ctx.Done():
		return &proxypb.PublishResponse{
			ErrorCode:               int32(codes.DeadlineExceeded),
			ErrorMessage:            ctx.Err().Error(),
			CallerAttributesMutated: item.mutated,
		}, nil
	}
}

// AwaitPublish waits for an async_fire publish token to resolve.
func (p *PublisherInstance) AwaitPublish(ctx context.Context, token string) (*proxypb.PublishResponse, error) {
	p.mu.Lock()
	ch, ok := p.asyncTokens[token]
	if ok {
		delete(p.asyncTokens, token)
	}
	p.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown publish_token %q", token)
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (p *PublisherInstance) removeFCWaiterLocked(w *fcWaiter) {
	for i, cur := range p.fcWaiters {
		if cur == w {
			copy(p.fcWaiters[i:], p.fcWaiters[i+1:])
			p.fcWaiters[len(p.fcWaiters)-1] = nil
			p.fcWaiters = p.fcWaiters[:len(p.fcWaiters)-1]
			return
		}
	}
}

func (p *PublisherInstance) wakeHeadFCWaiterLocked() {
	if len(p.fcWaiters) > 0 {
		select {
		case p.fcWaiters[0].notify <- struct{}{}:
		default:
		}
	}
}

func (p *PublisherInstance) releaseFlowControlLocked(sz int64) {
	p.fcOutstandingMsgs--
	if p.fcOutstandingMsgs < 0 {
		p.fcOutstandingMsgs = 0
	}
	p.fcOutstandingBytes -= sz
	if p.fcOutstandingBytes < 0 {
		p.fcOutstandingBytes = 0
	}
	p.wakeHeadFCWaiterLocked()
}

func (p *PublisherInstance) enqueueItemLocked(item *pendingPublishItem) {
	p.allPending[item] = struct{}{}
	q, ok := p.keyQueues[item.orderingKey]
	if !ok {
		q = &keyBatchQueue{key: item.orderingKey}
		p.keyQueues[item.orderingKey] = q
	}

	if p.cfg.PreAddByteThresholdSplit && len(q.items) > 0 {
		if len(q.items) >= p.countThreshold || q.totalBytes+item.msgSize > p.byteThreshold {
			p.flushQueueLocked(q)
		}
	}

	q.items = append(q.items, item)
	q.totalBytes += item.msgSize

	if item.useBlocking || len(q.items) >= p.countThreshold || q.totalBytes >= p.byteThreshold || p.delayThreshold == 0 {
		p.flushQueueLocked(q)
		return
	}

	if q.timer == nil {
		var t *time.Timer
		t = time.AfterFunc(p.delayThreshold, func() {
			p.mu.Lock()
			if q.timer == t {
				q.timer = nil
				p.flushQueueLocked(q)
			}
			p.mu.Unlock()
		})
		q.timer = t
	}
}

func (p *PublisherInstance) flushQueueLocked(q *keyBatchQueue) {
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	if len(q.items) == 0 {
		return
	}
	batch := q.items
	q.items = nil
	q.totalBytes = 0

	if q.key != "" && !p.cfg.HasFault("out-of-order-publish") {
		if p.pausedKeys[q.key] {
			p.failBatchLocked(batch, codes.FailedPrecondition, fmt.Sprintf("publishing paused for ordering key %q", q.key))
			return
		}
		if q.inFlight {
			q.pendingBatches = append(q.pendingBatches, batch)
			return
		}
		q.inFlight = true
		p.wg.Add(1)
		go p.runOrderedQueueWorker(q, batch)
		return
	}

	p.wg.Add(1)
	go p.runUnorderedBatch(batch)
}

func (p *PublisherInstance) failBatchLocked(batch []*pendingPublishItem, code codes.Code, msg string) {
	for _, it := range batch {
		delete(p.allPending, it)
		p.releaseFlowControlLocked(it.fcBoundedSize)
		it.trace.endCreate("", code, msg)
		it.resultCh <- &proxypb.PublishResponse{
			ErrorCode:               int32(code),
			ErrorMessage:            msg,
			CallerAttributesMutated: it.mutated,
		}
		close(it.settledCh)
	}
}

func (p *PublisherInstance) runUnorderedBatch(batch []*pendingPublishItem) {
	defer p.wg.Done()
	p.executeBatch(batch)
}

func (p *PublisherInstance) runOrderedQueueWorker(q *keyBatchQueue, batch []*pendingPublishItem) {
	defer p.wg.Done()
	for {
		ok := p.executeBatch(batch)
		p.mu.Lock()
		if !ok {
			p.pausedKeys[q.key] = true
			for _, rem := range q.pendingBatches {
				p.failBatchLocked(rem, codes.FailedPrecondition, fmt.Sprintf("publishing paused for ordering key %q", q.key))
			}
			q.pendingBatches = nil
			if len(q.items) > 0 {
				if q.timer != nil {
					q.timer.Stop()
					q.timer = nil
				}
				rem := q.items
				q.items = nil
				q.totalBytes = 0
				p.failBatchLocked(rem, codes.FailedPrecondition, fmt.Sprintf("publishing paused for ordering key %q", q.key))
			}
			q.inFlight = false
			p.mu.Unlock()
			return
		}
		if len(q.pendingBatches) > 0 {
			batch = q.pendingBatches[0]
			q.pendingBatches = q.pendingBatches[1:]
			p.mu.Unlock()
			continue
		}
		q.inFlight = false
		p.mu.Unlock()
		return
	}
}

func (p *PublisherInstance) executeBatch(batch []*pendingPublishItem) bool {
	if p.cfg.HasFault("out-of-order-publish") && len(batch) > 1 {
		for i, j := 0, len(batch)-1; i < j; i, j = i+1, j-1 {
			batch[i], batch[j] = batch[j], batch[i]
		}
	}

	isCppBlocking := p.cfg.CppSpans && len(batch) == 1 && batch[0].useBlocking
	if !isCppBlocking {
		for _, it := range batch {
			it.trace.endBatching()
			it.trace.addEvent("publish start", map[string]*proxypb.AttributeValue{
				"messaging.batch.message_count": attrInt(int64(len(batch))),
			})
		}
	}

	if p.cfg.Delay > 0 {
		time.Sleep(p.cfg.Delay)
	}

	if p.cfg.HasFault("missing-rpc") {
		p.mu.Lock()
		for i, it := range batch {
			delete(p.allPending, it)
			p.releaseFlowControlLocked(it.fcBoundedSize)
			msgID := fmt.Sprintf("missing-rpc-msg-%d-%d", time.Now().UnixNano(), i)
			if !isCppBlocking {
				it.trace.addEvent("publish end", nil)
			}
			it.trace.endCreate(msgID, codes.OK, "")
			it.resultCh <- &proxypb.PublishResponse{
				MessageId:               msgID,
				CallerAttributesMutated: it.mutated,
			}
			close(it.settledCh)
		}
		p.mu.Unlock()
		return true
	}

	var pubSpan *proxypb.TraceSpan
	if !isCppBlocking {
		pubSpan = newPublishRPCSpan(p.traces, p.enableMonitoring, p.projectID, p.fullTopic, p.topicID, len(batch))
	}
	pbMsgs := make([]*pubsubpb.PubsubMessage, len(batch))
	var batchMsgBytes int64
	var cppCompBytes int64
	for i, it := range batch {
		pbMsgs[i] = it.pbMsg
		batchMsgBytes += int64(proto.Size(it.pbMsg))
		cppCompBytes += cppMessageProtoSize(it.pbMsg)
		if pubSpan != nil {
			it.trace.linkToPublishSpan(pubSpan)
		}
	}

	topic := p.fullTopic
	if p.cfg.HasFault("wrong-rpc-method") {
		topic = p.fullTopic + "-wrong"
	}
	pbReq := &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: pbMsgs,
	}

	useCompress := false
	if p.enableCompression {
		if p.cfg.DefaultCompressionThreshAtZero {
			compBytes := batchMsgBytes
			if p.cfg.Language == "cpp" {
				compBytes = cppCompBytes
			}
			useCompress = compBytes >= p.compressionBytesThreshold
		} else {
			useCompress = batchMsgBytes > p.compressionBytesThreshold
		}
	}

	startTime := time.Now()
	var ctx context.Context
	var cancel context.CancelFunc
	if p.totalTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), p.totalTimeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	var resp *pubsubpb.PublishResponse
	var err error

	canHedge := p.hedgingEnabled && !p.enableOrdering && (p.totalTimeout <= 0 || p.totalTimeout > p.hedgingDelay) && len(batch) > 0 && batch[0].orderingKey == ""
	if canHedge {
		resp, err = p.sendWithHedging(ctx, pbReq, batch, startTime, useCompress)
	} else {
		resp, err = p.sendWithRetry(ctx, pbReq, startTime, 0, useCompress)
	}

	if pubSpan != nil {
		var fanoutSpans []*proxypb.TraceSpan
		if p.cfg.CppSpans && len(pubSpan.Links) > 128 {
			allLinks := pubSpan.Links
			pubSpan.Links = nil
			for idx, start := 0, 0; start < len(allLinks); idx, start = idx+1, start+128 {
				end := start + 128
				if end > len(allLinks) {
					end = len(allLinks)
				}
				fanoutSpans = append(fanoutSpans, newCppPublishFanoutSpan(p.traces, pubSpan, idx, allLinks[start:end]))
			}
			pubSpan.ChildSpanCount = int32(len(fanoutSpans))
		}
		pubSpan.EndTime = timestamppb.Now()
		p.traces.Record(p.enableMonitoring, pubSpan)
		for _, fsp := range fanoutSpans {
			p.traces.Record(p.enableMonitoring, fsp)
		}
	}

	if err == nil && p.cfg.HasFault("extra-unexpected-rpc") {
		extraCtx := p.outgoingContext(context.Background(), startTime, 0)
		_, _ = p.client.Publish(extraCtx, pbReq)
	}

	if err == nil && len(resp.GetMessageIds()) != len(batch) {
		err = status.Errorf(codes.Internal, "server returned %d message_ids for batch of %d messages", len(resp.GetMessageIds()), len(batch))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err != nil {
		code := status.Code(err)
		if code == codes.OK {
			code = codes.Unknown
		}
		errMsg := err.Error()
		for _, it := range batch {
			delete(p.allPending, it)
			p.releaseFlowControlLocked(it.fcBoundedSize)
			if p.cfg.CppSpans && !it.useBlocking {
				it.trace.addEvent("publish end", nil)
			}
			it.trace.endCreate("", code, errMsg)
			it.resultCh <- &proxypb.PublishResponse{
				ErrorCode:               int32(code),
				ErrorMessage:            errMsg,
				CallerAttributesMutated: it.mutated,
			}
			close(it.settledCh)
		}
		return false
	}

	for i, it := range batch {
		delete(p.allPending, it)
		p.releaseFlowControlLocked(it.fcBoundedSize)
		msgID := resp.MessageIds[i]
		if !isCppBlocking {
			it.trace.addEvent("publish end", nil)
		}
		it.trace.endCreate(msgID, codes.OK, "")
		it.resultCh <- &proxypb.PublishResponse{
			MessageId:               msgID,
			CallerAttributesMutated: it.mutated,
		}
		close(it.settledCh)
	}
	return true
}

func (p *PublisherInstance) outgoingContext(ctx context.Context, startTime time.Time, attempt int32) context.Context {
	pairs := make([]string, 0, 4+len(p.customHeaders)*2)
	if !p.cfg.HasFault("missing-routing-header") {
		topicVal := url.QueryEscape(p.fullTopic)
		if p.cfg.UnescapedRoutingHeader {
			topicVal = p.fullTopic
		}
		pairs = append(pairs, "x-goog-request-params", "topic="+topicVal)
	}
	if !p.cfg.HasFault("missing-custom-headers") {
		for k, v := range p.customHeaders {
			pairs = append(pairs, k, v)
		}
	}
	tel := &pubsubpb.PubsubClientTelemetry{
		Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{
			PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{
				HedgedAttemptCount: attempt,
				PublishStartTime:   timestamppb.New(startTime),
			},
		},
	}
	if b, err := proto.Marshal(tel); err == nil {
		pairs = append(pairs, "x-goog-pubsub-client-telemetry", base64.StdEncoding.EncodeToString(b))
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func (p *PublisherInstance) callOptions(useCompress bool) []grpc.CallOption {
	if !useCompress {
		return nil
	}
	if p.compressionAlgorithm == "deflate" {
		return []grpc.CallOption{grpc.UseCompressor("deflate")}
	}
	return []grpc.CallOption{grpc.UseCompressor(gzip.Name)}
}

func (p *PublisherInstance) sendWithRetry(ctx context.Context, pbReq *pubsubpb.PublishRequest, startTime time.Time, hedgeAttempt int32, useCompress bool) (*pubsubpb.PublishResponse, error) {
	delay := p.initialRetryDelay
	var attempt int32
	for {
		attempt++
		callCtx := p.outgoingContext(ctx, startTime, hedgeAttempt)
		resp, err := p.client.Publish(callCtx, pbReq, p.callOptions(useCompress)...)
		if err == nil {
			p.refillHedgeTokens()
			return resp, nil
		}
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, err
		}
		if p.maxAttempts > 0 && attempt >= p.maxAttempts {
			return nil, err
		}
		if !isPublishErrorRetryable(err, p.retryCodes) {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		next := time.Duration(float64(delay) * p.retryMultiplier)
		if next > p.maxRetryDelay {
			next = p.maxRetryDelay
		}
		delay = next
	}
}

func (p *PublisherInstance) refillHedgeTokens() {
	if !p.hedgingEnabled {
		return
	}
	add := int64(math.Round(p.hedgingRefillRatio * float64(tokenScaleFactor)))
	maxVal := p.hedgingMaxTokens * tokenScaleFactor
	for {
		cur := p.hedgingTokensMilli.Load()
		next := cur + add
		if next > maxVal {
			next = maxVal
		}
		if p.hedgingTokensMilli.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (p *PublisherInstance) tryConsumeHedgeToken() bool {
	for {
		cur := p.hedgingTokensMilli.Load()
		if cur < tokenScaleFactor {
			return false
		}
		if p.hedgingTokensMilli.CompareAndSwap(cur, cur-tokenScaleFactor) {
			return true
		}
	}
}

func (p *PublisherInstance) sendWithHedging(ctx context.Context, pbReq *pubsubpb.PublishRequest, batch []*pendingPublishItem, startTime time.Time, useCompress bool) (*pubsubpb.PublishResponse, error) {
	hedgeCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()

	type attemptRes struct {
		resp      *pubsubpb.PublishResponse
		err       error
		fromHedge bool
	}
	resCh := make(chan attemptRes, 8)
	var inflightWG sync.WaitGroup

	// Primary attempt with retry.
	inflightWG.Add(1)
	go func() {
		defer inflightWG.Done()
		resp, err := p.sendWithRetry(hedgeCtx, pbReq, startTime, 0, useCompress)
		select {
		case resCh <- attemptRes{resp: resp, err: err, fromHedge: false}:
		case <-hedgeCtx.Done():
		}
	}()

	var hedgedStarted atomic.Bool
	hedgeTimer := time.NewTimer(p.hedgingDelay)
	defer hedgeTimer.Stop()

	var attempt int32
	for {
		select {
		case res := <-resCh:
			if res.err == nil {
				cancelAll()
				if hedgedStarted.Load() {
					for _, it := range batch {
						it.trace.addEvent("publish end (hedged)", nil)
					}
				}
				inflightWG.Wait()
				return res.resp, nil
			}
			if !res.fromHedge {
				// Primary failed permanently or timed out.
				cancelAll()
				if hedgedStarted.Load() {
					for _, it := range batch {
						it.trace.addEvent("publish end (hedged)", nil)
					}
				}
				inflightWG.Wait()
				return nil, res.err
			}
		case <-hedgeTimer.C:
			if p.tryConsumeHedgeToken() {
				attempt++
				curAttempt := attempt
				hedgedStarted.Store(true)
				for _, it := range batch {
					it.trace.addEvent("publish start (hedged)", map[string]*proxypb.AttributeValue{
						"messaging.batch.message_count": attrInt(int64(len(batch))),
					})
				}
				inflightWG.Add(1)
				go func() {
					defer inflightWG.Done()
					var rem time.Duration
					if p.totalTimeout > 0 {
						rem = time.Until(startTime.Add(p.totalTimeout))
						if rem > 10*time.Second {
							rem = 10 * time.Second
						}
						if rem <= 0 {
							return
						}
					} else {
						rem = 10 * time.Second
					}
					attemptCtx, cancelAttempt := context.WithTimeout(hedgeCtx, rem)
					defer cancelAttempt()
					callCtx := p.outgoingContext(attemptCtx, startTime, curAttempt)
					resp, err := p.client.Publish(callCtx, pbReq, p.callOptions(useCompress)...)
					if err == nil {
						p.refillHedgeTokens()
					}
					select {
					case resCh <- attemptRes{resp: resp, err: err, fromHedge: true}:
					case <-hedgeCtx.Done():
					}
				}()
			}
			if hedgeCtx.Err() == nil {
				hedgeTimer.Reset(p.hedgingDelay)
			}
		case <-ctx.Done():
			cancelAll()
			inflightWG.Wait()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

// Flush triggers immediate send of all queued batches and waits until all pending items settle.
func (p *PublisherInstance) Flush(ctx context.Context) error {
	p.mu.Lock()
	for _, q := range p.keyQueues {
		p.flushQueueLocked(q)
	}
	snapshot := make([]*pendingPublishItem, 0, len(p.allPending))
	for it := range p.allPending {
		snapshot = append(snapshot, it)
	}
	p.mu.Unlock()

	for _, it := range snapshot {
		select {
		case <-it.settledCh:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	return nil
}

// ResumePublish clears pause state on orderingKey.
func (p *PublisherInstance) ResumePublish(orderingKey string) {
	p.mu.Lock()
	delete(p.pausedKeys, orderingKey)
	p.mu.Unlock()
}

// Stop marks publisher stopped, flushes all queued messages, and waits for all workers.
func (p *PublisherInstance) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		if p.stopCh != nil {
			close(p.stopCh)
		}
		for _, q := range p.keyQueues {
			p.flushQueueLocked(q)
		}
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// Close stops the publisher and closes its gRPC connection.
func (p *PublisherInstance) Close(ctx context.Context) error {
	_ = p.Stop(ctx)
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
