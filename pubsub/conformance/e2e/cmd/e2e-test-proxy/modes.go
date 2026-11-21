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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

// ProfileConfig holds the synthetic proxy behavior, capabilities, defaults, and
// fault injection controls configured by CLI flags.
type ProfileConfig struct {
	Mode       string
	Language   string
	Library    string
	Version    string
	Marker     string
	Delay      time.Duration
	CrashPhase string

	// Capabilities exposed via GetCapabilities.
	SupportsMonitoring           bool
	SupportsUnaryPull            bool
	SupportsBlockingPublish      bool
	SupportsPublishFlowControl   bool
	SupportsPublishCompression   bool
	SupportsPublishHedging       bool
	SupportsOrderingOptIn        bool
	SupportsSubscriberOrdering   bool
	SupportsKeepalive            bool
	SupportsEodReceiptModack     bool
	SupportsAutoNackOnDrop       bool
	SupportsAckWithResult        bool
	SupportsCustomRetryCodes     bool
	SupportsExactlyOnceDelivery  bool
	SupportsPerStreamFlowControl bool
	SupportsResumePublish        bool
	SupportsShutdownBehavior     bool
	SupportsShutdownTimeout      bool

	Defaults *proxypb.ClientDefaults

	// Behavioral divergences and fault switches.
	AllowUnorderedKey              bool
	RetryStreamingPullResExhausted bool
	OrderingHedgingConflictError   bool
	CppSpans                       bool
	UnescapedRoutingHeader         bool
	PreAddByteThresholdSplit       bool
	DefaultCompressionThreshAtZero bool
	DefaultCompressionAlg          proxypb.CompressionAlgorithm
	CrashAfterRPCCount             int64
	KeepalivePingOverride          time.Duration
	KeepalivePongOverride          time.Duration
	FastTimings                    bool

	faults map[string]bool
	rpcSeq atomic.Int64
}

// ApplyFastTimings configures compressed client defaults and proxy timers for fast E2E runs.
func (c *ProfileConfig) ApplyFastTimings() {
	if c == nil {
		return
	}
	c.FastTimings = true
	if c.Defaults == nil {
		return
	}
	c.Defaults.SubscriberMinAckExtensionMs = 250
	c.Defaults.SubscriberMaxExtensionMs = 350
	c.Defaults.RetryInitialDelayMs = 20
	if c.SupportsKeepalive {
		c.Defaults.KeepalivePingIntervalMs = 120
		c.Defaults.KeepalivePongTimeoutMs = 70
	}
}

func defaultGoClientDefaults() *proxypb.ClientDefaults {
	return &proxypb.ClientDefaults{
		PublishBatchCountThreshold:                100,
		PublishBatchByteThreshold:                 1_000_000,
		PublishMaxRequestBytes:                    10_000_000,
		PublishBatchDelayMs:                       10,
		PublishTotalTimeoutMs:                     60_000,
		PublishFlowControlMaxMessages:             1000,
		PublishFlowControlMaxBytes:                -1,
		PublishFlowControlBehavior:                proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE,
		PublishCompressionEnabledByDefault:        false,
		PublishCompressionBytesThreshold:          240,
		PublishCompressionAlgorithm:               proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP,
		PublishHedgingDelayMs:                     1000,
		PublishHedgingMaxTokens:                   50,
		PublishHedgingRefillRatio:                 0.1,
		SubscriberMaxOutstandingMessages:          1000,
		SubscriberMaxOutstandingBytes:             1_000_000_000,
		SubscriberMaxExtensionMs:                  3_600_000,
		SubscriberMinAckExtensionMs:               10_000,
		SubscriberMaxAckExtensionMs:               600_000,
		SubscriberAdaptiveAckExtension:            true,
		SubscriberPerStreamFlowControl:            false,
		SubscriberDefaultStreamAckDeadlineSeconds: 60,
		KeepalivePingIntervalMs:                   30_000,
		KeepalivePongTimeoutMs:                    15_000,
		RetryInitialDelayMs:                       100,
		RetryMaxDelayMs:                           60_000,
		RetryDelayMultiplier:                      4.0,
	}
}

func defaultCppClientDefaults() *proxypb.ClientDefaults {
	return &proxypb.ClientDefaults{
		PublishBatchCountThreshold:                100,
		PublishBatchByteThreshold:                 1_048_576,
		PublishMaxRequestBytes:                    10_000_000,
		PublishBatchDelayMs:                       10,
		PublishTotalTimeoutMs:                     60_000,
		PublishFlowControlMaxMessages:             1000,
		PublishFlowControlMaxBytes:                -1,
		PublishFlowControlBehavior:                proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK,
		PublishCompressionEnabledByDefault:        false,
		PublishCompressionBytesThreshold:          0,
		PublishCompressionAlgorithm:               proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE,
		PublishHedgingDelayMs:                     0,
		PublishHedgingMaxTokens:                   0,
		PublishHedgingRefillRatio:                 0,
		SubscriberMaxOutstandingMessages:          1000,
		SubscriberMaxOutstandingBytes:             104_857_600,
		SubscriberMaxExtensionMs:                  0,
		SubscriberMinAckExtensionMs:               60_000,
		SubscriberMaxAckExtensionMs:               600_000,
		SubscriberAdaptiveAckExtension:            false,
		SubscriberPerStreamFlowControl:            false,
		SubscriberDefaultStreamAckDeadlineSeconds: 10,
		KeepalivePingIntervalMs:                   0,
		KeepalivePongTimeoutMs:                    0,
		RetryInitialDelayMs:                       100,
		RetryMaxDelayMs:                           60_000,
		RetryDelayMultiplier:                      2.0,
	}
}

func defaultRustClientDefaults() *proxypb.ClientDefaults {
	return &proxypb.ClientDefaults{
		PublishBatchCountThreshold:                100,
		PublishBatchByteThreshold:                 1_000_000,
		PublishMaxRequestBytes:                    10_000_000,
		PublishBatchDelayMs:                       10,
		PublishTotalTimeoutMs:                     600_000,
		PublishFlowControlMaxMessages:             -1,
		PublishFlowControlMaxBytes:                -1,
		PublishFlowControlBehavior:                proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE,
		PublishCompressionEnabledByDefault:        false,
		PublishCompressionBytesThreshold:          0,
		PublishCompressionAlgorithm:               proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_UNSPECIFIED,
		PublishHedgingDelayMs:                     1000,
		PublishHedgingMaxTokens:                   50,
		PublishHedgingRefillRatio:                 0.1,
		SubscriberMaxOutstandingMessages:          1000,
		SubscriberMaxOutstandingBytes:             104_857_600,
		SubscriberMaxExtensionMs:                  3_600_000,
		SubscriberMinAckExtensionMs:               60_000,
		SubscriberMaxAckExtensionMs:               600_000,
		SubscriberAdaptiveAckExtension:            false,
		SubscriberPerStreamFlowControl:            false,
		SubscriberDefaultStreamAckDeadlineSeconds: 60,
		KeepalivePingIntervalMs:                   30_000,
		KeepalivePongTimeoutMs:                    15_000,
		RetryInitialDelayMs:                       100,
		RetryMaxDelayMs:                           60_000,
		RetryDelayMultiplier:                      2.0,
	}
}

// ParseProfileConfig constructs a ProfileConfig from CLI flags, supporting all
// documented modes, aliases, and overrides.
func ParseProfileConfig(mode, language, monitoring, unsupportedCSV, faultCSV, marker, crashPhase string, delay time.Duration) (*ProfileConfig, error) {
	cfg := &ProfileConfig{
		Mode:                         strings.TrimSpace(mode),
		Language:                     "go",
		Library:                      "cloud.google.com/go/pubsub/v2",
		Version:                      "v2.0.0-e2e-synthetic",
		Marker:                       marker,
		Delay:                        delay,
		CrashPhase:                   strings.ToLower(strings.TrimSpace(crashPhase)),
		SupportsMonitoring:           true,
		SupportsUnaryPull:            true,
		SupportsBlockingPublish:      true,
		SupportsPublishFlowControl:   true,
		SupportsPublishCompression:   true,
		SupportsPublishHedging:       true,
		SupportsOrderingOptIn:        true,
		SupportsSubscriberOrdering:   true,
		SupportsKeepalive:            true,
		SupportsEodReceiptModack:     true,
		SupportsAutoNackOnDrop:       false,
		SupportsAckWithResult:        true,
		SupportsCustomRetryCodes:     true,
		SupportsExactlyOnceDelivery:  true,
		SupportsPerStreamFlowControl: true,
		SupportsResumePublish:        true,
		SupportsShutdownBehavior:     true,
		SupportsShutdownTimeout:      true,
		Defaults:                     defaultGoClientDefaults(),
		OrderingHedgingConflictError: true,
		PreAddByteThresholdSplit:     true,
		DefaultCompressionAlg:        proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP,
		faults:                       make(map[string]bool),
	}

	if cfg.Mode == "" {
		cfg.Mode = "conformant-go"
	}

	explicitFaults := parseCSV(faultCSV)
	explicitUnsupported := parseCSV(unsupportedCSV)

	switch cfg.Mode {
	case "conformant-go":
		// All capabilities enabled by default per E2E_TEST_CATALOG.md § A.
	case "conformant-no-mon", "conformant-rust-nomon":
		cfg.Language = "rust"
		cfg.Library = "google-cloud-pubsub"
		cfg.Version = "v1.0.0-e2e-synthetic"
		cfg.SupportsMonitoring = false
		cfg.SupportsUnaryPull = false
		cfg.SupportsBlockingPublish = false
		cfg.SupportsPublishCompression = false
		cfg.SupportsPublishFlowControl = false
		cfg.SupportsOrderingOptIn = false
		cfg.SupportsEodReceiptModack = false
		cfg.SupportsSubscriberOrdering = false
		cfg.SupportsPerStreamFlowControl = false
		cfg.SupportsCustomRetryCodes = false
		cfg.AllowUnorderedKey = true
		cfg.RetryStreamingPullResExhausted = true
		cfg.SupportsAutoNackOnDrop = true
		cfg.Defaults = defaultRustClientDefaults()
		if cfg.Mode == "conformant-rust-nomon" {
			cfg.UnescapedRoutingHeader = true
		}
	case "conformant-partial-unimpl":
		cfg.SupportsMonitoring = true
		cfg.SupportsUnaryPull = false
		cfg.SupportsPublishHedging = false
	case "conformant-cpp-partial":
		cfg.Language = "cpp"
		cfg.Library = "google-cloud-cpp/pubsub"
		cfg.Version = "v2.0.0-e2e-synthetic"
		cfg.SupportsMonitoring = true
		cfg.SupportsPublishHedging = false
		cfg.SupportsKeepalive = false
		cfg.SupportsEodReceiptModack = false
		cfg.SupportsPerStreamFlowControl = false
		cfg.RetryStreamingPullResExhausted = true
		cfg.CppSpans = true
		cfg.DefaultCompressionThreshAtZero = true
		cfg.DefaultCompressionAlg = proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE
		cfg.SupportsAutoNackOnDrop = true
		cfg.Defaults = defaultCppClientDefaults()
	case "divergent-cpp", "divergent-client":
		cfg.Language = "cpp"
		cfg.Library = "google-cloud-cpp/pubsub"
		cfg.Version = "v2.0.0-e2e-synthetic"
		cfg.SupportsMonitoring = true
		cfg.SupportsPublishHedging = false
		cfg.SupportsUnaryPull = true
		cfg.SupportsBlockingPublish = true
		cfg.SupportsKeepalive = false
		cfg.SupportsEodReceiptModack = false
		cfg.SupportsPerStreamFlowControl = false
		cfg.RetryStreamingPullResExhausted = true
		cfg.CppSpans = true
		cfg.DefaultCompressionThreshAtZero = true
		cfg.DefaultCompressionAlg = proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE
		cfg.SupportsAutoNackOnDrop = true
		cfg.Defaults = defaultCppClientDefaults()
		if cfg.Mode == "divergent-client" {
			// Deliberate PORD-01 behavioral divergence for --compare detection.
			cfg.AllowUnorderedKey = true
		}
	case "missing-rpc":
		cfg.faults["missing-rpc"] = true
	case "wrong-rpc":
		if !hasAnyFault(explicitFaults, "wrong-payload", "wrong-attrs", "wrong-ordering-key", "missing-routing-header", "missing-custom-headers", "wrong-rpc-method", "extra-unexpected-rpc") {
			cfg.faults["wrong-payload"] = true
			cfg.faults["wrong-attrs"] = true
		}
	case "out-of-order-rpc":
		if !hasAnyFault(explicitFaults, "out-of-order-publish", "out-of-order-ack") {
			cfg.faults["out-of-order-publish"] = true
			cfg.faults["out-of-order-ack"] = true
		}
	case "missing-traces":
		cfg.SupportsMonitoring = true
		cfg.faults["missing-traces"] = true
	case "invalid-traces":
		cfg.SupportsMonitoring = true
		if !hasAnyFault(explicitFaults, "bad-span-name", "bad-span-kind", "missing-links", "missing-traceparent") {
			cfg.faults["bad-span-name"] = true
			cfg.faults["bad-span-kind"] = true
			cfg.faults["missing-links"] = true
			cfg.faults["missing-traceparent"] = true
		}
	case "crash-startup":
		if cfg.CrashPhase == "" {
			cfg.CrashPhase = "startup"
		}
	case "bad-ready", "malformed-ready":
		cfg.faults["bad-ready"] = true
	case "hang-scenario":
		if !hasAnyFault(explicitFaults, "hang-create", "hang-publish", "hang-subscribe", "hang-pull", "hang-drain") {
			cfg.faults["hang-publish"] = true
			cfg.faults["hang-subscribe"] = true
			cfg.faults["hang-pull"] = true
		}
	case "ignore-shutdown":
		cfg.faults["ignore-shutdown"] = true
	default:
		return nil, fmt.Errorf("unknown --mode %q", cfg.Mode)
	}

	if language != "" {
		lang := strings.ToLower(strings.TrimSpace(language))
		cfg.Language = lang
		switch lang {
		case "go":
			cfg.Library = "cloud.google.com/go/pubsub/v2"
			cfg.CppSpans = false
			cfg.SupportsAutoNackOnDrop = false
			cfg.Defaults = defaultGoClientDefaults()
		case "cpp", "c++":
			cfg.Language = "cpp"
			cfg.Library = "google-cloud-cpp/pubsub"
			cfg.CppSpans = true
			cfg.SupportsAutoNackOnDrop = true
			cfg.Defaults = defaultCppClientDefaults()
		case "rust":
			cfg.Library = "google-cloud-pubsub"
			cfg.CppSpans = false
			cfg.SupportsAutoNackOnDrop = true
			cfg.Defaults = defaultRustClientDefaults()
		default:
			cfg.Library = "custom/" + lang
		}
	}

	switch strings.ToLower(strings.TrimSpace(monitoring)) {
	case "", "auto":
		// Keep mode default.
	case "true", "1", "yes":
		cfg.SupportsMonitoring = true
	case "false", "0", "no":
		cfg.SupportsMonitoring = false
	default:
		return nil, fmt.Errorf("invalid --monitoring %q (expected auto, true, or false)", monitoring)
	}

	for _, feat := range explicitUnsupported {
		if err := cfg.disableFeature(feat); err != nil {
			return nil, err
		}
	}

	for _, token := range explicitFaults {
		if strings.HasPrefix(token, "crash-after=") {
			val := strings.TrimPrefix(token, "crash-after=")
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid crash-after value %q", token)
			}
			cfg.CrashAfterRPCCount = n
			continue
		}
		cfg.faults[token] = true
		switch token {
		case "allow-unordered-key":
			cfg.AllowUnorderedKey = true
		case "fatal-resource-exhausted":
			cfg.RetryStreamingPullResExhausted = false
		case "retry-resource-exhausted":
			cfg.RetryStreamingPullResExhausted = true
		case "cpp-spans":
			cfg.CppSpans = true
		case "unescaped-routing-header":
			cfg.UnescapedRoutingHeader = true
		case "wrong-rpc":
			cfg.faults["wrong-payload"] = true
			cfg.faults["wrong-attrs"] = true
		case "out-of-order-rpc":
			cfg.faults["out-of-order-publish"] = true
			cfg.faults["out-of-order-ack"] = true
		case "invalid-traces":
			cfg.faults["bad-span-name"] = true
			cfg.faults["bad-span-kind"] = true
			cfg.faults["missing-links"] = true
			cfg.faults["missing-traceparent"] = true
		}
	}

	return cfg, nil
}

func parseCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func hasAnyFault(explicit []string, candidates ...string) bool {
	if len(explicit) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		set[c] = struct{}{}
	}
	for _, tok := range explicit {
		if _, ok := set[tok]; ok {
			return true
		}
	}
	return false
}

func (c *ProfileConfig) disableFeature(feat string) error {
	norm := strings.ToLower(strings.TrimSpace(feat))
	norm = strings.ReplaceAll(norm, "-", "_")
	norm = strings.TrimPrefix(norm, "supports_")
	switch norm {
	case "monitoring":
		c.SupportsMonitoring = false
	case "unary_pull":
		c.SupportsUnaryPull = false
	case "blocking_publish":
		c.SupportsBlockingPublish = false
	case "publish_flow_control", "flow_control":
		c.SupportsPublishFlowControl = false
	case "publish_compression", "compression":
		c.SupportsPublishCompression = false
	case "publish_hedging", "hedging":
		c.SupportsPublishHedging = false
	case "ordering_opt_in":
		c.SupportsOrderingOptIn = false
	case "subscriber_ordering":
		c.SupportsSubscriberOrdering = false
	case "keepalive":
		c.SupportsKeepalive = false
	case "eod_receipt_modack":
		c.SupportsEodReceiptModack = false
	case "auto_nack_on_drop":
		c.SupportsAutoNackOnDrop = false
	case "ack_with_result":
		c.SupportsAckWithResult = false
	case "custom_retry_codes":
		c.SupportsCustomRetryCodes = false
	case "exactly_once_delivery", "eod":
		c.SupportsExactlyOnceDelivery = false
	case "per_stream_flow_control":
		c.SupportsPerStreamFlowControl = false
	case "resume_publish":
		c.SupportsResumePublish = false
	case "shutdown_behavior":
		c.SupportsShutdownBehavior = false
	case "shutdown_timeout":
		c.SupportsShutdownTimeout = false
	default:
		return fmt.Errorf("unknown unsupported feature %q", feat)
	}
	return nil
}

// HasFault reports whether the named fault token is enabled.
func (c *ProfileConfig) HasFault(name string) bool {
	if c == nil || c.faults == nil {
		return false
	}
	return c.faults[name]
}

// TrackScenarioRPC increments the scenario RPC counter and aborts the process
// immediately when --crash-phase=scenario or --fault=crash-after=N triggers.
func (c *ProfileConfig) TrackScenarioRPC() {
	if c == nil {
		return
	}
	n := c.rpcSeq.Add(1)
	if c.CrashPhase == "scenario" {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: simulated scenario crash at RPC #%d\n", n)
		os.Exit(2)
	}
	if c.CrashAfterRPCCount > 0 && n >= c.CrashAfterRPCCount {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: simulated crash-after=%d at RPC #%d\n", c.CrashAfterRPCCount, n)
		os.Exit(2)
	}
}

// BuildCapabilities constructs the GetCapabilitiesResponse for this profile.
func (c *ProfileConfig) BuildCapabilities() *proxypb.GetCapabilitiesResponse {
	return &proxypb.GetCapabilitiesResponse{
		ClientLanguage:               c.Language,
		ClientLibrary:                c.Library,
		ClientVersion:                c.Version,
		SupportsMonitoring:           c.SupportsMonitoring,
		SupportsUnaryPull:            c.SupportsUnaryPull,
		SupportsBlockingPublish:      c.SupportsBlockingPublish,
		SupportsPublishFlowControl:   c.SupportsPublishFlowControl,
		SupportsPublishCompression:   c.SupportsPublishCompression,
		SupportsPublishHedging:       c.SupportsPublishHedging,
		SupportsOrderingOptIn:        c.SupportsOrderingOptIn,
		SupportsSubscriberOrdering:   c.SupportsSubscriberOrdering,
		SupportsKeepalive:            c.SupportsKeepalive,
		SupportsEodReceiptModack:     c.SupportsEodReceiptModack,
		SupportsAutoNackOnDrop:       c.SupportsAutoNackOnDrop,
		SupportsAckWithResult:        c.SupportsAckWithResult,
		SupportsCustomRetryCodes:     c.SupportsCustomRetryCodes,
		SupportsExactlyOnceDelivery:  c.SupportsExactlyOnceDelivery,
		SupportsPerStreamFlowControl: c.SupportsPerStreamFlowControl,
		SupportsResumePublish:        c.SupportsResumePublish,
		SupportsShutdownBehavior:     c.SupportsShutdownBehavior,
		SupportsShutdownTimeout:      c.SupportsShutdownTimeout,
		Defaults:                     c.Defaults,
	}
}
