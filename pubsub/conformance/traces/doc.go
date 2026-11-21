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

// Package traces implements optional client-side OpenTelemetry trace
// verification for the Pub/Sub client library conformance harness (R2).
//
// The package operates exclusively on proxypb.TraceSpan slices and
// proxypb.GetCapabilitiesResponse, with zero dependency on otel/sdk or
// pubsubpb. When a target proxy reports supports_monitoring=false, trace hooks
// and verifiers skip ResetTraces/DrainTraces cleanly and return
// StatusSkippedNoMonitoring with no error. When supports_monitoring=true,
// captured spans are verified against either the strict canonical Go pubsub/v2
// profile (ProfileGoCanonical) or the C++ delta profile (ProfileCpp),
// recording structured Deviation entries for cross-client reporting.
package traces
