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

// Package selftest implements harness pass and
// failure-detection self-tests against both harness.NewRunner and the compiled
// cmd/pubsub-conformance CLI binary.
//
// Self-tests use GO_WANT_HELPER_PROCESS=1 subprocess mock PubSubProxy and
// descendant helpers built strictly on github.com/googleapis/gapic-showcase/
// pubsub/conformance/proxypb, without importing conformanceproxy/proxypb,
// go.opentelemetry.io/otel/sdk, or pubsub/conformance/e2e.
package selftest
