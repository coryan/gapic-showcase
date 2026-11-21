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

// Package fake provides an in-process plaintext gRPC fake Pub/Sub data-plane
// server, wire-level capture log, programmable fault/gate injector, and strict
// expectation verifier for Pub/Sub client library conformance testing.
//
// Only the five Pub/Sub data-plane RPCs are registered:
//   - google.pubsub.v1.Publisher/Publish
//   - google.pubsub.v1.Subscriber/ModifyAckDeadline
//   - google.pubsub.v1.Subscriber/Acknowledge
//   - google.pubsub.v1.Subscriber/Pull
//   - google.pubsub.v1.Subscriber/StreamingPull
//
// All admin, snapshot, seek, push-config, detach, schema, and unknown RPCs
// return codes.Unimplemented and are recorded as unexpected calls.
package fake
