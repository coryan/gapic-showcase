// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

// Package harness implements the Pub/Sub conformance runner, proxy subprocess
// lifecycle manager, optional OpenTelemetry trace gating, and single/multi-target
// JSON and Markdown reporting engine.
//
// Dependency & Isolation Contract:
//   - Depends strictly on pubsub/conformance/{proxypb,pubsubpb,fake,traces},
//     gRPC/protobuf libraries, and the Go standard library.
//   - Never imports pubsub/conformance/scenarios or go.opentelemetry.io/otel/sdk.
//   - Launches proxies directly without shell wrappers inside dedicated process
//     groups (Setpgid), strips PUBSUB_EMULATOR_HOST from normal runs, and isolates
//     use_emulator_env scenarios in disposable subprocesses.
package harness
