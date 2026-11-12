# `veneer-conformance`

`veneer-conformance` is a language-agnostic conformance test runner for handwritten Google Cloud client libraries ("veneers") built inside [`gapic-showcase`](https://github.com/googleapis/gapic-showcase).

Unlike auto-generated GAPIC clients tested against standard Showcase ECHO/Compliance RPCs, handwritten veneers implement rich client-side state machines—partition header routing, key validation, entity/value normalization, multi-round batch reads, multi-batch query cursor tracking, optimistic transactions, and RPC retry rules—whose wire compliance is cumbersome to re-implement identically in every language repository.

`veneer-conformance` reverses control: the **Go harness runs once**, acts as a scripted **dual-transport backend (`gRPC` + `HTTP/JSON`)**, and drives any language's thin **fixture process (test proxy)** over a simple unary gRPC contract.

## Architecture

```
+-------------------------------------------------------------------+
| Go Harness (cmd/veneer-conformance datastore)                     |
|                                                                   |
|  1. Scripts expectations & assertions per scenario                |
|  2. Calls DatastoreFixture unary RPCs -------------------------\  |
|  3. Simulates gRPC & HTTP/JSON google.datastore.v1.Datastore <-|--|--+
+----------------------------------------------------------------+--+  |
                                                                 v     |
+-------------------------------------------------------------------+  |
| Language Test Proxy (e.g., Rust / C++ / Swift / Dart / Go)        |  |
|                                                                   |  |
|  - Implements google.showcase.conformance.datastore.v1            |  |
|    DatastoreFixture over unary gRPC                               |  |
|  - Translates Fixture requests -> idiomatic veneer Client calls   |  |
|  - Translates veneer responses & errors -> VeneerError / DTOs ----|--+
+-------------------------------------------------------------------+
```

## Self-Contained Fixture Schema (`fixture.proto`)

All fixture contracts under `schema/google/showcase/conformance/` import **only standard `protoc` well-known types** (`google/protobuf/duration.proto`, `google/protobuf/struct.proto`, `google/protobuf/timestamp.proto`) and do **not** depend on the `schema/googleapis` git submodule or `google.datastore.v1` protos.

Why? Language repositories using `veneer-conformance`—including HTTP/JSON veneers in Rust, Swift, or Dart that avoid taking a `google.datastore.v1` protobuf dependency—can compile [`fixture.proto`](../../schema/google/showcase/conformance/datastore/v1/fixture.proto) cleanly with vanilla `protoc` or copy the single `.proto` file directly into a test-only target.

Meanwhile, the Go simulated backend inside `gapic-showcase` imports `cloud.google.com/go/datastore/apiv1/datastorepb` via `go.mod` to speak real Cloud Datastore v1 wire protocol over both `gRPC` and `HTTP/JSON`.

## CLI Usage (`datastore` Subcommand)

Run against an external test proxy subprocess (the harness allocates a free port and appends `--port <N>`, or substitutes `{port}` if present in `--fixture-cmd`):

```bash
go run ./cmd/veneer-conformance datastore \
  --fixture-cmd "go run ./testproxies/datastore/cmd" \
  --transports=grpc
```

Or run against a pre-started fixture server:

```bash
veneer-conformance datastore \
  --fixture-addr 127.0.0.1:9999 \
  --transports=grpc,rest \
  --strict
```

### Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--fixture-addr` | `""` | Address (`host:port`) of an already-running `DatastoreFixture` gRPC server. |
| `--fixture-cmd` | `""` | Command to launch the fixture subprocess (`--port <N>` appended automatically unless `{port}` placeholder is used). |
| `--transports` | `"grpc"` | Comma-separated transports to run (`grpc`, `rest`). |
| `--run` | `""` | Regular expression matching scenario names to run. |
| `--skip` | `""` | Regular expression matching scenario names to skip. |
| `--strict` | `false` | Fail instead of skip when a fixture returns `ERROR_KIND_FEATURE_NOT_IMPLEMENTED`. |
| `--timeout` | `30s` | Per-scenario timeout. |
| `--json` | `false` | Emit structured JSON report (`passed`, `failed`, `skipped`, `outcomes`). |

## Normalized Error Model (`VeneerError`)

Idiomatic veneer errors differ across languages (Go `error` / `MultiError` vs. Rust `Result<T, Error>` vs. Swift `throws` vs. C++ `StatusOr<T>`). All fixture RPCs return a `VeneerError` alongside normal outputs:

- `ERROR_KIND_SERVICE`: Error originating from the simulated backend (`rpc_code` and/or `http_status`, plus optional `error_info` decoded from `google.rpc.ErrorInfo`).
- `ERROR_KIND_CLIENT_VALIDATION`: Precondition or input validation rejected locally before sending an RPC (e.g., incomplete key passed to `Get`/`Update`/`Delete`, incomplete ancestor, directly nested array, negative query offset).
- `ERROR_KIND_CLIENT_STATE`: Invalid lifecycle/iterator/transaction state call (e.g., reading `ExplainMetrics` before exhausting an `analyze=true` query iterator, writing in a `ReadOnly` transaction).
- `ERROR_KIND_CALLBACK`: Application callback failure propagated out of `RunInTransaction`.
- `ERROR_KIND_TIMEOUT`: Client-side deadline/timeout cancellation.
- `ERROR_KIND_FEATURE_NOT_IMPLEMENTED`: Capability, option, or transport not implemented by the veneer under test. Maps to `SKIP` by default or `FAIL` under `--strict`.

Point-read misses (`LookupResponse.missing`) MUST normalize to `FoundEntity{found: false}` with no `error`.

## Datastore Scenario Coverage

`conformance/datastore` currently verifies behaviors derived from the Cloud Datastore Client Library Specification:

1. **Headers & Partitions (`headers`)**: Default vs. URL-encoded named `database_id` routing (`x-goog-request-params`, `google-cloud-resource-prefix`, `x-goog-api-client` with `gccl/` + `grpc/` or `rest/`), client default namespace, per-key/per-query namespace override, client-independent key equality.
2. **Keys & IDs (`keys`)**: Complete ID/name keys, multi-level ancestor paths, `AllocateIds`, `ReserveIds`, sequential ID reservation, client-side rejection of incomplete keys on `Get`/`Update`/`Delete` and incomplete ancestors.
3. **Values & Indexes (`values`)**: All 11 Datastore `Value` types (`null`, `bool`, `int64`, `double` including `NaN`/`±Inf`, microsecond `Timestamp`, `string`, `bytes`, `Key`, `GeoPoint`, embedded `Entity` with complete/incomplete/nil keys, `Array`), `Vector` (`__vector__`), scalar `exclude_from_indexes` (>1,500 B), wire array `exclude_from_indexes` element propagation, client-side rejection of directly nested arrays.
4. **Point Reads (`lookup`)**: Single `Get` found & missing normalization, `EntityResult` metadata (`version`, `create_time`, `update_time`), `GetMulti` key deduplication & out-of-order response alignment, multi-round `deferred` key continuation loops, `EVENTUAL` & snapshot `read_time`, read-side `PropertyMask`.
5. **Mutations & Transforms (`mutations`)**: `Put` upsert & incomplete insert key normalization, `Insert` `ALREADY_EXISTS` with `ErrorInfo`, `Update` `NOT_FOUND`, `Delete` / `DeleteMulti` deduplication, atomic `Mutate`, partial update `PropertyMask`, empty `PropertyMask` with all 6 `PropertyTransform`s, `MutationResult` metadata & `transform_results`, `base_version` / `update_time` preconditions.
6. **Queries, Aggregations & Profiling (`queries`)**: All property filter operators (`<`, `<=`, `>`, `>=`, `=`, `!=`, `in`, `not-in`), nested `AND`/`OR`, `HasAncestor` & kindless queries, `Project`/`DistinctOn`, keyless projection entities, `meaning = 18` write-back stripping, `KeysOnly`/`Order`/`Limit`/`Offset`, negative offset rejection, `NOT_FINISHED` multi-batch pagination (`skipped_results`, `skipped_cursor`, `end_cursor`, `limit`), mid-batch cursor tracking (`stop_after_items`), `FindNearest` vector search, `GqlQuery` bindings & compiled continuation, `COUNT`/`SUM`/`AVG` aggregations + `up_to`, `ExplainOptions` (`analyze=false/true`) & premature `ExplainMetrics` state errors.
7. **Transactions (`transactions`)**: Lazy initiation (`ReadOptions.new_transaction` on first read), serialized concurrent initial reads in lazy transactions, `PendingKey` resolution, write-only transactions & zero-RPC early callback abort, callback failure `Rollback`, `ABORTED` retry loop with `previous_transaction` (both on `Commit` and mid-query on batch 2+), `ReadOnly` with `read_time`, `ReadOnly` write rejection.
8. **Retries (`retries`)**: Automatic `UNAVAILABLE` retries on idempotent RPCs (`Lookup`, `RunQuery`, `RunAggregationQuery`, `ReserveIds` — including over HTTP `POST`), and no single-RPC retries on `DEADLINE_EXCEEDED` for stateful/non-idempotent RPCs (`Commit`, `AllocateIds`, `BeginTransaction`).

### Explicitly Excluded Legacy Features

Per Section 14 of the Cloud Datastore Client Library Specification, scenarios intentionally **exclude**:
- Legacy App Engine URL-safe Base64 `Key` serialization (`Key.Encode` / `toUrlSafe` / v3 `Reference`).
- Client-side `Query.Count` pagination fallback.
- Web framework session store adapters.
- Optional language-specific native struct/ORM tag mapping.

## Go Prototype Test Proxy (`testproxies/datastore`)

An in-repo reference Go proxy wrapping `cloud.google.com/go/datastore` lives under `testproxies/datastore/` and serves as a live self-test of the harness:

```bash
go run ./cmd/veneer-conformance datastore \
  --fixture-cmd "go run ./testproxies/datastore/cmd" \
  --skip "ValueRejectDirectlyNestedArray|LookupExposesEntityMetadata|MutationExposesMutationResultMetadata|QueryProjectionKeylessEntity|TxBeginLaterParallelReadsSerialized|TxReadOnlyRejectsWrite|RetryNoRetryBeginTransactionDeadlineExceeded"
```

Running without `--skip` demonstrates that the harness catches real spec divergences and omissions in `cloud.google.com/go/datastore` (such as failing to reject directly nested arrays locally, omitting `Lookup`/`Mutate` result metadata, rejecting keyless projection entities, unserialized concurrent lazy-tx initial reads, buffering writes in a `ReadOnly` tx until `Commit`, and retrying `BeginTransaction` on `DEADLINE_EXCEEDED`).
