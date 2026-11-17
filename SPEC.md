# olg-nats-agent-core Specification

## 1. Purpose

`olg-nats-agent-core` is a shared Go library for agents that communicate over a NATS bus.

The library provides common bus-facing behavior for long-running agents, including:
- NATS connection and reconnect handling
- JetStream access
- JetStream KV access for desired configuration
- standard subject naming
- standard message envelopes
- configure and action submission helpers
- result and status publication helpers
- desired configuration storage and retrieval
- handler registration and subscription restoration

The library itself is **not a daemon**.
It is intended to be embedded inside agent processes.

---

## 2. Scope

This library is intended to support agents such as:
- cloud-facing / ucentral-side agent
- host agent
- VyOS agent

The library standardizes transport, message contract, and desired-state handling.
It does **not** implement platform-specific business logic.

---

## 3. Non-goals

The shared library must **not** implement:
- workload-specific config translation
- host reboot/script execution
- trace, rtty, or other action business logic
- cloud-side business validation beyond transport/message sanity checks
- local apply/rollback engines
- historical config management or rollback orchestration

These responsibilities belong to the consuming agents.

---

## 4. Architecture model

The architecture is:

- **library** = shared NATS/JetStream/KV contract and helper layer
- **agent** = long-running process that owns local business logic

Each agent embeds the library and uses it to:
- connect to NATS
- publish and subscribe on standard subjects
- store and load desired configuration
- publish results and status
- recover subscriptions after reconnect

---

## 5. Desired configuration model

### 5.1 Latest-state model

This design uses a **UUID-based latest-state model**.

JetStream KV is used as a **single latest desired-config slot** per target, not as a historical configuration store.

The library stores only the current desired configuration for a target in KV.
Agents always load the **current** desired configuration from KV.

Rollback, revision-ordered history, and historical configuration lookup are out of scope for this design.

### 5.2 Config identity semantics

Each desired configuration carries a **config UUID** assigned by the cloud-facing side.

The config UUID is:
- an opaque identity token
- used for equality comparison only
- not an ordering field
- not a version sequence
- not a freshness indicator

Agents determine whether they are in sync by comparing:
- the UUID of the locally applied/running config
- the UUID stored in the desired config record in KV

If the UUIDs match, the running config is in sync with desired state.
If the UUIDs differ, the running config is not in sync with desired state.

### 5.3 rpc_id semantics

`rpc_id` is used only for request/response correlation.

It identifies which request produced a given result or status message.
It does **not** identify which configuration instance is running or applied.

---

## 6. Configure contract

### 6.1 Configure submit sequence

Configure is modeled as **store desired state, then notify**:

1. caller submits a validated configure request
2. library stores the desired configuration record in KV
3. library publishes a lightweight configure notification on the configure subject
4. target agent receives the notification
5. target agent loads the current desired config from KV
6. target agent performs local apply logic
7. target agent publishes configure result and/or status

### 6.2 Configure notification semantics

The configure notification is a trigger telling the target agent to reload desired state from KV.

The notification is **not** the full desired configuration payload.

Configure notification required fields are:
- `version`
- `rpc_id`
- `target`
- `command_type`
- `uuid`
- `kv_bucket`
- `kv_key`
- `timestamp`

### 6.3 Configure result contract

Configure result/status messages must include:
- `rpc_id` for correlation to the triggering request
- the config UUID that was attempted or applied

This is required so the cloud-facing side can determine:
- which request produced the outcome
- which desired config instance was actually processed

`rpc_id` alone is not sufficient for configure outcomes.

### 6.4 SubmitConfigure failure semantics

`SubmitConfigure(...)` succeeds only when both steps complete successfully:
1. desired config is written to KV
2. configure notification is published

When either step fails, the call returns a submission error and does not return a successful `SubmissionAck`.
`SubmissionAck` is a direct API return value from `SubmitConfigure(...)`, not a bus-delivered message.

The following cases define caller-visible behavior:

- Validation failure
  Missing or invalid required fields.
  Operational result: no KV write, no notification publish, returns submission error.

- KV store failure
  Desired config could not be persisted.
  Operational result: no successful `SubmissionAck`, no notification publish, returns submission error.

- Notification publish failure after KV write success
  Desired config was already written to KV, but notification publish failed.
  Operational result: returns submission error with no successful `SubmissionAck`; desired state may already have changed in KV.

- Context timeout or cancellation
  Context ended before submission completed.
  Operational result: returns submission error. Side effects depend on how far execution progressed before context end:
  if cancellation happened before KV write, no state change; if cancellation happened after KV write, desired state may already be updated even when the call returns error.

Caller guidance:
- Treat `SubmissionAck` as transport acceptance only, not apply success.
- Treat apply success as confirmed only by result/status from the owning target agent.
- For retries after error, assume partial side effects are possible and use config UUID comparison to determine current desired state.

---

## 7. Action contract

Actions are modeled as direct command messages.

Action submit sequence:
1. caller submits a validated action request
2. library publishes the action command on the target action subject
3. target agent receives the action
4. target agent executes local business logic
5. target agent publishes result and/or status

Actions are not stored in KV as desired state.

---

## 8. Result, status, and state contract

The library must provide standard result, status, and state message envelopes.

Result and status messages must preserve shared correlation fields consistently.

For configure flows:
- result/status must carry `rpc_id`
- result/status must carry config UUID

For action flows:
- result/status must carry `rpc_id`
- action-specific fields may be included as needed by the action contract

For periodic and on-demand device state:
- The library provides a dedicated `StateEnvelope` wire contract:
  ```go
  type StateEnvelope struct {
  	Version   string          `json:"version"`
  	Target    string          `json:"target"`
  	UUID      string          `json:"uuid"`
  	Payload   json.RawMessage `json:"payload"`
  	Timestamp time.Time       `json:"timestamp"`
  }
  ```
- **Envelope Field Validation Rules:**
  - `Version`: Must match supported wire protocol version (`"1.0"`).
  - `Target`: Validated against safe target naming rules (non-empty, alphanumeric and dashes, no wildcards).
  - `UUID`: Sourced from the local applied configuration checkpoint. Represents the **applied configuration identity**, NOT a request ID.
    - Its conversion to the Gateway's numeric `params.uuid` belongs to `uCentral`.
    - If the applied UUID is not known yet (e.g. initial boot before controller configuration), `UUID` is empty string `""` or `"0"`.
    - State **can and should** be emitted before first successful configuration to report baseline operational telemetry.
  - `Payload`: Non-empty, valid JSON, strictly bounded to a maximum size of **1 MB** (consistent with uCentral's 1 MB state limit). Oversized payloads must fail validation before callback delivery.
  - `Timestamp`: Must be a valid non-zero ISO 8601 timestamp (`time.Time`).
- **Strict Subject & Contract Separation:**
  - `status.<target>` and `status.get.<target>` are strictly reserved for lifecycle events (startup) and configure/action execution progress.
  - `state.<target>` is used exclusively for periodic operational state streams, and `state.get.<target>` is used exclusively for on-demand live state queries.
  - These message types are not interchangeable. Publishing full device-state payloads on the status subject could cause consumers to interpret them as progress/status messages or reject them as invalid.

---

## 9. Subject model

The default subject structure is target-oriented:

- `cmd.configure.<target>`
- `cmd.action.<target>.<action>`
- `result.<target>`
- `status.<target>`
- `status.get.<target>`
- `state.<target>`
- `state.get.<target>`
- `health.<target>`

All subject generation must be centralized in the library (`internal/subjects`).
Consuming agents must not construct raw subjects ad hoc throughout the codebase.

The library must validate subject inputs such as target and action before publishing or subscribing.

Subject routing rules:
- `cmd.configure.<target>`: Configure trigger notifications after storing desired config in KV.
- `cmd.action.<target>.<action>`: Direct action command delivery.
- `result.<target>`: Final command completion/failure outcomes.
- `status.<target>`: Lifecycle states and configure/action progress transitions.
- `status.get.<target>`: Request-reply queries for device status and upgrade recovery.
- `state.<target>`: Periodic operational device state and telemetry.
- `state.get.<target>`: On-demand request-reply queries for fresh, live operational state.
- `health.<target>`: Component liveness and health reports.

---

## 10. KV model

### 10.1 Bucket usage

The default KV bucket is used to store desired configuration records.

Default conventions:
- bucket: `cfg_desired`
- key pattern: `desired.<target>`

### 10.2 Contract-level behavior

At the design level, KV is treated as storage for the **current desired config only**.

The application contract does **not** depend on:
- KV revision ordering
- historical revision retrieval
- revision-based config freshness decisions

KV implementation metadata may exist internally, but it is not part of the design contract.

### 10.3 API expectation

The library must expose APIs to:
- store desired config
- load current desired config
- optionally watch desired config changes
- help agents reload current desired state on startup/recovery

The primary load path is **load current desired config**, not load arbitrary historical revisions.

### 10.4 KV configuration deletion delivery

In NATS JetStream Key-Value, deleting or purging a key sends an entry with an empty value (a tombstone).
When a deletion or purge event occurs, the desired-config watcher must deliver a `StoredDesiredConfig` structure with `Deleted = true` and the Record payload cleared (empty/nil) to the registered watch handler, rather than silently discarding the event.


---

## 11. Reconnect and recovery model

The library must support daemon-friendly reconnect behavior.

After reconnect, the library must:
- restore subscriptions
- restore handler registrations
- rebuild required NATS/JetStream/KV handles as needed
- invoke the custom reconnect handler (if registered using `WithReconnectHandler(handler func())`) only after all subscriptions and KV watches are successfully restored

The library must also support agent recovery flows in which an agent:
- starts or restarts
- reloads the current desired config from KV
- reconciles local state against desired state

Recovery is based on the **latest desired config** in KV.

---

## 12. Public API expectations

The public API should provide, at minimum, support for:
- connection lifecycle
- health reporting
- configure submission
- action submission
- result, status, and state publication (`PublishResult`, `PublishStatus`, `PublishState`, `RequestState`)
- desired config store/load/watch
- handler registration for configure, action, result, status, and state (`RegisterStateHandler`, `RegisterStateGetHandler`)
- custom reconnect handler registration via `WithReconnectHandler(handler func())`

The desired-config read API should return the decoded current desired-config record, including the config UUID needed for sync decisions.

The design does **not** require an API for loading arbitrary KV revisions.

### 12.1 State Messaging Architecture & Implementation Scope (Planned APIs)

State messaging is specified as a first-class message kind throughout `agentcore`. The planned implementation scope encompasses:

1. **First-Class Message Kind (`KindState`):** Registered alongside `KindConfigure`, `KindAction`, `KindResult`, and `KindStatus`.
2. **Standard Handler Signatures:**
   ```go
   type StateHandler func(context.Context, StateEnvelope) error
   type StateGetHandler func(context.Context) (StateEnvelope, error)
   ```
   * Receives `context.Context` to participate in connection/lifecycle cancellation.
   * Returns `error` to participate in error logging and metrics counters (`metrics.IncErrors`).
   * Supports `opts ...SubscriptionOption` (including `WithQueueGroup`).
3. **Public Client Methods:**
   ```go
   PublishState(ctx context.Context, envelope StateEnvelope) error
   RegisterStateHandler(target string, handler StateHandler, opts ...SubscriptionOption) error
   RequestState(ctx context.Context, target string) (StateEnvelope, error)
   RegisterStateGetHandler(target string, handler StateGetHandler, opts ...SubscriptionOption) error
   ```
4. **Subject Model & Builder Methods:**
   * Default patterns: `DefaultStatePattern = "state.%s"`, `DefaultStateGetPattern = "state.get.%s"`.
   * Optional validated pattern overrides configurable in `agentcore.Config`.
   * Centralized subject builders in `internal/subjects`: `State(target string) (string, error)` and `StateGet(target string) (string, error)`.
5. **Subscription Registry & Reconnect Restoration:** Handlers registered via `RegisterStateHandler` and `RegisterStateGetHandler` are stored in `internal/registry` and automatically re-subscribed upon broker reconnection.
6. **On-Demand State Request/Reply Wire Contract (`StateGetReply`):**
   * **Wire Model Structure:**
     ```go
     // StateGetReply models the request-reply wire payload returned on the NATS reply inbox for state.get.<target>.
     // It encapsulates versioning, target identity, and exactly one of state or error.
     type StateGetReply struct {
     	Version string             `json:"version"`
     	Target  string             `json:"target"`
     	State   *StateEnvelope     `json:"state,omitempty"`
     	Error   *StateGetWireError `json:"error,omitempty"`
     }

     type StateGetWireError struct {
     	Code    string `json:"code"`
     	Message string `json:"message"`
     }
     ```
   * **Mutual Exclusion & Validation Invariant:**
     * A valid `StateGetReply` MUST contain `version`, `target`, and **exactly one** of `state` or `error`.
     * If both `state` and `error` are populated, or if both are null/omitted, the reply is malformed and rejected with `ErrMalformedStateGetReply`.
   * **Destination & Correlation Semantics:**
     * The reply payload is published strictly to the ephemeral NATS reply inbox (`msg.Reply`, e.g. `_INBOX.<unique>`).
     * It is NEVER published to `result.<target>` or `status.<target>`.
     * Request correlation is handled natively by the standard NATS request-reply inbox mechanism; no duplicate RPC ID or wrapper transaction ID is needed in the payload.
   * **Wire Examples:**
     * **Success (Wraps StateEnvelope):**
       ```json
       {
         "version": "1.0",
         "target": "vyos",
         "state": {
           "version": "1.0",
           "uuid": "1793880893",
           "target": "vyos",
           "payload": {
             "version": 1,
             "unit": { "uptime": 45123 },
             "interfaces": []
           },
           "timestamp": "2026-05-12T10:30:00Z"
         }
       }
       ```
     * **Worker Saturation Rejection (`service_busy`):**
       ```json
       {
         "version": "1.0",
         "target": "vyos",
         "error": {
           "code": "service_busy",
           "message": "All state collection workers are occupied"
         }
       }
       ```
     * **Collection Failure & Timeout:**
       ```json
       {
         "version": "1.0",
         "target": "vyos",
         "error": {
           "code": "collection_failed",
           "message": "failed to sample kernel interface counters"
         }
       }
       ```
       ```json
       {
         "version": "1.0",
         "target": "vyos",
         "error": {
           "code": "collection_timeout",
           "message": "state collection exceeded 3s execution deadline"
         }
       }
       ```
7. **Public Go API & Error Decoding (`RequestState`):**
   * Method signatures remain idiomatic:
     ```go
     type StateGetHandler func(context.Context) (StateEnvelope, error)
     func (c *Client) RequestState(ctx context.Context, target string) (StateEnvelope, error)
     ```
   * Public typed error and error code constants:
     ```go
     const (
     	CodeServiceBusy       = "service_busy"
     	CodeCollectionFailed  = "collection_failed"
     	CodeCollectionTimeout = "collection_timeout"
     )

     type StateGetError struct {
     	Code    string
     	Message string
     }

     func (e *StateGetError) Error() string {
     	return fmt.Sprintf("state.get: [%s] %s", e.Code, e.Message)
     }
     ```
   * In `RequestState(ctx, target)`, decoding distinguishes:
     * **Success:** `reply.State != nil` unwraps and returns `(*reply.State, nil)`.
     * **Typed Error Reply:** `reply.Error != nil` returns `(StateEnvelope{}, &StateGetError{Code: reply.Error.Code, Message: reply.Error.Message})`. Callers can test for `errors.As(err, &stateErr)` and inspect `stateErr.Code == agentcore.CodeServiceBusy`.
     * **Malformed Reply:** Invalid JSON, missing both fields, or containing both `state` and `error` returns typed `ErrMalformedStateGetReply`.
     * **NATS No Responders:** Target daemon is not running or listening; returns `nats.ErrNoResponders`.
     * **Caller Deadline Exceeded:** Caller's context expires before reply arrives (e.g. uCentral's 5s budget); returns `context.DeadlineExceeded` (or `context.Canceled`).
8. **On-Demand Concurrency Bounding & Execution Limits:**
   * **Configurable Worker Capacity:** The library accepts a configurable execution capacity supplied during agentcore client initialization (`Config.StateGetWorkers`, default: `5`). Implementation uses a bounded worker pool or semaphore (default 5 slots).
   * **Immediate Rejection on Saturation (No Waiting Queue):** If all 5 worker slots are occupied when a request arrives, the handler must NOT buffer or queue the request. It immediately returns `StateGetReply` with `error.code: CodeServiceBusy` on the ephemeral NATS reply inbox.
   * **3-Second Collection Deadline:** Each live-state collection execution is bound to a strict 3-second context cancellation (`context.WithTimeout(ctx, 3*time.Second)`), preventing hung collections and ensuring completion well within caller NATS deadlines (e.g. uCentral's 5-second deadline).
   * **Nonblocking NATS Callback:** The NATS subscription callback must perform only envelope validation and semaphore admission, then hand off collection asynchronously to a worker goroutine. It must NEVER run the 3-second collection inline on the NATS subscription dispatcher thread, preventing starvation of configure or action handlers.
   * **Capacity Release & Observability:** Worker capacity is released on every terminal condition (`defer sem.Release()`), covering successful completion, error, panic recovery, and context cancellation. The library exposes metrics for active workers (`gauge`), busy rejections (`counter`), and timeouts (`counter`).
9. **Required Test Matrix:**
   * Malformed and wildcard subject rejection.
   * Malformed or oversized payload rejection (> 1 MB).
   * Callback error propagation and panic recovery metrics.
   * Reconnect restoration validation under broker restart.
   * RequestState positive unwrapping (`TestRequestState_Success`).
   * Concurrency saturation: 5 active requests saturate capacity; the 6th simultaneous request immediately receives `service_busy` (`TestRequestState_Saturation_ServiceBusy`).
   * Collection timeout: Handler exceeding 3-second deadline returns `collection_timeout` (`TestRequestState_CollectionTimeout`).
   * Collection error: Handler returning Go error maps to `collection_failed` (`TestRequestState_CollectionFailed`).
   * Malformed wire reply: Reply with both `state` and `error` or neither returns `ErrMalformedStateGetReply` (`TestRequestState_Malformed_Dual`, `TestRequestState_Malformed_Empty`).
   * Transport errors: `nats.ErrNoResponders` and caller `context.DeadlineExceeded` propagated cleanly.
   * Worker release verification across success, handler error, 3-second timeout, and panic recovery.
   * Subscription isolation: concurrent on-demand state collections do not starve or delay configure/action command dispatches.
10. **Scope Boundary:** PR #11 establishes the architectural and interface contract as **strictly docs-only** until code implementation begins.

---

## 13. Validation and error handling

The library must perform:
- transport-level validation
- message envelope sanity checks
- required field validation for standard contracts

The library must **not** implement deep cloud business-policy validation.

Public APIs must return clear, typed errors with structured codes where appropriate.

The library must safely recover from panics thrown in user-supplied configure, action, result, status, and state handlers (which are caught, logged, and recorded in metrics as failures) and the custom reconnect handler, preventing application crashes. For session-level connection/reconnection failures and panics originating in the custom reconnect handler callback, errors/panics must also be propagated to the registered `errorSink` (if provided).

### 13.1 Bounding State Handlers & Backpressure Policy

Because callbacks are invoked inline on the NATS subscription dispatcher:
* **Payload Bounding:** State payloads can contain large interface and neighbor snapshots. The library enforces a strict maximum payload size of **1 MB** (matching uCentral's 1 MB state limit). Payloads exceeding 1 MB are discarded with a validation error before callback execution.
* **Nonblocking Execution & Bounded Handoff:** Handlers must return quickly. Consuming agents (such as `ucentral-client`) must hand off incoming state envelopes to bounded queues or last-write-wins coalescers (`StateCoalescer`) rather than executing blocking I/O or spawning unbounded goroutines inline. For on-demand state queries (`state.get`), the subscription dispatcher validates and admits requests against the 5-slot worker limit before handing execution off to an asynchronous worker goroutine, keeping subscription dispatch loops completely nonblocking.
* **On-Demand Concurrency & Service Busy Policy:** On-demand state queries are subject to a strict worker capacity limit (default: 5 concurrent workers) with no waiting queue. If the pool is saturated, incoming requests receive an immediate `service_busy` error reply rather than queueing or timing out. Each admitted collection is bounded to a 3-second context deadline.
* **Best-Effort Telemetry Delivery:** Periodic state streams over Core NATS are inherently best-effort. Core NATS publication does not imply JetStream durable delivery; dropped intermediate samples during network disconnection are safely superseded by the subsequent periodic state snapshot.
* **Observability:** Dropped messages, oversized payload rejections, handler errors, and reconnect events must increment observable metric counters (`metrics.IncErrors`, `metrics.IncPanics`).

---

## 14. Health and observability

The library must expose health information in a safe, read-only form.

The library should support:
- logger hooks
- metrics hooks

These observability facilities must be pluggable and must not rely on global mutable state.

---

## 15. Concurrency model

The library must be safe for concurrent use within an agent process.

Internal mutable shared state such as:
- connection/session state
- handler registry
- subscription restoration state

must be protected appropriately inside the implementation.

The design does not require application-level locking around desired config identity decisions.
Sync decisions are based on comparing the running config UUID with the current desired config UUID.

---

## 16. Summary of key invariants

The following are normative design invariants:

1. `olg-nats-agent-core` is a library, not a daemon.
2. Configure uses **store desired state in KV, then notify**.
3. Action uses **direct publish to target subject**.
4. KV is a **single latest desired-config slot**, not a historical config store.
5. Config UUID is an **opaque identity token** used for equality comparison only.
6. `rpc_id` is used only for request/response correlation.
7. Agents determine sync by comparing **running UUID** vs **desired UUID in KV**.
8. Configure outcomes must include the **config UUID** that was attempted or applied.
9. Revision-driven config ordering and rollback semantics are out of scope.
10. Platform-specific execution logic remains outside the shared library.
11. `state.<target>` and `state.get.<target>` are used exclusively for periodic and on-demand operational state; `status.<target>` and `status.get.<target>` are reserved for lifecycle and execution progress.
