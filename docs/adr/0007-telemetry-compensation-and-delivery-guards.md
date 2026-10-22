# ADR 0007: Compensate Persisted Telemetry And Guard Command Delivery

## Status

Accepted

## Context

A PostgreSQL telemetry commit can succeed before Kafka publishing fails. Caller
retries and manual DLQ replay cannot repair every persisted row. Kafka redelivery
can also present a command whose device has already acknowledged it.

## Decision

- Worker scans unwritten PostgreSQL telemetry every 30 seconds, 500 rows at a
  time, excluding records received in the last two minutes. An indexed ID cursor
  advances through the backlog and resets at its end. Tenant filtering happens
  before LIMIT, so foreign tenants cannot starve a scoped worker.
- TDengine initialization and each write use context-aware SQL/HTTP calls, with
  a default five-second cap (`TDENGINE_WRITE_TIMEOUT`). Worker passes its caller
  context to consumption and compensation writes, so cancellation interrupts an
  in-flight sink call without advancing its completion marker.
- A successful TDengine write is followed by the completion marker. Write or
  marker failures leave the row pending for another sweep. Permanent field
  validation failures (UTF-8, controls, canonical IDs and VARCHAR byte bounds)
  and structured TDengine timestamp-range rejections (driver code `0x060b`)
  do not stop the remaining batch or pin the scan cursor and are never marked
  written. These rows retain their payload and pending marker for repair; a
  later sweep can revisit them after retention/configuration changes. Network,
  timeout, permission and unrecognized sink errors remain retryable and preserve
  the cursor. Ingress uses
  the same field contract. PostgreSQL retains the
  original payload; the scan neither consumes DLQ offsets nor emits commands.
- Dispatcher claims one command immediately before each publish. The publish
  context expires at half the 30-second lease; completions and reschedules are
  fenced by the claimed attempt and an active lease. Exhausted created commands
  become failed only after their lease/backoff expires. Claims never exceed the
  attempt cap. Command state changes and their audit events use one atomic SQL
  statement, including recovery, delivery and expiry.
- Runtime PostgreSQL operations propagate the request/worker/dispatcher context
  and use a default five-second cap, including lists, authentication, state writes
  and completion markers. Schema migration retains its separate maintenance budget.
  Expiry/recovery sweeps select at most 500 rows per transition using
  `FOR UPDATE SKIP LOCKED`; a locked row is retried later without blocking other
  commands. State and audit still commit in one statement.
- Live ingress stamps server receive time; trusted worker restoration preserves
  `receivedAt` from the Kafka record and uses the original Kafka message timestamp
  only for older records without that field. DLQ replay uses existing receive
  time, then DLQ `occurredAt`, then device event time for legacy metadata. It
  never labels replay processing as a fresh receive. Device liveness still uses
  receive time and the configured TTL, not device event time for normal ingress.
  PostgreSQL updates `last_seen_at` and `last_msg_id` together only when receive
  time strictly advances (NULL accepts the first sample); equal/older arrivals
  cannot regress state. `updated_at` is also monotonic. Duplicate storage keeps
  its original receive time and never refreshes liveness. Historical rows still
  commit even if their liveness update is skipped.
- Before command MQTT publishing, worker reads persisted state and verifies the
  tenant/device identity. Missing, foreign, sent and terminal commands are skipped.
  Database errors stop the consumer without committing the message. Failed status
  writes and offset commits are surfaced instead of silently declaring success.
- MQTT ACK writes retry three times with bounded database calls; exhausted
  failures are retained in DLQ as `command.ack`. Worker MQTT automatic ACK is
  disabled: a valid ACK gets PUBACK only after PostgreSQL or DLQ persistence.
  If both fail, leave it unacknowledged and surface a fatal error to Run.
  Invalid/foreign/unknown ACKs are explicitly rejected and acknowledged to avoid
  permanent redelivery loops. On shutdown, stop callback admission and drain
  admitted ACKs for up to three seconds before cancellation, MQTT disconnect and
  DLQ writer closure. Manual replay applies the ACK
  transition directly. Terminal command states retain their existing semantics.
- Worker waits for QoS 1 grants on every ACK subscription before starting business
  processing. Missing, rejected or downgraded SUBACK results and subscription
  errors/timeouts are fatal, including reconnect callbacks. Subscription setup
  shares a five-second deadline and is canceled on shutdown.
- ACK callbacks run independently of the MQTT packet router. Database retry
  latency must not block other commands' PUBACKs; PostgreSQL row locks serialize
  duplicate ACK transitions rather than relying on callback ordering.
- Tenant allowlists derive separate Kafka consumer groups from a normalized
  SHA-256 fingerprint, preventing independent tenant subsets from advancing
  each other's offsets. Changing an allowlist creates new offsets and can replay
  retained history; overlapping subsets should not be deployed simultaneously.
- Legacy sent commands with no deadline converge to timeout after the configured
  ACK grace measured from their last update; normal deadlines are unchanged.

## Consequences

- Multiple workers may retry the same telemetry row; sink writes remain idempotent
  under the existing per-device timestamp model. Different messages with the same
  device timestamp retain the existing TDengine collision semantics.
- Delivery is at least once. A crash between MQTT publish and the state write, or
  a publish queued across a reconnect, can still cause duplicate actuation. Devices
  must deduplicate by command ID; server guards do not provide exactly-once execution.
- When PostgreSQL and Kafka both fail, the worker reports failure without PUBACK.
  Redelivery after disconnect/replacement still depends on broker session and
  shared-subscription policy; forced termination and unavailable durable sinks
  are not covered by a durable application inbox. See ADR 0008 for MQTT hop
  boundaries.
- Broad production changes (HA data services, HPA/topology, device measurements in
  TDengine and lifecycle retention) require capacity and business decisions.

The command dispatcher runs timeout/recovery scans in a separate bounded loop
from publishing. Slow Kafka calls consume their own lease-bounded contexts and
do not delay the next scan. Both loops share cancellation and Run waits for the
scan loop to stop. State transitions and lease/attempt fencing stay in SQL.

TDengine databases must use millisecond precision (`ms`). Initialization explicitly
creates new databases with `PRECISION 'ms'`, then reads the quoted `precision`
column in `information_schema.ins_databases` under the existing startup deadline.
Existing `us`/`ns` databases fail startup before stable creation or business
consumption; the worker does not alter their precision or reinterpret stored rows.

Kafka command notifications with negative `dispatchAttempts` go to `command.decode`
DLQ before any device publish; commit follows successful DLQ persistence. Once
the persisted identity/state check passes, PostgreSQL supplies the command body
and retry attempt. Both dispatcher and worker bound the backoff exponent to 0–6
so corrupted negative counters cannot panic during a shift.
