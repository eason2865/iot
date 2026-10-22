# ADR 0008: Persist MQTT Telemetry Before Acknowledging And Deduplicate Command Requests

## Status

Accepted

## Context

The MQTT bridge returned from its callback before an asynchronous Kafka write
completed. Paho then sent PUBACK while the only copy held by the application was
in memory. Command API retries also created new command IDs, so device-side
command-ID deduplication could not prevent two actions after a lost HTTP response.

## Decision

- Disable automatic MQTT acknowledgements. Production ingestors write a
  PostgreSQL telemetry inbox row before PUBACK. Kafka publishing remains bounded
  to 64 concurrent tasks; the existing worker completion-marker scan repairs
  inbox rows whose Kafka event or TDengine write never completed.
- Telemetry subscriptions must actually receive a QoS 1 SUBACK grant. Missing,
  rejected or downgraded results fail the ingestor, including reconnects, because
  QoS 0 cannot support manual acknowledgement and broker redelivery.
- Require telemetry payload to be present and a valid JSON object, including `{}`.
  Share validation across MQTT/REST/gRPC ingress and apply the same stable shape
  check in stores to prevent bypass. Invalid payloads map to InvalidArgument/400;
  historical recovery does not reapply the live timestamp window. Existing stored
  rows are not migrated or deleted.
- If the inbox is unavailable, a successful Kafka or DLQ write can still establish
  durability. With no successful sink, leave the message unacknowledged and fail
  the ingestor. Invalid messages are acknowledged only after DLQ persistence.
- Close admission and drain pending bridge tasks on shutdown before closing
  shared resources. After three seconds cancel remaining tasks; committed inbox
  rows remain compensatable.
- Accept an optional `Idempotency-Key` on POST /api/v1/commands, forwarded as
  protobuf `idempotency_key`. Keys contain 1–128 printable ASCII characters
  without spaces and are unique within a tenant. Same key/device/JSONB payload
  returns the existing command and creates no extra command or audit event.
  A changed device or payload returns HTTP 409. Missing keys preserve existing
  clients' behavior. Keys remain reserved while their command rows exist.
- Reuse the active transaction for device checks and duplicate telemetry reads.
  Context-aware lookups distinguish missing rows from database failures.
- Embed SQL from migrations/ as the single schema source. Under a transactional
  advisory lock, run the legacy column upgrade (002) before the current initial
  schema (001), then hash legacy credentials in the same transaction. Deployment
  tooling uses the migrate binary. POSTGRES_AUTO_MIGRATE=false keeps DDL out of
  application startup and ACK DLQ replay; local development defaults to true.
  The replay CLI validates the flag before opening PostgreSQL, opens the store
  lazily only for actual command.ack replay, and needs no database in dry-run.
- Reserve colon in tenant identifiers across API, stores, envelopes, topic
  builders and authentication. MQTT device usernames use a validated first-colon
  split; device IDs may still contain colons. Existing colon-containing tenant
  IDs require a coordinated identity migration, not silent normalization.

## Consequences

- MQTT telemetry creates an extra PostgreSQL operation before the worker consumes
  it. Kafka remains the normal asynchronous path; PostgreSQL holds the recovery
  copy. Capacity planning must account for this additional database traffic.
- PUBACK proves application persistence in at least one configured sink. This does
  not provide durable device-to-broker offline delivery: broker sessions, shared
  subscription behavior, and messages not yet received by the application still
  require an EMQX delivery policy. Broker acknowledgement of a device's publish
  is a separate hop from the bridge's PUBACK.
- Command execution is still at least once; request keys prevent duplicate
  creation, while devices still deduplicate deliveries by command ID.
- TDengine measurement columns, same-timestamp collisions, data retention, HA
  infrastructure and throughput targets retain the business decisions in ADR 0007.
