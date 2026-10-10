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
- A successful TDengine write is followed by the completion marker. Write or
  marker failures leave the row pending for another sweep. Control-character
  validation failures do not stop the remaining batch. PostgreSQL retains the
  original payload; the scan neither consumes DLQ offsets nor emits commands.
- Before command MQTT publishing, worker reads persisted state and verifies the
  tenant/device identity. Missing, foreign, sent and terminal commands are skipped.
  Database errors stop the consumer without committing the message. Failed status
  writes and offset commits are surfaced instead of silently declaring success.
- MQTT ACK writes retry three times with bounded database calls; exhausted
  failures are retained in DLQ as `command.ack`. Manual replay applies the ACK
  transition directly. Terminal command states retain their existing semantics.
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
- MQTT ACK durability still depends on preserving the ACK successfully in PostgreSQL
  or Kafka. Simultaneous failures of both are observable through DLQ failure metrics
  but do not constitute a lossless durable MQTT inbox.
- Broad production changes (HA data services, HPA/topology, device measurements in
  TDengine and lifecycle retention) require capacity and business decisions.
