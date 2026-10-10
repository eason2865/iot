# ADR 0004: Require All In-Sync Replicas For Business Producers

## Status

Accepted

## Context

Telemetry and commands reach Kafka from two places: `management-api`/`iot-core` publish
directly through `KafkaPublisher`, and the DLQ paths publish through their own writers.
The business producers used `RequiredAcks: RequireOne`, while the DLQ writers already
used `RequireAll`.

That mismatch contradicts the durability story the rest of the system is built on. The
ingest path deliberately writes PostgreSQL first and then publishes to Kafka, and the
DLQ plus idempotent consumers exist to cover a failed publish. But `RequireOne` only
acknowledges the leader: if that leader dies before the record is replicated, the event
is gone even though PostgreSQL already holds the row. The DLQ cannot help, because the
DLQ only covers failures we observe — consumer-side errors and failed writes — not data
lost after an acknowledgement was already returned. The visible symptoms are telemetry
rows that never reach TDengine and commands that stay in `published` forever.

The cost of changing this is small here: both writers use `BatchSize: 1`, so every
message already pays a full round trip. `RequireAll` only adds waiting for the slowest
in-sync replica, and on the single-broker local stack it is exactly equivalent to
`RequireOne`.

Note that `RequireAll` alone is not sufficient. With `min.insync.replicas=1` the
in-sync replica set can shrink to the leader alone, writes still succeed there, and the
guarantee is again no stronger than `RequireOne`.

## Decision

- Business producers (`KafkaPublisher` for telemetry and commands) use
  `RequiredAcks: RequireAll`, matching the DLQ writers.
- When this service creates a topic, it sets `min.insync.replicas` from
  `KAFKA_TOPIC_MIN_INSYNC_REPLICAS` and the replication factor from
  `KAFKA_TOPIC_REPLICATION_FACTOR`. Defaults stay at 1 so the single-broker local stack
  keeps working.
- Production deployments must set the replication factor to at least 3 and
  `min.insync.replicas` to at least 2 on `iot.telemetry`, `iot.command`, and `iot.dlq`.
- A configured `min.insync.replicas` above the replication factor is invalid at the
  broker, so it is dropped with a log line instead of failing topic creation or
  pretending the setting was applied.
- Topic creation is idempotent. Partition count (`KAFKA_TOPIC_PARTITIONS`) and
  replication factor apply only when creating a missing topic; changing existing
  partitions or replica assignments remains an operator action.
- On startup, the service compares managed dynamic configuration entries
  (`min.insync.replicas` and DLQ `retention.ms`) and reconciles drift through
  `AlterConfigs`. Changes to the deployment's desired values therefore also affect
  existing topics. Partition-count and replication-factor drift is logged only.
- If telemetry throughput ever makes the round trip the bottleneck, the correct
  response is a larger `BatchSize`/`BatchTimeout`, not weaker acknowledgements.

## Consequences

- A degraded cluster now fails writes instead of silently losing them. REST ingestion
  returns 5xx with `codes.Unavailable`, and because the row is already stored and
  republishing is idempotent, the caller can retry safely.
- Failed command publishes are rescheduled by the dispatcher and eventually marked
  `failed` after the attempt limit, so the outcome is observable rather than a command
  stuck in `published`.
- The DLQ writers also require the full in-sync set, so a cluster below
  `min.insync.replicas` blocks dead-lettering too. The worker retries, then exits, and
  Kubernetes restarts it: a crash loop rather than data loss. That is the intended
  trade-off, but it makes dead-lettering unavailable exactly when the cluster is
  degraded, so DLQ depth and worker restarts need alerting.
- Write latency grows by one replication round trip in multi-broker deployments.
- Deployments that raise only the environment variables but leave the topics at
  replication factor 1 gain nothing. The prerequisites above are part of the decision,
  not optional guidance.
