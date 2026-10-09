# ADR 0006: Dead Letters Are Alerted On And Replayed Manually

## Status

Accepted

## Context

Failed messages land in `iot.dlq` with their stage and error, and `cmd/dlq-replay`
republishes them to their original topic. The question this ADR settles is whether
something should consume the dead-letter topic automatically.

Automatic replay was rejected for three reasons.

The first is that replay is only correct after the cause is fixed. A record
dead-lettered because TDengine was down, replayed while TDengine is still down,
simply lands back in the DLQ — now with a new offset and a newer timestamp, which
makes "how long has this been failing" harder to read. Worst case the loop
repeats and inflates the topic.

The second is that the failure modes are not symmetric across stages. A replayed
telemetry record is idempotent: `telemetry_records` has a unique constraint on
`(msg_id, tenant_id, device_id)` and the worker compensates the TDengine write
from `tdengine_written`. A replayed command is not: it goes back through
`iot.command`, `device-worker` publishes it over MQTT again, and the device acts a
second time. Automatic command replay would therefore mean duplicate actuation on
physical equipment.

The third is amplification. The events that fill a DLQ — a broker with a shrunken
ISR, a database outage, a bad deploy — are exactly the moments when automatically
pushing the whole backlog back through the pipeline is least welcome.

Set against that, the volume is low and a fix almost always needs a human anyway
(a deploy, a configuration change, a broker repair). Automation buys little here.

## Decision

- No consumer runs against `iot.dlq`. Recovery is an operator action.
- Visibility is provided instead: `iot_dlq_publish_total{stage,result}` counts
  every attempt, `result="error"` marks a message that was *not* preserved, and two
  Grafana rules alert on dead letters appearing (warning) and on dead-letter
  writes failing (critical).
- The backlog is bounded by `KAFKA_DLQ_RETENTION_MS`, applied when this service
  creates the topic (see ADR 0004 for the same caveat: an existing topic keeps its
  configuration).
- `cmd/dlq-replay` is the recovery tool, and it is deliberately conservative:
  `-dry-run` changes nothing at all, including the consumer group position; each
  stage has its own consumer group so a targeted run cannot hide another stage's
  records; and a run ends successfully when the topic goes idle rather than
  blocking until a timeout.
- The operator procedure is documented as the dead-letter replay runbook in the
  README.

## Consequences

- Recovery depends on a human and on the retention window. A dead letter older
  than `KAFKA_DLQ_RETENTION_MS` (default 7 days) is gone, so a fix deferred past
  that window loses data permanently and the alert is the only warning.
- The pipeline never re-injects traffic on its own, so a transient fault cannot be
  amplified by the recovery mechanism.
- Replaying a command still re-sends its downlink. The tool cannot know whether a
  device already acted, so the runbook requires fixing the cause and inspecting
  with `-dry-run` first, and treats command stages as the ones needing the most
  care.
- Because the DLQ is not consumed, its metric reflects writes only. There is no
  lag or depth signal, so "the backlog is still there" is not directly observable
  — the alert on new writes plus a dry run is the available evidence.
