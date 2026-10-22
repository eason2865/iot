# ADR 0009: Bound Observability And Require MQTT Subscription Readiness

## Status

Accepted

## Context

The default REST access logger dumps headers and request bodies on HTTP 5xx,
including bearer credentials and device secrets. Core RPC statistics also log
request content. MQTT services report ready based only on PostgreSQL while
subscriptions may still be pending. Default file tracing appends indefinitely
to a 128Mi temporary volume and includes health probes.

## Decision

- Disable the framework REST request logger and recovery logger. A shared access
  middleware records only method, route template, status, duration and request ID.
  It preserves the original request for authentication and handling. Panic logs
  retain a stack without the arbitrary panic value or request content. Core RPC
  statistics exclude content for every generated core method; metrics remain.
- Bootstrap supplies runtime readiness checks in addition to PostgreSQL health.
  Ingestor requires its telemetry QoS 1 grant; worker requires every configured
  ACK filter. Connection state and subscription generation are checked separately.
  Reconnecting invalidates grants before opening a new connection, successful
  SUBACKs confirm only their generation, and fatal errors/shutdown clear readiness.
  Physical connection state detects disconnects. An asynchronous connection-lost
  callback must not clear a later connection's grants. Liveness stays shallow.
- Use Paho SubscribeMultiple with a single wire filter for each subscription.
  Unlike Subscribe, it preserves the full `$share/<group>/...` filter as the
  SUBACK result key, so ordinary and shared subscriptions use the same strict
  QoS 1 check. Protocol regressions cover both filter forms and reconnects.
- Keep file tracing available by default, with parent-based root sampling of 0.1
  (override `OTEL_SAMPLER` in range 0–1). Own the file provider explicitly so the
  framework cannot install an append-only exporter over it. External exporters
  retain the framework implementation and existing environment configuration.
- Bound the active trace file to 8 MiB and retain two backups, each at most 8 MiB,
  named `.1` and `.2`. Serialize writes and rotation; rotation failures do not
  permit further append, and a single oversized record fails export. At startup,
  discard oversized legacy files at these managed paths. Local tracing is a
  bounded diagnostic buffer; use OTLP for longer retention. Shutdown flush is
  bounded to two seconds.
- Exclude `/healthz`, `/readyz` and `/metrics` from HTTP spans. The gateway readiness
  RPC uses a valid unsampled parent and outgoing trace context, so core does not
  generate a fresh root span even when client tracing is disabled or root sampling
  is set to 1. Business tracing remains enabled.

## Consequences

- Access logs no longer provide payload dumps for debugging; request IDs, status,
  latency, metrics and traces remain available without copying credentials.
- Rolling updates wait for the actual MQTT subscriptions. A disconnected replica
  can remain live while failing readiness until its subscriptions recover.
- A service's managed trace files use at most 24 MiB of its existing 128Mi `/tmp`
  volume. Old traces are intentionally evicted; optional telemetry can be dropped
  on export failure. This does not bound unrelated temporary files or external
  exporter storage. No real Pod storage-exhaustion test is claimed.

The management API explicitly enables client tracing on its programmatically
created direct gRPC client. Configuration-file defaults do not initialize that
struct's switches. Real HTTP -> gRPC tests check trace ID, sampling and request
ID, and verify readiness remains unsampled with no exported client span.

HTTP tracing uses a shared attribute allowlist: method, route template and status.
The compatibility `http.target` attribute also contains only the route template;
unknown paths become `other`. Raw queries, path identifiers, URL credentials,
headers, bodies, addresses and user agents never become span attributes. This
applies before authentication and to both file and external exporters. The
request is unchanged for handlers and trace context still propagates to gRPC.
Native go-zero HTTP tracing is disabled. A custom router supplies the same safe
404 tracing, because the framework's default 404 handler enables raw-URL tracing
even when its regular Trace middleware switch is false. Health/ready/metrics
requests remain span-free. Real middleware/file-exporter regressions cover
unauthorized, authorized and unknown routes, plus normal RPC propagation.
