# ADR 0003: Use Helm As The Kubernetes Manifest Source

## Status

Accepted

## Context

Kubernetes deployment definitions need one authoritative source to avoid duplicated manifests and drift.

## Decision

Use `charts/iot` as the Kubernetes manifest source for the four application services only. PostgreSQL, Kafka, EMQX, TDengine, Prometheus, Grafana, and the demo are independently managed dependencies and are not rendered by this chart. `management-api` uses the `iot-core` Kubernetes Service directly, so no business etcd deployment is required.

## Consequences

- Kubernetes changes should be made in the Helm chart first.
- Local dependencies and demo run through `monitoring/docker-compose.yml`; production dependencies use their own platform-managed deployment.
- Namespace creation is handled by Helm commands with `--create-namespace`.
- Kubernetes changes are made in `charts/iot`; no second manifest source is maintained.
