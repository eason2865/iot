# ADR 0003: Use Helm As The Kubernetes Manifest Source

## Status

Accepted

## Context

The repository previously had both Helm charts and `k8s/local` Kustomize manifests. Maintaining both caused duplicated deployment definitions and drift risk.

## Decision

Use `charts/iot` as the Kubernetes manifest source for the four application services only. PostgreSQL, Kafka, EMQX, TDengine, etcd, Prometheus, Grafana, and the demo are independently managed dependencies and are not rendered by this chart.

## Consequences

- Kubernetes changes should be made in the Helm chart first.
- Local dependencies and demo run through `monitoring/docker-compose.yml`; production dependencies use their own platform-managed deployment.
- Namespace creation is handled by Helm commands with `--create-namespace`.
- The removed `k8s/local` directory should not be recreated unless there is a new explicit deployment strategy decision.
