# Changelog

All notable changes to this project will be documented in this file.

## 1.0.0 - 2026-06-06

### Added

- Go service entrypoints for `admin`, `ingress`, `worker`, and `demo`.
- MQTT ingestion through EMQX and asynchronous event flow through Kafka.
- PostgreSQL persistence for tenants, devices, telemetry, state, and commands.
- TDengine writer for time-series telemetry.
- Command downlink and ACK handling.
- Demo simulator for multi-tenant device traffic.
- OpenAPI and MQTT JSON Schema documents.
- PostgreSQL migration script.
- Helm chart, local Kubernetes manifests, Dockerfile, and monitoring assets.
- Grafana dashboards and Prometheus deployment support.
- Open source governance files for the 1.0 release.
