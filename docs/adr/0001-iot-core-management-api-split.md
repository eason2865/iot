# ADR 0001: Split IoT Core From Management API

## Status

Accepted

## Context

The platform needs a stable business module for tenants, devices, telemetry, commands, and ACK transitions while still exposing a convenient REST interface for clients and demos.

## Decision

Keep `iot-core` as the core business gRPC service and keep `management-api`/`adminapi` as the REST gateway implementation. `adminapi` connects directly to `iot-core` through a configurable gRPC endpoint; Kubernetes deployments use the `iot-core` Service DNS.

## Consequences

- Core business behavior can evolve behind a smaller gRPC interface.
- REST routing and HTTP response shaping stay in `internal/adminapi`.
- Local deployment requires only the configured `IOT_CORE_ENDPOINTS` gRPC endpoint.
- Tests should prefer exercising `internal/core` behavior directly where possible, and use HTTP tests for gateway behavior.
