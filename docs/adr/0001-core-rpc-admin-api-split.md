# ADR 0001: Split Core RPC From Admin API

## Status

Accepted

## Context

The platform needs a stable business module for tenants, devices, telemetry, commands, and ACK transitions while still exposing a convenient REST interface for clients and demos.

## Decision

Keep `core-rpc` as the core business gRPC service and keep `admin`/`adminapi` as the REST gateway. `adminapi` discovers `core-rpc` through etcd and translates HTTP requests to protobuf calls.

## Consequences

- Core business behavior can evolve behind a smaller gRPC interface.
- REST routing and HTTP response shaping stay in `internal/adminapi`.
- Local deployment must include etcd or external etcd access.
- Tests should prefer exercising `internal/core` behavior directly where possible, and use HTTP tests for gateway behavior.
