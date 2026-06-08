# ADR 0002: Split Business Storage, Time Series Storage, And Event Flow

## Status

Accepted

## Context

The platform handles both business metadata and high-volume telemetry. These workloads have different query patterns and retention expectations.

## Decision

Use PostgreSQL for tenants, devices, device state, command state, and recent telemetry metadata. Use TDengine for time series telemetry writes. Use Kafka as the event flow between ingress, core command creation, and worker processing.

## Consequences

- PostgreSQL remains the source of truth for business state.
- TDengine receives telemetry writes from worker and can scale for time series queries.
- Kafka consumers must be idempotent because command and telemetry processing can be retried.
- Local and Helm deployment must keep Kafka listener configuration explicit for host and Kubernetes access.
