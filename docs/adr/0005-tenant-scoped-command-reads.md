# ADR 0005: Tenant-Scoped Command Reads And Status-Code Error Mapping

## Status

Accepted

## Context

The three command endpoints disagreed about their contract. `ListCommands` required
`tenantId`, `AckCommand` required `id`, `tenantId`, and `deviceId`, and `GetCommand`
accepted a bare `id` and returned any tenant's command. That made `GET
/api/v1/commands/{id}` the only lookup in the management API that could cross a tenant
boundary, and it relied on command IDs being unguessable. Command IDs are UUIDv7, so
they embed a timestamp and are far more enumerable than UUIDv4 — obscurity was never a
real defence.

Error mapping had a related flaw. `iot-core` returned plain Go errors and the REST
gateway decided the HTTP status by matching substrings in the message. Any error text
that matched no pattern fell through to `502 Bad Gateway`, so
`tenantId contains invalid MQTT topic characters` returned 502 instead of 400, and
`command does not belong to device` returned 502 instead of 404. Behaviour depended on
wording, and the in-process test harness — which validates directly and returns 400 —
disagreed with the production gateway about the same request.

## Decision

- `GetCommandRequest` gains a required `tenant_id`, matching `ListCommands` and
  `AckCommand`. A missing tenant is `InvalidArgument`.
- `iot-core` verifies that the stored command belongs to the requested tenant. A
  mismatch is reported as not-found, deliberately indistinguishable from a command that
  does not exist: confirming that another tenant's ID exists is itself a leak.
- The REST gateway reads `tenantId` from the query string and forwards it, leaving both
  checks in `iot-core` rather than duplicating them at the edge.
- The repositories return typed sentinels (`platform.ErrNotFound`,
  `platform.ErrAlreadyExists`) instead of formatted strings. `iot-core` translates them
  to gRPC status codes and the gateway maps codes to HTTP statuses. No layer matches on
  message text, and the response body carries the core message without the
  `rpc error: code = ... desc = ...` framing.
- The in-process harness mirrors the same tenant rule so both REST surfaces describe one
  contract.

## Consequences

- Every command endpoint now requires a tenant. An operator who holds only a command ID
  must look it up in the database; the API deliberately does not offer a cross-tenant
  read.
- Cross-tenant reads and ACKs answer 404 instead of 403 or 502, which also removes the
  pre-existing bug where a cross-device ACK surfaced as 502 Bad Gateway.
- Clients that call the detail endpoint without `tenantId` now receive 400 instead of
  200. The gateway, the harness, and both READMEs were updated together, and the live
  E2E test asserts the tenant-scoped path.
- Per-tenant authentication can later compare the caller's tenant from gRPC metadata
  against the request field without changing the API shape.
- Status codes are now part of the contract. Adding a new failure mode means choosing a
  code, and a test drives a real gRPC server through the real `zrpc` client to prove
  codes survive the client round trip, since the whole mapping depends on that.
