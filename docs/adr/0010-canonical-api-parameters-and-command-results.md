# ADR 0010: Canonical Tenant Resources, Explicit Filters And Device Results

## Status

Accepted, 2026-10-26. Replaces the REST path/query identity choice in ADR 0005
and extends ACK semantics in ADRs 0007 and 0008. Existing delivery, lease and
storage durability boundaries remain in effect.

## Context

Tenant identity had three sources (path, query and JSON). A tenant token could
create a device but could not enumerate its own devices. Unknown query filters
were ignored, telemetry had no time filter, and MQTT result fields were discarded
when applying ACKs. Duplicate parameters and incomplete gRPC identifiers had
inconsistent interpretations.

## Decision

Use tenant-nested REST resources with a shared path/query interpretation for
both authorization and handlers. Remove the old routes as explicitly requested;
there are no compatibility aliases. Business bodies do not duplicate path
identity. See [API contract](../api-contract.md) for the exact routes and fields.

Add tenant device enumeration, bounded RFC3339 time filters, type/device/status
filters, explicit sorting and scope-bound keyset cursors. Apply validation at
both REST and gRPC boundaries, and in persistent/in-memory stores where relevant.
Keep the existing response byte budget. Device event time drives telemetry
queries; command creation time drives command queries. Use half-open intervals
and millisecond precision to avoid boundary rounding ambiguity.

Persist device success/failure, error code and result payload through REST,
gRPC, MQTT and manual ACK replay. The first active-command result updates command
state and ACK/audit rows in one PostgreSQL transaction. Duplicate results compare
using JSONB equality; conflicting results return 409. Existing terminal states
retain their guards and are not overwritten by late results. The response's
optional result is distinct from the command input payload and internal dispatch
bookkeeping. Validate and bound result data before storage or replay.

## Consequences

- Clients, gateway, core, worker and Demo require a coordinated upgrade. MQTT
  ACK senders must include `status`; no received/executed intermediate state is
  inferred from an unspecified device protocol.
- Run the embedded idempotent migration before DML-only application startup.
  Existing rows have no fabricated result or inferred historical failure reason.
- Pagination is not a snapshot of mutable command statuses. Cursors bind query
  scope, but are not security tokens; SQL tenant predicates remain mandatory.
- Product-specific command validation still requires an explicit product model.
  This change enforces object shape without inventing device-specific schemas.
- Historical metadata is not rewritten. New names and product identifiers have
  business limits; page-budget tests inject explicit historical rows to retain
  coverage of large existing data.
- Current deployed versions and validation evidence remain separately recorded
  in the root handover. Publishing a Git tag does not deploy these route changes.
