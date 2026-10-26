# Management API contract (2.56)

This release intentionally replaces the old device/command/telemetry routes.
Upgrade callers and Demo together with the application. There are no legacy aliases.

## Resources and identity

All requests use `Authorization: Bearer <token>` except `/healthz`, `/readyz` and
`/schemas/mqtt-envelope.json`. A tenant token must match the tenant in the path;
the global administration token can access every tenant. Path identities are
parsed once for authorization and reused by the handlers. `tenantId` is never
accepted in a query or write body. For an existing device, `deviceId` is also
in the path; creating a device takes its new `deviceId` in the body.

| Method | Path | Body / query |
| --- | --- | --- |
| POST | `/api/v1/tenants` | `{ "id": "t", "name": "Factory" }`; global token |
| GET | `/api/v1/tenants` | `pageSize`, `cursor`; global token |
| POST | `/api/v1/tenants/{tenantId}/devices` | `{ "deviceId": "d", "productId": "p", "secret": "..." }` |
| GET | `/api/v1/tenants/{tenantId}/devices` | `pageSize`, `cursor`; tenant-scoped |
| GET | `/api/v1/tenants/{tenantId}/devices/{deviceId}` | None |
| GET | `/api/v1/tenants/{tenantId}/devices/{deviceId}/status` | None |
| POST | `/api/v1/tenants/{tenantId}/devices/{deviceId}/telemetry` | `msgId`, `ts`, `type`, `version`, object `payload` |
| GET | `/api/v1/tenants/{tenantId}/devices/{deviceId}/telemetry` | `pageSize`, `cursor`, `startTime`, `endTime`, `type`, `sort` |
| POST | `/api/v1/tenants/{tenantId}/devices/{deviceId}/commands` | `{ "payload": { "switch": "on" } }`; optional `Idempotency-Key` header |
| GET | `/api/v1/tenants/{tenantId}/commands` | `pageSize`, `cursor`, `deviceId`, `status`, `startTime`, `endTime`, `sort` |
| GET | `/api/v1/tenants/{tenantId}/devices/{deviceId}/commands` | Same filters except `deviceId`, which is fixed by the path |
| GET | `/api/v1/tenants/{tenantId}/commands/{commandId}` | None |
| POST | `/api/v1/tenants/{tenantId}/devices/{deviceId}/commands/{commandId}/acknowledgements` | Execution result described below |

## Input rules

- Bodies contain exactly one JSON object. Unknown fields, duplicate keys,
  duplicate case variants of top-level DTO keys, invalid UTF-8, unpaired Unicode
  surrogates, NUL and JSONB-incompatible numbers are rejected. JSON nesting is
  capped at 128 levels for REST and command/result objects. Payload object keys
  remain case-sensitive. HTTP bodies are limited to 1 MiB.
- Unknown query keys, repeated query keys, empty explicit values, malformed
  percent encoding and repeated `Idempotency-Key` headers return 400.
- Tenant/device identifiers keep the MQTT identity constraints: 1–64 UTF-8
  bytes, no control characters, leading/trailing whitespace or `/#+`; tenant
  identifiers additionally forbid `:`. Invalid or missing gRPC identities are
  `InvalidArgument` as well.
- Tenant `name` is 1–256 UTF-8 bytes, cannot be blank or contain control
  characters. `productId` matches `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.
  Existing oversized metadata remains readable under the existing page budget;
  it is not rewritten or deleted.
- Telemetry `payload` must be an object. Command `payload` must be an object
  when supplied; omission explicitly means `{}`. `null`, arrays and scalars
  are rejected. Product-specific argument semantics still belong to the device
  protocol; this release does not introduce an inferred product schema.
- Command creation returns 201; telemetry acceptance returns 202. Request keys
  retain tenant-scoped JSONB equality and conflict semantics. Creation does not
  imply device execution has succeeded.

## Filtering and cursors

- `pageSize` is optional (default 50). An explicit value must be an integer
  between 1 and 100; `0` and empty strings are errors. gRPC's omitted scalar
  page size is still represented by 0 and uses the default.
- `startTime` / `endTime` are RFC3339 timestamps with an explicit offset and at
  most millisecond precision, for example `2026-10-26T08:00:00.123+08:00`.
  URL-encode `+` as `%2B` in query strings. Bounds are `[startTime, endTime)`;
  when both exist, start must be strictly before end.
- Telemetry filters and sorting use device event time `ts`, not receive time.
  Command filters and sorting use `createdAt`. `sort=desc` is the default;
  `asc` is supported. Timestamp and message/command ID form the stable keyset.
  Devices use ascending device ID; tenants use ascending tenant ID.
- List responses are `{ "items": [], "nextCursor": "..." }`, limited by both
  count and the existing 2 MiB response budget. Use only `nextCursor` to decide
  whether another page exists. An absent or empty next cursor means completion.
- Telemetry/command cursors bind resource type, tenant, device, time bounds,
  filter values and sort direction. Keep these parameters unchanged between
  pages; a cursor from another scope returns 400. Device cursors bind tenant.
  Cursors are opaque pagination positions, not authorization credentials or
  snapshot isolation. Changing command status during a traversal changes the
  current matching set.
- Command `status` accepts `created`, `published`, `sent`, `acked`, `failed` and
  `timeout`. Unknown values are rejected. `type` follows telemetry field bounds.

## Execution results

REST acknowledgement body:

```json
{
  "status": "failed",
  "errorCode": "MOTOR_BLOCKED",
  "payload": { "reason": "jammed" }
}
```

- `status` is required and must be `acked` (device-reported success) or `failed`
  (device-reported failure). A successful result cannot carry `errorCode`.
  A failure may omit the error code; supplied codes are at most 64 UTF-8 bytes,
  without controls or leading/trailing whitespace.
- Result `payload` is an object, default `{}`, with both raw and expanded JSONB
  budget of 64 KiB. It is distinct from the command's input `payload`.
- Command state, `commands.result`, `command_ack` and the audit event commit
  atomically. Command responses and list items expose optional `result` with
  `status`, `errorCode` and `payload`.
- Repeating the same result is idempotent and returns 200 with no additional
  ACK or audit event. A different result after a recorded result returns 409.
  Existing terminal timeout/dispatch failure or historical success without a
  recorded result remains terminal; a late ACK does not overwrite it.
- MQTT ACKs retain `commandId`, `tenantId`, `deviceId` matching the source topic,
  and require the same `status`/`errorCode`/`payload` fields. Invalid results are
  rejected and confirmed without applying business state. Valid results receive
  MQTT confirmation only after PostgreSQL or DLQ persistence. DLQ replay retains
  the full result and applies it directly, without another device action.
- Execution remains at least once: devices must deduplicate command IDs. A
  device-reported `acked` result is not independent proof of physical success.

## Upgrade

Run the migration binary with a migration role before starting 2.56 with
`POSTGRES_AUTO_MIGRATE=false`. The embedded `001_init.sql` idempotently adds
nullable `commands.result` and indexes for the event-time/command list queries.
There is no historical result backfill. Update HTTP callers and MQTT ACK senders,
then deploy gateway, core, ingestor, worker and Demo together. Keep the previous
release and its client versions available for a coordinated rollback.
