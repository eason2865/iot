# IoT Platform

English | [简体中文](README.zh-CN.md)

<p align="center">
  <img src="docs/images/architecture.svg" alt="IoT platform architecture" />
</p>

<p align="center">
  <img alt="Go version" src="https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white" />
  <img alt="License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" />
</p>

An IoT platform skeleton built with go-zero + gRPC + protobuf + EMQX + Kafka + TDengine + PostgreSQL, covering device connectivity, telemetry ingestion, command delivery, status queries, and multi-tenant management.

This is open-source infrastructure with the key IoT loops already wired end to end. Highlights:

- `iot-core`: the core business microservice
- `management-api`: a go-zero REST gateway exposing the unified external API
- `iot-core` exposes core business over gRPC + protobuf; `management-api` reaches it through a fixed gRPC endpoint
- `telemetry-ingestor`: MQTT ingestion, normalization, and event decoupling
- `device-worker`: time-series persistence, business state updates, and command delivery
- `demo`: multi-tenant, multi-device traffic generation with ACK receipts, ideal for integration testing and load testing

## At a Glance

<p align="center">
  <img src="docs/images/command-cycle.svg" alt="IoT platform command lifecycle" />
</p>

## Features

- Device connectivity path: MQTT -> EMQX -> Go `telemetry-ingestor`
- Service split: `management-api` as the REST gateway, `iot-core` for core business
- Service calls: `management-api` reaches the core via the `iot-core:9001` Service DNS inside Kubernetes
- Async decoupling: telemetry, commands, and events all flow through Kafka
- Dual storage: TDengine for time-series data, PostgreSQL for business metadata and current state
- Command loop: create, dispatch, ACK, state-machine updates
- Reliable command delivery: database lease claiming, publish confirmation, timeout sweeping, UUIDv7 command IDs, ACK event auditing
- Failed messages land in `iot.dlq` and can be reviewed and replayed in batches via `dlq-replay`
- EMQX authenticates device bcrypt secrets via an `iot-core` HTTP callback and issues tenant-scoped MQTT ACLs
- The management API requires a Bearer Token on all endpoints except health checks and contract endpoints
- TDengine uses per-device subtables with tenant/device tags; full long payloads live in PostgreSQL JSONB, while the time-series store keeps only hashes and index fields
- Multi-tenant isolation: `tenantId` flows through topics, messages, storage, and queries
- Standard contracts: OpenAPI, MQTT JSON Schema, gRPC proto, and database migration scripts
- Locally runnable: connects to local Docker PostgreSQL / Kafka / EMQX / TDengine by default

## Current Implementation

- 6 runnable entrypoints: `cmd/management-api`, `cmd/iot-core`, `cmd/demo`, `cmd/telemetry-ingestor`, `cmd/device-worker`, `cmd/dlq-replay`
- `management-api` uses go-zero REST; `iot-core` uses gRPC + protobuf
- Local Docker orchestration no longer includes a business service-discovery component
- 1 PostgreSQL initialization migration: `migrations/001_init.sql`
- 1 OpenAPI definition: `docs/openapi.json`
- 1 MQTT message schema: `docs/mqtt-envelope.schema.json`
- Basic tests included; `go test ./...` runs directly

## Observability

- Every service exposes `/metrics`
- `management-api` and `iot-core` enable go-zero trace / log middleware
- Each service starts an OpenTelemetry trace agent, writing to `/tmp/<service>-traces.log` by default
- HTTP requests automatically get an `X-Request-Id`, propagated through the `management-api -> iot-core` gRPC call
- `demo` requests to `management-api` carry a request id and trace context, making load-test/integration chains easy to follow
- stdout logs are structured JSON, easy to search in containers and locally
- Tracing can be tuned via environment variables:
  - `OTEL_DISABLED=true`
  - `OTEL_BATCHER=file|jaeger|zipkin|otlpgrpc|otlphttp`
  - `OTEL_ENDPOINT=/tmp/iot-traces.log`
  - `OTEL_SAMPLER=1.0`

## Quick Start

### 1. Start dependencies

This project targets a local Docker environment by default. You need:

- PostgreSQL
- Kafka
- EMQX
- TDengine

### 2. Configure environment variables

Start from the example file:

```bash
cp .env.example .env
set -a
. ./.env
set +a
```

Default connection settings:

```bash
export POSTGRES_DSN=postgres://iot:iot123@localhost:5432/iot?sslmode=disable
export KAFKA_BROKERS=localhost:9092
export EMQX_URL=tcp://127.0.0.1:1883
export TDENGINE_DSN=root:taosdata@http(127.0.0.1:6041)/iot
export IOT_CORE_ENDPOINTS=127.0.0.1:9001
export IOT_CORE_LISTEN_ON=:9001
```

Optional topic and client-identity settings:

```bash
export KAFKA_TELEMETRY_TOPIC=iot.telemetry
export KAFKA_COMMAND_TOPIC=iot.command
export EMQX_TOPIC_FILTER=tenant/+/device/+/telemetry
export EMQX_TELEMETRY_INGESTOR_CLIENT_ID=iot-telemetry-ingestor
export EMQX_DEVICE_WORKER_CLIENT_ID=iot-device-worker
export TDENGINE_TABLE=telemetry_v2
export KAFKA_DLQ_TOPIC=iot.dlq
export MANAGEMENT_API_TOKEN=change-me
export EMQX_INTERNAL_PASSWORD=change-me
```

The listen address defaults to `:8080` and can be overridden:

```bash
export PORT=8081
# or
export LISTEN_ADDR=:8081
```

### 3. Start the services

```bash
go run ./cmd/iot-core
go run ./cmd/management-api
go run ./cmd/demo
go run ./cmd/telemetry-ingestor
go run ./cmd/device-worker
```

Common development commands:

```bash
make fmt-check
make test
make build
```

## API

Health check and contract files:

- `GET /healthz`
- `GET /openapi.json`
- `GET /schemas/mqtt-envelope.json`

All management API requests except the health-check and contract endpoints above must carry `Authorization: Bearer <MANAGEMENT_API_TOKEN>`.

Core business endpoints:

- `POST /api/v1/tenants`
- `GET /api/v1/tenants`
- `POST /api/v1/devices`
- `GET /api/v1/devices`
- `GET /api/v1/devices/{tenantId}/{deviceId}`

List endpoints accept `pageSize` and an opaque `cursor`, returning `{ "items": [], "nextCursor": "" }`; the server uses keyset pagination with a maximum of 100 items per page.
- `GET /api/v1/devices/{tenantId}/{deviceId}/status`
- `GET /api/v1/devices/{tenantId}/{deviceId}/telemetry`
- `POST /api/v1/telemetry`
- `POST /api/v1/commands`
- `GET /api/v1/commands?tenantId=<tenant-id>`
- `GET /api/v1/commands/{id}`
- `POST /api/v1/commands/{id}/ack`

The command list requires `tenantId`; the server filters by tenant at both the gRPC and PostgreSQL query layers, while `pageSize` and the opaque `cursor` paginate within the tenant.

See [docs/openapi.json](docs/openapi.json) for the full API definition.

## Architecture at a Glance

To quickly understand the current version, read in this order:

1. Devices connect to EMQX over MQTT
2. `telemetry-ingestor` normalizes messages and decouples events
3. `management-api` calls `iot-core` over gRPC
4. `management-api` reaches the core via `iot-core:9001`
5. `device-worker` consumes Kafka and persists time-series data and state

The design principle is to keep one clear core boundary first, then split further as business pressure demands — rather than fragmenting upfront into many small services that are hard to troubleshoot.

## Demo Simulator

`cmd/demo` is a standalone simulator that automatically:

- Creates a multi-tenant, multi-device topology from configuration
- Publishes random telemetry to MQTT
- Creates random command requests against `management-api`
- Subscribes to each tenant's command topic and replies with ACKs

Common environment variables:

```bash
export DEMO_MANAGEMENT_API_URL=http://127.0.0.1:8080
export DEMO_MQTT_URL=tcp://127.0.0.1:1883
export DEMO_TENANT_COUNT=5
export DEMO_DEVICES_PER_TENANT=10
export DEMO_TENANT_PREFIX=demo
export DEMO_PRODUCT_ID=product-demo
export DEMO_TICK_INTERVAL=250ms
export DEMO_TELEMETRY_BURST_MIN=1
export DEMO_TELEMETRY_BURST_MAX=5
export DEMO_COMMAND_BURST_MIN=1
export DEMO_COMMAND_BURST_MAX=3
```

It also exposes `GET /healthz` for K8s readiness/liveness probes.

## Topic Conventions

Use the following prefix consistently:

```text
tenant/{tenantId}/device/{deviceId}/...
```

Common topics:

- Uplink telemetry: `tenant/{tenantId}/device/{deviceId}/telemetry`
- Downlink command: `tenant/{tenantId}/device/{deviceId}/command`
- ACK receipt: `tenant/{tenantId}/device/{deviceId}/ack`

## Message Model

The platform uses a JSON envelope by default, which simplifies device integration, log troubleshooting, and protocol evolution.

Required fields:

- `msgId`
- `tenantId`
- `deviceId`
- `ts`
- `type`
- `version`
- `payload`

Recommended fields:

- `traceId`
- `productId`
- `region`
- `seq`
- `schemaVersion`

Example:

```json
{
  "msgId": "b7a0d8c8-4f8b-4b1e-9d7d-3ad4d7fe1d2a",
  "tenantId": "t1",
  "deviceId": "d1",
  "ts": 1717670000000,
  "type": "telemetry",
  "version": "v1",
  "traceId": "trace-001",
  "payload": {
    "temp": 23.4,
    "humi": 60.1
  }
}
```

See [docs/mqtt-envelope.schema.json](docs/mqtt-envelope.schema.json) for the full schema.

## Project Structure

```text
iot/
├── cmd/
│   ├── management-api/      # Query and management API
│   ├── iot-core/            # Core business gRPC service
│   ├── demo/                # Simulator generating random traffic and ACKs
│   ├── telemetry-ingestor/  # MQTT ingestion and event decoupling
│   └── device-worker/       # Kafka consumption, persistence, and downlink handling
├── internal/
│   ├── adminapi/   # REST gateway translating HTTP to iot-core calls
│   ├── bootstrap/  # Startup assembly
│   ├── contracts/  # Topic, envelope, state machine, OpenAPI, and schema contracts
│   ├── core/       # Core business gRPC service implementation
│   ├── demo/       # Traffic simulator runtime
│   ├── platform/   # Repositories, messaging, metrics, device-worker, MQTT/TDengine adapters
│   └── server/     # HTTP infrastructure
├── charts/iot/     # Helm deployment manifests
├── migrations/     # Database migrations
├── proto/          # iot-core protobuf contracts
├── monitoring/     # Local Prometheus / Grafana configuration
└── docs/           # OpenAPI, schemas, production deployment guide, and ADRs
```

## Documentation

Docs are layered by purpose to avoid maintaining duplicate architecture descriptions:

- [Production Deployment Guide](docs/production-deployment.md): production topology, deployment boundaries, long connections, scaling, and disaster-recovery baseline
- [EMQX long-connection cluster manifests](deploy/emqx/README.md): EMQX Operator, local single-node and production multi-node configurations
- [OpenAPI definition](docs/openapi.json): machine contract for the management API
- [MQTT Envelope Schema](docs/mqtt-envelope.schema.json): machine contract for MQTT messages
- [Architecture Decision Records](docs/adr/): key architecture decisions, without duplicating full design docs
- [Initial migration](migrations/001_init.sql): PostgreSQL initial schema

## Local Helm + Docker Deployment

The recommended local setup is:

- Docker: PostgreSQL / Kafka / TDengine / Prometheus / Grafana / demo, plus forwarders that reach Kubernetes services
- Kubernetes: `management-api` / `iot-core` / `telemetry-ingestor` / `device-worker`, plus an EMQX cluster in a separate `emqx` namespace

Prometheus and Grafana are the observability layer for the whole IoT pipeline, but locally they deliberately run as standalone Docker Compose services rather than as part of the business Helm release. They scrape the four business services in Kubernetes through `k8s-forward-*` containers, so monitoring configuration and history survive business redeployments.

Both local and production manage the EMQX cluster via the EMQX Operator in a dedicated `emqx` namespace. Production uses the licensed multi-node manifest [`deploy/emqx/cluster.yaml`](deploy/emqx/cluster.yaml); local uses the single-node manifest [`deploy/emqx/cluster.local.yaml`](deploy/emqx/cluster.local.yaml), which runs under the community license. Local Compose only port-forwards MQTT `1883` and Dashboard `18083` to that cluster; in production, MQTT should be exposed via an L4 load balancer with TLS, and the Dashboard should stay on the private network.

All local IoT dependencies in Docker Desktop are grouped under the Compose project `iot`. Native services keep their original names: `postgres`, `kafka`, `tdengine`, `prometheus`, `grafana`; project-custom containers use the `iot-` prefix, e.g. `iot-demo` and `iot-k8s-forward-*`. EMQX runs in the Kubernetes `emqx` namespace.

First make sure the local Docker dependencies are running, and that Kafka advertises listeners reachable from both host-side tests and k8s Pods:

```bash
docker rm -f kafka 2>/dev/null || true
docker run -d --name kafka \
  -p 9092:9092 \
  -p 29092:29092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_LISTENERS=HOST://:9092,DOCKER://:29092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=HOST://localhost:9092,DOCKER://192.168.65.254:29092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,DOCKER:PLAINTEXT \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=HOST \
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_CFG_DELETE_TOPIC_ENABLE=true \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnamilegacy/kafka:latest

docker inspect kafka --format '{{range .Config.Env}}{{println .}}{{end}}' | grep KAFKA_CFG_ADVERTISED_LISTENERS
# Expected: KAFKA_CFG_ADVERTISED_LISTENERS=HOST://localhost:9092,DOCKER://192.168.65.254:29092
```

Host-side Go E2E tests use `localhost:9092`; k8s Pods reach Docker Kafka via `192.168.65.254:29092`. If Kafka advertises only a single listener, clients receive broker metadata pointing to an address unreachable from the other side, surfacing as Kafka produce/consume timeouts in `iot-core` / `telemetry-ingestor` / `device-worker`. Docker Desktop uses `192.168.65.254` by default as the gateway for k8s Pods to reach Docker dependencies.

Start local EMQX first, then install the business services:

```bash
kubectl apply -f deploy/emqx/cluster.local.yaml
kubectl wait --for=condition=Ready emqx/emqx -n emqx --timeout=10m
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
kubectl rollout status deploy/iot-core -n iot
kubectl rollout status deploy/management-api -n iot
kubectl rollout status deploy/telemetry-ingestor -n iot
kubectl rollout status deploy/device-worker -n iot
```

Or use the repo script to run dependency connectivity checks, the Helm install, and rollout verification in one shot:

```bash
scripts/helm-deploy-local.sh
```

The script enforces an apps-only deployment: it installs only `management-api`, `iot-core`, `telemetry-ingestor`, `device-worker`, and their shared configuration — not PostgreSQL, Kafka, EMQX, TDengine, Prometheus, Grafana, or demo. Create the local EMQX first as described above.
`iot-core` is the gRPC core dependency of `management-api`; the script waits for all four services to become ready.
On Docker Desktop Kubernetes, the script generates a temporary immutable `iot-app:local-<image-id>` tag from the image ID, imports it into the `desktop-control-plane` containerd, and passes it to Helm — preventing a rebuilt fixed tag from being shadowed by a stale image under k8s `IfNotPresent`. The temporary tag is removed after the import; locally you only need to keep `iot-app:2.0`.

The Helm chart defines only the four business services: PostgreSQL, Kafka, and TDengine are reached through the Docker Desktop gateway IP, while EMQX is reached via Kubernetes Service DNS. Containers accessing host ports still use `host.docker.internal`, e.g. Prometheus scraping metrics behind k8s port-forwards.

Create the channels for local Prometheus and demo to reach the k8s business services:

```bash
scripts/port-forward-local-monitoring.sh
```

Start all local Docker dependencies, monitoring, and demo:

```bash
docker compose -f monitoring/docker-compose.yml up -d
```

Verify:

```bash
curl http://127.0.0.1:18080/healthz
curl http://127.0.0.1:18090/metrics
curl http://127.0.0.1:18084/healthz
curl 'http://127.0.0.1:9090/api/v1/targets?state=active'
docker exec iot-grafana wget -qO- 'http://prometheus:9090/api/v1/query?query=up'
```

## Local Monitoring

Both Prometheus and Grafana run locally in Docker. Prometheus scrapes the k8s business services' `/metrics` through the `iot-k8s-forward-*` containers on the Docker network; the Grafana data source is preconfigured to the Compose-internal address `http://prometheus:9090`.
Local monitoring now covers `management-api / iot-core / telemetry-ingestor / device-worker / demo`, with `iot-core` on its dedicated gRPC metrics port `9101`.

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace
scripts/port-forward-local-monitoring.sh
docker compose -f monitoring/docker-compose.yml up -d
```

Grafana default account:

- URL: http://localhost:3000
- Fixed IoT folder URL: http://localhost:3000/dashboards/f/efobmswzeefi8d/
- Grafana data lives in the named Docker volume `iot-grafana-data`; the container uses the `unless-stopped` restart policy. Recreating the container preserves login settings and the database — do not delete this volume.
- User: `admin`
- Password: `admin`
- Available dashboards: `IoT Overview`, `IoT Management API`, `IoT Pipeline`, `IoT Core`

Pre-provisioned dashboards:

- [IoT Overview](http://localhost:3000/d/iot-overview/iot-overview)
- [IoT Management API](http://localhost:3000/d/iot-management-api/iot-management-api)
- [IoT Pipeline](http://localhost:3000/d/iot-pipeline/iot-pipeline)

### Management API Alerting Visualization

The top of `IoT Management API` shows related alerts with instance labels; the HTTP request panel highlights the 5xx series in red and marks the firing/resolution times of the linked rule. The marks correspond to alert evaluation state changes, not the exact time of any single request.

The dashboard top provides a rule entrypoint with a fixed UID; the Alert list's built-in data source is named `-- Grafana --`, which differs from the rule-source identifier `grafana` used in the API.

The rule template is `monitoring/grafana/alerts/management-api-http-5xx.json`: it computes the 5-minute average HTTP 5xx QPS by `route/status` and fires when `> 0` for 1 minute. It falls back to 0 when no 5xx series exists; the rule does not detect service outages. The 5-minute window also means the alert does not resolve immediately after the last error.

The template is imported via the Grafana API rather than read-only file provisioning, so thresholds, pause state, and notification channels remain editable in the UI after import. On a fresh environment, first create a contact point named "钉钉" (DingTalk) (or modify the receiver in the template); webhook credentials live only in Grafana and never enter the repo. First-time import example:

```bash
curl --fail-with-body -u "admin:${GRAFANA_ADMIN_PASSWORD}" \
  -H 'Content-Type: application/json' -H 'X-Disable-Provenance: true' \
  --data-binary @monitoring/grafana/alerts/management-api-http-5xx.json \
  http://localhost:3000/api/v1/provisioning/alert-rules
```

To update an existing rule, use `PUT /api/v1/provisioning/alert-rules/iot-management-api-http-5xx` instead. The template never overwrites edits made in the Grafana UI. A paused rule produces no new firing marks; historical marks are recorded only from the moment the panel association is created — earlier events are not backfilled.

There is also `monitoring/grafana/alerts/management-api-healthz-qps.json`: it only watches the `/healthz`, `2xx` series, using the same 5-minute average QPS, and fires on the next evaluation when strictly `> 0.3 req/s` (`for: 0s`, notification `group_wait: 0s`). The local `Management API HTTP` group evaluates every 60 seconds; notifications use the existing "钉钉" (DingTalk) contact point. The panel shows an orange dashed threshold, the rule links to the same HTTP panel, and it does not change the 5xx rule. For first-time import reuse the POST command above with the new filename; for updates use UID `iot-management-api-healthz-qps`. The health-check rate itself hovers near 0.3, so sampling jitter may cause repeated firing/resolution.

### DingTalk Notification Template

`monitoring/grafana/notifications/dingtalk.tmpl` defines `iot.dingtalk.title`, `iot.dingtalk.message`, and `iot.dingtalk.payload`, stored locally in Grafana's `iot.dingtalk` template group. Contact-point references live in `dingtalk-contact.json`; that snippet excludes the URL and must not be applied over a complete contact point.

- The title contains the fixed keyword `grafana`, the firing/resolved state, the rule name, and the alert count.
- The contact point is still named "钉钉" (DingTalk), uses the Webhook integration type, and POSTs DingTalk Markdown JSON directly to the original bot URL via a Custom Payload — the native DingDing whole-card-jump ActionCard (whose singleURL always jumps to the alert list) is not used. No forwarding service is needed.
- The body shows, per instance, severity, service, route, HTTP status, summary, details, and firing/resolved times in UTC+8; optional fields without values are omitted. The JSON is encoded via `data.ToJSON` rather than manual string concatenation, so quotes and newlines are handled correctly.
- Links to the chart, dashboard, rule, runbook, and a temporary silence are rendered per rule, with at most 10 instances per message. Links inherit Grafana's external URL — currently localhost; other devices need a reachable address configured separately.
- `disableResolveMessage: false` enables resolved notifications; this template normalization does not change rule thresholds, evaluation intervals, grouping, or repeat frequency.
- Deployment order: save the template first via `PUT /api/v1/provisioning/templates/iot.dingtalk` (`X-Disable-Provenance: true` preserves UI editability), then apply the contact-point snippet. The webhook URL is a protected configuration: restrict contact-point read permissions; do not print it, export it into the repo, or commit bot tokens.
- Prefer editing the template in the notification template group. An already-open contact-point edit page must be refreshed before saving, to avoid the stale form overwriting the template reference.

After setting `GRAFANA_ADMIN_PASSWORD`, run `python3 scripts/test-grafana-notification-template.py -v` to verify firing, resolution, missing fields, multi-instance truncation, Markdown JSON, and the four standalone links via the local Grafana template-preview API — no DingTalk messages are sent. Use `GRAFANA_URL` and `GRAFANA_USER` to target another instance. Historical DingTalk cards do not update with the template, so verify on new messages; a contact-point test notification may lack the rule's GeneratorURL and therefore omit "View rule" — only real rule notifications include that link.

## Helm Deployment

The repo ships a Helm chart: [`charts/iot`](charts/iot)

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
```

One-shot local script:

```bash
scripts/helm-deploy-local.sh
```

By default the script deploys only the applications:

- `management-api`
- `iot-core`
- `telemetry-ingestor`
- `device-worker`

By default the script first checks from inside a k8s Pod that the external PostgreSQL, Kafka, EMQX, and TDengine ports are reachable. If the target environment uses cloud services or CI where this check is unnecessary, disable it:

```bash
CHECK_EXTERNAL_DEPS=0 scripts/helm-deploy-local.sh
```

The current Helm deployment contains only the applications and their shared configuration. PostgreSQL, Kafka, TDengine, Prometheus, Grafana, and demo are managed by independent platforms or local Docker Compose; EMQX is managed by a separate Operator release. None of them ship in the business Helm release, and there are no built-in dependency templates that can be re-enabled.

### iot-core gRPC mTLS

The `iot-core` gRPC port (9001) supports mutual TLS: when enabled, the server requires client certificates, and only callers holding the `management-api` client certificate can connect — no longer relying solely on NetworkPolicy for namespace isolation.

- Driven by environment variables: `IOT_CORE_TLS_CERT` / `IOT_CORE_TLS_KEY` / `IOT_CORE_TLS_CA` (same on server and client); the client can additionally override the verification name with `IOT_CORE_TLS_SERVER_NAME`. When all three are unset the connection stays plaintext (compatible with bare local runs); setting only some of them makes the process fail at startup.
- Enable in Helm via `grpcTLS.enabled=true`; the Secret (default `iot-grpc-tls`) must contain `ca.crt`, `server.crt`, `server.key`, `client.crt`, `client.key`, mounted at `/etc/iot/grpc-tls` in both Deployments.
- The local script `scripts/helm-deploy-local.sh` enables mTLS by default: it automatically calls `scripts/gen-grpc-certs.sh` to generate a local self-signed CA plus server/client certificates (output to `deploy/grpc-certs/`, gitignored) and creates the Secret. Disable with `GRPC_TLS_ENABLED=0`.
- In production, use certificates issued by a real CA or cert-manager, and note that Pods must be restarted for rotated certificates to take effect.

## Development Guidelines

- `tenantId` must flow through every write and query path
- Kafka consumers must be designed idempotently
- TDengine holds time-series data; PostgreSQL holds business metadata and state
- Keep the command state machine as `pending -> dispatched -> sent -> acked / timeout / failed`
- Keep logical multi-tenant isolation; avoid premature sharding
- The demo simulator currently runs as an external traffic generator and is not part of the business Helm release

## Contributing

Issues and Pull Requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before getting started.

We recommend running before submitting:

```bash
go test ./...
```

For security issues, see [SECURITY.md](SECURITY.md); do not disclose vulnerability details in public Issues.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
