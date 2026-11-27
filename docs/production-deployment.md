# IoT Platform Production Deployment Guide

English | [简体中文](production-deployment.zh-CN.md)

## Target Architecture

Production is deployed by responsibility; not all components live in a single business Helm release:

- `iot` namespace: the four stateless business services and the optional read-only ops-agent.
- `emqx` namespace: the device MQTT long-connection cluster.
- `observability` namespace: Prometheus, Grafana, a Tempo query/OTLP endpoint for command tracing, and an optionally enabled Alertmanager.
- Data and messaging platforms: PostgreSQL, Kafka, and TDengine are managed by independent platform teams or by their respective Operators, and must not be tied to the business services' lifecycle.

`charts/iot` deploys the four business services and, when enabled, ops-agent in the `iot` namespace. The cluster control plane's etcd is managed by the Kubernetes platform; this project no longer deploys a business etcd. `management-api` calls `iot-core` through the Kubernetes Service DNS `iot-core:9001`.

## Production-to-Local Mapping

Local is not a production substitute, but it mirrors production deployment boundaries: capabilities that would be purchased or managed are simulated with single Docker instances, while capabilities that would run on self-managed Kubernetes are actually deployed to local Kubernetes. This validates networking, service discovery, credential injection, and release boundaries early — without faking high availability or multi-AZ behavior.

| Capability | Production recommendation | Local equivalent |
| --- | --- | --- |
| Kubernetes control plane | Managed Kubernetes; the platform owns control-plane etcd | Docker Desktop Kubernetes; no business etcd deployed |
| Business services | The `iot` namespace in Kubernetes | Also deployed to the `iot` namespace in local Kubernetes |
| EMQX long connections | The `emqx` namespace in Kubernetes, cluster managed by the EMQX Operator | Same Operator, namespace, PVCs, and Service DNS; limited to a single Core node by the community license |
| PostgreSQL, Kafka, TDengine | Prefer managed data/messaging/time-series services | Docker Compose containers simulating private managed endpoints |
| Cross-cluster registry and config | Managed registry/config center, enabled as needed | Single cluster today, no registry; integrate once the cross-cluster design is settled |
| Metrics, dashboards, alerting | Prefer managed observability services | Prometheus and Grafana in Docker Compose; configuration managed via Git/Provisioning |
| Public API and MQTT entrypoints | Managed L4/L7 load balancers, gateways, and TLS | `kubectl port-forward` to local ports; not equivalent to a public load balancer |

## Cross-Cluster Principles

- Synchronous calls within a cluster always use Kubernetes Service DNS; no business etcd or registry is introduced.
- Cross-cluster traffic first goes through private connectivity, global traffic management, and gateways, then uses a managed registry/config center for dynamic backends as needed.
- A registry stores service instances and configuration; it does not carry device MQTT long connections. Devices always reach EMQX through an L4 load balancer.
- For multi-region, prefer an independent Kubernetes cluster per region with local service discovery, rather than concentrating all business calls on a single cross-region registry.

## Service Inventory and Recommendations

| Service | Responsibility | Recommended location | Recommended approach |
| --- | --- | --- | --- |
| API Gateway / Ingress | TLS termination, authentication, rate limiting, public API entrypoint | Platform gateway namespace | A highly available Gateway or Ingress Controller deployed uniformly by the platform; expose only `management-api` as the HTTP API. The current chart does not install a gateway. |
| `ops-agent` | Tenant-scoped command diagnosis | `iot`, optional | Separate Deployment, ClusterIP and credentials. Optionally connects to an inference endpoint. |
| Tempo | Ingest and query sampled traces | Observability platform | Expose OTLP ingestion and HTTP query endpoints; size retention for the site. |
| `management-api` | REST API, auth boundary, calls the core service | `iot` | Kubernetes Deployment; in production at least 2 replicas with HPA, PDB, rolling updates, and topology spread. Calls the core via the `iot-core:9001` Service DNS. |
| `iot-core` | Core gRPC business: tenants, devices, commands, status | `iot` | Kubernetes Deployment; in production at least 2 replicas with HPA, PDB, rolling updates, and topology spread. Expose gRPC and metrics ports via ClusterIP Services only. |
| `telemetry-ingestor` | Parses MQTT uplink messages and writes to Kafka | `iot` | Kubernetes Deployment. Scales via EMQX shared subscriptions with a unique MQTT Client ID per Pod; the current chart defaults to 2 replicas with both already configured. The chart has a PDB plus optional HPA and topology spread; enable the production overlay and tune through load tests. |
| `device-worker` | Kafka consumption, state/time-series persistence, command delivery, ACK updates | `iot` | Kubernetes Deployment. Scales via Kafka partitions and consumer groups; the ACK MQTT subscription uses a shared subscription with the Client ID uniquified by Pod name. The chart defaults to 2 replicas; the worker performs a terminal-state precheck before delivery (`sent/acked/timeout/failed` are skipped and the offset committed), and failed ACK persistence is retried then routed to the DLQ, reducing duplicate deliveries (devices must still deduplicate command IDs). Further scaling still requires more Kafka partitions (`kafka.topicPartitions`, applied only at topic creation). |
| EMQX | MQTT access, sessions, subscriptions, device long connections | `emqx` | A standalone EMQX cluster managed by the EMQX Operator; at minimum multi-node with anti-affinity, PDB, rolling upgrades, and TCP load balancing. Device long connections enter EMQX through an L4 load balancer, not business Pods. EMQX 5.9+ clusters require a license Secret. See the repo manifest [`deploy/emqx/cluster.yaml`](../deploy/emqx/cluster.yaml). |
| PostgreSQL | Transactional data: tenants, devices, commands, current state | Independent data platform | Prefer managed HA PostgreSQL; when self-hosting, use a supported PostgreSQL Operator with primary/standby, backups, PITR, monitoring, and regular restore drills. |
| Kafka | Async event streams: telemetry, commands | Independent messaging platform | Prefer managed Kafka; when self-hosting, use a Kafka Operator with at least 3 brokers across failure domains, replication factor, idempotent producers, and consumer-lag monitoring. Business consumers must remain idempotent. |
| TDengine | Telemetry time-series writes and queries | Independent time-series platform | Use a managed service or a standalone TDengine cluster; when self-hosting, use a supported Operator/Helm with persistent volumes, backups, pinned versions, and restore drills. Must not share a lifecycle with `device-worker`. |
| Prometheus | Metrics scraping, rule evaluation, short-term metrics storage | Prefer managed observability | Prefer a managed metrics service; for self-hosting, use the Prometheus Operator in the `observability` namespace with ServiceMonitor/PodMonitor, persistent volumes, alert rules, and retention sizing. |
| Grafana | Dashboards, alert rules, contact points, notification policies | Prefer managed observability | Prefer managed dashboard/alerting services; for self-hosting, deploy independently of the business release and manage data sources, dashboards, and alert templates via Provisioning/Git. For HA, use an external shared database — never the in-container SQLite. |
| Alertmanager | Prometheus alert dedup, grouping, silencing, routing | `observability`, as needed | Deploy as an HA cluster when alert rules are evaluated centrally by Prometheus, routed by team, on-call, and escalation policy. This project's alerting is currently managed by Grafana Alerting — do not run two notification pipelines for the same rule. |
| `demo` | Local traffic generation, integration testing, load testing | Not in production | Run only in development, staging, or a dedicated load-test environment with dedicated tenants and rate limiting; never connect it to production MQTT, Kafka, or databases. |

## Service Discovery and Connection Principles

- Synchronous calls within the same Kubernetes cluster and business domain use Service DNS, e.g. `iot-core.iot.svc.cluster.local:9001`; no additional business etcd is needed for this.
- Service DNS only provides discovery and network load balancing; it cannot replace strongly consistent configuration, leases, leader election, or distributed locks. Such cross-cluster or coordination needs require a dedicated registry/coordination system.
- PostgreSQL, Kafka, TDengine, and EMQX are all reached through stable internal DNS names or private network entrypoints; connection credentials are managed as Secrets and must never be baked into images, chart defaults, or the repository.

## Production Go-Live Baseline

1. Business services are configured with readiness/liveness/startup probes, resource requests/limits, PDBs, HPAs, and topology spread across nodes/AZs.
2. All images use immutable versions or digests — never `latest` in production; releases go through CI builds, image scanning, and Helm render validation.
3. EMQX, PostgreSQL, Kafka, and TDengine each get capacity, availability, latency, error-rate, and backup/restore monitoring; scaling, eviction, and upgrades of device TCP long connections are rehearsed with a connection-migration strategy.
4. Prometheus handles metrics collection and Grafana handles dashboards and the current alerting pipeline; notification channels are configured per team, on-call, and escalation policy, with regular tests of firing and resolved messages.
5. Regularly run restore drills for PostgreSQL, TDengine, and Grafana data; design Kafka replay around retention policies and consumer offsets.

## Security, Reliability, and Data-Model Baselines

- MQTT authentication uniformly calls back into `iot-core`'s internal auth endpoint; the device username is `tenantId:deviceId`, passwords appear only at registration time, and the database stores bcrypt hashes. EMQX runs with `authorization.no_match=deny`, and precise tenant/device ACLs are issued by the auth response. In production, additionally protect the callback endpoint with NetworkPolicy, service-identity authentication, and TLS.
- `management-api` exposes only `/healthz`, `/readyz`, and the MQTT schema without a token (kubelet probes carry no credentials); all business REST requests require a Bearer Token injected via a Secret Manager — never baked into images or Git. Optionally, `MANAGEMENT_API_TOKENS` binds tokens to specific tenants so a leaked tenant token cannot reach other tenants' data.
- Command creation first writes the `created` state to PostgreSQL; `iot-core` replicas claim leases via `FOR UPDATE SKIP LOCKED`, transition to `published` after a successful Kafka publish, then to `sent` (with a deadline) once the worker delivers to the device over MQTT, to `timeout` if the deadline passes without an ACK, and ACKs write `command_ack` and `command_events`. Command IDs use UUIDv7 for sortability and cross-replica uniqueness.
- Kafka messages that fail JSON decoding, PostgreSQL, TDengine, or MQTT delivery are first written to `iot.dlq`; the original consumer offset is committed only after success. After manually inspecting `stage/error`, replay with `dlq-replay --limit N`; never auto-retry indefinitely and create poison-message loops.
- Tenant and command lists use keyset cursor pagination with a page-size cap of 100; never pull entire tables through the REST layer.
- TDengine uses the `telemetry_v2` super table with per-device subtables, `tenant_id/device_id` as tags, storing `msg_id/type/version/payload_hash/payload_bytes`; the authoritative copy of the full payload is PostgreSQL JSONB, avoiding the 4096-character limit and time-series string truncation.
- The current source and build baseline is Go `1.27.1`, with `go.mod` pinned to `go 1.27.0` and `toolchain go1.27.1`; production image builds should install the same toolchain in CI and lock dependency checksums.

## Current Chart vs. Production

The current `charts/iot` is a locally runnable, applications-only chart that by default deploys all four services — `management-api`, `iot-core`, `telemetry-ingestor`, and `device-worker` — at 2 replicas each, with a PDB (`minAvailable: 1`) per service, connecting to external dependencies. It does not include the gateway, EMQX, PostgreSQL, Kafka, TDengine, Prometheus, Grafana, Alertmanager, or demo.

Before going to production, build these platform services separately per this guide, enable HPA/topology spread through the production overlay, and configure TLS/Secrets, capacity baselines, and disaster-recovery drills (the chart already ships PDBs for all four business services). Scaling `device-worker` additionally requires Kafka partition planning (`kafka.topicPartitions`).

## Implemented reliability and monitoring baseline

All four application Deployments now have a PDB. Gateway readiness checks the
core/SQL path; ingestor and worker readiness checks PostgreSQL, while liveness
remains separate. Local Prometheus discovers and scrapes all Pods through a
namespace-scoped apiserver proxy role with CA verification, retaining service job
labels. Production should use direct Pod scraping through its observability stack.

Optional tenant tokens are a JSON token-to-tenant map in the runtime Secret.
Global lists require the operator token. Kafka partition count is configurable
for newly created topics; live partition/replica changes remain operator actions,
while managed dynamic config entries are reconciled at startup. Command publishing
skips delivered/terminal states; bounded ACK retries and manual ACK replay preserve
existing terminal semantics. Telemetry compensation uses the PostgreSQL completion
marker. See ADR 0007 for duplicate delivery and simultaneous-outage limitations.

## Production overlay and database releases

Overlay `charts/iot/values-production.yaml` after replacing its internal DNS,
image and Secrets. It enables mTLS, four HPAs, node/zone spreading and startup
probes. New Kafka topics use 6 partitions, RF=3 and min ISR=2. Provide
metrics-server and sufficient fault domains; existing topic partitions/replicas
need separate operator changes. Spreading is a soft ScheduleAnyway constraint.

Run `/usr/local/bin/migrate` with POSTGRES_DSN injected for a migration role,
then release application services with `postgres.autoMigrate=false` and a
DML-only role. Both paths use the same embedded SQL and serialize migration in a
transaction. Follow the data platform's backup policy before migrating.

Use Idempotency-Key for command retries. MQTT telemetry acknowledges after a
PostgreSQL inbox write. ADR 0008 describes guarantees and broker offline-delivery
boundaries. TDengine measurement columns, timestamp collisions, lifecycle and
external HA capacity remain business planning decisions.

For actual `command.ack` DLQ replay with a DML-only account, inject
`POSTGRES_AUTO_MIGRATE=false` together with `POSTGRES_DSN`. The CLI defaults to
automatic migration for local compatibility. MQTT ACK consumers now confirm
valid ACKs only after PostgreSQL or DLQ persistence; simultaneous failures leave
them unacknowledged and fail the worker. Configure EMQX session/shared-delivery
policy for redelivery after disconnect or replacement; application manual ACK
alone does not supply broker persistence.

## Operations agent 3.0

Build the current application image with `make build && docker build -t iot-app:3.1 .`. Push to a registry accessible to the cluster and set `images.app`. The agent has a separate ClusterIP service on port 8084. The four business deployments can operate independently.

1. Generate separate operator and command-reader credentials for each tenant. Add `MANAGEMENT_COMMAND_READ_TOKENS` with value `{"<read-token>":"factory"}` to the management-api runtime Secret and roll management-api. The local deployment script accepts `IOT_MANAGEMENT_COMMAND_READ_TOKENS`. A reader can only GET an individual command in its tenant.
2. Create a dedicated Secret named `iot-ops-agent` in the release namespace. Set `OPS_TENANTS` to `{"factory":{"operatorToken":"<operator-token>","managementToken":"<read-token>"}}`. Add `OPS_MODEL_KEY` if the model requires authentication. Roll the consuming Deployments after rotating credentials.
3. Merge these values into the release configuration. Use addresses reachable from the pods. The model URL is the base of a Chat Completions compatible API, typically ending in `/v1`. The Prometheus, Tempo and model integrations can each be enabled independently.

```yaml
opsAgent:
  enabled: true
  existingSecret: iot-ops-agent
  prometheusURL: http://prometheus.observability:9090
  tempoURL: http://tempo.observability:3200
  modelURL: http://inference.models:8000/v1
  model: your-installed-model
tracing:
  batcher: otlpgrpc
  endpoint: tempo.observability:4317
  sampler: 0.1
```

Deploy with the existing release values, or run `IOT_VALUES_FILE=/path/to/ops-values.yaml scripts/helm-deploy-local.sh` locally. Roll the business pods after a tracing configuration change. Use sampler 1 temporarily for verification. Export and indexing take time; persisted status is available even when trace search is empty. Grafana can use the same query URL as a Tempo data source. The local Compose profile `tracing` has one Tempo instance with 24-hour retention. When reaching it from pods, choose a routable address on a trusted network and set `TEMPO_BIND_IP` explicitly if necessary.

The model receives the operator’s question as written, the verified finding, stage evidence and scrape health. Keep credentials out of questions. Model output is advisory. The agent disables Kubernetes service account token mounting. Use internal TLS or a service mesh for credential-bearing traffic, and trusted internal endpoints or an authenticating internal proxy for Prometheus and Tempo.

## Automatic Grafana alert analysis

3.1 adds an optional second webhook integration to the existing DingTalk contact point. Grafana delivers the direct notification independently. The agent posts a later evidence summary for firing and recovery.

1. Install [the queue schema](../deploy/ops-agent/alert-queue.sql) as the owner of a dedicated PostgreSQL database. Give the runtime role CONNECT, schema USAGE and SELECT, INSERT, UPDATE, DELETE on `ops_alert_jobs`. It needs no business database access.
2. Add `OPS_ALERT_DSN`, `OPS_GRAFANA_TOKEN` (dedicated bearer credential), and `OPS_DINGTALK_URL` (complete robot webhook URL) to the agent Secret. Add `OPS_DINGTALK_SECRET` for a signed robot. If keyword filtering is active, allow “IoT 告警补充分析”. Roll the agent after rotating credentials.
3. Enable `opsAgent.enabled` and `opsAgent.alerts.enabled`, configure `opsAgent.prometheusURL`, and optionally configure Tempo and a model. An alert-only deployment can omit `OPS_TENANTS`.
4. Add a Webhook integration to the existing Grafana contact point. Keep the original robot integration. Use POST, the default Grafana payload, Bearer authentication, and send resolved notifications. In-cluster URL: `http://ops-agent.iot.svc:8084/api/v1/webhooks/grafana`. For local Compose, run `docker compose -f monitoring/docker-compose.yml --profile ops-alerts up -d k8s-forward-ops-agent`. The [contact point example](../monitoring/grafana/notifications/ops-agent-contact.json) uses its internal address. Supply `settings.authorization_credentials` securely. Use TLS across trust boundaries. Set Max Alerts to zero and group alerts into batches of at most 128.
5. Exercise the shipped healthz QPS rule with test traffic. Verify the direct notification and a separately labeled supplement. A generic Grafana TestAlert is ignored. The four shipped rule UIDs are recognized via `rule_uid`; existing rules with their original titles also work.

The agent queries five-minute Prometheus rates and up to five sampled error traces for the service at firing or recovery time. Missing observations remain distinct from a measured zero. Each replica shares the durable queue. Events are deduplicated by rule, fingerprint, start time and status for seven days after completion. Delivery retries reuse the saved summary, up to six attempts. An accepted message whose acknowledgement was lost may be delivered again. Inspect `finished_at IS NOT NULL AND NOT delivered` for exhausted jobs.
