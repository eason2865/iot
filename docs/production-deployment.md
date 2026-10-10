# IoT Platform Production Deployment Guide

English | [简体中文](production-deployment.zh-CN.md)

## Target Architecture

Production is deployed by responsibility; not all components live in a single business Helm release:

- `iot` namespace: the four stateless business services.
- `emqx` namespace: the device MQTT long-connection cluster.
- `observability` namespace: Prometheus, Grafana, and an optionally enabled Alertmanager.
- Data and messaging platforms: PostgreSQL, Kafka, and TDengine are managed by independent platform teams or by their respective Operators, and must not be tied to the business services' lifecycle.

`charts/iot` deploys only the four business services in the `iot` namespace. The cluster control plane's etcd is managed by the Kubernetes platform; this project no longer deploys a business etcd. `management-api` calls `iot-core` through the Kubernetes Service DNS `iot-core:9001`.

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
| `management-api` | REST API, auth boundary, calls the core service | `iot` | Kubernetes Deployment; in production at least 2 replicas with HPA, PDB, rolling updates, and topology spread. Calls the core via the `iot-core:9001` Service DNS. |
| `iot-core` | Core gRPC business: tenants, devices, commands, status | `iot` | Kubernetes Deployment; in production at least 2 replicas with HPA, PDB, rolling updates, and topology spread. Expose gRPC and metrics ports via ClusterIP Services only. |
| `telemetry-ingestor` | Parses MQTT uplink messages and writes to Kafka | `iot` | Kubernetes Deployment. Scales via EMQX shared subscriptions with a unique MQTT Client ID per Pod; the current chart defaults to 2 replicas with both already configured. The chart already has a PDB; production should still add HPA and topology spread. |
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

Before going to production, build these platform services separately per this guide, and fill in HPAs, topology spread, TLS/Secrets, capacity baselines, and disaster-recovery drills (the chart already ships PDBs for all four business services). Scaling `device-worker` additionally requires Kafka partition planning (`kafka.topicPartitions`).

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
