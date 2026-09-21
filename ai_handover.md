# IoT 当前交接信息

> 本文件只记录当前有效架构、运行状态、验证结果和操作入口。历史迁移过程不在这里保留。

## 当前架构

- Kubernetes namespace `iot`：`management-api`、`iot-core`、`telemetry-ingestor`、`device-worker`。
- Kubernetes namespace `emqx`：EMQX Operator 管理的 EMQX 集群；本地为单 Core 节点，生产使用独立多节点清单。
- Docker Compose：PostgreSQL、Kafka、TDengine、Prometheus、Grafana、demo 和 Kubernetes port-forward 容器。
- 业务同步调用使用 Kubernetes Service DNS：`management-api -> iot-core:9001`。
- 设备链路：MQTT -> EMQX -> `telemetry-ingestor` -> Kafka -> `device-worker` -> PostgreSQL/TDengine。
- 命令链路：`management-api` -> `iot-core` -> Kafka -> `device-worker` -> MQTT -> 设备 ACK -> PostgreSQL。
- 业务 Helm release 只部署四个业务服务和共享配置，不部署数据、消息、EMQX、监控或 demo。

## 当前服务职责

- `management-api`：REST API、Bearer Token 鉴权、分页和协议转换。
- `iot-core`：租户、设备、遥测、命令、状态和 MQTT 认证回调的核心 gRPC 服务。
- `telemetry-ingestor`：通过 EMQX 共享订阅接收遥测并发布 Kafka 事件。
- `device-worker`：消费遥测和命令事件，写入 PostgreSQL/TDengine，执行 MQTT 下发和 ACK 更新。
- `demo`：仅用于本地造流和联调，不进入业务 Helm release。

## 关键配置

- Go：`go 1.27.0`，toolchain `go1.27.1`。
- MQTT topic：`tenant/{tenantId}/device/{deviceId}/{telemetry|command|ack}`；消息体里的 tenantId/deviceId 必须与真实 topic 一致，否则进 DLQ（身份伪造防护）。
- MQTT 设备用户名：`tenantId:deviceId`；设备密码在 PostgreSQL 中保存为 bcrypt 哈希。
- MQTT 服务账号：`iot-service`，由 `EMQX_INTERNAL_PASSWORD` 注入。
- MQTT 认证回调：`iot-core:9090/internal/mqtt/authenticate`，fail-closed——必须配置 `IOT_CORE_MQTT_AUTH_TOKEN`，EMQX 通过 `X-Iot-Auth-Token` 头携带；本地脚本在 `iot` 和 `emqx` 两个 namespace 各建一份同名 Secret；ACL 只授予 `$share` 共享订阅。
- MQTT 订阅默认值：`telemetry-ingestor` 用 `$share/iot-telemetry/...`，`device-worker` 用 `$share/iot-device-worker/...`（多副本负载均衡）。
- NetworkPolicy：`iot-core-mqtt-auth` 限制 9090 仅 `emqx` namespace 可达，9001/9101 仅本 namespace。
- Kafka topics：`iot.telemetry`、`iot.command`、`iot.dlq`。
- TDengine：超级表 `telemetry_v2`，按设备建立子表；完整 payload 的权威副本为 PostgreSQL JSONB。`telemetry_records.tdengine_written` 记录 TDengine 完成状态，DLQ 重放只补偿未完成的 TDengine 写，PG/TDengine 均幂等。
- HTTP 遥测幂等重发：`IngestTelemetry` 无论首次写入还是命中唯一约束（重复），都会发布 Kafka——因为上一次可能 PG 写成功但 Kafka 失败（broker 故障），若重复分支跳过 Kafka，该数据将永远不进 worker/TDengine。重复消费由 worker 的 `tdengine_written` 补偿兜住，重发安全。
- 命令状态机：`created → published（Kafka 写入）→ sent（MQTT 下发成功，启动 ACK deadline）→ acked/timeout/failed`；`published` 状态无 deadline。**竞态处理**：dispatcher 先发 Kafka 再 `MarkCommandPublished`，worker 可能在 published 落库前完成 MQTT 下发——`MarkCommandSent` 因此接受 `status IN ('created','published')`，保证命令必到 `sent` 且有 deadline；随后 dispatcher 的 `MarkCommandPublished`（要求 `created`）命中 0 行，不会把 `sent` 降级。
- 命令列表：`GET /api/v1/commands?tenantId=<tenant-id>` 必须指定租户，分页使用 `pageSize` 和 opaque `cursor`。
- 列表分页：`/api/v1/tenants`、`/api/v1/devices`、`/api/v1/devices/{t}/{d}/telemetry`、`/api/v1/commands` 均为 keyset 分页（`pageSize` 1-100，响应 `{items, nextCursor}`）。**注意**：`NormalizePageRequest(size, cursor)` 只返回 Size/Cursor，会丢弃 TenantID/DeviceID——带过滤器的分页方法必须先保存过滤器再恢复（参照 `ListCommandsPage`/`ListTelemetryPage` 的 `page.TenantID = tenantID` 模式），否则查询条件丢失返回空。
- REST 写接口仅 `management-api` 暴露；`telemetry-ingestor`/`device-worker` 只提供 `/healthz` 和 `/metrics`。
- 生产环境禁止使用 `latest`，使用不可变镜像版本或 digest。

## 本地运行

```bash
docker compose -f monitoring/docker-compose.yml up -d
scripts/helm-deploy-local.sh   # 先跑：在 iot 和 emqx 两个 namespace 各建 iot-runtime-secrets
sh deploy/emqx/render-local.sh # 再跑：envsubst 渲染 token 后 apply EMQX CR
```

**EMQX token 渲染（关键）**：EMQX 认证器 headers 里的 `${...}` 是**运行时模板占位符**（只允许 username/clientid/password 等连接变量），**不做环境变量展开**。所以 `x-iot-auth-token` 不能写 `${IOT_CORE_MQTT_AUTH_TOKEN}` 让 EMQX 读 env——必须部署前用 `envsubst` 把字面值渲染进 manifest。`render-local.sh` 从 `emqx/iot-runtime-secrets` 读 token 渲染 `cluster.local.yaml`。生产 `cluster.yaml` 同理需渲染后再 apply。

**EMQX 单节点滚动更新**：本地单节点 license 不允许滚动更新时瞬时双 core（报 `SINGLE_NODE_LICENSE` 崩溃）。改 EMQX CR 后需 `kubectl delete sts -n emqx --all` 让 Operator 重建单节点。

一键脚本默认检查 PostgreSQL、Kafka、EMQX 和 TDengine 的可达性，然后只部署 `iot` namespace 中的业务服务。监控端口转发由 Compose 中的 `iot-k8s-forward-*` 容器维护。

常用地址：

- Management API：`http://localhost:18080`
- Demo：`http://localhost:18084`
- Prometheus：`http://localhost:9090`
- Grafana：`http://localhost:3000`
- EMQX Dashboard：`http://localhost:18083`

## 当前本地资源

- Host Docker 镜像：仅保留业务镜像 `iot-app:2.0`，供 Compose demo 使用。
- Kubernetes containerd：仅保留当前业务镜像 `iot-app:local-030599ad5a35`。
- 当前 Helm release：`iot`，namespace `iot`。
- 当前 EMQX release：`emqx`，namespace `emqx`。
- Grafana 数据卷：`iot-grafana-data`。
- PostgreSQL 数据卷：`iot-postgres-data`。
- Kafka 数据卷：`iot-kafka-data`。
- TDengine 数据卷：`iot-tdengine-data`、`iot-tdengine-log`、`iot-tdengine-corefile`。

## 验证基线

已通过：

- `go test ./...`
- `helm lint charts/iot`
- Docker Compose 配置解析和 Helm apps-only 部署
- 真实 E2E：MQTT 鉴权、遥测上报、Kafka 消费、TDengine 入库、命令下发、设备 ACK、PostgreSQL 状态更新
- 管理 API 鉴权：无 Token 返回 `401`，合法 Token 返回 `200`
- 命令租户隔离：缺少 `tenantId` 返回 `400`，指定租户只返回该租户命令
- Kubernetes 业务 Pod 和 EMQX Pod Ready
- Prometheus 业务 targets 全部 `up`

## 维护规则

- 业务 Kubernetes 变更只修改 `charts/iot`。
- 生产部署不使用 `latest`、明文密码或仓库内 Webhook token。
- 新增 REST 字段时同步更新 protobuf、`docs/openapi.json`、`internal/contracts/docs.go` 和 README。
- 修改代码或 SQL 后更新本文件，执行测试、部署验证、提交并推送。
- 本次加固（2026-09-20）：MQTT 认证回调加共享密钥头、ACL 覆盖共享/普通订阅、DLQ 重放遥测幂等（基于 `telemetry_records` 唯一约束）、`migrations/001_init.sql` 与 `ensureSchema` 对齐、TDengine 默认表名统一为 `telemetry_v2`。已通过 `go build ./...`、`go test -count=1 ./...`、`make fmt-check`、`make build`。

## 文档入口

- [README](README.md)
- [生产部署指南](docs/生产部署指南.md)
- [EMQX 部署清单](deploy/emqx/README.md)
- [OpenAPI](docs/openapi.json)
- [MQTT Schema](docs/mqtt-envelope.schema.json)
- [架构 ADR](docs/adr/)
