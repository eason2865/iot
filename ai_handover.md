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
- MQTT topic：`tenant/{tenantId}/device/{deviceId}/{telemetry|command|ack}`。
- MQTT 设备用户名：`tenantId:deviceId`；设备密码在 PostgreSQL 中保存为 bcrypt 哈希。
- MQTT 服务账号：`iot-service`，由 `EMQX_INTERNAL_PASSWORD` 注入。
- Kafka topics：`iot.telemetry`、`iot.command`、`iot.dlq`。
- TDengine：超级表 `telemetry_v2`，按设备建立子表；完整 payload 的权威副本为 PostgreSQL JSONB。
- 命令列表：`GET /api/v1/commands?tenantId=<tenant-id>` 必须指定租户，分页使用 `pageSize` 和 opaque `cursor`。
- 生产环境禁止使用 `latest`，使用不可变镜像版本或 digest。

## 本地运行

```bash
docker compose -f monitoring/docker-compose.yml up -d
kubectl apply -f deploy/emqx/cluster.local.yaml
scripts/helm-deploy-local.sh
```

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

## 文档入口

- [README](README.md)
- [生产部署指南](docs/生产部署指南.md)
- [EMQX 部署清单](deploy/emqx/README.md)
- [OpenAPI](docs/openapi.json)
- [MQTT Schema](docs/mqtt-envelope.schema.json)
- [架构 ADR](docs/adr/)
