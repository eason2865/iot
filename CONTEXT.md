# IoT Platform Context

## Domain Terms

- Tenant: 逻辑租户，是设备、遥测、命令和 topic 的隔离维度。
- Device: 租户下的设备，用 `tenantId` + `deviceId` 唯一标识。
- Telemetry: 设备上行遥测，先进入 MQTT，再进入 Kafka，最终写入业务状态和 TDengine。
- Command: 平台下行命令，先写入 PostgreSQL 状态，再通过 Kafka/worker 投递到 MQTT。
- ACK: 设备对命令的回执，运行时 topic 为 `tenant/{tenantId}/device/{deviceId}/ack`。
- Envelope: MQTT 上行消息的 JSON 外壳，包含 `msgId`、`tenantId`、`deviceId`、`ts`、`type`、`version` 和 `payload`。
- Core RPC: 核心业务 gRPC 服务，负责租户、设备、遥测、命令和 ACK 的业务行为。
- Admin API: 对外 REST 网关，通过 etcd 发现并调用 Core RPC。
- Ingress: MQTT telemetry 接入模块，负责解析 Envelope 并写入 Kafka telemetry topic。
- Worker: Kafka 消费和下行处理模块，负责遥测落库、TDengine 写入、命令投递和 ACK 更新。
- Demo: 本地造流模拟器，负责创建多租户多设备拓扑、发布 telemetry、创建 command 并回 ACK。

## Runtime Terms

- External dependencies mode: 默认 Helm 部署模式，应用 Pod 连接 Docker Desktop 或外部 PostgreSQL、Kafka、EMQX、TDengine、etcd。
- Local stack mode: `charts/iot/values-local-stack.yaml` 模式，在 Kubernetes 内同时部署应用和依赖。
- Application release: Helm release 中的 `admin`、`core-rpc`、`ingress`、`worker` 业务服务。

## Module Ownership

- `internal/contracts`: 外部契约和领域约束，包括 topic、Envelope、命令状态机、OpenAPI 和 MQTT Schema。
- `internal/core`: Core RPC 服务实现，承载核心业务用例。
- `internal/adminapi`: REST 网关，负责 HTTP 到 Core RPC 的协议转换。
- `internal/platform`: 运行时适配和旧内存 HTTP app，包含仓储、消息、指标、worker、MQTT bridge、TDengine writer 等实现。
- `internal/bootstrap`: `ingress` 和 `worker` 的运行时装配。
- `internal/demo`: Demo 造流运行时和模拟业务。
