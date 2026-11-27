# IoT Platform Context

## Release 3.1

The [architecture](docs/images/architecture.svg) and [command lifecycle](docs/images/command-cycle.svg) show the device and operations paths. The optional `ops-agent` is an independent Helm Deployment and ClusterIP service. A command diagnosis combines tenant-authorized command state, worker scrape health and sampled command spans; model advice is optional. Grafana can also send service alerts to the agent. A dedicated PostgreSQL queue drives time-window evidence collection and a supplementary DingTalk notification. Grafana sends the original notification independently. Prometheus, Tempo and inference run outside the application release.

## Domain Terms

- Tenant: 逻辑租户，是设备、遥测、命令和 topic 的隔离维度。
- Device: 租户下的设备，用 `tenantId` + `deviceId` 唯一标识。
- Telemetry: 设备上行遥测，先进入 MQTT，再进入 Kafka，最终写入业务状态和 TDengine。
- Command: 平台下行命令，先写入 PostgreSQL 状态，再通过 Kafka/device-worker 投递到 MQTT。
- ACK: 设备对命令的执行结果（acked/failed、errorCode、payload），状态、结果和审计原子持久化，运行时 topic 为 `tenant/{tenantId}/device/{deviceId}/ack`。
- Envelope: MQTT 上行消息的 JSON 外壳，包含 `msgId`、`tenantId`、`deviceId`、`ts`、`type`、`version` 和 `payload`。
- IoT Core: 核心业务 gRPC 服务，负责租户、设备、遥测、命令和 ACK 的业务行为。
- Management API: 对外 REST 网关，通过固定 gRPC endpoint 调用 IoT Core；在 Kubernetes 内使用 `iot-core` Service DNS。
- Telemetry Ingestor: MQTT telemetry 接入模块，负责解析 Envelope 并写入 Kafka telemetry topic。
- Device Worker: Kafka 消费和下行处理模块，负责遥测落库、TDengine 写入、命令投递和 ACK 更新。
- Demo: 本地造流模拟器，负责创建多租户多设备拓扑、发布 telemetry、创建 command 并回 ACK。

## Runtime Terms

- External dependencies mode: 默认 Helm 部署模式，应用 Pod 连接 Docker Desktop 或外部 PostgreSQL、Kafka、EMQX、TDengine。
- Application release: Helm release 中的 `management-api`、`iot-core`、`telemetry-ingestor`、`device-worker` 业务服务。

## Module Ownership

- `internal/contracts`: 外部契约和领域约束，包括 topic、Envelope、命令状态机、REST 响应形状（`CommandResponse`）和 MQTT Schema。
- `internal/core`: IoT Core 服务实现，承载核心业务用例。
- `internal/opsagent`: 租户鉴权、固定只读诊断工具、证据汇总、可选模型建议，以及 Grafana 告警队列和钉钉补充通知。
- `internal/adminapi`: Management API 的 REST 网关，负责 HTTP 到 IoT Core 的协议转换。
- `internal/platform`: 运行时适配和平台能力，包含仓储、消息、指标、device-worker、MQTT bridge、TDengine writer 等实现。
- `internal/bootstrap`: `telemetry-ingestor` 和 `device-worker` 的运行时装配。
- `internal/demo`: Demo 造流运行时和模拟业务。
