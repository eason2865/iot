# IoT Platform

<p align="center">
  <img src="docs/images/architecture.svg" alt="IoT platform architecture" />
</p>

<p align="center">
  <img alt="Go version" src="https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white" />
  <img alt="License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" />
</p>

Go-zero + gRPC + protobuf + EMQX + Kafka + TDengine + PostgreSQL 的物联网平台骨架，面向设备接入、遥测采集、命令下发、状态查询和多租户管理。

这是一套已经把物联网关键闭环打通的开源基础设施，重点能力包括：

- 核心业务微服务 `iot-core`
- `management-api` 作为 go-zero REST 网关，统一对外提供 API
- `iot-core` 通过 gRPC + protobuf 暴露核心业务，`management-api` 通过固定 gRPC endpoint 访问
- `telemetry-ingestor` 负责 MQTT 接入、标准化和事件解耦
- `device-worker` 负责时序落库、业务状态更新和命令投递
- `demo` 负责多租户多设备造流和 ACK 回执，适合联调和压测

## 一图看懂

<p align="center">
  <img src="docs/images/command-cycle.svg" alt="IoT platform command lifecycle" />
</p>

## 核心特性

- 设备接入链路：MQTT -> EMQX -> Go `telemetry-ingestor`
- 核心服务拆分：`management-api` 负责 REST 网关，`iot-core` 负责核心业务
- 服务调用：`management-api` 在 Kubernetes 内通过 `iot-core:9001` Service DNS 访问核心服务
- 本地改名部署：新的 `iot-device-worker` Kafka 消费组从最新 offset 开始，避免重放迁移前已完成的命令和遥测；后续重启继续使用已提交的 offset。
- 异步解耦：遥测、命令和事件统一进入 Kafka
- 双存储分工：TDengine 保存时序数据，PostgreSQL 保存业务元数据和当前态
- 命令闭环：创建、下发、ACK、状态机更新
- 命令可靠投递：数据库领取租约、发布确认、超时扫描、UUIDv7 命令 ID、ACK 事件审计
- 失败消息进入 `iot.dlq`，可通过 `dlq-replay` 按批次审核后重放
- EMQX 通过 `iot-core` HTTP 回调认证设备 bcrypt 密钥并下发租户级 MQTT ACL
- 管理 API 除健康检查和契约端点外均要求 Bearer Token
- TDengine 使用设备子表 + 租户/设备 Tags；完整长载荷保存在 PostgreSQL JSONB，时序库仅存哈希和索引字段
- 多租户隔离：`tenantId` 贯穿 topic、消息、存储和查询
- 标准契约：提供 OpenAPI、MQTT JSON Schema、gRPC proto 和数据库迁移脚本
- 本地可运行：默认可以连接本机 Docker 的 PostgreSQL / Kafka / EMQX / TDengine

## 当前实现

- 6 个可启动入口：`cmd/management-api`、`cmd/iot-core`、`cmd/demo`、`cmd/telemetry-ingestor`、`cmd/device-worker`、`cmd/dlq-replay`
- `management-api` 使用 go-zero REST，`iot-core` 使用 gRPC + protobuf
- 本地 Docker 编排不再包含业务服务发现组件
- 1 份 PostgreSQL 初始化迁移：`migrations/001_init.sql`
- 1 份 OpenAPI 定义：`docs/openapi.json`
- 1 份 MQTT 消息 Schema：`docs/mqtt-envelope.schema.json`
- 已包含基础测试，`go test ./...` 可直接运行

## 观测

- 每个服务都暴露 `/metrics`
- `management-api` 和 `iot-core` 已启用 go-zero 的 trace / log middleware
- 服务启动时会开启 OpenTelemetry trace agent，默认写到 `/tmp/<service>-traces.log`
- HTTP 请求会自动补 `X-Request-Id`，并透传到 `management-api -> iot-core` 的 gRPC 调用
- `demo` 发往 `management-api` 的请求会带上 request id 和 trace 上下文，便于串联压测/联调链路
- 标准输出日志已切换为结构化 JSON，便于在容器和本地直接检索
- 可通过以下环境变量调整 tracing：
  - `OTEL_DISABLED=true`
  - `OTEL_BATCHER=file|jaeger|zipkin|otlpgrpc|otlphttp`
  - `OTEL_ENDPOINT=/tmp/iot-traces.log`
  - `OTEL_SAMPLER=1.0`

## 快速开始

### 1. 启动依赖

本项目默认面向本机 Docker 环境。你需要先准备：

- PostgreSQL
- Kafka
- EMQX
- TDengine

### 2. 配置环境变量

可以从示例文件开始：

```bash
cp .env.example .env
set -a
. ./.env
set +a
```

默认连接配置如下：

```bash
export POSTGRES_DSN=postgres://iot:iot123@localhost:5432/iot?sslmode=disable
export KAFKA_BROKERS=localhost:9092
export EMQX_URL=tcp://127.0.0.1:1883
export TDENGINE_DSN=root:taosdata@http(127.0.0.1:6041)/iot
export IOT_CORE_ENDPOINTS=127.0.0.1:9001
export IOT_CORE_LISTEN_ON=:9001
```

可选的 topic 和客户端标识配置：

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

监听地址默认是 `:8080`，也可以通过以下变量覆盖：

```bash
export PORT=8081
# 或者
export LISTEN_ADDR=:8081
```

### 3. 启动服务

```bash
go run ./cmd/iot-core
go run ./cmd/management-api
go run ./cmd/demo
go run ./cmd/telemetry-ingestor
go run ./cmd/device-worker
```

常用开发命令：

```bash
make fmt-check
make test
make build
```

## API

健康检查与契约文件：

- `GET /healthz`
- `GET /openapi.json`
- `GET /schemas/mqtt-envelope.json`

除上述健康检查和契约端点外，管理 API 请求必须携带 `Authorization: Bearer <MANAGEMENT_API_TOKEN>`。

核心业务接口：

- `POST /api/v1/tenants`
- `GET /api/v1/tenants`
- `POST /api/v1/devices`
- `GET /api/v1/devices`
- `GET /api/v1/devices/{tenantId}/{deviceId}`

列表接口支持 `pageSize` 和不透明 `cursor`，响应格式为 `{ "items": [], "nextCursor": "" }`；服务端使用 keyset pagination，单页最大 100 条。
- `GET /api/v1/devices/{tenantId}/{deviceId}/status`
- `GET /api/v1/devices/{tenantId}/{deviceId}/telemetry`
- `POST /api/v1/telemetry`
- `POST /api/v1/commands`
- `GET /api/v1/commands`
- `GET /api/v1/commands/{id}`
- `POST /api/v1/commands/{id}/ack`

完整接口定义请查看 [docs/openapi.json](docs/openapi.json)。

## 架构速览

如果你只想快速理解当前版本，按这个顺序看：

1. 设备通过 MQTT 进入 EMQX
2. `telemetry-ingestor` 负责标准化和事件解耦
3. `management-api` 通过 gRPC 调用 `iot-core`
4. `management-api` 通过 `iot-core:9001` 调用核心服务
5. `device-worker` 消费 Kafka，完成时序和状态落库

这套拆法的原则是先保留一个清晰的核心边界，再根据业务压力继续扩展，而不是一下拆成很多很难排障的小服务。

## Demo 模拟器

`cmd/demo` 是一个独立的模拟服务，启动后会自动：

- 按配置创建多租户和多设备拓扑
- 随机发布 telemetry 到 MQTT
- 随机向 `management-api` 创建 command 请求
- 订阅各租户 command topic，并自动回 ACK

常用环境变量：

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

启动后会额外暴露 `GET /healthz`，方便 K8s readiness/liveness 探针使用。

## Topic 约定

建议统一使用以下前缀：

```text
tenant/{tenantId}/device/{deviceId}/...
```

常用 Topic：

- 上行遥测：`tenant/{tenantId}/device/{deviceId}/telemetry`
- 下行命令：`tenant/{tenantId}/device/{deviceId}/command`
- ACK 回执：`tenant/{tenantId}/device/{deviceId}/ack`

## 消息模型

平台默认使用 JSON Envelope，便于设备联调、日志排查和协议演进。

必备字段：

- `msgId`
- `tenantId`
- `deviceId`
- `ts`
- `type`
- `version`
- `payload`

建议字段：

- `traceId`
- `productId`
- `region`
- `seq`
- `schemaVersion`

示例：

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

完整 Schema 请查看 [docs/mqtt-envelope.schema.json](docs/mqtt-envelope.schema.json)。

## 项目结构

```text
iot/
├── cmd/
│   ├── management-api/      # 查询与管理 API
│   ├── iot-core/            # 核心业务 gRPC 服务
│   ├── demo/       # 随机造流与 ACK 的模拟器
│   ├── telemetry-ingestor/  # MQTT 接入与事件解耦
│   └── device-worker/       # Kafka 消费、落库和下行处理
├── internal/
│   ├── adminapi/   # REST 网关，负责 HTTP 到 iot-core 的转换
│   ├── bootstrap/  # 启动装配
│   ├── contracts/  # topic、envelope、状态机、OpenAPI 和 Schema 契约
│   ├── core/       # 核心业务 gRPC 服务实现
│   ├── demo/       # 造流模拟器运行时
│   ├── platform/   # 仓储、消息、指标、device-worker、MQTT/TDengine 适配
│   └── server/     # HTTP 基础能力
├── charts/iot/     # Helm 部署清单
├── migrations/     # 数据库迁移
├── proto/          # iot-core protobuf 契约
├── monitoring/     # 本地 Prometheus / Grafana 配置
└── docs/           # OpenAPI、Schema、技术方案和 ADR
```

## 文档

- [整体技术方案](docs/物联网平台技术方案.html)
- [生产部署指南](docs/生产部署指南.md)
- [EMQX 长连接集群清单](deploy/emqx/README.md)
- [OpenAPI 定义](docs/openapi.json)
- [MQTT Envelope Schema](docs/mqtt-envelope.schema.json)
- [初始化迁移](migrations/001_init.sql)

## 本地 Helm + Docker 部署

当前推荐的本地形态是：

- Docker：PostgreSQL / Kafka / TDengine / Prometheus / Grafana / demo，以及访问 Kubernetes 服务的转发器
- Kubernetes：`management-api` / `iot-core` / `telemetry-ingestor` / `device-worker`，以及独立 `emqx` 命名空间中的 EMQX 集群

Prometheus 和 Grafana 是 IoT 全链路的观测层，但在本地刻意作为 Docker Compose 独立服务运行，而不是随业务 Helm release 发布。它们经由 `k8s-forward-*` 容器抓取 Kubernetes 中四个业务服务的指标；这样可以在重新部署业务服务时保留监控配置与历史数据。

本地与生产都通过 EMQX Operator 在独立 `emqx` 命名空间管理 EMQX 集群。生产使用持证多节点清单 [`deploy/emqx/cluster.yaml`](deploy/emqx/cluster.yaml)，本地使用社区许可可运行的单节点清单 [`deploy/emqx/cluster.local.yaml`](deploy/emqx/cluster.local.yaml)。本地 Compose 仅转发 MQTT `1883` 和 Dashboard `18083` 到该集群；生产环境应通过 L4 负载均衡和 TLS 暴露 MQTT，Dashboard 保持私网访问。

Docker Desktop 中所有本地 IoT 依赖均归入 Compose 项目 `iot`。原生服务使用原名：`postgres`、`kafka`、`tdengine`、`prometheus`、`grafana`；项目自定义容器采用 `iot-` 前缀，例如 `iot-demo` 与 `iot-k8s-forward-*`。EMQX 运行在 Kubernetes 的 `emqx` 命名空间。

先确认本机 Docker 依赖已经启动，并且 Kafka 同时给宿主机测试和 k8s Pod 暴露了各自可达的 advertised listener：

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
# 期望：KAFKA_CFG_ADVERTISED_LISTENERS=HOST://localhost:9092,DOCKER://192.168.65.254:29092
```

宿主机运行 Go E2E 时使用 `localhost:9092`，k8s Pod 访问 Docker Kafka 时使用 `192.168.65.254:29092`。如果 Kafka 只配置单个 advertised listener，客户端会在拿到 broker metadata 后被引导到另一侧不可达的地址，表现为 `iot-core` / `telemetry-ingestor` / `device-worker` Kafka 写入或消费超时。本地 Docker Desktop 默认使用 `192.168.65.254` 作为 k8s 访问 Docker 依赖的网关地址。

先启动本地 EMQX，再安装业务服务：

```bash
kubectl apply -f deploy/emqx/cluster.local.yaml
kubectl wait --for=condition=Ready emqx/emqx -n emqx --timeout=10m
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
kubectl rollout status deploy/iot-core -n iot
kubectl rollout status deploy/management-api -n iot
kubectl rollout status deploy/telemetry-ingestor -n iot
kubectl rollout status deploy/device-worker -n iot
```

也可以使用仓库脚本一键完成外部依赖连通性检查、Helm 安装和 rollout 验证：

```bash
scripts/helm-deploy-local.sh
```

该脚本会强制 apps-only 部署，只安装 `management-api`、`iot-core`、`telemetry-ingestor`、`device-worker` 以及它们共享的配置，不会安装 PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana 或 demo。运行前需要按上一步先创建本地 EMQX。
其中 `iot-core` 是 `management-api` 的 gRPC 核心依赖，脚本会等待四个服务全部就绪。
在 Docker Desktop Kubernetes 环境中，脚本会用镜像 ID 生成临时不可变 `iot-app:local-<image-id>` 标签，导入 `desktop-control-plane` 的 containerd 后再传给 Helm，避免固定 tag 重建后被 k8s `IfNotPresent` 复用旧镜像；导入完成即删除本机临时标签。本地镜像只需保留 `iot-app:2.0`。

Helm Chart 只定义四个业务服务：PostgreSQL、Kafka 与 TDengine 通过 Docker Desktop 网关 IP 连接，EMQX 则通过 Kubernetes Service DNS 连接。Docker 容器内访问宿主机端口时仍使用 `host.docker.internal`，例如 Prometheus 抓取 k8s port-forward 后的 metrics。

给本地 Prometheus 和 demo 建立访问 k8s 业务服务的通道：

```bash
scripts/port-forward-local-monitoring.sh
```

启动全部本地 Docker 依赖、监控和 demo：

```bash
docker compose -f monitoring/docker-compose.yml up -d
```

验证：

```bash
curl http://127.0.0.1:18080/healthz
curl http://127.0.0.1:18090/metrics
curl http://127.0.0.1:18084/healthz
curl 'http://127.0.0.1:9090/api/v1/targets?state=active'
docker exec iot-grafana wget -qO- 'http://prometheus:9090/api/v1/query?query=up'
```

## 本地监控

Prometheus 和 Grafana 都用 Docker 本地启动。Prometheus 通过 Docker 网络内的 `iot-k8s-forward-*` 容器抓取 k8s 业务服务的 `/metrics`，Grafana 数据源已经预置为 Compose 内部地址 `http://prometheus:9090`。
现在本地监控会同时覆盖 `management-api / iot-core / telemetry-ingestor / device-worker / demo`，其中 `iot-core` 走独立的 gRPC 指标端口 `9101`。

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace
scripts/port-forward-local-monitoring.sh
docker compose -f monitoring/docker-compose.yml up -d
```

Grafana 默认账号：

- URL: http://localhost:3000
- IoT 目录固定地址：http://localhost:3000/dashboards/f/efobmswzeefi8d/
- Grafana 数据保存在 Docker 命名卷 `iot-grafana-data`，容器使用 `unless-stopped` 自动重启策略；重建容器保留登录配置和数据库，勿删除该数据卷。
- User: `admin`
- Password: `admin`
- 可用 dashboard：`IoT Overview`、`IoT Management API`、`IoT Pipeline`、`IoT Core`

已预置的面板：

- [IoT Overview](http://localhost:3000/d/iot-overview/iot-overview)
- [IoT Management API](http://localhost:3000/d/iot-management-api/iot-management-api)
- [IoT Pipeline](http://localhost:3000/d/iot-pipeline/iot-pipeline)

### Management API 告警可视化

`IoT Management API` 顶部显示关联告警及实例标签；HTTP 请求图表将 5xx 曲线标红，并展示关联规则的触发、恢复时间标记。标记对应告警评估状态变化，不是单次请求的精确时间。

看板顶部另有两条固定 UID 的规则入口，避免通过目录显示名拼接规则地址。若旧浏览器页面中的 `View alert rule` 跳到 `pri%24grafana%24IoT...` 并报 403，请完整刷新浏览器页面（不是仅点击看板的 Refresh），或使用顶部固定入口；不要为此扩大目录权限。Alert list 的内置数据源名称为 `-- Grafana --`，与 API 中的规则源标识 `grafana` 不同。

规则模板为 `monitoring/grafana/alerts/management-api-http-5xx.json`：按 `route/status` 计算最近 5 分钟的 HTTP 5xx 平均 QPS，`> 0` 持续 1 分钟触发。没有 5xx 序列时回退到 0；此规则不负责检测服务离线。5 分钟窗口也意味着最后一次错误后不会立刻恢复。

该模板通过 Grafana API 导入，不做只读文件 provisioning，导入后仍可在页面调整阈值、暂停和通知渠道。新环境先创建名为“钉钉”的联系人（或修改模板中的 receiver）；Webhook 凭证只保存在 Grafana，不进入仓库。首次导入示例：

```bash
curl --fail-with-body -u "admin:${GRAFANA_ADMIN_PASSWORD}" \
  -H 'Content-Type: application/json' -H 'X-Disable-Provenance: true' \
  --data-binary @monitoring/grafana/alerts/management-api-http-5xx.json \
  http://localhost:3000/api/v1/provisioning/alert-rules
```

已有规则更新时改用 `PUT /api/v1/provisioning/alert-rules/iot-management-api-http-5xx`。模板不会自动覆盖在 Grafana 页面中做的修改。暂停中的规则不会产生新的触发标记；历史标记从关联面板之后开始记录，不会补写之前的事件。

另有 `monitoring/grafana/alerts/management-api-healthz-qps.json`：只检测 `/healthz`、`2xx` 序列，与图表一样使用 5 分钟平均 QPS，严格 `> 0.3 req/s` 在下次评估时触发（`for: 0s`，通知 `group_wait: 0s`）。本地 `Management API HTTP` 分组每 60 秒评估一次；通知使用已有“钉钉”联系人。该曲线显示橙色虚线阈值，规则关联同一 HTTP 面板，且不会改变 5xx 规则。首次导入沿用上面的 POST 命令、更换文件名；更新使用 UID `iot-management-api-healthz-qps`。健康检查速率本身接近 0.3，采样波动可能造成反复触发/恢复。

### 钉钉通知模板

`monitoring/grafana/notifications/dingtalk.tmpl` 定义 `iot.dingtalk.title`、`iot.dingtalk.message` 和 `iot.dingtalk.payload`，本地保存于 Grafana 的 `iot.dingtalk` 模板组。联系人引用见 `dingtalk-contact.json`；这是不含 URL 的配置片段，不可直接覆盖完整联系人。

- 标题包含固定 `grafana` 关键词、触发/恢复状态、规则名称和数量。
- 联系人名称仍为“钉钉”，集成类型使用 Webhook，通过 Custom Payload 直接向原机器人 URL POST 钉钉 Markdown JSON；不使用原生 DingDing 的整体跳转 ActionCard（其 singleURL 固定跳到告警列表）。无需转发服务。
- 正文按实例展示级别、服务、路由、HTTP 状态、摘要、详情，以及 UTC+8 开始/恢复时间；无值的可选字段不展示。JSON 由 `data.ToJSON` 编码，不手动拼接文案，以正确处理引号和换行。
- 按规则提供的 URL 展示图表、看板、规则、处理手册和临时静默链接，单条消息最多展示 10 个实例。链接继承 Grafana 对外地址，目前为 localhost，其他设备访问需另行配置可达地址。
- `disableResolveMessage: false` 开启恢复通知；此次模板规范化不改变规则阈值、评估周期、分组或重复通知频率。
- 部署顺序：先通过 `PUT /api/v1/provisioning/templates/iot.dingtalk` 保存模板（`X-Disable-Provenance: true` 保留 UI 编辑能力），再应用联系人片段并保留 UID、名称和原始 URL。由原生 DingDing 迁移到 Webhook 时，只在内存中读取原 URL，并替换类型对应的 settings、清除旧 secureFields，不能将 `[REDACTED]` 当作 URL 保存。Webhook 的 URL 是受保护配置字段而非原生 DingDing 的加密秘密字段，需限制联系人读取权限；不要打印、导出到仓库或提交机器人 token。
- 修改模板优先在通知模板组中进行。已打开的联系人编辑页必须刷新后再保存，避免旧表单覆盖模板引用。

设置 `GRAFANA_ADMIN_PASSWORD` 后运行 `python3 scripts/test-grafana-notification-template.py -v`，通过本地 Grafana 模板预览 API 验证触发、恢复、缺失字段、多实例截断、Markdown JSON 和四个独立链接；不会向钉钉发消息。可用 `GRAFANA_URL`、`GRAFANA_USER` 指定其他测试实例。钉钉历史卡片不会随模板更新，应在新消息上验证；联系人测试通知可能缺少规则 GeneratorURL，因此不展示“查看规则”，真实规则通知才包含该链接。

## Helm 部署

仓库里已经提供 Helm Chart：[`charts/iot`](charts/iot)

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
```

本地一键脚本：

```bash
scripts/helm-deploy-local.sh
```

脚本默认只部署应用本身：

- `management-api`
- `iot-core`
- `telemetry-ingestor`
- `device-worker`

脚本默认会先从 k8s Pod 内检查外部 PostgreSQL、Kafka、EMQX、TDengine 端口是否可达。若目标环境使用云服务或 CI 不需要这个检查，可以关闭：

```bash
CHECK_EXTERNAL_DEPS=0 scripts/helm-deploy-local.sh
```

当前 Helm 部署固定只包含应用本身和共享配置。PostgreSQL、Kafka、TDengine、Prometheus、Grafana、demo 都由独立平台或本地 Docker Compose 管理；EMQX 由独立的 Operator Release 管理，均不进入业务 Helm release，也没有可重新启用的内置依赖模板。

## 开发建议

- `tenantId` 必须贯穿所有写入和查询路径
- Kafka 消费端必须按幂等设计
- TDengine 负责时序数据，PostgreSQL 负责业务元数据和状态
- 命令状态机建议保持 `pending -> dispatched -> sent -> acked / timeout / failed`
- 保持逻辑多租户隔离，避免过早引入复杂分库分表
- Demo 模拟器当前作为外部造流服务运行，不进入业务 Helm release

## 路线图

- 设备预注册与鉴权增强
- 命令 ACK 的完整 MQTT 闭环
- 告警和规则能力增强
- DLQ 与消息重放
- 更完整的可观测性和运维面板

## 贡献

欢迎提交 Issue 和 Pull Request。开始前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 和 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。

建议在提交前先运行：

```bash
go test ./...
```

安全问题请参考 [SECURITY.md](SECURITY.md)，不要在公开 Issue 中披露漏洞细节。

## 许可证

本项目使用 [Apache License 2.0](LICENSE)。
