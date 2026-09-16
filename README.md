# IoT Platform

<p align="center">
  <img src="docs/images/architecture.svg" alt="IoT platform architecture" />
</p>

<p align="center">
  <img alt="Go version" src="https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white" />
  <img alt="License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" />
</p>

Go-zero + gRPC + protobuf + etcd + EMQX + Kafka + TDengine + PostgreSQL 的物联网平台骨架，面向设备接入、遥测采集、命令下发、状态查询和多租户管理。

这是一套已经把物联网关键闭环打通的开源基础设施，重点能力包括：

- 先拆出一个核心业务微服务 `core-rpc`
- `admin` 作为 go-zero REST 网关，统一对外提供 API
- `core-rpc` 通过 gRPC + protobuf 暴露核心业务，并用 etcd 做服务发现
- `ingress` 负责 MQTT 接入、标准化和事件解耦
- `worker` 负责时序落库、业务状态更新和命令投递
- `demo` 负责多租户多设备造流和 ACK 回执，适合联调和压测

## 一图看懂

<p align="center">
  <img src="docs/images/command-cycle.svg" alt="IoT platform command lifecycle" />
</p>

## 核心特性

- 设备接入链路：MQTT -> EMQX -> Go `ingress`
- 核心服务拆分：`admin` 负责 REST 网关，`core-rpc` 负责核心业务
- 服务发现：`core-rpc` 通过 etcd 注册与发现，支持本地 Docker 部署
- 异步解耦：遥测、命令和事件统一进入 Kafka
- 双存储分工：TDengine 保存时序数据，PostgreSQL 保存业务元数据和当前态
- 命令闭环：创建、下发、ACK、状态机更新
- 多租户隔离：`tenantId` 贯穿 topic、消息、存储和查询
- 标准契约：提供 OpenAPI、MQTT JSON Schema、gRPC proto 和数据库迁移脚本
- 本地可运行：默认可以连接本机 Docker 的 PostgreSQL / Kafka / EMQX / TDengine / etcd

## 当前实现

- 5 个可启动入口：`cmd/admin`、`cmd/core-rpc`、`cmd/demo`、`cmd/ingress`、`cmd/worker`
- `admin` 使用 go-zero REST，`core-rpc` 使用 gRPC + protobuf + etcd
- 本地 Docker 编排包含 etcd，适配 Helm / k8s 本地联调
- 1 份 PostgreSQL 初始化迁移：`migrations/001_init.sql`
- 1 份 OpenAPI 定义：`docs/openapi.json`
- 1 份 MQTT 消息 Schema：`docs/mqtt-envelope.schema.json`
- 已包含基础测试，`go test ./...` 可直接运行

## 观测

- 每个服务都暴露 `/metrics`
- `admin` 和 `core-rpc` 已启用 go-zero 的 trace / log middleware
- 服务启动时会开启 OpenTelemetry trace agent，默认写到 `/tmp/<service>-traces.log`
- HTTP 请求会自动补 `X-Request-Id`，并透传到 `admin -> core-rpc` 的 gRPC 调用
- `demo` 发往 `admin` 的请求会带上 request id 和 trace 上下文，便于串联压测/联调链路
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
- etcd

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
export CORE_RPC_ETCD_HOSTS=localhost:2379
export CORE_RPC_ETCD_KEY=iot/core-rpc
export CORE_RPC_LISTEN_ON=:9001
```

可选的 topic 和客户端标识配置：

```bash
export KAFKA_TELEMETRY_TOPIC=iot.telemetry
export KAFKA_COMMAND_TOPIC=iot.command
export EMQX_TOPIC_FILTER=tenant/+/device/+/telemetry
export EMQX_INGRESS_CLIENT_ID=iot-ingress
export EMQX_WORKER_CLIENT_ID=iot-worker
export TDENGINE_TABLE=telemetry
```

监听地址默认是 `:8080`，也可以通过以下变量覆盖：

```bash
export PORT=8081
# 或者
export LISTEN_ADDR=:8081
```

### 3. 启动服务

```bash
go run ./cmd/core-rpc
go run ./cmd/admin
go run ./cmd/demo
go run ./cmd/ingress
go run ./cmd/worker
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

核心业务接口：

- `POST /api/v1/tenants`
- `GET /api/v1/tenants`
- `POST /api/v1/devices`
- `GET /api/v1/devices`
- `GET /api/v1/devices/{tenantId}/{deviceId}`
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
2. `ingress` 负责标准化和事件解耦
3. `admin` 通过 gRPC 调用 `core-rpc`
4. `core-rpc` 使用 etcd 做服务发现
5. `worker` 消费 Kafka，完成时序和状态落库

这套拆法的原则是先保留一个清晰的核心边界，再根据业务压力继续扩展，而不是一下拆成很多很难排障的小服务。

## Demo 模拟器

`cmd/demo` 是一个独立的模拟服务，启动后会自动：

- 按配置创建多租户和多设备拓扑
- 随机发布 telemetry 到 MQTT
- 随机向 `admin` 创建 command 请求
- 订阅各租户 command topic，并自动回 ACK

常用环境变量：

```bash
export DEMO_ADMIN_URL=http://127.0.0.1:8080
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
│   ├── admin/      # 查询与管理 API
│   ├── core-rpc/   # 核心业务 gRPC 服务
│   ├── demo/       # 随机造流与 ACK 的模拟器
│   ├── ingress/    # MQTT 接入与事件解耦
│   └── worker/     # Kafka 消费、落库和下行处理
├── internal/
│   ├── adminapi/   # REST 网关，负责 HTTP 到 core-rpc 的转换
│   ├── bootstrap/  # 启动装配
│   ├── contracts/  # topic、envelope、状态机、OpenAPI 和 Schema 契约
│   ├── core/       # 核心业务 gRPC 服务实现
│   ├── demo/       # 造流模拟器运行时
│   ├── platform/   # 仓储、消息、指标、worker、MQTT/TDengine 适配
│   └── server/     # HTTP 基础能力
├── charts/iot/     # Helm 部署清单
├── migrations/     # 数据库迁移
├── proto/          # core-rpc protobuf 契约
├── monitoring/     # 本地 Prometheus / Grafana 配置
└── docs/           # OpenAPI、Schema、技术方案和 ADR
```

## 文档

- [整体技术方案](docs/物联网平台技术方案.html)
- [OpenAPI 定义](docs/openapi.json)
- [MQTT Envelope Schema](docs/mqtt-envelope.schema.json)
- [初始化迁移](migrations/001_init.sql)

## 本地 Helm + Docker 部署

当前推荐的本地形态是：

- Docker：PostgreSQL / Kafka / EMQX / TDengine / etcd / Prometheus / Grafana / demo
- Kubernetes + Helm：`admin` / `core-rpc` / `ingress` / `worker`

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

宿主机运行 Go E2E 时使用 `localhost:9092`，k8s Pod 访问 Docker Kafka 时使用 `192.168.65.254:29092`。如果 Kafka 只配置单个 advertised listener，客户端会在拿到 broker metadata 后被引导到另一侧不可达的地址，表现为 `core-rpc` / `ingress` / `worker` Kafka 写入或消费超时。本地 Docker Desktop 默认使用 `192.168.65.254` 作为 k8s 访问 Docker 依赖的网关地址。

安装业务服务：

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
kubectl rollout status deploy/core-rpc -n iot
kubectl rollout status deploy/admin -n iot
kubectl rollout status deploy/ingress -n iot
kubectl rollout status deploy/worker -n iot
```

也可以使用仓库脚本一键完成外部依赖连通性检查、Helm 安装和 rollout 验证：

```bash
scripts/helm-deploy-local.sh
```

该脚本会强制 apps-only 部署，只安装 `admin`、`ingress`、`worker` 以及它们共享的配置，不会安装 PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana 或 demo。
现在脚本同样会部署并等待 `core-rpc`，它是 `admin` 的 gRPC 核心依赖。
脚本从当前本地 `APP_IMAGE` 读取仓库摘要（RepoDigest），以 `iot-app@sha256:...` 传给 Helm，避免固定 tag 重建后被 k8s `IfNotPresent` 复用旧镜像。本地只保留 `iot-app:2.0`，不再生成 `local-<hash>` tag；若镜像尚无仓库摘要，脚本会提示先拉取或发布镜像再部署。

默认 Helm values 会跳过 Postgres/Kafka/EMQX/TDengine/Prometheus/demo 的 k8s 资源，并通过 Docker Desktop 网关 IP 连接 Docker 服务。Docker 容器内访问宿主机端口时仍使用 `host.docker.internal`，例如 Prometheus 抓取 k8s port-forward 后的 metrics。

给本地 Prometheus 和 demo 建立访问 k8s 业务服务的通道：

```bash
scripts/port-forward-local-monitoring.sh
```

启动 Docker Prometheus、Grafana 和 demo：

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

Prometheus 和 Grafana 都用 Docker 本地启动。Prometheus 通过本机 port-forward 抓取 k8s 业务服务的 `/metrics`，Grafana 数据源已经预置为 Docker Compose 内部地址 `http://prometheus:9090`。
现在本地监控会同时覆盖 `admin / core-rpc / ingress / worker / demo`，其中 `core-rpc` 走独立的 gRPC 指标端口 `9101`。

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
- 可用 dashboard：`IoT Overview`、`IoT Admin API`、`IoT Pipeline`、`IoT Core RPC`

已预置的面板：

- [IoT Overview](http://localhost:3000/d/iot-overview/iot-overview)
- [IoT Admin API](http://localhost:3000/d/iot-admin-api/iot-admin-api)
- [IoT Pipeline](http://localhost:3000/d/iot-pipeline/iot-pipeline)

### Admin API 告警可视化

`IoT Admin API` 顶部显示关联告警及实例标签；HTTP 请求图表将 5xx 曲线标红，并展示关联规则的触发、恢复时间标记。标记对应告警评估状态变化，不是单次请求的精确时间。

看板顶部另有两条固定 UID 的规则入口，避免通过目录显示名拼接规则地址。若旧浏览器页面中的 `View alert rule` 跳到 `pri%24grafana%24IoT...` 并报 403，请完整刷新浏览器页面（不是仅点击看板的 Refresh），或使用顶部固定入口；不要为此扩大目录权限。Alert list 的内置数据源名称为 `-- Grafana --`，与 API 中的规则源标识 `grafana` 不同。

规则模板为 `monitoring/grafana/alerts/admin-http-5xx.json`：按 `route/status` 计算最近 5 分钟的 HTTP 5xx 平均 QPS，`> 0` 持续 1 分钟触发。没有 5xx 序列时回退到 0；此规则不负责检测服务离线。5 分钟窗口也意味着最后一次错误后不会立刻恢复。

该模板通过 Grafana API 导入，不做只读文件 provisioning，导入后仍可在页面调整阈值、暂停和通知渠道。新环境先创建名为“钉钉”的联系人（或修改模板中的 receiver）；Webhook 凭证只保存在 Grafana，不进入仓库。首次导入示例：

```bash
curl --fail-with-body -u "admin:${GRAFANA_ADMIN_PASSWORD}" \
  -H 'Content-Type: application/json' -H 'X-Disable-Provenance: true' \
  --data-binary @monitoring/grafana/alerts/admin-http-5xx.json \
  http://localhost:3000/api/v1/provisioning/alert-rules
```

已有规则更新时改用 `PUT /api/v1/provisioning/alert-rules/iot-admin-http-5xx`。模板不会自动覆盖在 Grafana 页面中做的修改。暂停中的规则不会产生新的触发标记；历史标记从关联面板之后开始记录，不会补写之前的事件。

另有 `monitoring/grafana/alerts/admin-healthz-qps.json`：只检测 `/healthz`、`2xx` 序列，与图表一样使用 5 分钟平均 QPS，严格 `> 0.3 req/s` 在下次评估时触发（`for: 0s`，通知 `group_wait: 0s`）。本地 `Admin HTTP` 分组每 60 秒评估一次；通知使用已有“钉钉”联系人。该曲线显示橙色虚线阈值，规则关联同一 HTTP 面板，且不会改变 5xx 规则。首次导入沿用上面的 POST 命令、更换文件名；更新使用 UID `iot-admin-healthz-qps`。健康检查速率本身接近 0.3，采样波动可能造成反复触发/恢复。

### 钉钉通知模板

`monitoring/grafana/notifications/dingtalk.tmpl` 定义 `iot.dingtalk.title` 和 `iot.dingtalk.message`，本地保存于 Grafana 的 `iot.dingtalk` 模板组。联系人引用见 `dingtalk-contact.json`；这是不含 URL 的合并配置，不可直接覆盖完整联系人。

- 标题包含固定 `grafana` 关键词、触发/恢复状态、规则名称和数量。
- 使用 ActionCard，正文按实例展示级别、服务、路由、HTTP 状态、摘要、详情，以及 UTC+8 开始/恢复时间；无值的可选字段不展示。
- 按规则提供的 URL 展示图表、看板、规则、处理手册和临时静默链接，单条消息最多展示 10 个实例。链接继承 Grafana 对外地址，目前为 localhost，其他设备访问需另行配置可达地址。
- `disableResolveMessage: false` 开启恢复通知；此次模板规范化不改变规则阈值、评估周期、分组或重复通知频率。
- 部署顺序：先通过 `PUT /api/v1/provisioning/templates/iot.dingtalk` 保存模板（`X-Disable-Provenance: true` 保留 UI 编辑能力），再将联系人片段合并到现有“钉钉”联系人，保留 UID 和加密 URL。不要把机器人 URL/token 写入仓库。
- 修改模板优先在通知模板组中进行。已打开的联系人编辑页必须刷新后再保存，避免旧表单覆盖模板引用。

设置 `GRAFANA_ADMIN_PASSWORD` 后运行 `python3 scripts/test-grafana-notification-template.py -v`，通过本地 Grafana 模板预览 API 验证触发、恢复、缺失字段和多实例截断；不会向钉钉发消息。可用 `GRAFANA_URL`、`GRAFANA_USER` 指定其他测试实例。

## Helm 部署

仓库里已经提供 Helm Chart：[`charts/iot`](charts/iot)

```bash
helm upgrade --install iot charts/iot -n iot --create-namespace --wait --timeout 180s
```

如果需要复刻旧本地集群内全量依赖部署，使用本地 stack values：

```bash
helm upgrade --install iot charts/iot \
  -n iot \
  --create-namespace \
  --wait \
  --timeout 180s \
  -f charts/iot/values-local-stack.yaml
```

本地一键脚本：

```bash
scripts/helm-deploy-local.sh
```

脚本默认只部署应用本身：

- `admin`
- `core-rpc`
- `ingress`
- `worker`

脚本默认会先从 k8s Pod 内检查外部 PostgreSQL、Kafka、EMQX、TDengine 端口是否可达。若目标环境使用云服务或 CI 不需要这个检查，可以关闭：

```bash
CHECK_EXTERNAL_DEPS=0 scripts/helm-deploy-local.sh
```

当前 Helm 部署只允许包含应用本身和共享配置。PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana、demo 都作为外部依赖或本地 Docker 服务，不进入业务 Helm release。

如果你已经用旧 Helm values 起过同名/依赖资源，先清理旧资源再装：

```bash
helm uninstall iot -n iot --ignore-not-found
kubectl delete pvc postgres-data kafka-data emqx-data tdengine-data -n iot --ignore-not-found
scripts/helm-deploy-local.sh
```

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
