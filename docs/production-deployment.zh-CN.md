# IoT 平台生产部署指南

[English](production-deployment.md) | 简体中文

## 目标架构

生产环境按职责拆分部署，不将所有组件放进同一个业务 Helm Release：

- `iot` 命名空间：四个无状态业务服务，以及可选的只读 ops-agent。
- `emqx` 命名空间：设备 MQTT 长连接集群。
- `observability` 命名空间：Prometheus、Grafana、用于命令追踪的 Tempo，以及按需启用的 Alertmanager。
- 数据与消息平台：PostgreSQL、Kafka、TDengine 由独立平台团队管理，或采用各自的 Operator 管理，不能与业务服务生命周期绑定。

`charts/iot` 部署 `iot` 命名空间中的四个业务服务，并可按需启用 ops-agent。集群控制面的 etcd 由 Kubernetes 平台管理；本项目不再部署业务 etcd。`management-api` 通过 Kubernetes Service DNS `iot-core:9001` 调用 `iot-core`。

## 生产与本地映射

本地不是生产替代品，但按部署边界模拟生产：需要购买或托管的能力以 Docker 单实例模拟，需要自建 Kubernetes 的能力实际部署到本地 Kubernetes。这样可以提前验证网络、服务发现、凭证注入和发布边界，而不伪造高可用或跨可用区能力。

| 能力 | 生产推荐 | 本地对应方式 |
| --- | --- | --- |
| Kubernetes 控制面 | 托管 Kubernetes；平台负责控制面 etcd | Docker Desktop Kubernetes；不部署业务 etcd |
| 业务服务 | Kubernetes 的 `iot` 命名空间 | 同样部署到本地 Kubernetes 的 `iot` 命名空间 |
| EMQX 长连接 | Kubernetes 的 `emqx` 命名空间，EMQX Operator 管理集群 | 同一 Operator、命名空间、PVC 与 Service DNS；受社区许可限制为单 Core 节点 |
| PostgreSQL、Kafka、TDengine | 优先托管数据/消息/时序服务 | Docker Compose 容器模拟私网托管端点 |
| 跨集群注册与配置 | 托管注册配置中心，按需启用 | 当前单集群不启用注册中心；跨集群方案确定后再接入 |
| 指标、看板与告警 | 优先托管可观测服务 | Docker Compose 的 Prometheus 与 Grafana；配置通过 Git/Provisioning 管理 |
| API 与 MQTT 对外入口 | 托管 L4/L7 负载均衡、网关和 TLS | `kubectl port-forward` 转发到本地端口；不等价于公网负载均衡 |

## 跨集群原则

- 集群内同步调用一律使用 Kubernetes Service DNS；不引入业务 etcd 或注册中心。
- 跨集群流量先经私网连通、全局流量调度和网关，再按需使用托管注册配置中心发现动态后端。
- 注册中心保存的是服务实例和配置，不承担设备 MQTT 长连接；设备始终通过 L4 负载均衡访问 EMQX。
- 多地域优先采用每地域独立 Kubernetes 与本地服务发现，避免把全部业务调用集中依赖一个跨地域注册中心。

## 服务清单与推荐方式

| 服务 | 职责 | 推荐部署位置 | 推荐方式 |
| --- | --- | --- | --- |
| API Gateway / Ingress | TLS 终止、认证、限流和对外 API 入口 | 平台网关命名空间 | 由平台统一部署高可用 Gateway 或 Ingress Controller；仅将 `management-api` 暴露为 HTTP API。当前 Chart 不安装网关。 |
| `ops-agent` | 租户命令只读诊断 | `iot`，按需启用 | 独立 Deployment/ClusterIP、专用 Secret；可连接外部模型。 |
| Tempo | 采样链路导入与查询 | 可观测平台 | OTLP 写入、HTTP 查询；按容量设计保留期。 |
| `management-api` | REST API、鉴权边界、调用核心服务 | `iot` | Kubernetes Deployment；生产至少 2 副本、HPA、PDB、滚动更新和拓扑分散。通过 `iot-core:9001` Service DNS 调用核心服务。 |
| `iot-core` | 租户、设备、命令、状态等核心 gRPC 业务 | `iot` | Kubernetes Deployment；生产至少 2 副本、HPA、PDB、滚动更新和拓扑分散。仅用 ClusterIP Service 暴露 gRPC 与指标端口。 |
| `telemetry-ingestor` | MQTT 上行消息解析并写入 Kafka | `iot` | Kubernetes Deployment。通过 EMQX 共享订阅扩容，并为每个 Pod 注入唯一 MQTT Client ID；当前 Chart 默认 2 副本，已具备这两项配置。Chart 已有 PDB、可选 HPA 和拓扑分散，生产应通过 overlay 启用并压测调优。 |
| `device-worker` | Kafka 消费、状态/时序落库、命令投递、ACK 更新 | `iot` | Kubernetes Deployment。通过 Kafka 分区和消费者组扩容；ACK MQTT 订阅使用共享订阅且 Client ID 由 Pod 名唯一化。当前 Chart 默认 2 副本；worker 在投递前做命令终态预检（`sent/acked/timeout/failed` 直接跳过并提交位点），ACK 落库失败重试后转入 DLQ，具备多副本下的幂等语义。进一步扩容前仍需增加 Kafka 分区（`kafka.topicPartitions`，仅在 topic 创建时生效）。 |
| EMQX | MQTT 接入、会话、订阅和设备长连接 | `emqx` | 独立 EMQX 集群，由 EMQX Operator 管理；至少多节点、反亲和、PDB、滚动升级和 TCP 负载均衡。设备长连接通过 L4 负载均衡进入 EMQX，而不是进入业务 Pod。EMQX 5.9+ 的集群必须配置许可证 Secret。仓库清单见 [`deploy/emqx/cluster.yaml`](../deploy/emqx/cluster.yaml)。 |
| PostgreSQL | 租户、设备、命令、当前状态等事务数据 | 独立数据平台 | 优先使用托管高可用 PostgreSQL；自建时使用受支持的 PostgreSQL Operator，配置主备、备份、PITR、监控和定期恢复演练。 |
| Kafka | 遥测、命令等异步事件流 | 独立消息平台 | 优先使用托管 Kafka；自建时使用 Kafka Operator，至少 3 broker、跨故障域、副本因子、幂等生产和消费者 lag 监控。业务消费者必须保持幂等。 |
| TDengine | 遥测时序数据写入与查询 | 独立时序数据平台 | 采用托管服务或独立 TDengine 集群；自建时使用受支持的 Operator/Helm、持久卷、备份、版本固定和恢复演练。不能与 `device-worker` 部署在同一生命周期中。 |
| Prometheus | 指标抓取、规则评估和短期指标存储 | 托管可观测平台优先 | 优先托管指标服务；需要私有化时在 `observability` 命名空间使用 Prometheus Operator，配置 ServiceMonitor/PodMonitor、持久卷、告警规则与容量保留策略。 |
| Grafana | 看板、告警规则、联系人和通知策略 | 托管可观测平台优先 | 优先托管看板与告警服务；需要私有化时独立于业务 Release 部署，使用 Provisioning/Git 管理数据源、看板和告警模板。高可用时使用外部共享数据库，不能依赖容器内 SQLite。 |
| Alertmanager | Prometheus 告警去重、分组、静默和路由 | `observability`，按需 | 当告警规则由 Prometheus 统一评估时部署为高可用集群，并按团队、值班和升级策略路由。当前项目告警由 Grafana Alerting 管理，不应对同一规则同时使用两套通知链路。 |
| `demo` | 本地造流、联调和压测 | 不进入生产 | 仅在开发、预发或独立压测环境运行，使用独立租户和限流，禁止接入生产 MQTT、Kafka 或数据库。 |

## 服务发现与连接原则

- 同一 Kubernetes 集群、同一业务域内的同步调用使用 Service DNS，例如 `iot-core.iot.svc.cluster.local:9001`；无需为此额外部署业务 etcd。
- Service DNS 只负责发现与网络负载均衡，不能替代强一致配置、租约、选主和分布式锁。此类跨集群或协调需求需要专门的注册/协调系统。
- PostgreSQL、Kafka、TDengine、EMQX 均通过稳定的内部域名或私有网络入口连接；连接凭证使用 Secret 管理，禁止写入镜像、Chart 默认值或仓库。

## 生产上线基线

1. 业务服务配置 readiness/liveness/startup probe、资源 request/limit、PDB、HPA 和跨节点/可用区拓扑分散。
2. 所有镜像使用不可变版本或摘要，禁止生产使用 `latest`；发布经 CI 构建、镜像扫描和 Helm 渲染校验后执行。
3. EMQX、PostgreSQL、Kafka、TDengine 分别建立容量、可用性、延迟、错误率和备份恢复监控；设备 TCP 长连接的扩容、驱逐和升级要结合连接迁移策略演练。
4. Prometheus 负责指标采集，Grafana 负责看板与当前告警链路；通知渠道按团队、值班与升级策略配置，并定期测试触发和恢复消息。
5. 定期执行 PostgreSQL、TDengine 与 Grafana 数据恢复演练；Kafka 按保留策略和消费者 offset 设计重放方案。

## 安全、可靠性与数据模型基线

- MQTT 认证统一回调 `iot-core` 内部认证端点，设备用户名为 `tenantId:deviceId`，密码只在注册时出现，数据库保存 bcrypt 哈希；EMQX `authorization.no_match=deny`，由认证响应下发精确租户/设备 ACL。生产环境应再通过 NetworkPolicy、服务身份认证和 TLS 保护回调端点。
- `management-api` 免鉴权只开放 `/healthz`、`/readyz` 和 MQTT Schema（kubelet 探针不携带凭证）；所有业务 REST 请求要求 Bearer Token，令牌通过 Secret Manager 注入，不写入镜像和 Git。可选配置 `MANAGEMENT_API_TOKENS` 将令牌绑定到指定租户，单个租户令牌泄露时无法访问其他租户数据。
- 命令创建先写 PostgreSQL `created` 状态；`iot-core` 多副本通过 `FOR UPDATE SKIP LOCKED` 领取租约，发布到 Kafka 成功后变为 `published`，worker 投递到设备（MQTT）成功后变为 `sent` 并写入 deadline，达到 deadline 未 ACK 变为 `timeout`，ACK 写入 `command_ack` 和 `command_events`。命令 ID 使用 UUIDv7，便于排序和跨副本唯一。
- JSON 解码、PostgreSQL、TDengine、MQTT 投递失败的 Kafka 消息先写入 `iot.dlq`，成功后才提交原消费位点。人工检查 `stage/error` 后使用 `dlq-replay --limit N` 重放，禁止自动无限重试造成毒丸消息循环。
- 租户和命令列表使用 keyset cursor 分页，单页上限 100；禁止在 REST 层一次性拉取全表。
- TDengine 使用 `telemetry_v2` 超级表和每设备子表，`tenant_id/device_id` 为 Tags，保存 `msg_id/type/version/payload_hash/payload_bytes`；完整 payload 的权威副本为 PostgreSQL JSONB，避免 4096 字符限制和时序库字符串截断。
- 当前源码和构建基线为 Go `1.27.1`，`go.mod` 固定 `go 1.27.0` 与 `toolchain go1.27.1`；生产镜像构建应在 CI 中安装同一工具链并锁定依赖校验和。

## 当前 Chart 与生产差异

当前 `charts/iot` 是本地可运行的 applications-only Chart，默认将 `management-api`、`iot-core`、`telemetry-ingestor`、`device-worker` 四个服务各部署 2 副本，且每个服务均带 PDB（`minAvailable: 1`），并连接外部依赖。它不包含网关、EMQX、PostgreSQL、Kafka、TDengine、Prometheus、Grafana、Alertmanager 或 demo。

投入生产前，应按本指南分别建设这些平台服务，并启用生产 overlay 的 HPA/拓扑分散，配置 TLS/Secret、容量基线与灾备演练（Chart 已为四个业务服务内置 PDB）；`device-worker` 扩容前还必须完成 Kafka 分区规划（`kafka.topicPartitions`）。

## 已实现的可靠性与监控基线

四个业务 Deployment 均已有 PDB。management-api readiness 验证核心 gRPC 与 PostgreSQL，ingestor/worker readiness 验证 PostgreSQL；存活检查独立。本地 Prometheus 使用命名空间级 Role 经 apiserver proxy 抓取每个 Pod，并校验 CA、保留服务 job 标签；生产由可观测平台直接抓取 Pod。

可选租户 Token 使用 Secret 中的 JSON token→tenant 映射，全局列表仍要求管理 Token。Kafka 新 topic 的分区数可配；现存分区及副本变更由运维执行，动态配置在启动时对账。命令重投跳过已 sent/终态，ACK 有限重试与人工重放保留终态语义；PostgreSQL 遥测完成标记支持后台补偿。重复投递和双依赖同时故障的边界见 ADR 0007。

## 生产配置与数据库发布

使用 `charts/iot/values-production.yaml` 叠加默认配置，替换其中的内部 DNS、镜像与 Secret。该配置启用 mTLS、四个 HPA、节点/可用区分散和 startup probes；Kafka 新 topic 为 6 分区、RF=3、min ISR=2。集群需提供 metrics-server 和足够故障域，现有 topic 的分区与副本需运维单独调整。调度分散采用 ScheduleAnyway，是软约束。

先以迁移角色注入 POSTGRES_DSN 执行镜像中的 `/usr/local/bin/migrate`，然后发布配置 `postgres.autoMigrate=false` 的业务服务；业务数据库角色不需 DDL 权限。迁移与启动使用同一份嵌入 SQL，并以事务锁串行执行。迁移前按数据平台要求完成备份。

命令重试使用 Idempotency-Key，遥测使用 PostgreSQL inbox 后确认 MQTT。完整语义和 broker 离线投递边界见 ADR 0008；TDengine 数值列、同时间戳冲突、数据生命周期与外部 HA 容量仍需按业务规划。

使用仅具备业务读写权限的账号实际重放 `command.ack` 时，需同时注入
`POSTGRES_AUTO_MIGRATE=false` 和 `POSTGRES_DSN`；CLI 为兼容本地环境默认开启迁移。
worker 对合法 ACK 仅在 PostgreSQL 或 DLQ 保存成功后确认，双失败时保留未确认状态并报错退出。
断连或副本替换后的重投依赖 EMQX 会话及共享订阅投递策略，应用手动确认本身不提供 broker 持久化。

## 运维 Agent 3.0

构建当前版本（`make build && docker build -t iot-app:3.1 .`），生产环境将镜像推送到集群可用的仓库并设置 `images.app`。Agent 使用独立 ClusterIP 服务，端口 8084。四个业务服务可独立部署。

1. 为每个租户分别生成运维令牌和命令只读令牌。在 management-api 使用的现有 Secret 中添加 `MANAGEMENT_COMMAND_READ_TOKENS`，值为 `{"<只读令牌>":"factory"}`，并滚动更新 management-api。本地脚本接受 `IOT_MANAGEMENT_COMMAND_READ_TOKENS`。只读凭据仅能 GET 该租户的一条命令。
2. 在同一命名空间创建专用 Secret `iot-ops-agent`。键 `OPS_TENANTS` 的值为 `{"factory":{"operatorToken":"<运维令牌>","managementToken":"<只读令牌>"}}`。模型要求鉴权时增加键 `OPS_MODEL_KEY`。轮换 Secret 后滚动更新使用它的 Deployment。
3. 将下列 values 合并到已有部署参数。所有地址以 Pod 实际可达的服务为准。模型 URL 是兼容 Chat Completions 的 API 根路径，通常以 `/v1` 结尾。使用自部署或云端模型均可。Prometheus、Tempo 和模型可独立启用。

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

使用现有 Helm 环境参数发布，或本地通过 `IOT_VALUES_FILE=/path/to/ops-values.yaml scripts/helm-deploy-local.sh` 部署。若只改 tracing 配置，请滚动更新业务 Pod。联调可临时将采样率设为 1。Tempo 导出与索引存在延迟，查不到 span 时仍可根据持久化状态诊断。Grafana 可添加类型 Tempo 的数据源，使用同一查询地址。本地 Compose profile `tracing` 使用单实例、24 小时保留期；跨 Pod 访问时，使用受控网络上的可达地址，按需显式设置 `TEMPO_BIND_IP`。

诊断只会向模型发送用户自填的问题、已确认结论、阶段和指标；问题原文会发送给配置的模型，请勿在其中包含设备密钥。模型输出仅用于建议。Agent 不挂载 Kubernetes 服务账号令牌。部署时为凭据和数据流配置内部 TLS 或服务网格；Prometheus 与 Tempo 查询 URL 适用于可信内部网络或已鉴权的内部代理。

## Grafana 自动告警分析

3.1 的告警分析按服务和时间窗取证。现有 Grafana 联系人继续直接发送原始告警。同一联系人增加一个独立的 Webhook 集成后，Agent 会在稍后发送标为「IoT 告警补充分析」的钉钉消息。

1. 给告警队列准备独立 PostgreSQL 数据库。库所有者运行 `psql "$QUEUE_OWNER_DSN" -v ON_ERROR_STOP=1 -f deploy/ops-agent/alert-queue.sql`。给运行账号授予连接数据库、使用对应 schema 以及对 `ops_alert_jobs` 的 SELECT、INSERT、UPDATE、DELETE 权限。运行账号仅访问队列，不需要业务表权限。
2. 在 Agent 专用 Secret（默认 `iot-ops-agent`）中增加 `OPS_ALERT_DSN`（运行账号 DSN）、`OPS_GRAFANA_TOKEN`（独立随机 Bearer 凭据）、`OPS_DINGTALK_URL`（包含机器人 access_token 的完整 URL）。加签机器人另外设置 `OPS_DINGTALK_SECRET`。若启用关键词校验，放行「IoT 告警补充分析」。通过 Secret 管理工具写入这些值；轮换后滚动更新 Agent。
3. 设置 `opsAgent.enabled=true`、`opsAgent.alerts.enabled=true` 和 Pod 可访问的 `opsAgent.prometheusURL`。沿用可选的 `opsAgent.tempoURL`、`modelURL`、`model` 和模型凭据。仅启用告警时可省略 `OPS_TENANTS`。使用原有 Helm 环境参数升级到 3.1。
4. 在 Grafana 的「告警 → 联系人 → 钉钉」中**添加** Webhook 集成，保留原来的机器人集成。使用默认 Grafana JSON、POST、Bearer，凭据填 `OPS_GRAFANA_TOKEN`，启用恢复通知。集群内地址为 `http://ops-agent.iot.svc:8084/api/v1/webhooks/grafana`。本地 Compose 的 Grafana 可先启动 `docker compose -f monitoring/docker-compose.yml --profile ops-alerts up -d k8s-forward-ops-agent`，地址见[联系人样例](../monitoring/grafana/notifications/ops-agent-contact.json)。样例省略凭据；API 配置时在内存中补入 `settings.authorization_credentials`。跨信任边界使用 HTTPS 或服务网格。保留 Max Alerts 为 0，按告警规则分组，每组最多 128 个实例。
5. 用仓库的 healthz QPS 规则做联调。提高测试请求速率后，先检查原始通知，再检查包含服务、UTC 时间窗和证据的补充通知。Grafana 的通用 TestAlert 不属于这四条规则，会返回 204。恢复通知按恢复时刻重新取证。

规则由 `rule_uid` 标签识别。已有同名规则可直接使用；自定义过标题的实例请添加对应的规则 UID 标签。支持管理 API 的 5xx、healthz QPS、成功写入死信及死信写入失败。Agent 取触发或恢复时刻之前五分钟的 Prometheus 平均速率，并查询同一服务与时间窗的 Tempo 错误链路，最多五条。死信有已识别的 job、stage 时缩小指标范围。原始注解、业务载荷和任意标签不会送入模型。

HTTP 202 表示整组已入队。共享队列按「规则、指纹、本次开始时间、触发/恢复」去重，处理记录在结束后保留七天。机器人响应 `errcode=0` 确认送达；失败最多尝试六次，重试复用已有分析。进程退出后其他副本可在租约到期后接手。远端已收取消息但回包丢失时可能重复送达。检查队列中 `finished_at IS NOT NULL AND NOT delivered` 的记录及 Agent 日志可定位最终失败。
