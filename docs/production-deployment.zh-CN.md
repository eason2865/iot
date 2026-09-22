# IoT 平台生产部署指南

[English](production-deployment.md) | 简体中文

## 目标架构

生产环境按职责拆分部署，不将所有组件放进同一个业务 Helm Release：

- `iot` 命名空间：四个无状态业务服务。
- `emqx` 命名空间：设备 MQTT 长连接集群。
- `observability` 命名空间：Prometheus、Grafana 和按需启用的 Alertmanager。
- 数据与消息平台：PostgreSQL、Kafka、TDengine 由独立平台团队管理，或采用各自的 Operator 管理，不能与业务服务生命周期绑定。

`charts/iot` 仅部署 `iot` 命名空间中的四个业务服务。集群控制面的 etcd 由 Kubernetes 平台管理；本项目不再部署业务 etcd。`management-api` 通过 Kubernetes Service DNS `iot-core:9001` 调用 `iot-core`。

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
| `management-api` | REST API、鉴权边界、调用核心服务 | `iot` | Kubernetes Deployment；生产至少 2 副本、HPA、PDB、滚动更新和拓扑分散。通过 `iot-core:9001` Service DNS 调用核心服务。 |
| `iot-core` | 租户、设备、命令、状态等核心 gRPC 业务 | `iot` | Kubernetes Deployment；生产至少 2 副本、HPA、PDB、滚动更新和拓扑分散。仅用 ClusterIP Service 暴露 gRPC 与指标端口。 |
| `telemetry-ingestor` | MQTT 上行消息解析并写入 Kafka | `iot` | Kubernetes Deployment。通过 EMQX 共享订阅扩容，并为每个 Pod 注入唯一 MQTT Client ID；当前 Chart 默认 2 副本，已具备这两项配置。生产仍应补齐 HPA、PDB 和拓扑分散。 |
| `device-worker` | Kafka 消费、状态/时序落库、命令投递、ACK 更新 | `iot` | Kubernetes Deployment。通过 Kafka 分区和消费者组扩容；ACK MQTT 订阅使用共享订阅且 Client ID 由 Pod 名唯一化。当前默认 1 副本，扩容前需增加 Kafka 分区并验证命令、ACK 的幂等语义。 |
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
- `management-api` 默认只开放 `/healthz`、`/openapi.json` 和 MQTT Schema；所有业务 REST 请求要求 Bearer Token，令牌通过 Secret Manager 注入，不写入镜像和 Git。
- 命令创建先写 PostgreSQL `created` 状态；`iot-core` 多副本通过 `FOR UPDATE SKIP LOCKED` 领取租约，发布成功后变为 `sent`，达到 deadline 变为 `timeout`，ACK 写入 `command_ack` 和 `command_events`。命令 ID 使用 UUIDv7，便于排序和跨副本唯一。
- JSON 解码、PostgreSQL、TDengine、MQTT 投递失败的 Kafka 消息先写入 `iot.dlq`，成功后才提交原消费位点。人工检查 `stage/error` 后使用 `dlq-replay --limit N` 重放，禁止自动无限重试造成毒丸消息循环。
- 租户和命令列表使用 keyset cursor 分页，单页上限 100；禁止在 REST 层一次性拉取全表。
- TDengine 使用 `telemetry_v2` 超级表和每设备子表，`tenant_id/device_id` 为 Tags，保存 `msg_id/type/version/payload_hash/payload_bytes`；完整 payload 的权威副本为 PostgreSQL JSONB，避免 4096 字符限制和时序库字符串截断。
- 当前源码和构建基线为 Go `1.27.1`，`go.mod` 固定 `go 1.27.0` 与 `toolchain go1.27.1`；生产镜像构建应在 CI 中安装同一工具链并锁定依赖校验和。

## 当前 Chart 与生产差异

当前 `charts/iot` 是本地可运行的 applications-only Chart，默认部署 `management-api`、`iot-core`、`telemetry-ingestor` 各 2 副本和 `device-worker` 1 副本，并连接外部依赖。它不包含网关、EMQX、PostgreSQL、Kafka、TDengine、Prometheus、Grafana、Alertmanager 或 demo。

投入生产前，应按本指南分别建设这些平台服务，并补齐 HPA、PDB、拓扑分散、TLS/Secret、容量基线与灾备演练；`device-worker` 扩容前还必须完成 Kafka 分区规划和多副本消费语义验证。
