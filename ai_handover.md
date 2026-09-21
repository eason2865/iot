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
- MQTT 认证回调：`iot-core:9090/internal/mqtt/authenticate`，fail-closed——必须配置 `IOT_CORE_MQTT_AUTH_TOKEN`，EMQX 通过 `X_Iot_Auth_Token` 头携带（下划线命名，原因见下）；本地脚本在 `iot` 和 `emqx` 两个 namespace 各建一份同名 Secret；ACL 按角色授予（全部经 `withACLGuard` 追加 `{deny, all, #}` 兜底）：telemetry-ingestor 订阅 `tenant/+/device/+/telemetry`、device-worker 订阅 `tenant/+/device/+/ack` + 发布 `tenant/+/device/+/command`、设备账号 `eq` 本设备三条 topic、demo 账号（`iot-demo-<tenant>-<ts>` clientID）收敛到单租户通配（由 clientID 提取租户，提取失败拒绝）。
- EMQX 6.3.1 回调 token 注入（三个坑，均已验证）：① authn HTTP 模板**不解析** `${VAR}` 环境变量占位符（`auth_template_invalid` 告警后原样传递字面量）；② env 覆盖名只允许 `[A-Z0-9_]`，`EMQX_AUTHENTICATION__1__HEADERS__x-iot-auth-token`（连字符）被**静默丢弃**，`...__x_iot_auth_token`/`...__X_IOT_AUTH_TOKEN`（小写/大写 key）报 `unknown_env_vars` 被拒——**按 key 覆盖 headers 不可行**；③ 唯一可行方案：`EMQX_AUTHENTICATION__1__HEADERS` 整体 JSON 覆盖 headers map，值存 emqx namespace `iot-runtime-secrets` 的 `EMQX_AUTHN_HEADERS_JSON`（`helm-deploy-local.sh` 生成）。清单里 config.data 的 headers 字面值只是必定被 iot-core 拒绝的 fallback。
- EMQX ACL 双防线：① iot-core 回调每条角色 ACL 末尾 `withACLGuard` 追加 deny-all；② ConfigMap `emqx-acl` 挂载自定义 `acl.conf`（`{deny, all}.` 兜底）覆盖默认文件——默认 acl.conf 末尾的 `{allow, {security_profile, legacy}}` 在 legacy profile（EMQX 6.3 默认）下会放行一切。**不启用 hardened profile**：hardened 限制 authn HTTP 模板占位符，与回调 token 注入冲突。
- EMQX 6.3 ACL topic 语法（emqx_authz_rule.erl 验证）：仅支持 `eq ` 前缀（精确匹配）；**`match ` 前缀已废弃**——会被当作字面 topic 层级导致规则永不匹配。共享订阅在 authz 前被解包成裸 filter（`$share/g/t/#` → `t/#`），规则必须写裸 filter，authz 层无法区分共享/普通订阅。
- MQTT ACL 测试断言坑：paho 的 QoS1 Publish token 与 Subscribe token 都**不会**把 broker 拒绝（PUBACK/SUBACK 0x87/0x80）返回为 error；订阅断言须读 `SubscribeToken.Result()[topic] == 0x80`，发布断言须查 EMQX 日志 `authorization_source_denied`/`cannot_publish_to_topic_due_to_not_authorized`。
- 含凭据 DSN 入 Secret：`POSTGRES_DSN`/`TDENGINE_DSN` 不再出现在 ConfigMap 和 `values.yaml`，由 `iot-runtime-secrets`（`security.existingSecret`）注入，各 Deployment `envFrom` 同时引用 ConfigMap 与 Secret；`helm-deploy-local.sh` 自动把 `IOT_POSTGRES_DSN`/`IOT_TDENGINE_DSN`（有默认值）写入 Secret。
- REST 摄入校验：`IngestTelemetry`/`RecordTelemetry` 在落库前执行 `contracts.ValidateEnvelope`，非法 envelope 直接报错，不再产生绕过校验的 poison message。
- TDengine 转义：`escapeTD` 先转义反斜杠再转义单引号（TDengine 中 `\` 是转义字符）。
- Kafka 消费起点：新消费者组从 `FirstOffset` 启动重放积压（幂等消费兜底），避免 `LastOffset` 丢宕机期间消息。
- DLQ 重放：`cmd/dlq-replay` 支持 `-stage` 按阶段过滤、`-dry-run` 只检查不发布不提交（不匹配的 offset 不提交，重启会重扫）。
- MQTT 订阅默认值：`telemetry-ingestor` 用 `$share/iot-telemetry/...`，`device-worker` 用 `$share/iot-device-worker/...`（多副本负载均衡）。
- NetworkPolicy：`iot-core-mqtt-auth` 限制 9090 仅 `emqx` namespace 可达，9001/9101 仅本 namespace。
- gRPC mTLS：`iot-core:9001` 支持双向 TLS，由 `IOT_CORE_TLS_CERT`/`IOT_CORE_TLS_KEY`/`IOT_CORE_TLS_CA`（+客户端 `IOT_CORE_TLS_SERVER_NAME`）环境变量驱动，实现在 `internal/platform/grpc_tls.go`；三变量全空保持明文（本地裸跑兼容），部分设置启动报错。Helm 用 `grpcTLS.enabled=true` 开启，Secret `iot-grpc-tls`（含 ca/server/client 五件套）挂载到 `/etc/iot/grpc-tls`；`scripts/helm-deploy-local.sh` 默认启用并自动调 `scripts/gen-grpc-certs.sh` 生成本地自签证书（`deploy/grpc-certs/`，已 gitignore），`GRPC_TLS_ENABLED=0` 可关闭。证书轮换后需重启 Pod。
- Kafka topics：`iot.telemetry`、`iot.command`、`iot.dlq`。
- TDengine：超级表 `telemetry_v2`，按设备建立子表；完整 payload 的权威副本为 PostgreSQL JSONB。`telemetry_records.tdengine_written` 记录 TDengine 完成状态，DLQ 重放只补偿未完成的 TDengine 写，PG/TDengine 均幂等。
- HTTP 遥测幂等重发：`IngestTelemetry` 无论首次写入还是命中唯一约束（重复），都会发布 Kafka——因为上一次可能 PG 写成功但 Kafka 失败（broker 故障），若重复分支跳过 Kafka，该数据将永远不进 worker/TDengine。重复消费由 worker 的 `tdengine_written` 补偿兜住，重发安全。
- 命令状态机：`created → published（Kafka 写入）→ sent（MQTT 下发成功，启动 ACK deadline）→ acked/timeout/failed`。**published 恢复**：dispatcher 每轮先跑 `RecoverStaleCommands`——`published` 超过 timeout（默认 5m）未流转的重置为 `created` 重新派发（记 `requeued` 事件），`dispatch_attempts >= 10` 的置 `failed`（重派安全：`MarkCommandSent`/`AckCommand` 幂等守卫兜底）。**竞态处理**：dispatcher 先发 Kafka 再 `MarkCommandPublished`，worker 可能在 published 落库前完成 MQTT 下发——`MarkCommandSent` 因此接受 `status IN ('created','published')`，保证命令必到 `sent` 且有 deadline；随后 dispatcher 的 `MarkCommandPublished`（要求 `created`）命中 0 行，不会把 `sent` 降级。
- 命令列表：`GET /api/v1/commands?tenantId=<tenant-id>` 必须指定租户，分页使用 `pageSize` 和 opaque `cursor`。
- 列表分页：`/api/v1/tenants`、`/api/v1/devices`、`/api/v1/devices/{t}/{d}/telemetry`、`/api/v1/commands` 均为 keyset 分页（`pageSize` 1-100，响应 `{items, nextCursor}`）。**注意**：`NormalizePageRequest(size, cursor)` 只返回 Size/Cursor，会丢弃 TenantID/DeviceID——带过滤器的分页方法必须先保存过滤器再恢复（参照 `ListCommandsPage`/`ListTelemetryPage` 的 `page.TenantID = tenantID` 模式），否则查询条件丢失返回空。
- REST 写接口仅 `management-api` 暴露；`telemetry-ingestor`/`device-worker` 只提供 `/healthz` 和 `/metrics`。
- 生产环境禁止使用 `latest`，使用不可变镜像版本或 digest。

## 本地运行

```bash
docker compose -f monitoring/docker-compose.yml up -d
scripts/helm-deploy-local.sh   # 先跑：在 iot 和 emqx 两个 namespace 各建 iot-runtime-secrets（含 EMQX_AUTHN_HEADERS_JSON）
kubectl apply -f deploy/emqx/cluster.local.yaml  # 再跑：部署/更新 EMQX CR
```

**EMQX token 注入（关键）**：见"关键配置"的 EMQX 6.3.1 三条坑。`render-local.sh`（envsubst 方案）已删除；生产 `cluster.yaml` 与本地同机制，core 和 replicant 模板都挂 acl.conf 并引用 `EMQX_AUTHN_HEADERS_JSON`（客户端连接落在 replicant 上，authn 在那里执行）。

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
- 本次加固（2026-09-21）：iot-core gRPC 接入 mTLS（凭证加载器 + 服务端 `grpc.Creds` + 客户端 `zrpc.WithTransportCredentials` + 证书脚本 + Helm 开关）。已通过 `go build ./...`、`go vet`、`go test ./...`（含 net.Pipe 真实 mTLS 握手与无证书拒绝用例）、`helm template` 开关两种模式渲染校验、证书脚本 openssl 生成/校验。
- 本次加固（2026-09-21，二轮审查修复）：H1 命令 `published` 断链恢复（`RecoverStaleCommands` 超时重排队 + 次数上限置 failed）、消费者组 `FirstOffset`、dlq-replay `-stage`/`-dry-run`；H3 `escapeTD` 反斜杠转义、REST 摄入 `ValidateEnvelope`；H5 DSN 移出 ConfigMap/values 入 Secret、`.env.example` 补密钥项、demo ACL 收敛到单租户。已通过 `go build ./...`、`go vet ./...`、`go test ./...`、`helm template`（ConfigMap 无 DSN、envFrom 引用 Secret 渲染正常）。

## 文档入口

- [README](README.md)
- [生产部署指南](docs/生产部署指南.md)
- [EMQX 部署清单](deploy/emqx/README.md)
- [OpenAPI](docs/openapi.json)
- [MQTT Schema](docs/mqtt-envelope.schema.json)
- [架构 ADR](docs/adr/)
