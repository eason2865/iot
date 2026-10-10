# IoT 当前交接信息

> 本文件只记录当前有效架构、运行状态、验证结果和操作入口。历史迁移过程不在这里保留。

## 全链路故障验证追加修复（2026-10-10，版本 2.42 验证通过）

- 使用实际部署 management-api/iot-core、EMQX、Kafka 和双存储验证完整遥测/命令/ACK链路、非法输入DLQ、幂等和活跃worker Pod故障接替；隔离租户 closed-20261010-160635，30条接替期间遥测全部写入PG/TD，新命令ACK成功。
- 对测试命令单行持有PG锁，触发已部署worker的3次ACK超时，6.62s后进入command.ack DLQ；解除锁后实际dlq-replay CLI补ACK成功；dry-run未移动独立验证group位点。未中断数据库服务，测试锁已释放。
- 该注入暴露串行MQTT ACK回调阻塞后续入站包，造成其他命令PUBACK超时。新增真实MQTT协议回归测试先失败，修复worker SetOrderMatters(false)后通过；PG行锁维持重复ACK幂等。
- Pod重建发现scratch镜像非root进程无法创建/tmp，tracing exporter启动报错。四个Helm Deployment添加fsGroup=65534及128Mi emptyDir挂载/tmp，保留非root；本地trace随Pod删除，需要持久化时使用外部OTLP。
- 验证证据及临时运行器位于被gitignore的 outputs/closed-loop-20261010/。
- 2.42已部署：Helm revision 4，镜像iot-app:local-21f4cc7aea70，8个业务Pod全Ready、0重启；Prometheus 9目标全up，Grafana→Prometheus健康检查OK，DLQ写入失败计数0。
- 修复后实际服务闭环：隔离租户closed-20261010-161645，33条遥测全部PG/TD完成；5个命令4个acked、1个有意超时。再次删除活跃worker Pod，30/30遥测及命令ACK闭环通过。
- ACK单行锁注入期间另一命令在0.99s内完成下发/设备ACK；原ACK重试6.63s进入死信，dry-run位点不变，真实CLI重放后acked，ACK记录与审计各1，设备重下发0。测试锁已释放。
- go test -count=1 ./...、go vet ./...、make fmt-check、三包-race、Helm两种TLS配置渲染/lint、Compose配置及git diff --check通过；新MQTT协议回归测试修复前失败、修复后通过。
- 各Pod trace文件存在且UID/GID=65534；management-api和iot-core trace有实际内容。全局超期未完成遥测=0，sent无deadline=0。


## 上一轮修复（2026-10-10，发布版本 2.41）

- 接续用户确认的现有未提交改动，完成租户 Token 鉴权、逐 Pod 指标、命令终态防重投、ACK 有限重试/死信重放、遥测补偿和旧命令 NULL deadline 收敛。
- 租户 Token `MANAGEMENT_API_TOKENS` 是 JSON token→tenant 映射（本地脚本用 `IOT_MANAGEMENT_API_TOKENS`），写接口只按实际 body 租户鉴权；全局租户/设备列表拒绝租户 Token，任意 query 不能覆盖 body；必须有 Bearer scheme，配置错误启动失败。
- Prometheus 改为 namespace=iot 的 Pod discovery + apiserver proxy，kind 网络内连 desktop-control-plane:6443，校验挂载 CA，TLS server_name=localhost；job 仍为服务名，instance 为 Pod 名。首次启动先执行 scripts/port-forward-local-monitoring.sh，生成被 gitignore 的 k8s-token/k8s-ca.crt。旧 4 个指标 forward 容器不再需要。
- 发现并修复本地 containerd busybox:1.36 错指 Prometheus digest；重新拉取、导入 busybox，并将 initContainer 与 netcheck 固定到 sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662。
- 四个服务均有 PDB；gateway /readyz 检查 gRPC/SQL，ingestor、worker /readyz 检查 SQL；/healthz 仍为存活检查。
- worker 发送前查 PostgreSQL 并校验消息 tenant/device，sent/acked/timeout/failed 或未知/伪造身份跳过；数据库错误、发送后状态写失败、消费位点提交失败会停止消费，避免假成功。依旧 at-least-once，设备必须按 commandId 去重。
- ACK 数据库调用单次最多 2s、最多 3 次，失败写 command.ack DLQ（限时 5s）。dlq-replay 用 POSTGRES_DSN 直接补 ACK，不向 MQTT topic 名的 Kafka topic 写入；mqtt.kafka 会转成 TelemetryRecord，发送 KAFKA_TELEMETRY_TOPIC；非法 MQTT decode/identity 不可盲目重放。
- 遥测补偿：30s/500条，received_at 早于 2min，部分索引 idx_telemetry_unwritten + ID 游标，SQL 在 LIMIT 前过滤 tenantIDs，游标扫完归零。失败保留标记等待后续，控制字符记录不阻塞其余记录。多个 worker 重复补写仍按现有设备时间戳幂等；同设备同 ts 的不同消息冲突语义未改变。
- 租户 worker 的 Kafka group 从去重排序的 tenant 集合 SHA-256 派生；改变集合会建立新消费位点并重读保留历史，勿部署重叠集合。ACK 仍由共享订阅统一处理。
- 旧 sent+NULL deadline 按 updated_at + COMMAND_ACK_TIMEOUT 收敛为 timeout，不重发设备动作。
- 数据库在本轮开始前已由外部操作清理旧记录，本轮基线 pending=0、sent+NULL deadline=0，不能声称恢复了上次分析的 27 万遥测/418 条命令。
- E2E 主链路测试改为使用已部署 ingestor/worker，不再额外启动不同 consumer group 来双发同一设备命令；补偿、NULL deadline 和 ACK replay 分别增加真实存储验证。
- 可靠性边界和取舍见 docs/adr/0007。HPA、跨节点/地域 HA、TDengine 业务数值列与数据生命周期仍是独立生产规划项。

- 最终验证：go test -count=1 ./...、go vet ./...、平台/网关/核心 go test -race、helm lint、Compose 配置和 promtool 均通过。真实 E2E（遥测/命令/ACK + ACK 后事件重投 + NULL deadline + 无 Kafka 事件遥测补偿 + PostgreSQL ACK 重放）通过。
- 部署：Helm iot revision 3，业务镜像 iot-app:local-bc5c9e1f4bca（对应 host iot-app:2.41），8 个 Pod Ready、4 个 PDB；Prometheus 9 个目标（8业务Pod+demo）全部 up，旧4个指标forward容器已移除。demo 已更新至2.41。
- 线上租户鉴权验证：本租户200，跨租户/全局列表/query-body绕过403，管理Token200；临时验证Token已移除并重启gateway。最终 overdue遥测=0、sent无deadline=0。Kafka仍为1分区，2副本提供故障接替。

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
- REST 摄入校验：`IngestTelemetry`/`RecordTelemetry` 在落库前执行 `contracts.ValidateEnvelope`，非法 envelope 直接报错，不再产生绕过校验的 poison message。**`ts` 有区间校验**（2026-10-08 加）：小于 2000-01-01（`946684800000` ms）或超过服务端时间 +5 分钟一律拒绝，错误为 `contracts.ErrEnvelopeTimestampOutOfRange`（包装 `ErrInvalidEnvelope`，`errors.Is` 兼容）。这同时把"设备误把秒当毫秒"（落到 1970）从静默写入变成显式拒绝。校验**只在摄入侧**（MQTT bridge / REST）执行，worker 消费 Kafka 和 DLQ 重放不再校验，历史消息不会因"太旧"被拒。
- device-worker 租户隔离：`DEVICE_WORKER_TENANT_IDS`（CSV，默认空=处理全部租户，保持旧行为），Helm 对应 `deviceWorker.tenantIDs`。订阅仍是通配，过滤在 worker 内做，被跳过的消息记 result=`filtered` 并打日志（`telemetry skipped`/`command skipped`）。
- TDengine 写入两层防护：`escapeTD` 先转义反斜杠再转义单引号（TDengine 中 `\` 是转义字符；taosRestful 驱动无参数化，`InterpolateParams` 对字符串零转义）；第二层 `validateTDField` 逐字节拒绝 `<0x20`/`0x7f` 控制字符，`writeRecord` 开头校验 TenantID/DeviceID/MsgID/Type/Version，拒绝即进 DLQ（stage `telemetry.tdengine`，error `tdengine field contains control characters`），拦截转义无法表达的字符。
- Kafka 消费起点：新消费者组从 `FirstOffset` 启动重放积压（幂等消费兜底），避免 `LastOffset` 丢宕机期间消息。
- DLQ 可见性与保留（2026-10-08 起）：每次死信写入都计入 `iot_dlq_publish_total{stage,result}`（`result=error` 表示**消息没被保全**，是阻塞消费/crash loop 的状态，不只是普通错误）；stage 收敛为 `platform.Stage*` 常量 + `DeadLetterStages()`，指标对每个 stage 预置序列，`cmd/dlq-replay -stage` 的帮助文本也从该列表生成（此前写的是 `tdengine`/`kafka-publish` 这类不存在的值）。死信保留由 `KAFKA_DLQ_RETENTION_MS` 约束（默认 7 天，Helm `kafka.dlqRetentionMs`），只在本发布创建 `iot.dlq` 时生效。告警模板：`monitoring/grafana/alerts/dlq-writes.json`（warning）与 `dlq-write-failures.json`（critical），均按 Grafana API 手工导入（非文件 provision），且**目前只关联 iot-pipeline 看板、无 panel**——该看板尚无死信面板。
- DLQ 重放（人工，策略见 `docs/adr/0006`）：`cmd/dlq-replay` 支持 `-stage` 过滤、`-limit`、`-dry-run`、`-idle-timeout`。**每个 stage 用独立 consumer group**（`iot-dlq-replay-<stage>`，`-group` 可覆盖）：kafka-go 的 `StartOffset` 只在"分区没有已提交 offset"时生效，所以共用单一 group 时，提交任意一条匹配记录就会把 group 位置推过它之前的所有记录，之后针对其他 stage 的运行将**永远看不到**那些记录（此前代码正是共用 `iot-dlq-replay` 单个 group）。`-dry-run` 现在真正零副作用——**不提交任何 offset**（此前 malformed/invalid-payload 分支在 dry-run 下也会提交）。topic 静默 `-idle-timeout`（默认 5s）后正常退出并打印 scanned/replayed/skipped（此前会阻塞到 10 分钟总超时再 `log.Fatal`，让"只重放了 3 条"看起来像失败）。
- MQTT 订阅默认值：`telemetry-ingestor` 用 `$share/iot-telemetry/...`，`device-worker` 用 `$share/iot-device-worker/...`（多副本负载均衡）。
- NetworkPolicy：`iot-core-mqtt-auth` 限制 9090 仅 `emqx` namespace 可达，9001 仅本 namespace；9101（metrics）放开为所有 namespace（2026-10-09 起，否则外部 namespace 的 Prometheus 无法抓取）。kind 的 kindnet 不执行 NetworkPolicy，仅真实集群生效。
- gRPC mTLS：`iot-core:9001` 支持双向 TLS，由 `IOT_CORE_TLS_CERT`/`IOT_CORE_TLS_KEY`/`IOT_CORE_TLS_CA`（+客户端 `IOT_CORE_TLS_SERVER_NAME`）环境变量驱动，实现在 `internal/platform/grpc_tls.go`；三变量全空保持明文（本地裸跑兼容），部分设置启动报错。Helm 用 `grpcTLS.enabled=true` 开启，Secret `iot-grpc-tls`（含 ca/server/client 五件套）挂载到 `/etc/iot/grpc-tls`；`scripts/helm-deploy-local.sh` 默认启用并自动调 `scripts/gen-grpc-certs.sh` 生成本地自签证书（`deploy/grpc-certs/`，已 gitignore），`GRPC_TLS_ENABLED=0` 可关闭。证书轮换后需重启 Pod。
- Kafka topics：`iot.telemetry`、`iot.command`、`iot.dlq`。**生产者一律 `RequiredAcks: RequireAll`**（2026-10-08 起，此前业务写为 `RequireOne`）：PG 先写、Kafka 后发，`RequireOne` 下 leader 在副本同步前宕机就会丢掉 PG 已认为发布成功的事件，DLQ 只兜消费侧失败、兜不住这种。topic 由本服务创建时按 `KAFKA_TOPIC_REPLICATION_FACTOR`（默认 1）与 `KAFKA_TOPIC_MIN_INSYNC_REPLICAS`（默认 1，Helm 为 `kafka.topicReplicationFactor`/`topicMinInsyncReplicas`）设置；**生产必须 RF≥3、min.insync.replicas≥2，否则 acks=all 等价于 acks=1**。注意 `CreateTopics` 对已存在 topic 是幂等跳过（kafka-go 跳过 `TopicAlreadyExists`），所以分区数/RF 只在首次创建生效；但 config entries（min.insync.replicas、retention.ms）的漂移自十六轮起由 `reconcileKafkaTopics` 自动用 `AlterConfigs` 修正，分区/RF 漂移只告警。详见 `docs/adr/0004`。
- TDengine：超级表 `telemetry_v2`，按设备建立子表；完整 payload 的权威副本为 PostgreSQL JSONB。`telemetry_records.tdengine_written` 记录 TDengine 完成状态，DLQ 重放只补偿未完成的 TDengine 写，PG/TDengine 均幂等。**库名可配**（2026-10-09 起）：`TDENGINE_DATABASE`（默认 `iot`），SQL 全部用限定名 `db.table`（`CREATE STABLE db.table`/`INSERT INTO db.child USING db.stable`），不再依赖 DSN 中的默认库。
- HTTP 遥测幂等重发：`IngestTelemetry` 无论首次写入还是命中唯一约束（重复），都会发布 Kafka——因为上一次可能 PG 写成功但 Kafka 失败（broker 故障），若重复分支跳过 Kafka，该数据将永远不进 worker/TDengine。重复消费由 worker 的 `tdengine_written` 补偿兜住，重发安全。
- 命令状态机：`created → published（Kafka 写入）→ sent（MQTT 下发成功，启动 ACK deadline）→ acked/timeout/failed`。**published 恢复**：dispatcher 每轮先跑 `RecoverStaleCommands`——`published` 超过 timeout（默认 5m）未流转的重置为 `created` 重新派发（记 `requeued` 事件），`dispatch_attempts >= 10` 的置 `failed`（重派安全：`MarkCommandSent`/`AckCommand` 幂等守卫兜底）。**立即重排（2026-10-09 起）**：worker MQTT 下发失败（含 5s publish 超时）立即调 `RequeueCommand`（只匹配 `status='published'`，backoff `1s << min(attempts,6)` 即 2,4,8…64s，记 `requeued` 事件），不再干等 dispatcher 5 分钟恢复兜底；attempts 上限 10 仍由 `RecoverStaleCommands` 置 `failed`。**竞态处理**：dispatcher 先发 Kafka 再 `MarkCommandPublished`，worker 可能在 published 落库前完成 MQTT 下发——`MarkCommandSent` 因此接受 `status IN ('created','published')`，保证命令必到 `sent` 且有 deadline；随后 dispatcher 的 `MarkCommandPublished`（要求 `created`）命中 0 行，不会把 `sent` 降级。
- 命令接口统一租户隔离（2026-10-08 起）：列表 `GET /api/v1/commands?tenantId=` 与详情 `GET /api/v1/commands/{id}?tenantId=` 都必须指定租户，ACK 为 `id`+`tenantId`+`deviceId`。`GetCommandRequest` 已加 `tenant_id`（proto 字段 2），iot-core 校验 `command.TenantID == tenantId`，不匹配返回 **NotFound**（与"不存在"不可区分，避免泄露他租户数据存在性）。详见 `docs/adr/0005`。
- **错误映射按 gRPC status code，不再按消息文本**：仓储返回类型化哨兵 `platform.ErrNotFound`/`ErrAlreadyExists`，iot-core 经 `mapRepoError` 转 status code，adminapi 经 `httpStatusFromGRPC` 转 HTTP（InvalidArgument→400、NotFound→404、AlreadyExists→409、Unavailable→502、其余→500）。此前靠 `strings.Contains`，导致 `"tenantId contains invalid MQTT topic characters"` 和 `"command does not belong to device"` 都掉进默认分支返回 **502**。响应体只含 core 的消息，不带 `rpc error: code = ... desc = ...` 外壳。
- 列表分页：`/api/v1/tenants`、`/api/v1/devices`、`/api/v1/devices/{t}/{d}/telemetry`、`/api/v1/commands` 均为 keyset 分页（`pageSize` 1-100，响应 `{items, nextCursor}`）。**注意**：`NormalizePageRequest(size, cursor)` 只返回 Size/Cursor，会丢弃 TenantID/DeviceID——带过滤器的分页方法必须先保存过滤器再恢复（参照 `ListCommandsPage`/`ListTelemetryPage` 的 `page.TenantID = tenantID` 模式），否则查询条件丢失返回空。
- REST 写接口仅 `management-api` 暴露；`telemetry-ingestor`/`device-worker` 只提供 `/healthz` 和 `/metrics`（业务 REST 在 `platform.App` 中是显式 opt-in 的 `EnableBusinessAPI`，不从服务名推断，bootstrap 不开启；生产 REST 只由 adminapi 提供且带 Bearer 校验）。
- 生产环境禁止使用 `latest`，使用不可变镜像版本或 digest。

## 本地运行

```bash
docker compose -f monitoring/docker-compose.yml up -d
scripts/helm-deploy-local.sh   # 先跑：在 iot 和 emqx 两个 namespace 各建 iot-runtime-secrets（含 EMQX_AUTHN_HEADERS_JSON）
kubectl apply -f deploy/emqx/cluster.local.yaml  # 再跑：部署/更新 EMQX CR
```

**EMQX token 注入（关键）**：见"关键配置"的 EMQX 6.3.1 三条坑。`render-local.sh`（envsubst 方案）已删除；生产 `cluster.yaml` 与本地同机制，core 和 replicant 模板都挂 acl.conf 并引用 `EMQX_AUTHN_HEADERS_JSON`（客户端连接落在 replicant 上，authn 在那里执行）。

**EMQX 单节点滚动更新**：本地单节点 license 不允许滚动更新时瞬时双 core（报 `SINGLE_NODE_LICENSE` 崩溃）。改 EMQX CR 后需 `kubectl delete sts -n emqx --all` 让 Operator 重建单节点。

一键脚本默认检查 PostgreSQL、Kafka、EMQX 和 TDengine 的可达性，然后只部署 `iot` namespace 中的业务服务。API/MQTT 端口转发由 Compose 管理，指标由 Prometheus 逐 Pod proxy 抓取。

**Docker Desktop 新版（kind v1.36.1+）变化（2026-10-10 实记）**：① cloud-provider-kind（`kindccm-*` 容器）会把 LoadBalancer 类型 Service 直接发布到宿主机——`emqx-listeners`（1883/8883/8083/8084）和 `emqx-dashboard`（18083）均已被它占用，Compose 里 `k8s-forward-emqx-dashboard` 已删除、`k8s-forward-emqx-listeners` 不再绑宿主机端口（仅保留容器名供 docker 网络内 demo 访问）；宿主机访问 EMQX 直接用 127.0.0.1:1883/18083。② forward 容器挂的 kubeconfig 是 `monitoring/.env` 里 `KUBECONFIG_HOST_PATH` 指定的 `~/.kube/iot-local.config` **静态副本**——Docker Desktop 升级重建集群（控制面节点换 kindest 版本，etcd 数据保留）后 CA 轮换，该副本过期导致全部 forward 容器 x509 崩溃，需 `kubectl config view --raw --minify --flatten > ~/.kube/iot-local.config` 刷新后 `docker compose up -d --force-recreate`。③ 集群重建后 EMQX Operator 需重装：`curl -fsSL https://ghfast.top/https://github.com/emqx/emqx-operator/releases/download/2.3.0/install.yaml | kubectl apply --server-side=true -f -`（github 直连不通时走 ghfast.top 镜像）。

常用地址：

- Management API：`http://localhost:18080`
- Demo：`http://localhost:18084`
- Prometheus：`http://localhost:9090`
- Grafana：`http://localhost:3000`
- EMQX Dashboard：`http://localhost:18083`

## 当前本地资源

- Host Docker 镜像：当前业务镜像 `iot-app:2.41`，供 Compose demo 使用。**重建镜像前必须先 `make build`**——Dockerfile 是 `FROM scratch` 直接 COPY `bin/` 预编译二进制，`docker build` 本身不编译 Go。
- Kubernetes containerd：当前业务镜像 `iot-app:local-bc5c9e1f4bca`（保留上一版供回滚）。
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
- 新增 REST 字段时同步更新 protobuf、README 和 `contracts.CommandResponse`（命令的 REST 形状）。**REST 接口没有 OpenAPI 文档**（2026-10-08 已删除，见下），路由表以 `internal/adminapi` 为唯一事实来源。`docs/mqtt-envelope.schema.json` 是 MQTT 契约的唯一人工维护源：Go 侧通过 `docs/contracts.go` 的 `//go:embed` 引入，`internal/contracts` 只做解码，**不要在 Go 里再写第二份 schema**（`internal/contracts/docs_test.go` 会比对服务端返回与磁盘文件）。
- 修改代码或 SQL 后更新本文件，执行测试、部署验证、提交并推送。
- 本次变更（2026-10-10，Docker 镜像版本治理）：① compose 7 个 port-forward 容器 `rancher/kubectl` v1.26.8 → v1.34.9（与 kind 集群 v1.34.3 同 minor，±1 兼容规则内）；② Docker Desktop 内部组件升级：`desktop-containerd-registry-mirror` v0.0.3→v0.0.4、`desktop-cloud-provider-kind` v0.5.0→v0.7.0（方法：删容器后重启 Docker Desktop 自动按新版重建；旧 tag 已 rmi）；③ 删除 envoyproxy/envoy v1.36.4/v1.36.7 残留；④ Docker Desktop 已配置手动代理 `127.0.0.1:7897`（settings-store.json `ProxyHttpMode: manual`，拉 Docker Hub 必需，直连超时；Clash 关闭时拉取会失败）；⑤ **重启坑**：Docker Desktop 重启后 cloud-provider 会创建 `kindccm-*` envoy LB 容器抢占宿主 1883/18083，导致两个 emqx forward 容器起不来——先 `docker rm -f kindccm-*` 再 `docker compose up -d`；另 `docker restart` forward 容器可能丢 kind 网络（DNS network unreachable），须用 `docker compose up -d --force-recreate` 重建。已知残留：`kindest/node` 仍是 v1.34.3（v1.36.1 已预拉取），升级需重置 Kubernetes 集群（会清空 iot/emqx 全部工作负载并需完整重新部署），暂不动。已验证：iot/emqx 全 pod Running、7 个 forward Up、management-api 200、EMQX dashboard 200、demo 200。
- 本次加固（2026-09-20）：MQTT 认证回调加共享密钥头、ACL 覆盖共享/普通订阅、DLQ 重放遥测幂等（基于 `telemetry_records` 唯一约束）、`migrations/001_init.sql` 与 `ensureSchema` 对齐、TDengine 默认表名统一为 `telemetry_v2`。已通过 `go build ./...`、`go test -count=1 ./...`、`make fmt-check`、`make build`。
- 本次加固（2026-09-21）：iot-core gRPC 接入 mTLS（凭证加载器 + 服务端 `grpc.Creds` + 客户端 `zrpc.WithTransportCredentials` + 证书脚本 + Helm 开关）。已通过 `go build ./...`、`go vet`、`go test ./...`（含 net.Pipe 真实 mTLS 握手与无证书拒绝用例）、`helm template` 开关两种模式渲染校验、证书脚本 openssl 生成/校验。
- 本次加固（2026-09-21，二轮审查修复）：H1 命令 `published` 断链恢复（`RecoverStaleCommands` 超时重排队 + 次数上限置 failed）、消费者组 `FirstOffset`、dlq-replay `-stage`/`-dry-run`；H3 `escapeTD` 反斜杠转义、REST 摄入 `ValidateEnvelope`；H5 DSN 移出 ConfigMap/values 入 Secret、`.env.example` 补密钥项、demo ACL 收敛到单租户。已通过 `go build ./...`、`go vet ./...`、`go test ./...`、`helm template`（ConfigMap 无 DSN、envFrom 引用 Secret 渲染正常）。
- 本次清理（2026-10-08，死代码）：删除 `internal/server` 包（仅被自身测试引用）、`errMQTTBridgeDLQ` 死变量、TDengine writer 从未启动的批处理路径（`pendingCh`/`closedCh`/`run()`/`flushInterval`/`batchSize`/`wg`，`WriteTelemetry` 改为 `writeRecord` 逐条写）。**TDengine 不引入内存攒批**：worker 是"写入成功才提交 Kafka offset"，接批处理会把语义变成"入队即提交"，凭空新增丢数据窗口；吞吐靠 TDengine 侧连接能力而非攒批。`internal/platform/handlers.go` 与 `memory_store.go` 在生产路径无调用方，但是单元测试和 `e2e_test.go`/`e2e_load_test.go` 两个真实 E2E 的 HTTP harness，故保留并加注释标注，未删除。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test ./...`。
- 本次加固（2026-10-08，二轮：业务 REST 改为显式 opt-in）：`platform.App` 的业务路由（`/api/v1/*`）**没有任何鉴权中间件**（Bearer 校验只在 `internal/adminapi`），而 `platform.New` 原来用 `cfg.EnableBusinessAPI || cfg.ServiceName == "management-api"` 隐式开启——任何名字叫 `management-api` 的服务走 bootstrap 就会无鉴权暴露租户/设备/遥测/命令写接口。现改为：① `platform.New` 只认 `EnableBusinessAPI`，不再从服务名推断；② `bootstrap.Run` 不再设置该开关（注释说明原因），worker 只暴露 `/healthz` 与 `/metrics`；③ 8 处测试调用（`platform_test.go` ×5、`docs_test.go`、`e2e_test.go`、`e2e_load_test.go`）改为显式传 `EnableBusinessAPI: true`，单测统一走新增 helper `newBusinessAPIApp`；④ 新增回归用例 `TestBusinessAPIIsOptIn`：名字为 management-api/device-worker/telemetry-ingestor/iot-core 时业务路由必须 404、`/healthz` 仍 200，且显式开启后仍 200。已用"临时恢复隐式分支 → 用例变红 → 恢复"验证该用例真的拦得住。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`。
- 本次重构（2026-10-08，三轮：契约单一来源）：`internal/contracts/docs.go` 原来用 ~270 行 Go map 字面量重写了 `docs/mqtt-envelope.schema.json`（OpenAPI 部分见四轮，已删除）。现改为 `docs/contracts.go`（`package docs`，`//go:embed`，返回 `bytes.Clone` 防篡改）承载 embed——`go:embed` 不能跨目录，所以 embed 宿主包必须和 JSON 同目录，故放在 `docs/`；`internal/contracts/docs.go` 缩到 32 行，只把 embed 字节解码成 `map[string]any`。已双向验证：只改 JSON 不改 Go → 服务端返回值随之变化；故意在 Go 侧注入漂移 → 用例变红。
- 本次变更（2026-10-08，四轮：删掉 OpenAPI + 修 Command 字段泄漏）：① **删除 OpenAPI**——`docs/openapi.json`、`/openapi.json` 路由（adminapi + platform harness）、`contracts.OpenAPISpec()`、`docs/contracts.go` 的对应 embed、`BearerTokenMiddleware` 的免鉴权白名单条目、`routeLabel` 分支，以及 README（中英各 6 处）、`production-deployment*.md`、本文件的引用。理由：仓库内**零消费方**（无 codegen、demo 用自写 HTTP 客户端、prototype 不读它），且它是免鉴权公开的完整 API 地图，而 management-api 定位是运营方内部接口。同时发现 spec 当时**已漏字段**（Command 的 `dispatchAttempts`/`deadlineAt`），留着会误导。MQTT Schema 保留。② **修 Command 字段泄漏**：`platform.Command` 带 `json:"dispatchAttempts"`（无 omitempty）和 `json:"deadlineAt,omitempty"`（Go 的 omitempty 对 `time.Time` 无效），但 `adminapi` 的 `commandFromPB` 只填 protobuf 里有的 7 个字段，导致每个命令响应都带 `"dispatchAttempts":0` 和 `"deadlineAt":"0001-01-01T00:00:00Z"` 两个永远无意义的内部字段。新增 `contracts.CommandResponse`（只有 7 个公开字段），`adminapi` 与 `platform` harness 共用同一形状（避免两套 REST 各写一份 DTO）；Kafka 事件仍用 `platform.Command`，worker 不受影响。新增 `TestCommandResponseHidesInternalDispatchFields` 锁定该形状。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`make build`。

- 本次变更（2026-10-08，五轮：三个小项）：① `ts` 区间校验（见"关键配置"），新增 `TestValidateEnvelopeTimestampBounds` 覆盖时钟偏移/下限/秒当毫秒/0/负数；② **dispatcher 优雅停机**：`internal/core/server.go` 改用 `signal.NotifyContext(SIGINT/SIGTERM)`，`Run(ctx)` 替代 `Run(context.Background())`，新增 `TestCommandDispatcherRunStopsOnContextCancel` 锁定"cancel 后必须返回"；③ **接线 `DEVICE_WORKER_TENANT_IDS`**（bootstrap → `WorkerConfig.TenantIDs`），补 Helm `deviceWorker.tenantIDs` + ConfigMap 键（`helm template` 空/非空两种模式已验证，`helm lint` 通过）、`.env.example` 与 README 双语文档，新增 `TestWorkerTenantAllowlistFromEnv` 锁定"空值=不限制"。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`helm lint charts/iot`。

- 本次变更（2026-10-08，六轮：Kafka 持久性 + 命令租户隔离 + 错误码）：① 生产者 `RequireAll` + topic RF/min.insync.replicas 可配（`KafkaTopicConfig`/`buildTopicConfigs`，新增 `TestBuildTopicConfigs`）；② `GetCommandRequest` 加 `tenant_id`（用 `protoc` + `protoc-gen-go v1.36.8` 重新生成，先做空改动重生成验证 diff 为 0；`go install` 该插件可离线从 module cache 完成），iot-core 校验归属，adminapi 从 query 透传，harness 同步；③ 仓储改类型化哨兵 + iot-core `mapRepoError` + adminapi `httpStatusFromGRPC`，彻底去掉字符串匹配（顺带修掉跨设备 ACK 返回 502 的既有缺陷）。新增 ADR `0004-kafka-producer-durability.md`、`0005-tenant-scoped-command-reads.md`。新增测试：`TestGetCommandScopesToTenant`、`TestValidationErrorsUseInvalidArgument`、`TestRepoErrorsMapToStatusCodes`、`TestHTTPStatusFromGRPC`、`TestZRPCClientPreservesStatusCode`（真实 gRPC 服务 + 真实 zrpc 客户端，验证 status code 不被客户端吞掉——整个映射依赖这一点）、`TestWriteRPCErrorBodyOmitsTransportPrefix`、`TestBuildTopicConfigs`。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`helm lint`、`helm template`、`make build`。

- 本次变更（2026-10-08，七轮：DLQ 可见性/告警/保留）：① 新增 `iot_dlq_publish_total{stage,result}` 指标与 `IncDLQPublish`，`publishDeadLetter`/`commitAfterDeadLetter` 增加 metrics 参数，`result=error` 覆盖"writer 未配置/marshal 失败/写入重试后仍失败"三条路径；② stage 魔法字符串收敛为 `Stage*` 常量 + `DeadLetterStages()`，并修正 `cmd/dlq-replay -stage` 的错误帮助文本（原写 `tdengine`、`kafka-publish` 均非真实 stage）；③ `KafkaTopicConfig.RetentionMs` 新增，`retention.ms` 与 `min.insync.replicas` 一样只在首次创建 topic 时生效，DLQ 走独立的 `DLQTopicConfig` 以免truncate 遥测 topic；④ 新增两条 Grafana 告警模板 dlq-writes / dlq-write-failures；⑤ **顺带修掉一个真 bug**：`helm template` 渲染出 `KAFKA_DLQ_RETENTION_MS: "6.048e+08"`（YAML 大整数被解析为 float64，`| quote` 输出科学计数法），会让 `strconv.Atoi` 失败并静默回退默认值——图表侧三个 Kafka 整数改为 `| int64 | quote`，同时让 `runtimeconfig.Int` 在解析失败时打日志而不是静默吞掉，并新增 `internal/runtimeconfig/config_test.go` 覆盖该场景。新增测试：`TestPublishDeadLetterCountsEveryOutcome`、`TestBuildTopicConfigs` 的保留用例、`TestIntHandlesValidUnparseableAndUnset`、`TestKafkaTopicDurabilityDefaults`。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`helm lint`、`helm template`（默认与生产值）、`make build`。

- 本次变更（2026-10-08，八轮：iot-core 辅助 HTTP 服务接入优雅停机）：`serveMQTTAuthentication`/`serveIotCoreMetrics` 原为裸 `http.ListenAndServe` goroutine，改为 `auxHTTPServer`（`newAuxHTTPServer` 先同步 `net.Listen` 绑定，再用 `http.Server.Serve`）。三个效果：① **绑定失败即启动失败**——MQTT 认证端点是 fail-closed，绑定失败会导致全部设备认证失败而 Pod 仍 Ready，原来只有一行日志；现在 `Run` 直接返回错误（`ls` 冲突表现为 CrashLoopBackOff，可见）；② SIGTERM 时 `Shutdown` 排空在途请求（预算 `auxShutdownTimeout=2s`，远小于 go-zero 5.5s 的强杀窗口）并释放端口；③ **`Run` 会等待排空完成再返回**——这是必须的，因为 go-zero 的 gRPC `GracefulStop` 在信号后约 1 秒就完成并让 `Run` 返回，不等待的话进程会在排空途中退出、截断在途请求。另外给两个端点加了 `ReadHeaderTimeout: 5s`（MQTT 认证端点对 emqx namespace 可达，防慢速连接占用）。新增测试：绑定冲突报错、在途请求排空（已用 `Shutdown`→`Close` 红测验证该用例能区分"优雅"与"直接关闭"）、排空后端口释放、失败路径 `closeAuxServers` 不漏 listener、两个 mux 的路由存在性。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`helm lint`、`helm template`、`make build`。

- 本次变更（2026-10-08，九轮：把"告警 + 人工重放"策略做成真正可用）：① 新增 ADR `0006-dead-letter-alerting-and-manual-replay.md`，明确不自动消费 DLQ 的三条理由（根因未修则重放回环、遥测幂等但**命令重放会让设备重复动作**、故障期自动回灌会放大故障）；② 修掉 `cmd/dlq-replay` 三个缺陷：共用单一 consumer group 会隐藏其他 stage 的记录（见上方"关键配置"）、`-dry-run` 在 malformed/invalid-payload 分支仍提交 offset（与文档承诺相反）、匹配记录不足 `-limit` 时会阻塞 10 分钟再 `log.Fatal` 而非正常结束；③ 新增 `-idle-timeout` 与 `-group`，结束时输出 scanned/replayed/skipped 汇总；④ README 中英新增「死信重放运行手册」（含 `kubectl exec` 具体命令与"先 dry-run、先修根因、命令类 stage 最需谨慎"的顺序要求），两条 DLQ 告警的描述改为引用该手册；⑤ 新增 `cmd/dlq-replay/main_test.go` 锁定 per-stage group 且各 stage 不重复。已通过 `gofmt -l`、`go build ./...`、`go vet ./...`、`go test -count=1 ./...`、`helm lint`、`helm template`、`make build`。

- 本次变更（2026-10-08，十轮：DLQ 看板面板 + 监控资产一致性测试）：① `monitoring/grafana/dashboards/iot-pipeline.json` 新增 panel 14「DLQ 死信与写入失败 (by stage)」（`sum by (stage, result) (rate(iot_dlq_publish_total[5m])) or vector(0)`），放在「链路错误」行下；dashboard `version` 2→3（provision 文件靠版本递增触发更新）；② 两条 DLQ 告警的 `__panelId__` 补为 `14`，看板链接从"只打开看板"变成直接定位到该面板；③ 新增 `internal/platform/monitoring_assets_test.go` 三个用例，把这类漂移变成 CI 强制：**引用的指标必须真实存在**（如把 `iot_dlq_publish_total` 写成 `..._totals` 会失败）、告警的 `__dashboardUid__`/`__panelId__` 必须指向存在的看板与面板、看板 JSON 必须可解析且含必需字段。已红测验证后两个用例确实能拦住对应漂移。
- **指标名来源必须用源码提取，不能用 `Registry().Gather()`**：Gather 只返回已有序列的 family，而 `iot_http_requests_total` / `*_duration_seconds` 是懒创建的（`seedSeries` 未预置），用 Gather 会把它们误判为"不存在"（本轮就是这么先踩了一次假阳性）。新增监控图表面板时，若引用的指标名不在 `internal/platform/metrics.go` 的 `Name:` 字面量里，测试会失败。

- 本次变更（2026-10-09，十一轮：大批量 review 的高危+中危全量修复，13 项）：① **demo `EnsureTopology` 原子化**：tenants/agents 先建到局部变量、全部成功后才安装到 service，中途失败重试可补齐（此前 `len(s.tenants)>0` 提前返回导致部分失败后永久空转）；新增 `closeAgents` 防 bus 泄漏；factory==nil 时豁免 agents 检查。② **device_state UPSERT 移到去重判断之后**：DLQ 重放旧遥测不再刷新 `connected`/`last_seen_at`；重复分支改为返回库中已存整行（core 透传，修掉"重复返回新构造记录"）。③ **mqtt_bridge 背压进 DLQ**：publish slot 超时分支也写 DLQ（同 `StageMQTTKafka`），DLQ 写失败由 `_ =` 改为记日志。④ **worker 无 MQTT 不再静默记 ok**：`w.mqtt==nil` 时 log+`("command","error")`+commit，命令保持原状态由 dispatcher `RecoverStaleCommands` 兜底（store 无单条置 failed 方法，未新增接口）；`Run` 的 errCh 分支 return 前关闭两个 reader。⑤ **Kafka 重试可中断**：`MessagePublisher` 接口及调用链全部透传 ctx，每次尝试 5s 超时从 ctx 派生、退避 `select ctx.Done()`，停机不再被拖 ~32s；mqtt_bridge 回调无生命周期 ctx 仍传 Background（与原行为一致）。⑥ **`rpcListenOn` 去掉 LISTEN_ADDR fallback**：只认 `IOT_CORE_LISTEN_ON` > 默认 `:9001`（charts 已显式设置，无依赖）。⑦ **管理 API body 限 1 MiB**：adminapi 中间件链统一 `http.MaxBytesReader`。⑧ **Helm 加固**：4 个业务 Deployment 加 resources（values 可配，默认 50m/64Mi–500m/256Mi）+ pod seccompProfile RuntimeDefault + 容器 allowPrivilegeEscalation:false/drop ALL（镜像 FROM scratch 无 USER，**刻意不加 runAsNonRoot**）；iot-core/management-api 各加 PDB minAvailable:1；NetworkPolicy 9101 放开所有 namespace；**不加 HPA、不改硬编码名称**（单 release/namespace 为刻意设计）。⑨ **helm-deploy-local.sh**：本地镜像无 RepoDigests 时警告并回退本地 tag 继续 load，不再 exit 1。⑩ **compose 7 个 port-forward 端口绑 `127.0.0.1:`**（容器内 `--address=0.0.0.0` 保留）。⑪ **healthz-qps 告警阈值 0.3→1.0 req/s**（探针自身 0.3/s/副本），README 同步。已通过 `gofmt -l`、`go vet`、`go build ./...`、`go test ./...`、`helm lint`、`helm template`、`bash -n`、`compose config -q`。

- 本次变更（2026-10-09，十二轮：大批量 review 的低危修复，5 项；review 第 6 项 "memory_store 推进 published" 核实不属实，实为 created，不修）：① **删除自定义 `min()`**（`command_dispatcher.go`），Go 1.21+ 内建 min 直接可用。② **`seedSeries` 补齐 13 个 gRPC method**（原只预置 7 个，缺 CreateTenant/ListTenants/CreateDevice/ListDevices/GetDevice/GetDeviceStatus），指标按 proto 顺序预置。③ **TDengine 库名可配**：`TDengineConfig.Database` + `TDENGINE_DATABASE`（默认 `iot`），SQL 用限定名 `db.table`，不再依赖 DSN 默认库。④ **删除 demo 死配置权重**：`Config.TelemetryWeight`/`CommandWeight` 及 normalize 校验块（cmd/demo 无对应 flag，全仓库无引用）。⑤ **proto 再生成入口 + CI 漂移检查**：Makefile 新增 `proto`（钉版本变量 `PROTOC_VERSION 36.2`/`PROTOC_GEN_GO_VERSION v1.36.8`/`PROTOC_GEN_GO_GRPC_VERSION v1.5.1`，与 pb.go 头部对齐）与 `proto-tools`；CI 追加 setup-protoc 36.2 → proto-tools → `make proto && git diff --exit-code proto/`。本地 libprotoc 36.2 重生成与已提交 pb.go **零漂移**已实证。已通过 `gofmt -l`、`go vet`、`go build ./...`、`go test ./...`。

- 本次变更（2026-10-09，十三轮：busybox initContainer securityContext 补齐）：4 个 Deployment 的 `wait-for-deps`（busybox:1.36）补容器级 `allowPrivilegeEscalation: false` + `capabilities.drop: [ALL]`，与主容器对齐（pod 级 seccompProfile RuntimeDefault 原本已覆盖 initContainer，此轮补的是容器级纵深防御）。主容器 runAsNonRoot/runAsUser 当时定不做，**十四轮已反转加上**。已 kind 实测：7 个 pod 正常 Running（initContainer 正常通过）、遥测/命令链路持续流转、`helm lint` 通过。

- 本次变更（2026-10-09，十四轮：主容器非 root 加固）：4 个 Deployment 的 pod 级 securityContext 增加 `runAsNonRoot: true` + `runAsUser: 65534`（覆盖主容器与 busybox initContainer；全部监听端口 >1024，无需 NET_BIND_SERVICE）。kind 实测：crictl 确认容器进程 uid=65534、7 pod 正常 Running、遥测/命令链路无损。

- 本次变更（2026-10-09，十六轮：Kafka topic 对账自愈 + deviceWorker 2 副本 + 401 审计日志）：① **Kafka topic 对账**：`ensureKafkaTopics` 在 `CreateTopics`（对已存在 topic 幂等跳过）之后新增 `reconcileKafkaTopics`——`kafka.Client.Metadata` 查分区数/副本数，漂移只 log 告警不自动改（分区扩缩/RF 变更是运维操作）；`DescribeConfigs` 只查本服务管理的 config entries（min.insync.replicas、retention.ms），漂移的用 `AlterConfigs` 修正并逐条记日志（`kafka topic <name>: reconciled <key>=<value>`）；transport 错误返回交给 60s 重试循环，resource 级错误 log 后继续。iot-core 管 telemetry/command，device-worker 与 telemetry-ingestor 管 iot.dlq。新增纯函数 `driftedConfigEntries` + `TestDriftedConfigEntries` 三用例。② **deviceWorker 默认 2 副本**（values.yaml）：遥测/命令 topic 各 1 分区，消费组 `iot-device-worker-telemetry`/`-command` 只有 1 个活跃成员，第二副本为空闲 standby，提供 failover 而非吞吐。③ **401 审计日志**：`BearerTokenMiddleware` 拒绝分支打 `bearer auth rejected: method=... path=... remote=...`。RBAC 仍刻意不造。kind 实证：注入 `iot.dlq retention.ms=1000` → 重启 device-worker → 日志出现 reconciled 且配置恢复 604800000；curl 无 token → 401 + 审计日志；8 pod（device-worker×2）Running、遥测 60s 748 条。**部署坑**：`scripts/helm-deploy-local.sh` 默认 `APP_IMAGE=iot-app:2.0`，`docker build -t iot-app:local` 后必须 `APP_IMAGE=iot-app:local scripts/helm-deploy-local.sh`，否则部署的是旧镜像（脚本会 re-tag 为 `iot-app:local-<imageID前12位>` 导入 desktop-control-plane）。已通过 `gofmt -l`、`go vet`、`go build ./...`、`go test -count=1 ./...`、`helm lint`、`make build`。
- 本次变更（2026-10-09，十五轮：文档漂移修正 + bin 残留清理）：① README.md / README.zh-CN.md 的 Development Guidelines 命令状态机从旧名 `pending -> dispatched -> sent` 改为实际 `created -> published -> sent -> acked / timeout / failed`（与 `domain.go:57-62` 对齐）。② 删除 `bin/` 下 4 个旧名二进制（`admin`/`core-rpc`/`ingress`/`worker`，gitignored 不入库）。同轮 review 的其余薄弱点（deviceWorker 单副本、单一静态 Bearer、Kafka topic 不 AlterConfigs 只对首次创建生效）**核实全部属实**，属生产化评估项，未动。

- 本次变更（2026-10-09，十七轮：TDengine 控制字符第二层防御 + 命令下发失败立即重排 + paho 断连阻塞修复）：① **TDengine `validateTDField`**：`tdengine_writer.go` 新增逐字节校验（拒 `<0x20`/`0x7f`），`writeRecord` 开头校验 TenantID/DeviceID/MsgID/Type/Version，`errTDFieldRejected` 进 DLQ——`escapeTD` 只转义 `\`/`'`，控制字符（如 msgId 里的 `\n`）能穿透进 SQL，taosRestful 驱动又无参数化可依赖。新增 `TestValidateTDField` + `TestWriteRecordRejectsControlCharactersBeforeExec`。② **`RequeueCommand` 立即重排**：`postgres_store.go` 新增（只匹配 `status='published'`，复用 `updateCommandsWithEvent` 记 `requeued` 事件），worker MQTT 下发失败分支接入，backoff `1s << min(attempts,6)`；此前只能靠 dispatcher `RecoverStaleCommands` 5 分钟兜底。③ **paho 断连阻塞修复（故障注入实测发现的真 bug）**：worker paho client 配 `AutoReconnect+ConnectRetry`，断连时 `Publish` 排队、QoS1 token 挂起，原 `token.Wait()` **无超时**导致整个 consumeCommands 循环卡死（不走失败分支、不消费后续消息）——EMQX 停机 5 分钟内命令零重排。改为 `token.WaitTimeout(mqttPublishTimeout)`（常量 5s），超时视为失败走 requeue+DLQ；paho 重连后可能补发排队消息，at-least-once 下可接受（ADR 0006）。故障注入实证：EMQX replicas=0 后命令 9.4s 首次 requeued（5s 超时+处理），attempts 按 2/4/8/16s backoff 递增；恢复 EMQX + 重启 iot-demo（模拟器断连后不自动重连）后新命令全链路 acked。④ **EMQX 6 Operator 节点驱逐坑（本地必记）**：对 CR `patch coreTemplate.spec.replicas=0` 会触发 operator node evacuation，scale-up 恢复后 operator 常卡 `status.nodeEvacuationsStatus: prohibiting`，pod readiness gates（`on-serving`/`ds-replication-site`）保持 False → EndpointSlice ready:false → ClusterIP 不通（pod IP 直连通）。解法：`emqx ctl cluster force-leave <旧stopped节点>` + 用 bootstrap API key 调 `POST /api/v5/load_rebalance/{node}/evacuation/stop`（dashboard admin:public 对该 API 401，必须用 `emqx-bootstrap-api-key` secret；EMQX 6 无 `node_evacuation` CLI）。⑤ **helm-deploy-local.sh 两个坑**：EMQX 停机时 netcheck 失败脚本 set -e 退出，需 `CHECK_EXTERNAL_DEPS=0`；device-worker/telemetry-ingestor initContainer 等 EMQX，EMQX 停机时新 pod 卡 Init:0/1，必须先恢复 EMQX 再部署。部署镜像 `iot-app:local-4c38a8361d1e`，8 pod Running，命令 happy-path 回归通过。已通过 `gofmt -l`、`go vet`、`go build ./...`、`go test -count=1 ./...`、`make build`。

## 遗留说明

- `internal/platform/handlers.go` + `memory_store.go`：**已确认保留，不删**。它们在生产路径无调用方（现在 `EnableBusinessAPI` 也没有任何生产入口会开启），但承载 8 处测试调用，含两个真实 E2E 的 HTTP 入口；删除只减测试覆盖、无生产收益。风险仍在：它与 `internal/adminapi` 是两套 REST 实现，改契约时容易只改一边。新增 REST 一律进 adminapi，此文件只维护测试所需行为。
- `iot.dlq` 指标/告警/保留已完成（第七、九、十轮，ADR 0006）。已知残留：DLQ 无消费侧所以**深度不可观测**（无 Kafka exporter），告警靠"新写入"信号，这是 ADR 0006 已接受的取舍。

## 文档入口

- [README](README.md)
- [生产部署指南（英文）](docs/production-deployment.md) / [中文](docs/production-deployment.zh-CN.md)
- [EMQX 部署清单](deploy/emqx/README.md)
- [MQTT Schema](docs/mqtt-envelope.schema.json)
- [架构 ADR](docs/adr/)
