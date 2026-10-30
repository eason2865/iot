# IoT 当前交接信息

> 本文件是当前状态和维护入口。历史修复、旧部署快照及当时的验证结论已归档至 [截至 2.54 的交接历史](docs/history/ai-handover-through-2.54.md)。历史版本中的“当前”不代表现状。

## Deployment state (2026-10-30, 已清理并停服)

| Item | Verified value | Evidence |
| --- | --- | --- |
| Git documentation release | 2.58, cleanup/shutdown record only | main commit and tag |
| Application | stopped; configured image is 2.56 / 041fe10 | no business Pods remain |
| Helm release | iot, namespace iot, revision 6, Chart 0.2.11 | live Deployment replicas are 0; Helm values still specify 2 |
| Business Pods | four Deployments at 0 replicas; 0 Pods | docker-desktop cluster |
| EMQX | Core StatefulSet and operator Deployment at 0 replicas; 0 Pods | CR retains its original desired Core count of 1 |
| Runtime image | iot-app:local-b2620114a9d9 | digest sha256:b2620114a9d98458a6e82b4cf40869324ce480f73f012a6c5de810636d463b89 |
| Host image / Demo | iot-app:2.56 / stopped | Docker inspect |
| Dependencies / monitoring | PostgreSQL, Kafka, TDengine, Prometheus, Grafana and both port-forwards stopped | Docker inspect |
| Cluster access | docker-desktop context is available again | deployment and reads succeeded on 2026-10-30 |

2.56 completed local deployment and validation, then all local IoT services and dependencies were stopped at the user's request. 2.58 records cleanup/shutdown only; no new runtime image was built. Business history and test data were cleared; configuration, credentials, gRPC mTLS certificates and the existing private backup were preserved. Docker Desktop's Kubernetes infrastructure remains running.

## 历史数据清理、空库验证与停服（2026-10-30）

- 用户明确要求清除历史数据，验证通过后停止全部本地 IoT 服务及依赖，包括数据库、Kafka、EMQX 和监控。
- 暂停 Demo 并将四个业务 Deployment 缩到 0 后清理。清理前有 9 个租户、57 个设备、5,015,787 条遥测、3,346,349 条命令、3,345,594 条 ACK、13,376,564 条命令事件；七张业务表全部 TRUNCATE，保留表结构、索引和权限。
- 删除 `iot.telemetry`、`iot.command`、`iot.dlq` 及两个 worker 消费组；TDengine 删除 `iot` 库下的超级表和子表，保留库配置和用户；清理 EMQX retained messages。未删除数据库卷、Kafka 内部 topic、Grafana 配置或已有备份。
- 从空库重新启动 2.56，20 项部署闭环检查全部通过，包括 API、MQTT 鉴权/ACL、PG/TDengine 入库、成功/失败命令结果、幂等、409 冲突、租户隔离、DLQ、补偿、超时和活跃 worker 故障接替。首条 MQTT 遥测完整链路约 0.30 秒完成；接替期间 30/30 条遥测完成双库写入，命令 ACK 成功。
- Demo 重建 5 租户/50 设备并造流，采样的 topology/telemetry/command/ack 错误计数均为 0；监控 9/9 up；停写后两个 Kafka 消费组 lag 均为 0。空库并发建表时一个 worker 曾因 TDengine `Conflict transaction not completed` 重启一次并自动恢复，之后八个业务 Pod Ready；该现象已留证，未修改应用代码。
- 验证后再次暂停所有写入并重复清理：七张业务表均为 0 行，TDengine 业务表为 0，三个业务 topic 和两个消费组均不存在，EMQX 无客户端、retained messages 为 0。测试租户和 Demo 数据也已删除。
- 停服顺序：先 Demo 和四个业务 Deployment，再 EMQX operator，然后 EMQX Core StatefulSet，最后停止两个转发容器、PostgreSQL、Kafka、TDengine、Prometheus、Grafana。暂停 operator 是为了避免它把手工缩到 0 的 EMQX StatefulSet 恢复。保留 CR、PVC、Secret 及 Helm release；停服后两个 namespace 的业务组件和 operator namespace 均无 Pod。
- 私有证据：`outputs/reset-stop-20261030/report.md`、`report.json`、`runtime-validation.log`、`before-data.json`、`final-empty.json`、`*-lag.txt`、`containers-stopped.json` 和 namespace 停服快照。目录 700、文件 600，均受 Git ignore；原 PostgreSQL 备份仍在 `outputs/deployment-20261030-256/postgres-backup.dump`。
- 恢复时先启动 Docker 依赖及转发容器，再恢复 EMQX operator 到 1 副本（由 CR 恢复 Core 到 1），恢复业务副本。空 TDengine 库建议先启动一个 device-worker 完成建表、Ready 后再扩到 2，以避开本次观察到的并发 DDL 冲突；Demo 按需最后启动，启动后会重新生成演示数据。不要直接使用会覆盖现有凭据和证书的本地一键部署脚本。

## Local deployment validation (2026-10-30, 清理前的部署记录)

- 20 deployed checks passed: MQTT authentication/ACL, Kafka publication, telemetry PG/TD completion, duplicate telemetry, command idempotency, successful/failed ACK results, 409 result conflict, tenant lists, strict parameters, malformed MQTT DLQ, timestamp checks, compensation, poison-row skipping and timeout audit.
- Before migration: verified a PostgreSQL custom-format backup (~653 MiB), paused Demo and the four services, ran the 2.56 migration, imported the immutable image, then upgraded Helm. Recreated the API forward and Demo using the preserved environment.
- Active-worker Pod deletion: 30/30 telemetry records completed PG/TD; the test command was ACKed; two workers recovered Ready. Demo has 5 tenants/50 devices; observed topology, telemetry, command and ACK error counters were zero.
- Before the requested reset, retained telemetry backlog was about 578669 messages, later 520154; command lag was 0. The first 45-second wait timed out; the final run allowed compensation to complete. That deployment run did not prove realtime telemetry latency or a drained Kafka backlog. The later user-authorized cleanup removed this backlog; the fresh validation and final empty state are recorded above.
- Evidence: outputs/deployment-20261030-256/report.md, report.json, runtime-validation.log, initial-* and *-lag.txt. Directory 700, files 600, Git-ignored. Credentials and database backup are private local artifacts.
- Validation tenant `closed-20261030-150141` was retained at deployment time and has now been deleted by the user-authorized cleanup; deployment evidence remains in the private report.

## 本次有效变更与升级入口（2.56）

- 按用户要求移除旧路由及兼容别名。资源统一为 `/api/v1/tenants/{tenantId}/...`；已有设备/命令身份在路径，Body 只承载业务字段。设备创建的 Body 仍提供新 `deviceId`。认证和 handler 共用解析结果，不从 Query/Body 获取租户。
- 租户 Token 可分页枚举本租户设备。遥测支持设备事件时间的起止范围、type、排序；命令支持 deviceId/status/创建时间/排序；范围为 RFC3339、最多毫秒精度、左闭右开，默认时间倒序。设备列表按 deviceId 升序。时间类游标绑定资源、租户和过滤条件，设备游标绑定租户。
- 拒绝重复、未知、空值或非法编码 Query；HTTP 显式 pageSize 仅1–100，缺省50。JSON 必须单对象，拒绝未知/重复字段、顶层字段大小写别名、非法 Unicode，最多128层。gRPC 独立校验必填身份、过滤值和时间范围。
- 新建 name 为1–256 UTF-8字节、非空白且无控制字符；productId 为 `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`。命令 payload 如提供必须为对象，省略明确为 `{}`，拒绝 null/数组/标量。已有历史大行不改写，仍保留字节分页预算。
- REST/MQTT ACK 均明确提供 `status=acked|failed`，支持 errorCode 及对象结果 payload（原文及保守展开预算64KiB）。结果随命令查询返回 `result`；状态、结果、ACK记录和审计同事务。相同结果幂等，已记录结果冲突返回409；既有终态保护不被迟到回执覆盖。MQTT 无持久化仍不确认，DLQ replay 完整恢复结果，不重发设备动作。
- `migrations/001_init.sql` 增加幂等的 `commands.result JSONB` 列及事件时间/命令列表索引，由原有 embed/事务锁流程执行；没有伪造历史执行结果。生产 DML-only 启动前用迁移角色执行新版 migrate。
- Demo、测试 harness、低基数 metrics/trace 路由模板、双语 README 同步新契约。完整路径、Body、Query、错误及升级说明见 [API契约](docs/api-contract.md) 与 [ADR0010](docs/adr/0010-canonical-api-parameters-and-command-results.md)。

## Code validation (2026-10-26) and scope

- 本轮使用独立 PostgreSQL 容器 `iot-api256-pg`（验证结束后清理），每个测试创建独立 schema；真实 Kafka 使用私有 topic/group，TDengine 使用测试自建库。验证不读写运行服务业务数据，也不启动会抢占生产 ClientID 的新业务服务。
- 验证覆盖实际 go-zero REST→真实gRPC→隔离PG：租户列表隔离、旧路由404、重复参数/JSON拒绝、过滤/排序/多页无遗漏、半开时间边界和显式最小时间、结果持久化/查询/幂等/409冲突。另覆盖 MQTT失败结果、审计故障整体回滚、DML-only DLQ成功/失败结果重放。
- 全量测试（真实PG/Kafka/TD）、全量race（Kafka IP bootstrap）、定向最终race、vet、fmt/diff、7个Linux arm64二进制、Helm双profile lint/template、Compose config、部署脚本语法、固定插件protobuf重生与正式镜像连续迁移两次均通过，证据见 `outputs/fixes-256/`；outputs受Git ignore，不提交凭据或私有运行快照。
- The code-only release on 2026-10-26 was not deployed then. The current local deployment and deployed validation evidence are now recorded above (2026-10-30).
- 本地复验设置独立测试资源 `IOT_POSTGRES_TEST_DSN`、`IOT_KAFKA_TEST_BROKERS`、`IOT_TDENGINE_TEST_DSN` 后执行 `go test -count=1 ./...`。未设置时对应集成测试跳过。CI启用PG和Kafka双bootstrap矩阵，未提供真实TDengine service。

## 当前架构

- Kubernetes namespace `iot`：`management-api`、`iot-core`、`telemetry-ingestor`、`device-worker`。
- Kubernetes namespace `emqx`：EMQX Operator 管理的 EMQX 集群；本地为单 Core 节点，生产使用独立多节点清单。
- Docker Compose：PostgreSQL、Kafka、TDengine、Prometheus、Grafana、demo 和 Kubernetes port-forward 容器。
- 业务同步调用使用 Kubernetes Service DNS：`management-api -> iot-core:9001`。
- 设备链路：MQTT -> EMQX -> `telemetry-ingestor`（优先写 PostgreSQL inbox 后确认）-> Kafka -> `device-worker` -> PostgreSQL/TDengine；worker 补偿未完成的 TDengine 写入。
- 命令链路：`management-api` -> `iot-core` 写 PostgreSQL -> dispatcher 后台发布 Kafka -> `device-worker` -> MQTT -> 设备 ACK -> PostgreSQL。
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
- MQTT 设备用户名：`tenantId:deviceId`；租户 ID 禁冒号，设备 ID 允许冒号；构造/解析由 contracts 统一校验。设备密码在 PostgreSQL 中保存为 bcrypt 哈希。
- MQTT 服务账号：`iot-service`，由 `EMQX_INTERNAL_PASSWORD` 注入。
- MQTT 认证回调：`iot-core:9090/internal/mqtt/authenticate`，fail-closed——必须配置 `IOT_CORE_MQTT_AUTH_TOKEN`，EMQX 通过 `X_Iot_Auth_Token` 头携带（下划线命名，原因见下）；本地脚本在 `iot` 和 `emqx` 两个 namespace 各建一份同名 Secret；ACL 按角色授予（全部经 `withACLGuard` 追加 `{deny, all, #}` 兜底）：telemetry-ingestor 订阅 `tenant/+/device/+/telemetry`、device-worker 订阅 `tenant/+/device/+/ack` + 发布 `tenant/+/device/+/command`、设备账号 `eq` 本设备三条 topic、demo 账号（`iot-demo-<tenant>-<ts>` clientID）收敛到单租户通配（由 clientID 提取租户，提取失败拒绝）。
- EMQX 6.3.1 回调 token 注入（三个坑，均已验证）：① authn HTTP 模板**不解析** `${VAR}` 环境变量占位符（`auth_template_invalid` 告警后原样传递字面量）；② env 覆盖名只允许 `[A-Z0-9_]`，`EMQX_AUTHENTICATION__1__HEADERS__x-iot-auth-token`（连字符）被**静默丢弃**，`...__x_iot_auth_token`/`...__X_IOT_AUTH_TOKEN`（小写/大写 key）报 `unknown_env_vars` 被拒——**按 key 覆盖 headers 不可行**；③ 唯一可行方案：`EMQX_AUTHENTICATION__1__HEADERS` 整体 JSON 覆盖 headers map，值存 emqx namespace `iot-runtime-secrets` 的 `EMQX_AUTHN_HEADERS_JSON`（`helm-deploy-local.sh` 生成）。清单里 config.data 的 headers 字面值只是必定被 iot-core 拒绝的 fallback。
- EMQX ACL 双防线：① iot-core 回调每条角色 ACL 末尾 `withACLGuard` 追加 deny-all；② ConfigMap `emqx-acl` 挂载自定义 `acl.conf`（`{deny, all}.` 兜底）覆盖默认文件——默认 acl.conf 末尾的 `{allow, {security_profile, legacy}}` 在 legacy profile（EMQX 6.3 默认）下会放行一切。**不启用 hardened profile**：hardened 限制 authn HTTP 模板占位符，与回调 token 注入冲突。
- EMQX 6.3 ACL topic 语法（emqx_authz_rule.erl 验证）：仅支持 `eq ` 前缀（精确匹配）；**`match ` 前缀已废弃**——会被当作字面 topic 层级导致规则永不匹配。共享订阅在 authz 前被解包成裸 filter（`$share/g/t/#` → `t/#`），规则必须写裸 filter，authz 层无法区分共享/普通订阅。
- MQTT ACL 测试断言坑：paho 的 QoS1 Publish token 与 Subscribe token 都**不会**把 broker 拒绝（PUBACK/SUBACK 0x87/0x80）返回为 error；订阅断言须读 `SubscribeToken.Result()[topic] == 0x80`，发布断言须查 EMQX 日志 `authorization_source_denied`/`cannot_publish_to_topic_due_to_not_authorized`。
- 含凭据 DSN 入 Secret：`POSTGRES_DSN`/`TDENGINE_DSN` 不再出现在 ConfigMap 和 `values.yaml`，由 `iot-runtime-secrets`（`security.existingSecret`）注入，各 Deployment `envFrom` 同时引用 ConfigMap 与 Secret；`helm-deploy-local.sh` 自动把 `IOT_POSTGRES_DSN`/`IOT_TDENGINE_DSN`（有默认值）写入 Secret。
- REST 摄入校验：`IngestTelemetry`/`RecordTelemetry` 在落库前执行 `contracts.ValidateEnvelope`，非法 envelope 直接报错，不再产生绕过校验的 poison message。**`ts` 有区间校验**（2026-10-08 加）：小于 2000-01-01（`946684800000` ms）或超过服务端时间 +5 分钟一律拒绝，错误为 `contracts.ErrEnvelopeTimestampOutOfRange`（包装 `ErrInvalidEnvelope`，`errors.Is` 兼容）。这同时把"设备误把秒当毫秒"（落到 1970）从静默写入变成显式拒绝。校验**只在摄入侧**（MQTT bridge / REST）执行，worker 消费 Kafka 和 DLQ 重放不再校验，历史消息不会因"太旧"被拒。
- device-worker 租户隔离：`DEVICE_WORKER_TENANT_IDS`（CSV，默认空=处理全部租户，保持旧行为），Helm 对应 `deviceWorker.tenantIDs`。订阅仍是通配，过滤在 worker 内做，被跳过的消息记 result=`filtered` 并打日志（`telemetry skipped`/`command skipped`）。
- TDengine 写入两层防护：`escapeTD` 先转义反斜杠再转义单引号（TDengine 中 `\` 是转义字符；taosRestful 驱动无参数化，`InterpolateParams` 对字符串零转义）；第二层 `writeRecord` 使用与入站共用的 UTF-8、控制字符和字段字节长度校验，拒绝即走 DLQ（stage `telemetry.tdengine`，error 包含 `tdengine field rejected`）；历史补偿遇到此类永久字段错误继续处理后续记录。
- Kafka 消费起点：新消费者组从 `FirstOffset` 启动重放积压（幂等消费兜底），避免 `LastOffset` 丢宕机期间消息。
- DLQ 可见性与保留（2026-10-08 起）：每次死信写入都计入 `iot_dlq_publish_total{stage,result}`（`result=error` 表示**消息没被保全**，是阻塞消费/crash loop 的状态，不只是普通错误）；stage 收敛为 `platform.Stage*` 常量 + `DeadLetterStages()`，指标对每个 stage 预置序列，`cmd/dlq-replay -stage` 的帮助文本也从该列表生成（此前写的是 `tdengine`/`kafka-publish` 这类不存在的值）。死信保留由 `KAFKA_DLQ_RETENTION_MS` 约束（默认 7 天，Helm `kafka.dlqRetentionMs`），创建及存量 topic 启动对账均生效。告警模板：`monitoring/grafana/alerts/dlq-writes.json`（warning）与 `dlq-write-failures.json`（critical），均按 Grafana API 手工导入（非文件 provision），均关联 `iot-pipeline` 看板的 panel 14（DLQ 死信与写入失败）；运行中 Grafana 是否已导入这些模板需另行核实。
- DLQ 重放（人工，策略见 `docs/adr/0006`）：`cmd/dlq-replay` 支持 `-stage` 过滤、`-limit`、`-dry-run`、`-idle-timeout`。**每个 stage 用独立 consumer group**（`iot-dlq-replay-<stage>`，`-group` 可覆盖）：kafka-go 的 `StartOffset` 只在"分区没有已提交 offset"时生效，所以共用单一 group 时，提交任意一条匹配记录就会把 group 位置推过它之前的所有记录，之后针对其他 stage 的运行将**永远看不到**那些记录（此前代码正是共用 `iot-dlq-replay` 单个 group）。`-dry-run` 现在真正零副作用——**不提交任何 offset**（此前 malformed/invalid-payload 分支在 dry-run 下也会提交）。topic 静默 `-idle-timeout`（默认 5s）后正常退出并打印 scanned/replayed/skipped（此前会阻塞到 10 分钟总超时再 `log.Fatal`，让"只重放了 3 条"看起来像失败）。
- MQTT 订阅默认值：`telemetry-ingestor` 用 `$share/iot-telemetry/...`，`device-worker` 用 `$share/iot-device-worker/...`（多副本负载均衡）。
- NetworkPolicy：`iot-core-mqtt-auth` 限制 9090 仅 `emqx` namespace 可达，9001 仅本 namespace；9101（metrics）放开为所有 namespace（2026-10-09 起，否则外部 namespace 的 Prometheus 无法抓取）。kind 的 kindnet 不执行 NetworkPolicy，仅真实集群生效。
- gRPC mTLS：`iot-core:9001` 支持双向 TLS，由 `IOT_CORE_TLS_CERT`/`IOT_CORE_TLS_KEY`/`IOT_CORE_TLS_CA`（+客户端 `IOT_CORE_TLS_SERVER_NAME`）环境变量驱动，实现在 `internal/platform/grpc_tls.go`；三变量全空保持明文（本地裸跑兼容），部分设置启动报错。Helm 用 `grpcTLS.enabled=true` 开启，Secret `iot-grpc-tls`（含 ca/server/client 五件套）挂载到 `/etc/iot/grpc-tls`；`scripts/helm-deploy-local.sh` 默认启用并自动调 `scripts/gen-grpc-certs.sh` 生成本地自签证书（`deploy/grpc-certs/`，已 gitignore），`GRPC_TLS_ENABLED=0` 可关闭。证书轮换后需重启 Pod。
- Kafka topics：`iot.telemetry`、`iot.command`、`iot.dlq`。**生产者一律 `RequiredAcks: RequireAll`**（2026-10-08 起，此前业务写为 `RequireOne`）：PG 先写、Kafka 后发，`RequireOne` 下 leader 在副本同步前宕机就会丢掉 PG 已认为发布成功的事件，DLQ 只兜消费侧失败、兜不住这种。topic 由本服务创建时按 `KAFKA_TOPIC_REPLICATION_FACTOR`（默认 1）与 `KAFKA_TOPIC_MIN_INSYNC_REPLICAS`（默认 1，Helm 为 `kafka.topicReplicationFactor`/`topicMinInsyncReplicas`）设置；**生产高可用配置使用 RF≥3、min.insync.replicas≥2；默认 RF=1/ISR=1 无法提供副本故障容忍**。注意 `CreateTopics` 对已存在 topic 是幂等跳过（kafka-go 跳过 `TopicAlreadyExists`），所以分区数/RF 只在首次创建生效；但 config entries（min.insync.replicas、retention.ms）的漂移由 `reconcileKafkaTopics` 自动用 `IncrementalAlterConfigs` SET 增量修正（2.44 起，保留其他覆盖配置），分区总数或任一分区 RF 不匹配会使初始化失败并阻止启动，需运维先调整；自动建 topic 已关闭。详见 `docs/adr/0004`。
- TDengine：超级表 `telemetry_v2`，按设备建立子表；完整 payload 的权威副本为 PostgreSQL JSONB。`telemetry_records.tdengine_written` 记录 TDengine 完成状态，DLQ 重放只补偿未完成的 TDengine 写，PG/TDengine 均幂等。**库名可配**（2026-10-09 起）：`TDENGINE_DATABASE`（默认 `iot`），SQL 全部用限定名 `db.table`（`CREATE STABLE db.table`/`INSERT INTO db.child USING db.stable`），不再依赖 DSN 中的默认库。
- HTTP 遥测幂等重发：`IngestTelemetry` 无论首次写入还是命中唯一约束（重复），都会发布 Kafka——因为上一次可能 PG 写成功但 Kafka 失败（broker 故障），若重复分支跳过 Kafka，该数据将永远不进 worker/TDengine。重复消费由 worker 的 `tdengine_written` 补偿兜住，重发安全。
- 命令状态机：`created → published（Kafka 写入）→ sent（MQTT 下发成功，启动 ACK deadline）→ acked/timeout/failed`。**published 恢复**：dispatcher 独立扫描循环执行 `RecoverStaleCommands`——`published` 超过 timeout（默认 5m）未流转的重置为 `created` 重新派发（记 `requeued` 事件），`dispatch_attempts >= 10` 且租约/退避到期的 created，或已超时的 published，置 `failed`（重派安全：`MarkCommandSent`/`AckCommand` 幂等守卫兜底）。**立即重排（2026-10-09 起）**：worker MQTT 下发失败（含 5s publish 超时）立即调 `RequeueCommand`（只匹配 `status='published'`，backoff 使用数据库 attempt 并将指数限制在 0..6（正常 attempt≥1 时为 2,4,8…64s），记 `requeued` 事件），不再干等 dispatcher 5 分钟恢复兜底；attempts 上限 10 仍由 `RecoverStaleCommands` 置 `failed`。**竞态处理**：dispatcher 先发 Kafka 再 `MarkCommandPublished`，worker 可能在 published 落库前完成 MQTT 下发——`MarkCommandSent` 因此接受 `status IN ('created','published')`，保证命令必到 `sent` 且有 deadline；随后 dispatcher 的 `MarkCommandPublished`（要求 `created`）命中 0 行，不会把 `sent` 降级。
- 命令接口统一租户路径隔离：`/api/v1/tenants/{tenantId}/commands` 及其 `{commandId}` 详情；设备级命令集合为 `/api/v1/tenants/{tenantId}/devices/{deviceId}/commands`。设备执行结果 POST 到该集合下 `{commandId}/acknowledgements`。租户Token与路径不符返回403，命令不属于指定租户/设备返回404；完整契约见ADR0010。
- **错误映射按 gRPC status code，不再按消息文本**：仓储返回类型化哨兵 `platform.ErrNotFound`/`ErrAlreadyExists`，iot-core 经 `mapRepoError` 转 status code，adminapi 经 `httpStatusFromGRPC` 转 HTTP（InvalidArgument/FailedPrecondition→400、NotFound→404、AlreadyExists→409、Unauthenticated→401、PermissionDenied→403、ResourceExhausted→429、DeadlineExceeded→504、Unavailable→502；完整映射见 `internal/adminapi/server.go`）。此前靠 `strings.Contains`，导致 `"tenantId contains invalid MQTT topic characters"` 和 `"command does not belong to device"` 都掉进默认分支返回 **502**。响应体只含 core 的消息，不带 `rpc error: code = ... desc = ...` 外壳。
- 列表均使用 keyset 分页（pageSize缺省50，HTTP显式1–100；`{items,nextCursor}`），同时受2MiB预算约束。`NormalizeListRequest` 保留并校验所有过滤字段，禁止归一化分页时丢失租户/设备过滤器；时间类游标绑定过滤范围。
- REST 写接口仅 `management-api` 暴露；`telemetry-ingestor`/`device-worker` 只提供 `/healthz`、`/readyz` 和 `/metrics`（业务 REST 在 `platform.App` 中是显式 opt-in 的 `EnableBusinessAPI`，不从服务名推断，bootstrap 不开启；生产 REST 只由 adminapi 提供且带 Bearer 校验）。
- 生产环境禁止使用 `latest`，使用不可变镜像版本或 digest。

## 当前可靠性与容量约束

- MQTT 遥测和合法设备 ACK 均关闭自动确认；至少一个持久化落点成功才确认，全部失败则不确认并退出。ingestor 优先写 PostgreSQL inbox，worker ACK 优先写 PG、失败后写 `command.ack` DLQ。停机先关闭回调 admission，最多排空 3s，再取消在途调用并关闭资源。该保证覆盖已到达应用的消息；设备离线及 broker 尚未投递消息仍依赖 EMQX 会话/共享订阅策略。
- `POST /api/v1/tenants/{tenantId}/devices/{deviceId}/commands` 支持 `Idempotency-Key`：1–128 个无空格可打印 ASCII 字符，同租户同 key/device/JSONB payload 返回原命令，变更设备或 payload 返回 409；不传 key 保持原行为。命令至少一次下发，设备必须按 command ID 去重。
- dispatcher 即时领取单条、租约 30s、发布最多占半个租约；完成/重排校验 attempt 和租约。超时/恢复扫描与发布独立，每种状态转换最多 500 条，`FOR UPDATE SKIP LOCKED` 避免已锁行阻塞其他命令；状态和审计原子提交。负 Kafka attempt 进 `command.decode` DLQ；合法事件按数据库内容和 attempt 下发。
- worker 每 30s 按 ID 游标补偿最多 500 条、接收时间超过 2min 的未完成遥测，租户过滤在 LIMIT 前，扫完归零。TDengine 永久字段错误和结构化时间范围拒绝 `0x060b` 跳过后继续，原记录及未完成标记保留；网络、超时、权限和未知错误保留游标重试。
- TDengine 使用整数毫秒时间戳，启动显式创建并检查 `ms` 精度，已有 `us`/`ns` 库在消费前失败；不自动修正历史时间。完整 JSONB 保存在 PostgreSQL，TDengine 保存属性、payload 哈希和字节数；同设备同毫秒的不同消息仍有碰撞边界。写入成功后才标记完成，不引入“内存入队即提交 Kafka offset”的批处理。
- 实时摄入使用服务端接收时间，恢复保留原 `receivedAt`，旧 Kafka 记录回退原消息时间；DLQ 按已有接收时间、`occurredAt`、历史设备时间顺序恢复。重复或更旧的记录不刷新在线状态，`last_seen_at` / `last_msg_id` 原子且单调更新。
- worker 租户 allowlist 派生独立 Kafka consumer group；改变集合会使用新位点并重读保留历史，避免部署重叠集合。MQTT 共享订阅仍统一接收后过滤。
- 设备 secret 限 1–72 UTF-8 字节，存储为 bcrypt；旧凭据迁移验证完整规范编码，有效 hash 保留、明文清理，异常则整事务回滚。设备凭据不得使用 worker/ingestor/demo 保留 ClientID 及短横线后缀；普通设备 ClientID 尚未强制绑定身份，迁移需协调设备端。
- JSON 输入拒绝 NUL、无效 UTF-8、未配对 surrogate、超 JSONB 范围数字；遥测 payload 必须是对象。JSONB 展开预算保守估算：一般输入 1MiB、遥测 payload 为 1MiB 减 4096 字节；PG 事务内检查规范化结果。新建 name 限256字节且非空白、productId 为1–64字符ASCII标识；历史超大行不自动改写。
- HTTP body 限 1MiB 且仅接受一个 JSON 文档；pageSize 完整解析整数。四类列表同时按条数和 2MiB 响应预算分页，cursor 指向最后实际返回行；单条历史行超预算明确报错，不跳过。命令受理还需满足完整 Kafka 消息 1MiB 预算并预留 1024 字节传输开销，DLQ writer/topic 至少 2MiB，不截断原文。
- PostgreSQL 运行操作透传调用方 context 且默认最多 5s；TDengine 初始化/写入默认 5s（`TDENGINE_WRITE_TIMEOUT`）；Kafka 初始化共享 60s 总预算和每次 5s 上限，预算内探测多个 bootstrap，失败阻止启动。
- `/healthz` 是浅存活检查，`/readyz` 检查 PG；网关经 gRPC 检查 core/PG，ingestor/worker 还要求物理 MQTT 已连接且所有订阅实际获准 QoS 1。共享订阅使用 `SubscribeMultiple` 保留完整 filter 供 SUBACK 校验；拒绝、降级、缺失、重连待确认和停机均不就绪。
- 管理入口访问日志和 HTTP trace 使用安全字段白名单，core RPC 不记录请求内容；trace 仅采集方法、路由模板、状态，401/404 也不写原始路径、查询、请求头或正文。探针导出 0 span，gRPC 业务上下文保留传播。默认根采样 0.1，file tracing 为 8MiB 活动文件和两个备份（每服务最多 24MiB）；Pod 删除会淘汰，长期保存用外部 OTLP。
- SQL 唯一来源 `migrations/` 并 embed；事务 advisory lock 下先 `002_upgrade.sql` 再 `001_init.sql`，同事务迁移旧密钥。生产用迁移角色先执行 `cmd/migrate`，应用设 `POSTGRES_AUTO_MIGRATE=false`；本地默认 true，ACK replay 同样遵守该开关。
- Helm 四服务均有 PDB；默认每 topic 1 分区，双 worker 主要提供接替。生产 overlay 提供 6 分区、RF3/ISR2、HPA 和软拓扑分散示例，不创建外部 HA 依赖。数据归档/清理、TDengine 测点列、容量目标与外部 HA/恢复演练仍待业务规划。

## 本地运行与操作入口

- 完整前置条件、启动顺序和外部依赖配置见 [README 中文版](README.zh-CN.md) 的“本地 Helm + Docker 部署”及 [生产部署指南](docs/production-deployment.zh-CN.md)。`scripts/helm-deploy-local.sh` 默认检查外部依赖，并使用 `iot-app:2.56`；Python 3 用于安全编码 EMQX 回调 headers JSON。
- 重建镜像先执行 `make build`。Dockerfile 最终阶段为 scratch，直接复制 `bin/`，`docker build` 不编译 Go；公共 CA 包由独立 trust 阶段复制。
- EMQX 回调 headers Secret 必须先准备；单节点 license 的滚动更新、节点驱逐和依赖停机须按实际状态诊断，历史故障中的删除/重建命令不作为日常启动步骤。
- Docker Desktop 集群重建或 CA 轮换后，检查 port-forward 使用的静态 kubeconfig 和 Prometheus token/CA 是否仍有效；必要时按 README 更新，不直接重用旧集群快照。监控入口为 `scripts/port-forward-local-monitoring.sh`。
- 常用本地地址：Management API `http://localhost:18080`，Demo `http://localhost:18084`，Prometheus `http://localhost:9090`，Grafana `http://localhost:3000`，EMQX Dashboard `http://localhost:18083`，MQTT `localhost:1883`。端口以实际部署为准。

## 当前本地资源（Docker容器2026-10-30核实）

- 应用镜像、demo及最近核实的Helm状态统一见顶部；运行集群状态必须注明核实日期和权限范围，不能把历史Ready或仓库默认值视为实时状态。
- `docker ps` 可见 `postgres`、`kafka`、`tdengine`、`prometheus`、`grafana`、`iot-demo` 和两个 k8s-forward 容器运行；PostgreSQL 显示 healthy，其余容器 Up 不代表业务链路验证通过。
- 持久卷：Grafana `iot-grafana-data`，PostgreSQL `iot-postgres-data`，Kafka `iot-kafka-data`，TDengine `iot-tdengine-data` / `iot-tdengine-log` / `iot-tdengine-corefile`，已按运行容器挂载核对。
- 当前 Grafana 容器声明的镜像 tag 为 `grafana/grafana:latest`，仓库 Compose 则固定 `13.2.1` digest；这是实际容器与仓库配置的差异，不能把仓库默认值当作已部署版本。本次仅记录，未重建容器。

## 维护规则

- 业务 Kubernetes 变更只修改 `charts/iot`；生产部署使用不可变版本或 digest，凭据、Webhook token、私有运行快照不入库。
- 修改代码或 SQL 后更新本文件，按 `AGENTS.md` 完成验证、提交、推送；每次提交打简单递增 tag，tag 位于 main 并随提交推送。纯测试/文档提交明确标注范围，不自动升级镜像、Chart 或运行环境。
- 本文件顶部只保留最近一次核实的版本/部署状态及其时间、来源；验证结论区分本次检查、完整集成测试和历史部署证据。历史修复/部署过程移入 `docs/history/` 并保留链接，不把旧“当前”描述重新追加到正文。
- 新增 REST 字段同步更新 protobuf、双语 README 和命令 REST DTO `contracts.CommandResponse`。REST 路由以 `internal/adminapi` 为事实来源，仓库没有 OpenAPI 文档。MQTT Schema 唯一人工维护源为 `docs/mqtt-envelope.schema.json`，由 `docs/contracts.go` embed，不在 Go 中重写第二份。
- `internal/platform/handlers.go` / `memory_store.go` 保留测试 harness，生产入口不启用 `EnableBusinessAPI`；新增生产 REST 进 adminapi，契约变化同时核对测试入口。
- DLQ 保留人工重放策略（见 ADR 0006）：先 dry-run、修根因，再定向重放；命令相关重放可能触发设备动作。面板/告警已有写入和写失败信号，DLQ 未配置消费侧深度监控，仍需关注保留窗口。

## 文档入口

- [README 中文版](README.zh-CN.md) / [英文版](README.md)
- [领域术语与模块职责](CONTEXT.md)
- [生产部署指南（中文）](docs/production-deployment.zh-CN.md) / [英文](docs/production-deployment.md)
- [EMQX 部署说明](deploy/emqx/README.zh-CN.md)
- [MQTT Schema](docs/mqtt-envelope.schema.json)
- [架构 ADR](docs/adr/)
- [截至 2.54 的完整历史交接快照](docs/history/ai-handover-through-2.54.md)
