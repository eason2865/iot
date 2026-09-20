# AI Handover

## 2026-09-20 生产风险整改与 Go 升级
- 当前源码与构建基线升级到 Go `1.27.1`：`go.mod` 使用 `go 1.27.0` 和 `toolchain go1.27.1`；`go.sum` 已由 `go mod tidy` 更新，README/生产文档同步。
- MQTT 认证已收敛到 `iot-core` 内部 HTTP 回调：设备用户名为 `tenantId:deviceId`，PostgreSQL 保存 bcrypt `secret_hash`，EMQX 配置 `authorization.no_match=deny` 并按设备下发 ACL；管理 API 业务路由要求 Bearer Token，健康检查和契约端点保持公开。旧本地明文 secret 在启动迁移时转哈希并清空。
- 命令投递改为 PostgreSQL `created` 入库、`FOR UPDATE SKIP LOCKED` 领取租约、发布成功后 `sent`、deadline 扫描 `timeout`；UUIDv7 作为命令 ID；ACK 写入 `command_ack`/`command_events`，允许发布和 ACK 的竞态顺序以及重复/迟到 ACK。
- Kafka 解码、业务落库、TDengine 和 MQTT 投递失败先进入 `iot.dlq`，成功后才提交原位点；新增 `cmd/dlq-replay`，人工审核后按批重放。TDengine 改为 `telemetry_v2` supertable + 每设备子表，租户/设备为 Tags，写入 payload hash/size，完整长 payload 由 PostgreSQL JSONB 保存。
- 租户/命令列表新增 protobuf cursor 分页，REST 返回 `items/nextCursor`，单页上限 100；生成的 `core.pb.go`/`core_grpc.pb.go` 已同步。
- Helm 为业务服务注入 `iot-runtime-secrets`，`iot-core` 暴露 9090 MQTT 认证端口；本地部署脚本负责创建开发 Secret。EMQX 本地/生产清单都包含 HTTP 认证回调配置。
- 验证结果：`go test ./...`、`make build`、Docker 镜像构建、`helm lint charts/iot`、Helm revision 31 本地滚动部署均通过；7 个业务 Pod 全部 `1/1`，管理 API 无 Token 返回 401、带 Token 返回 200，设备正确密钥 allow/错误密钥 deny，新命令实测 `created -> acked`，DLQ 主题存在，Prometheus 的 5 个 target 均 `up`，TDengine `telemetry_v2` 已有数据和设备子表。

## 2026-09-20 EMQX 长连接集群方案
- 已安装 EMQX Operator `2.3.0` 到本地 Kubernetes 的 `emqx-operator-system` 命名空间；该 Operator 作为独立基础设施保留，当前为 `1/1 Available`。
- 新增 `deploy/emqx/cluster.yaml`：独立 `emqx` 命名空间、3 个持久化 Core 节点、2 个 Replicant 节点、LoadBalancer listener/dashboard Service、PDB、平滑连接迁移参数与 `standard` StorageClass PVC。应用侧 Helm 默认改用 `emqx-listeners.emqx.svc.cluster.local:1883`。
- 应用已支持 `EMQX_CLIENT_ID_SUFFIX`；Helm 将 Pod 名注入为后缀，并使用 EMQX 共享订阅处理遥测和 ACK。`telemetry-ingestor` 默认 2 副本；`device-worker` 暂保持 1 副本，后续扩容需先增加 Kafka 分区并验证消费语义。
- 本地已实际应用 `deploy/emqx/cluster.local.yaml`：`emqx/emqx` 为单 Core、PVC 已 Bound、CR 状态 `Ready`。Compose 已移除旧 Docker EMQX 和 `iot-emqx-data`/`iot-emqx-log` 卷，改为转发 `emqx-listeners:1883` 与 `emqx-dashboard:18083`；业务入口转发器也发布宿主机 `18080/18081/18082/18090/18091`。四个业务 Deployment 已发布为 `iot-app:local-cdbcd42483ee`，分别为 2/2、2/2、2/2、1/1 Ready。
- 实测发现 EMQX 6.3.1 的默认社区许可仅允许单节点，多节点 Core 会报 `SINGLE_NODE_LICENSE`。生产集群清单要求 `emqx-license` Secret 的 `key` 字段；收到商业或试用许可证后，创建 Secret、应用 `cluster.yaml`、等待 `emqx/emqx Ready`，再完成持证 3 Core + 2 Replicant 切流验证。本地全链路已通过 `IOT_E2E=1 go test ./internal/platform -run TestE2ESchemeTelemetryCommandAck -count=1`，并确认 Prometheus 5 个 target 均 `up`、EMQX 中两条 telemetry-ingestor 和一条 device-worker 长连接在线。

## 2026-09-20 移除业务 etcd 服务发现
- 用户确认按 Kubernetes Service DNS 直接调用的方案执行。`management-api` 现在读取 `IOT_CORE_ENDPOINTS`，默认直连 `127.0.0.1:9001`；Helm 环境设置为 `iot-core:9001`。
- `iot-core` 不再向 etcd 注册，`management-api` 不再从 etcd 发现服务；两侧均保留 go-zero gRPC 的原有中间件、超时和重试逻辑。新增单元测试，断言客户端使用 direct endpoints、服务端没有 etcd 配置。
- 已从 `monitoring/docker-compose.yml` 删除 `etcd` 容器及 `iot-etcd-data` 卷；Helm values、ConfigMap、initContainer 检查和本地部署脚本不再引用 etcd。`go.mod` 中的 etcd 仍可能是 go-zero 间接编译依赖，不代表运行时需要 etcd。
- 已同步 `.env.example`、README、CONTEXT、ADR 0001/0003 与技术方案 HTML。旧 handover 条目中的 etcd 记载仅为历史记录。
- Docker Desktop 本地发布会将当前应用镜像临时标记为 `iot-app:local-<image-id>` 并导入 `desktop-control-plane` 的 containerd，再使用该不可变标签滚动发布；导入后自动删除本机临时标签，避免重用 `iot-app:2.0` 时命中集群旧镜像缓存。
- 实机验证：Helm revision 26 的四个业务 Pod 均为 `1/1 Running`；`iot-common-config` 为 `IOT_CORE_ENDPOINTS=iot-core:9001`；通过 management API 新建并读取临时租户，iot-core 的 `CreateTenant` 成功指标为 1。Prometheus 四个业务 target 均为 `up`，EMQX 有 7 个在线客户端，两个实时 Kafka 消费组 lag 为 0。Compose 已移除 etcd 容器和 `iot-etcd-data` 卷。

## 2026-09-20 Helm 禁用依赖清理
- 用户要求清理当前 Helm 中所有已禁用的旧部署分支。`charts/iot` 已收敛为固定部署四个业务服务：`management-api`、`iot-core`、`telemetry-ingestor`、`device-worker`。
- 已删除 Kubernetes 内置 PostgreSQL、Kafka、EMQX、TDengine、etcd、Prometheus、demo 及其 Secret/初始化 SQL 模板，同时删除 `values-local-stack.yaml`；这些依赖不再存在可重新启用的 Helm 开关。
- `externalDependencies` 现在是唯一配置来源；业务 Pod 始终等待并连接外部 PostgreSQL、Kafka、EMQX、TDengine。本地依赖、监控和 demo 仍由 `monitoring/docker-compose.yml` 管理。
- 已同步 `README.md`、`CONTEXT.md` 和 ADR 0003。后续生产化 EMQX 应独立于此 Chart，在 `emqx` namespace 由 EMQX Operator 管理。

## 2026-09-20 本地 Docker 项目统一与 EMQX 升级
- 用户要求不保留历史数据或兼容性，按干净状态重建本地 IoT Docker 环境。`monitoring/docker-compose.yml` 现在使用 Compose 项目名 `iot`，Docker Desktop 中所有相关容器归入同一 `iot` 组。
- 命名规则：原生依赖使用 `postgres`、`kafka`、`tdengine`、`emqx`、`etcd`、`prometheus`、`grafana`；项目自定义容器使用 `iot-` 前缀，例如 `iot-demo` 和 `iot-k8s-forward-*`。所有持久卷也使用 `iot-` 前缀。
- PostgreSQL、Kafka、TDengine、EMQX、etcd、Prometheus、Grafana 与 demo 都由同一 Compose 文件声明；旧独立容器、旧 `monitoring` Compose 网络和历史数据卷已按用户授权删除。TDengine 现在也使用 Docker 命名卷而非 `~/tdengine` bind mount。
- EMQX 固定为 `emqx/emqx-enterprise:6.3.1`，不再使用 `latest`；单节点名固定为 `emqx@127.0.0.1`，数据与日志卷分别为 `iot-emqx-data`、`iot-emqx-log`。已验证 `emqx ctl status` 显示 `6.3.1` 且 healthcheck 正常。
- 清空 Grafana 数据卷意味着此前仅保存在 Grafana 数据库中的钉钉联系人、通知策略和已导入告警规则已被重置；仓库 provisioning 已自动恢复数据源和 4 个 IoT 看板。钉钉 Webhook 不存储在仓库，需要用户在新的 Grafana 中重新配置后才能恢复通知。
- 已重启 `iot` namespace 的四个业务 Deployment；从空 PostgreSQL、Kafka、TDengine 和 EMQX 环境完成验证：四个 Pod 均 `1/1 Ready`，Prometheus 五个 target 均 `up`，Demo 约 `12/s` telemetry、worker ACK 约 `11/s`、worker error 为 `0`，EMQX 有 7 个在线客户端。

## 2026-09-16 运行服务命名统一与全链路验证
- 用户要求不保留兼容别名，运行服务已统一重命名为：`management-api`（原 `admin`）、`iot-core`（原 `core-rpc`）、`telemetry-ingestor`（原 `ingress`）、`device-worker`（原 `worker`）。已同步 Go 入口、Dockerfile、Makefile、Helm 资源与 values、环境变量、etcd 注册键、Kafka 消费组与客户端标识、Prometheus job、Grafana 看板/规则、demo、部署脚本、README、上下文、ADR 和架构图。
- 本地部署边界：四个业务服务由 Helm 部署在 Kind 的 `iot` namespace；PostgreSQL、Kafka、EMQX、TDengine、etcd、Prometheus、Grafana 和 demo 由 `monitoring/docker-compose.yml` 运行。Prometheus/Grafana 属于 IoT 观测链路，但不随业务 Helm release 发布；Prometheus 通过 Docker 网络内的 `k8s-forward-management-api*`、`k8s-forward-iot-core`、`k8s-forward-telemetry-ingestor`、`k8s-forward-device-worker` 抓取指标。
- Kafka 迁移策略：新的 `iot-device-worker-command` 和 `iot-device-worker-telemetry` group 首次从 `LastOffset` 启动，避免新服务名重放迁移前已完成的业务消息；已有 group 的已提交 offset 不受影响。
- E2E 测试 MQTT client ID 与 trace 文件名也已改用新服务术语，`CHANGELOG.md` 的入口清单同步为新名称。
- 本地全链路已实际验证：四个业务 Deployment 均为 `1/1 Ready`；Prometheus 的 `demo-docker`、`management-api`、`iot-core`、`telemetry-ingestor`、`device-worker` 五个 target 均为 `up`。一分钟窗口内 Demo telemetry 约 `12.4/s`、telemetry-ingestor MQTT/Kafka 约 `12.3/s`、device-worker telemetry 约 `12.3/s`、命令 ACK 约 `8.3/s`，worker error 为 `0`；两个新 Kafka group 的 lag 均为 `0`，日志持续出现 `command consumed` 与 `command ack consumed`。
- 验证期间 `device-worker` 曾因 TDengine 内 `taosd` 未就绪而重启；`docker restart tdengine` 后 `SHOW DATABASES` 成功且 `iot` 数据库仍在，worker 已恢复。若再次出现 `Unable to establish connection`，先检查 `docker exec tdengine taos -s 'show databases'`，再恢复 TDengine，不要误判为服务命名问题。
- 已通过 `go test ./...`、`docker compose -f monitoring/docker-compose.yml config`、Grafana 钉钉模板回归（6/6）以及 Grafana 新看板/告警规则 API 查询；告警联系人 URL 未修改。

## 2026-09-16 钉钉独立链接修复
- 用户反馈卡片正文多个链接点击目标相同。捕获原生 DingDing 请求确认正文链接不同，但 actionCard 还包含固定到 `/alerting/list` 的 singleURL；未直接验证钉钉客户端拦截行为。
- 改为 Webhook v1 + Custom Payload，发送钉钉 markdown JSON，移除整体跳转 singleURL，保留联系人名称“钉钉”、UID `ffyeiqivgwi68f` 及机器人 URL。模板新增 `iot.dingtalk.payload`，使用 coll.Dict/tmpl.Exec/data.ToJSON 安全编码。
- 已用临时本地捕获服务验证候选实际请求，再保存联系人。解密读取原 URL 仅在内存中操作，迁移后逐字比对 URL 不变，所有规则及通知策略不变，无临时代理地址留在配置中。
- 注意 Webhook URL 在 Grafana 中属于受保护普通配置，不再是 DingDing 的 secure URL 字段；不要输出完整联系人 settings 或提交 URL。恢复通知仍开启。
- 新增回归测试验证 JSON 引号/换行、非 ActionCard、四个链接目标不同及联系人 payload 引用；已有恢复/缺失字段/多实例测试保留。用 Grafana 渲染发送“Markdown 独立链接修复测试”，钉钉返回 errcode=0。
- 未实际操作钉钉客户端验证点击；请在新消息中确认（历史卡片不会更新）。测试通知没有 GeneratorURL 时省略规则链接，预览 fixture 和真实规则通知包含该链接。localhost 仍仅在本机有效，未扩大 Grafana 网络访问范围。

## 2026-09-16 钉钉联系人与通知模板规范化
- 用户明确限定范围为联系人标题、正文、卡片格式、恢复通知和模板，要求 Webhook URL 保持不变；不调整告警条件、阈值、防抖或通知频率。
- 新增 `monitoring/grafana/notifications/dingtalk.tmpl`，通过 API 保存为可编辑模板组 `iot.dingtalk`，定义 `iot.dingtalk.title` / `iot.dingtalk.message`；联系人 `ffyeiqivgwi68f` 改为引用模板，保留 actionCard，`disableResolveMessage=false`。
- 标题和正文固定带 `grafana` 关键词。区分 firing/resolved，显示数量、级别、服务、路由、摘要/详情、UTC+8 时间和可用的看板/图表/规则/静默链接；可选字段隐藏，最多显示 10 个实例。不虚构环境、负责人等标签。
- 仓库联系人片段不含 URL，更新时合并并保留加密 URL。已在内存中对比更新前后解密 URL 完全一致，同时断言所有规则及通知策略均未改变；未记录或提交 token。
- `scripts/test-grafana-notification-template.py` 通过 Grafana 预览 API 验证触发、恢复、缺失可选字段、混合状态及 10 条截断，4 项通过，不发送群消息。
- 另用新联系人模板渲染一条明确标注的“钉钉模板格式测试”，临时捕获并转发到原 URL，确认钉钉业务响应 errcode=0；捕获服务已关闭，联系人未保存临时转发地址。未伪造真实业务恢复。
- 图表链接仍继承 localhost，仅本机可达；未修改全局 root_url。用户旧联系人编辑页需刷新，避免覆盖新模板引用。

## 2026-09-16 告警规则跳转入口修正
- 用户旧页面的 `View alert rule` 生成 `pri$grafana$IoT$Admin HTTP$...` 标识，后续按目录显示名 `IoT` 请求 ruler API，复现 HTTP 403。同一管理员用目录 UID `efobmswzeefi8d` 查询该组成功，不应通过扩大权限绕过。
- 本次新打开的内置浏览器在修改前已生成两条正确规则 UID 链接；未复现同样的前端错误生成过程，因此旧页面状态/规则元数据未加载只是可能原因，不能断言为权限或单一配置错误。
- 将 Alert list 数据源名称规范化为 `-- Grafana --`，并在看板顶部增加两条固定 UID 的规则入口，避免依赖旧组件的合成标识；版本升到 5，已重新加载。未修改规则、通知或权限。
- 已验证实际看板 API 包含正确数据源和固定链接，两条规则详情 API 成功、页面入口 HTTP 200。浏览器后续复查因工具超时未完成，不把 SPA HTTP 200 当作详情页面渲染测试。
- 如用户停留在旧标签页，需完整刷新浏览器页面（不是看板内的 Refresh），或直接使用固定规则入口。旧 pri 地址不会自动重定向。

## 2026-09-16 healthz QPS 阈值告警
- 按用户截图新增 `/healthz`、`2xx` 专属规则 `iot-admin-healthz-qps`，模板 `monitoring/grafana/alerts/admin-healthz-qps.json`；与 HTTP 图表保持相同的 5 分钟 rate，严格 > 0.3 req/s。
- 使用已有 `Admin HTTP` 分组（60 秒评估），`for: 0s`、通知 `group_wait: 0s`，接收人为已有“钉钉”。不修改 5xx 或暂停中的 GC 规则。
- 关联 `iot-admin-api` panel 6；该序列增加 0.3 橙色虚线阈值，看板版本升到 4 并重新加载。
- 已用 Grafana 表达式 API 验证 0.299、0.3 不触发，0.301 触发。真实指标在 15:07:30 评估为 0.3016918472，health=ok、状态 Alerting；panel 6 产生 Normal -> Alerting 标记，通知日志确认向钉钉发起发送（未代替用户确认群内收件）。
- 健康检查速率贴近 0.3，采样抖动可能导致反复触发/恢复；当前遵循用户要求，未额外添加持续时间或恢复滞回。

## 2026-09-16 Admin HTTP 告警与看板关联
- 用户选择监控 HTTP 5xx 持续 1 分钟；新增可导入规则模板 `monitoring/grafana/alerts/admin-http-5xx.json`，本地已通过 provisioning API 导入且禁用只读 provenance，允许后续在 UI 编辑。
- 规则 UID `iot-admin-http-5xx`，分组 `Admin HTTP`，本地评估间隔 60 秒；表达式按 route/status 计算 5 分钟 5xx QPS，阈值 > 0，pending 1 分钟，无 5xx 序列时回退 0（不负责服务离线检测）。发送到已有“钉钉”联系人，仓库无 Webhook/token。
- 规则关联 `iot-admin-api` 的 panel 6；看板顶部增加当前告警/实例列表，开放 Annotations & Alerts 开关，HTTP 5xx 序列红色加粗。原有 GC 测试规则 `cfyeixhhzn4zkb` 保持暂停，不改条件。
- 已验证 JSON、面板布局与规则关联；Grafana API 显示规则 health=ok、状态 Normal；浏览器确认告警列表、Normal 实例和 HTTP 5xx QPS=0 正常显示。
- 未制造真实 HTTP 5xx 或伪造历史告警。触发/恢复时间标记在后续实际状态变化时产生；标记为评估时间，不能定位单次请求。新环境导入需先配置相应联系人，详见 README。

## 2026-09-16 Grafana 链接与持久化恢复
- 原目录链接 `efobmswzeefi8d` 返回 404；Grafana 未配置自动重启，数据库此前位于容器可写层，provisioning 也未固定 folderUid。
- Compose 增加 `restart: unless-stopped` 和命名卷 `iot-grafana-data:/var/lib/grafana`；固定 provisioning 的 `folderUid: efobmswzeefi8d`。
- 四个仪表盘 JSON 的版本从 1 调整为 2，触发 provisioning 更新并迁移到固定目录，仪表盘 UID 和面板内容不变。
- 已停止旧容器做冷备份：`/Users/lyc/.local/share/iot/backups/grafana-before-persistence-20260916/`；将数据库及插件等文件复制到命名卷，再重建容器。
- 已验证强制重建后健康检查正常、原目录 API 正常、目录下 4 个仪表盘保留，Prometheus 数据源健康检查为 OK。

## 2026-09-10 应用镜像单 tag
- 本地应用镜像只保留 `iot-app:2.0`；本地 Helm 脚本改用该镜像的 RepoDigest 部署，不再创建 `local-<hash>` tag，并保留按内容更新 Pod 的能力。
- kind 导入仍使用原始 `APP_IMAGE` tag；若本地镜像缺少匹配的 RepoDigest，则明确失败，避免把经典 Docker 的 config ID 当成 manifest digest。
- 备用 `values-local-stack.yaml` 的旧 `metrics2` 引用同步为 `2.0`。
- 已验证 shell 语法、Helm lint 和实际本地部署（revision 23）；四个 Deployment 均 Ready，Prometheus 5 个 target 均 up。已删除 `local-f08659f9994b`，Docker 应用镜像列表只剩 `2.0`。

## 2026-09-10 监控与注册中心镜像版本
- 用户要求将 IoT 使用的 etcd、Prometheus、Grafana 镜像切换为最新版本。
- 已更新本地 Docker Compose：
  - `quay.io/coreos/etcd:v3.6.14`：上游仓库不发布 `latest` 标签，故使用 Quay 已发布的最新 3.6 系列版本。
  - `prom/prometheus:latest`
  - `grafana/grafana:latest`
- 已同步 Helm：Prometheus 默认镜像改为 `prom/prometheus:latest`，etcd 模板改为 `quay.io/coreos/etcd:v3.6.14`。
- 风险：后续 `pull` 或重建会获取上游当时的最新版本，可能跨越主版本；升级前应备份 etcd / Prometheus 数据并验证 Grafana dashboard。

## 2026-09-10 本地全链路恢复：Worker MQTT 启动韧性
- 本地 Docker 依赖已恢复：PostgreSQL（`postgres-local`，数据卷 `iot-postgres-data`）、Kafka、EMQX、TDengine、etcd 3.6.14、Prometheus、Grafana。
- `Worker` 现在与 `MQTTBridge` 一致，开启 Paho `SetConnectRetry(true)` 并以 2 秒间隔重试；因此 EMQX 重启或短暂不可用时，worker 会等待连接恢复而非退出。
- 本次 `CrashLoopBackOff` 的实际根因是 TDengine 容器中 `taosd` 已退出，仅 adapter/keeper 仍在运行；worker 初始化 TDengine schema 时收到 `Unable to establish connection`。重启 `tdengine` 后，`taosd`、`SHOW DATABASES` 与 worker 均恢复，挂载目录 `/Users/lyc/tdengine/data` 中的数据保留。
- Docker Desktop 不能从容器回连宿主机 `kubectl port-forward` 监听端口。监控 Compose 现通过 `k8s-forward-*` 容器加入 `kind` 网络，在 Docker 内部转发 admin、ingress、worker、core-rpc；demo 与 Prometheus 使用这些 Docker DNS 名称，不依赖宿主机回环网络。

## 2026-06-08 全链路回归与本地部署镜像修复
- 用户要求“全链路再测一遍”。
- 已完成基础回归：
  - `go test ./...` 通过。
  - `make build` 通过。
  - `helm lint charts/iot` 通过。
  - 默认 Helm template 与 `charts/iot/values-local-stack.yaml` template 均通过。
- 已完成代码层 E2E：
  - `IOT_E2E=1 go test ./internal/platform -run TestE2ESchemeTelemetryCommandAck -count=1 -v` 通过。
  - `IOT_E2E=1 IOT_E2E_LOAD=1 go test ./internal/platform -run TestE2ELoadMultiTenantMultiDevice -count=1 -v` 通过，覆盖 5 租户、50 设备、300 telemetry、50 command/ACK。
- 回归中发现并修复两个本地部署问题：
  - scratch 镜像内默认不存在 `/tmp`，file trace exporter 默认写 `/tmp/<service>-traces.log` 时会报 `file exporter endpoint error`；现 `TraceConfig` 会在 file exporter 配置阶段创建 endpoint 父目录，并补 `TestTraceConfigCreatesFileEndpointDir`。
  - `scripts/helm-deploy-local.sh` 之前没有把 `APP_IMAGE` 传给 Helm，且固定 tag 重建后 k8s `IfNotPresent` 可能复用旧镜像；现脚本会根据当前本地镜像内容生成 `local-<hash>` tag，传入 `images.app`，kind 场景也加载该内容 tag。
- 已重新构建并部署：
  - `docker build -t iot-app:2.0 .` 通过。
  - `scripts/helm-deploy-local.sh` 通过，Helm revision 20，`admin`、`core-rpc`、`ingress`、`worker` 均 Running/Ready。
  - Deployment 镜像已切到 `iot-app:local-df4bca89ae27`。
- 已完成部署级 smoke：
  - 集群内 `admin`、`ingress`、`worker` `/healthz` 正常。
  - 集群内 `core-rpc:9101/metrics` 正常。
  - 通过前台 `kubectl port-forward` 短跑 demo，k8s admin 产生 tenant/device/command 201；k8s worker 日志确认 `smoke-1780890578` 的 telemetry、command consumed、command ack consumed。
  - 新 Pod 近 3 分钟日志未再出现 trace exporter、panic、fatal 错误。
- 对应提交 tag：`2.6`。

## 2026-06-08 架构收敛：契约文档迁移
- 用户要求直接优化项目架构、目录结构和文件组织，并要求每次提交新增递增 tag。
- 已将 OpenAPI 与 MQTT Envelope Schema 的生成函数从 `internal/platform` 迁移到 `internal/contracts/docs.go`。
- `internal/adminapi` 与旧 `platform.App` 文档端点现在都直接引用 `contracts.OpenAPISpec()` 和 `contracts.MQTTEnvelopeSchema()`。
- 目的：让外部契约归属 `contracts`，减少 `platform` 同时承担 HTTP 契约和应用运行时职责的混杂。
- 已验证：
  - `go test ./internal/contracts ./internal/platform ./internal/adminapi` 通过。
- 对应提交 tag：`2.1`。

## 2026-06-08 架构收敛：接口去重
- 已删除 `platform.Store` / `platform.Publisher` 与 `core.Repository` / `core.Publisher` 的重复接口定义。
- 现在统一使用：
  - `platform.Repository`
  - `platform.MessagePublisher`
- `PostgresStore`、`memoryStore`、`KafkaPublisher` 只保留一套公开方法实现，删除原大小写双方法包装。
- `platform.App`、`MQTTBridge`、`Worker`、`bootstrap`、`core.Service` 均已切到统一接口。
- 目的：减少浅接口和重复 seam，让测试面和运行时装配面一致。
- 已验证：
  - `go test ./internal/core ./internal/platform` 通过。
  - `go test ./internal/platform ./internal/bootstrap ./internal/core` 通过。
- 对应提交 tag：`2.2`。

## 2026-06-08 架构收敛：platform 文件拆分
- 已保持 `internal/platform` package 不变，仅按职责拆分文件，降低迁移风险。
- 新增：
  - `internal/platform/domain.go`：领域数据结构与命令状态别名。
  - `internal/platform/handlers.go`：旧 `platform.App` REST handlers。
  - `internal/platform/http_helpers.go`：HTTP JSON 编解码与响应辅助。
  - `internal/platform/memory_store.go`：内存仓储实现。
- `internal/platform/platform.go` 现在只保留 `App` 配置、构造、路由和 HTTP 观测包装，文件从约 380 行降到约 82 行。
- 目的：不改变运行行为的前提下，让 platform 目录职责更清楚，后续继续拆 adapter/package 时更安全。
- 已验证：
  - `go test ./internal/platform ./internal/core ./internal/demo` 通过。
- 对应提交 tag：`2.3`。

## 2026-06-08 架构收敛：上下文与 ADR
- 已新增 `CONTEXT.md`，记录平台稳定领域术语、运行模式术语和 module ownership。
- 已新增 ADR：
  - `docs/adr/0001-core-rpc-admin-api-split.md`
  - `docs/adr/0002-storage-and-event-split.md`
  - `docs/adr/0003-helm-only-kubernetes-manifests.md`
- 已更新 README 的项目结构说明，补齐 `adminapi`、`core`、`demo`、`charts/iot`、`proto`、`monitoring` 和 ADR 的目录职责。
- 目的：让后续会话和新开发者不再反复猜核心架构决策。
- 对应提交 tag：`2.4`。

## 2026-06-08 架构收敛：运行时配置辅助
- 已新增 `internal/runtimeconfig/config.go`，统一无业务语义的环境变量解析：
  - `EnvOrDefault`
  - `SplitCSV`
  - `Int`
  - `Duration`
  - `ListenAddr`
  - `ListenHost`
  - `ListenPort`
- `internal/bootstrap`、`internal/core`、`internal/adminapi`、`internal/demo` 已切到共享 runtimeconfig helper。
- 业务默认值仍保留在各运行模块调用处，避免 runtimeconfig 变成新的全局配置大杂烩。
- 已验证：
  - `go test ./internal/runtimeconfig ./internal/bootstrap ./internal/core ./internal/adminapi ./internal/demo` 通过。
- 对应提交 tag：`2.5`。

## 2026-06-08 Helm 与 k8s 本地部署目录收敛
- 用户要求对比 Helm 与 `k8s` 目录后删除重复的 `k8s` 目录。
- 已读取 `ai_readme`，当前目录下无可读文档文件。
- 已完成对比：
  - `helm template iot ./charts/iot -n iot -f charts/iot/values-local-stack.yaml`
  - `kubectl kustomize k8s/local`
  - 资源集合仅差 `Namespace/iot`，由 Helm 安装命令的 `--create-namespace` 覆盖。
  - 去掉 Helm 自动 labels、namespace 字段和单独 Namespace 资源后，归一化 manifest 字段级一致。
- 已新增 `charts/iot/values-local-stack.yaml`，用于复刻旧 `k8s/local` 的集群内全量依赖部署：
  - `externalDependencies.enabled=false`
  - `admin/coreRpc/ingress/worker/demo` 均启用
  - `postgres/kafka/emqx/tdengine/prometheus` 均启用
  - `images.app=iot-app:metrics2`
  - 清空 `prometheus.extraScrapeTargets`
- 已删除 `k8s` 目录。
- 已更新 README 的 Helm 部署说明，加入 full-stack values 用法，并移除旧 `kubectl apply -k k8s/local` 提示。
- 已验证：
  - `helm lint charts/iot` 通过。
  - `helm template iot charts/iot -n iot -f charts/iot/values-local-stack.yaml` 通过。

## 2026-06-07 TDD 全链路回归与测试隔离修复
- 用户要求用 TDD 验证全部链路、全部功能、全部服务、全部模块；本轮读取 `tdd` 技能后执行红绿回归。
- 首轮验证结果：
  - `go test ./... -count=1` 通过。
  - `helm lint charts/iot` 通过。
  - 并行执行单设备 E2E 与多租户负载 E2E 均通过，但日志暴露测试隔离问题。
- 发现并修复的问题：
  - 并行 E2E 时多个测试 worker 使用不同 Kafka group，会各自消费全局 command/telemetry topic，导致重复下发其他测试的命令，设备重复 ACK，出现 `invalid command transition` 噪声。
  - 多轮 E2E 复用固定 MQTT client id，上一轮连接尚未完全释放时下一轮会踢掉旧连接，偶发 `Subscribe` 连接噪声。
- 已按 TDD 增量补强：
  - `WorkerConfig` 新增 `AckTopicFilters []string`，支持多租户测试或租户分片部署订阅多个 ACK filter；未配置时仍默认 `tenant/+/device/+/ack`。
  - `WorkerConfig` 新增 `TenantIDs []string` allowlist；未配置时处理全部租户，配置后 telemetry/command 消费只处理指定租户并提交跳过其他租户消息。
  - E2E/load 测试为每轮生成唯一 MQTT client id，避免连接互踢。
  - E2E/load 测试为 worker 配置本轮租户 allowlist 和 ACK filter，避免并行/连续运行串扰。
  - 新增 `internal/platform/worker_test.go`，覆盖 ACK filter 归一化和 worker 租户 allowlist 行为。
- 最终验证结果：
  - `go test ./internal/platform -run 'TestNormalizeAckTopicFilters|TestWorkerTenantAllowed' -count=1` 通过。
  - `go test ./... -count=3` 通过。
  - `make build` 通过，`admin`、`core-rpc`、`demo`、`ingress`、`worker` 均构建成功。
  - `helm lint charts/iot` 通过。
  - 并行执行 `IOT_E2E=1 go test ./internal/platform -run TestE2ESchemeTelemetryCommandAck -count=3 -v` 通过。
  - 并行执行 `IOT_E2E=1 IOT_E2E_LOAD=1 go test ./internal/platform -run TestE2ELoadMultiTenantMultiDevice -count=3 -v` 通过，未再出现 `command not found`、`invalid command transition` 或 ACK 订阅连接错误。

## 2026-06-07 消息链路生命周期诊断与修复
- 本轮按 `diagnose` 流程排查 `messages.dropped.no_subscribers=436836`，覆盖连接、发布、订阅、消费、处理、日志、监控和端到端测试。
- 关键根因已定位：
  - `ingress` 与 `worker` 共用 `EMQX_CLIENT_ID=iot-ingress`，后连接的 worker 会踢掉 ingress，导致 EMQX 只剩 ACK 订阅而没有 telemetry 订阅，上行消息找不到 MQTT 订阅者。
  - 本地 Kafka 同时被宿主机测试和 k8s Pod 访问，但 advertised listener 只有单一地址，曾出现宿主机连接后被 metadata 重定向到不可达地址的问题。
  - demo 设备侧 MQTT 回调里同步发布 QoS1 ACK，叠加回调锁持有，长时间运行后可能出现健康检查正常但业务指标停止增长。
  - E2E 关闭链路时 TDengine writer 可能在 channel close 后继续写入，引发 `send on closed channel` panic。
- 已将 MQTT 运行时契约统一为：
  - telemetry：`tenant/{tenantId}/device/{deviceId}/telemetry`
  - command：`tenant/{tenantId}/device/{deviceId}/command`
  - ack：`tenant/{tenantId}/device/{deviceId}/ack`
- 已在 `internal/contracts/topic.go` 增加统一 topic 构造、过滤器和 topic segment 校验，并在 envelope、core service、platform API 入口拒绝包含 `/`、`+`、`#` 的 tenant/device/topic 标识。
- 已拆分 MQTT client id：
  - `EMQX_INGRESS_CLIENT_ID` 默认 `iot-ingress`
  - `EMQX_WORKER_CLIENT_ID` 默认 `iot-worker`
  - 已同步 `.env.example`、Helm ConfigMap、k8s local ConfigMap、README 和 bootstrap 读取逻辑。
- 已验证 EMQX 订阅恢复为稳定双订阅：
  - `iot-ingress -> tenant/+/device/+/telemetry`
  - `iot-worker -> tenant/+/device/+/ack`
- 已补 Kafka 冷启动和本地网络可靠性：
  - 新增 `internal/platform/kafka_topics.go`，publisher/worker 启动时 best-effort 确保 Kafka topic 存在。
  - Kafka writer 开启 `AllowAutoTopicCreation`，写入增加瞬时错误重试。
  - Helm 本地部署改为 k8s Pod 使用 Docker Desktop 网关 `192.168.65.254:29092`，宿主机测试使用 `localhost:9092`。
  - `scripts/helm-deploy-local.sh` 增加 `DOCKER_GATEWAY_KAFKA_PORT=29092`，并将 Helm wait/netcheck 一并切到该端口。
- 已补 MQTT bridge 背压处理：
  - `MQTTBridgeConfig` 新增 `PublishConcurrency` 和 `PublishSlotTimeout`。
  - 上行消息不再因瞬时 backlog 满而立即丢弃，默认等待 publish slot，减少内部链路人为丢消息。
- 已补 worker 消费可靠性：
  - Kafka reader 改为稳定 consumer group，默认 `iot-worker-telemetry` / `iot-worker-command`。
  - E2E/load 测试使用唯一 group id 与 `kafka.LastOffset`，避免历史 demo 或旧测试消息串扰。
  - Kafka transient fetch error 会计数、短暂等待并继续消费；`context.Canceled` 正常退出。
  - 异常 command topic / marshal error 会记录、计数并提交消息，避免 poison message 卡住消费。
- 已补 QoS：
  - telemetry 订阅、command 下发、ACK 订阅、demo bus、E2E 发布订阅均统一使用 QoS1。
- 已修复 demo MQTT 回调停滞：
  - Paho 设置 `SetOrderMatters(false)`。
  - demo MQTT bus 分发 handler 时先复制 handler 列表再释放锁，避免回调内发布和订阅分发互相阻塞。
- 已修复 TDengine writer 关闭竞态：
  - writer 增加 `closedCh`、锁和 closed 状态。
  - close 后 `WriteTelemetry` 返回 `errTDengineWriterClosed`，不再向已关闭 channel 发送。
  - 新增 `TestTDengineWriterWriteAfterCloseDoesNotPanic`。
- 已完成多轮验证：
  - `go test ./... -count=3` 通过。
  - `helm lint charts/iot` 通过。
  - `IOT_E2E=1 go test ./internal/platform -run TestE2ESchemeTelemetryCommandAck -count=3 -v` 通过。
  - `IOT_E2E=1 IOT_E2E_LOAD=1 go test ./internal/platform -run TestE2ELoadMultiTenantMultiDevice -count=3 -v` 通过，单轮覆盖 5 租户、50 设备、300 telemetry、50 command/ACK。
  - 已重新构建 `iot-app:2.0` 并通过 `scripts/helm-deploy-local.sh` 部署，`admin`、`core-rpc`、`ingress`、`worker` 均 Running/Ready。
  - 已验证 `/healthz`、服务 `/metrics`、Prometheus `/-/healthy` 正常。
  - Prometheus 1 分钟窗口内 demo、MQTT bridge、worker telemetry/command/ack 均有 ok 速率，error 速率为 0。
- 注意事项：
  - 为避免 E2E/load 测试与持续造流互相干扰，验证期间曾停止 demo；需要恢复监控造流时，先启动本地 port-forward，再执行 `docker compose -f monitoring/docker-compose.yml up -d demo`。
  - Docker Kafka 推荐使用双 listener：宿主机 `localhost:9092`，Docker/k8s 网关 `192.168.65.254:29092`。

## 2026-06-07 MQTT topic 契约统一
- 已统一当前运行时的 MQTT 设备契约为三类 canonical topic：
  - 上行遥测：`tenant/{tenantId}/device/{deviceId}/telemetry`
  - 下行命令：`tenant/{tenantId}/device/{deviceId}/command`
  - ACK 回执：`tenant/{tenantId}/device/{deviceId}/ack`
- 已在 `internal/contracts/topic.go` 抽出 topic suffix 常量和构造函数：
  - `TopicSuffixTelemetry`
  - `TopicSuffixCommand`
  - `TopicSuffixAck`
  - `BuildTelemetryTopic`
  - `BuildCommandTopic`
  - `BuildAckTopic`
  - `TelemetryTopicFilter`
  - `AckTopicFilter`
  - `BuildTenantCommandTopicFilter`
- 已把 `demo`、`worker`、`ingress`、`e2e` 和 `simulator` 测试全部切到统一构造函数和过滤器常量，减少 topic 裸字符串分散。
- 已同步 `README.md` 和 `docs/物联网平台技术方案.html` 的 Topic 说明与时序图，只保留当前运行时契约。
- 已补回归测试，确认 canonical topic 构造函数输出符合统一契约。
- 已验证 `go test ./...` 通过。

## 2026-06-06 Grafana 业务监控与 demo 造流闭环
- 已按 IoT 业务闭环重新整理 Grafana dashboard：
  - `IoT Overview`：服务在线数、demo 拓扑规模、遥测入库速率、命令创建速率、ACK 成功率、上行链路、命令闭环、HTTP/gRPC 延迟和错误汇总
  - `IoT Admin API`：Admin HTTP QPS、5xx、命令创建/ACK、路由级延迟、业务操作计数和错误拆解
  - `IoT Core RPC`：core-rpc gRPC QPS、错误 QPS、p95、方法拆分和核心业务 RPC
  - `IoT Pipeline`：demo 造流、MQTT bridge、Kafka publish、worker、TDengine 和 pipeline 错误
- 已补 Prometheus 指标：
  - demo 拓扑 gauge：`iot_demo_topology_tenants`、`iot_demo_topology_devices`、`iot_demo_topology_agents`
  - demo ACK 事件：`iot_demo_events_total{kind="ack",result="ok|error"}`
  - 常见 ok/error 标签组合预置为 0，避免 Grafana 在无错误时显示 `No data`
- 已增强 demo：拓扑初始化后写入拓扑 gauge，设备侧 ACK 发布会统计成功/失败。
- 已增强 MQTT/Kafka 链路：
  - Kafka 写入增加 5 秒超时，避免本地 Kafka/metadata 异常导致 MQTT 回调永久卡住
  - MQTT bridge 增加连接、订阅、解析、发布失败日志
  - MQTT bridge 改为异步限流发布 telemetry 到 Kafka，避免 Paho 消息回调被 Kafka 写入阻塞
- 已修复本地 Docker + k8s 依赖寻址：
  - Helm values / 部署脚本改为默认使用 Docker Desktop 网关 `192.168.65.254`
  - Docker etcd advertise client URL 改为 `http://192.168.65.254:2379`
  - Kafka 容器已用 `KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://192.168.65.254:9092` 重建
  - 应用镜像 tag 已统一为 `iot-app:2.0`，避免 `IfNotPresent` 复用旧镜像导致新代码没有进入 k8s
- 已重新部署并验证：
  - `go test ./...` 通过
  - `helm lint charts/iot` 通过
  - `scripts/helm-deploy-local.sh` 部署通过
  - Prometheus targets：`admin`、`core-rpc`、`ingress`、`worker`、`demo-docker` 全部 `up`
  - demo 拓扑：5 个租户、50 台设备
  - 当前 1 分钟错误窗口：demo error、worker error、admin 5xx、core-rpc non-OK 均为 0
  - 5 分钟业务窗口已确认有数据：demo telemetry/command/ack、MQTT bridge、Kafka telemetry publish、worker telemetry/command/ack、TDengine write、HTTP/gRPC p95

## 2026-06-06 go-zero 核心微服务拆分
- 已按用户确认的推荐方案落地：保留现有接入/消费链路，同时将核心业务拆出为 `core-rpc`，`admin` 改为 go-zero REST 网关并通过 gRPC 调用核心服务
- 已新增 `proto/core/v1/core.proto` 及生成代码，服务发现和注册使用 etcd，核心服务和 admin 均已接入 go-zero / grpc / protobuf / etcd
- 已补齐本地 Docker etcd：`monitoring/docker-compose.yml` 新增 etcd 服务，Helm / 本地 k8s / 启动脚本也同步支持
- 已更新部署脚本与部署模板：
  - `scripts/helm-deploy-local.sh`
  - `charts/iot/templates/core-rpc.yaml`
  - `charts/iot/templates/etcd.yaml`
  - `k8s/local/core-rpc.yaml`
  - `k8s/local/etcd.yaml`
- 已更新构建与发布链路：
  - `Makefile`、`Dockerfile`
  - `.env.example`
  - `README.md`
  - `docs/物联网平台技术方案.html`
- 已完成回归验证：
  - `go test ./...` 通过
  - 本地 Helm 部署通过
  - 端到端 smoke test 通过：创建 tenant、device、telemetry、command、ACK 全链路正常
- 最新代码已经完成本地构建、Helm 部署和 Prometheus targets 验证，发布 tag 已按用户要求重新调整

## 2026-06-06 观测能力补强
- 已启用 go-zero 的 REST / gRPC tracing middleware，并通过 `core/trace.StartAgent` 接入 OpenTelemetry
- `admin`、`core-rpc`、`ingress`、`worker`、`demo` 启动时都会初始化 tracing / logging 基础设施
- 默认 trace exporter 使用 file batcher，输出到 `/tmp/<service>-traces.log`，支持通过 `OTEL_*` 环境变量调整
- 标准输出日志已改为结构化 JSON 风格，减少后续排障时的人工 grep 成本
- README 已新增“观测”小节，说明 `/metrics`、trace 和日志约定

## 2026-06-06 requestId 透传补强
- HTTP 请求会自动补 `X-Request-Id`，并在 `admin -> core-rpc` 的 gRPC 调用链里继续传递
- `demo` 发往 `admin` 的请求会带上 request id 和 trace 上下文，便于联调和压测时串联链路
- `platform` 新增 requestId middleware / gRPC interceptor / context helper，供各服务统一复用

## 2026-06-06 core-rpc 监控补齐
- `core-rpc` 已启用独立 Prometheus 端口 `9101`，可通过 `/metrics` 暴露 gRPC 请求量、延迟和状态码统计
- 本地 Prometheus / k8s local / Helm charts 的 scrape 配置均已补上 `core-rpc`
- Grafana 新增 `IoT Core RPC` 仪表盘，并在 `IoT Overview` 中补了一块 `core-rpc` 请求速率视图
- 本地 port-forward 脚本已加入 `core-rpc -> 18091:9101`，方便 Docker Prometheus 抓取

## 2026-06-06 README 图片与发布 tag 调整
- README 顶部图已改为直接引用技术方案 HTML 中的总体架构 Mermaid 图生成的静态 SVG
- README 命令闭环图已改为直接引用技术方案 HTML 中的命令时序 Mermaid 图生成的静态 SVG
- 当前 README 图片只保留：
  - `docs/images/architecture.svg`
  - `docs/images/command-cycle.svg`
- 已删除旧的版本化 SVG 图片和手画顶部图，README 也不再引用版本化图片名
- README 图片和文档说明已去掉发布版本字样，仅保留架构能力描述
- 旧版本化 tag 已删除，当前最新提交已重新打新 tag 并推送

## 2026-06-06 Grafana 面板无数据排查与修复
- Prometheus targets 已验证为 `up`：`admin`、`core-rpc`、`ingress`、`worker`、`demo-docker`
- Grafana `IoT Core RPC` 面板无数据的直接原因是 `iot_grpc_*` 指标在没有 core-rpc 业务调用前不会产生时间序列
- `demo` 重启后遇到已存在的 tenant/device 会收到 `409 Conflict`，此前会停止拓扑初始化，导致没有持续业务流量；现已将 demo 的 tenant/device 创建改为 409 幂等继续
- Docker etcd 的 `advertise-client-urls` 已从 `127.0.0.1:2379` 改为 `host.docker.internal:2379`，避免 k8s 内客户端自动同步到不可达地址
- Grafana `Error QPS` 查询已补 `or vector(0)`，没有错误时显示 0，而不是 `No data`
- 已重新构建镜像、Helm 部署应用服务、重建 Docker Compose 监控栈，并验证：
  - `iot_grpc_requests_total` 已出现
  - core-rpc QPS 查询有值
  - core-rpc p95 latency 查询有值

## 当前状态
- 已根据用户确认的架构决策，整理出两份方案 HTML，并合并为一份合并版：`物联网平台技术方案.html`
- 合并版补充了主架构图、部署图、上行/下行/告警时序图、命令状态机，并按模块做成标签页，便于阅览
- 已按用户要求删除旧版文档，仅保留合并版：`物联网平台技术方案.html`
- 已修复合并版标签页点击问题：外层/内层标签页改为只绑定直接子元素，避免互相串台
- 已修复左侧快速导航点击问题：点击目录会先切换顶部标签，再滚动到对应模块
- 已按用户要求移除顶部主标签栏，仅保留左侧快速导航和各模块内子标签
- 已按用户要求调整主标题字号并禁止换行，且 HTML `<title>` 与主标题同名
- 已为时序图/架构图补充点击放大弹窗，便于查看细节
- 已将图示点击改为图本体直连，修复部分图表点击无效的问题
- 已增强图示点击命中逻辑，兼容 SVG/sequence diagram 的路径点击
- 已移除上行/命令时序图的“放大查看”按钮，改为点击图本体放大
- 已支持在放大层中再次点击图片还原
- 已将首页长句改为项目总体简介，突出平台定位与技术栈
- 已将上行/命令时序图本体直接设为可点击区域，并支持键盘回车/空格打开放大层
- 已按 TDD 落地 Go 项目骨架，并实现了可直接运行的内存版平台
- 平台已挂载 `OpenAPI` 输出和 MQTT JSON Schema 输出
- 已将平台接入层补齐为可运行的本地联调版本：
  - `admin` 使用 PostgreSQL 持久化租户、设备、状态、遥测、命令
  - `ingress` 订阅 EMQX telemetry topic，并转发到 Kafka
  - `worker` 消费 Kafka telemetry/command 事件，写 TDengine，并把命令下发到 EMQX
- 已补充本地默认连接配置，三进程可直接连接 `postgres / kafka / emqx / tdengine`
- 已在本地验证 `admin / ingress / worker` 三个进程健康检查可用，并完成 telemetry 上行与 command 下行闭环
- 项目模块名已从 `mqtt` 统一更名为 `iot`，代码 import 路径和测试引用均已同步
- 已补齐 command 下发 envelope 和 ACK 处理：下发消息携带 `id/tenantId/deviceId/status/payload`，ACK 消息携带 `commandId/tenantId/deviceId`
- 已新增并通过多租户多设备负载验证：`internal/platform/e2e_load_test.go`，一次性模拟 5 个租户、50 台设备、300 条 telemetry、50 条 command/ACK
- 已修复 TDengine 负载写入的吞吐和主键冲突问题：
  - `TDengineWriter` 改为批量刷盘，减少单条 INSERT 开销
  - 负载测试的 telemetry `ts` 改为单调递增，避免 TDengine 以时间戳主键覆盖同毫秒数据
- 已新增可部署的 `demo` 模拟服务：
  - 启动后自动创建多租户多设备拓扑
  - 随机发布 telemetry
  - 随机创建 command
  - 自动订阅 command topic 并回 ACK
  - 暴露 `/healthz`，适合 K8s readiness/liveness 探针
- 仓库已初始化为 Git 仓库，并成功推送到 GitHub：`git@github.com:eason2865/iot.git`
- 本地工作目录已从 `/Users/lyc/codex/go/mqtt` 改名为 `/Users/lyc/codex/go/iot`
- 本地已通过 Docker 启动 PostgreSQL 容器：`postgres-local`

## 关键方案
- 技术栈：Go + EMQX + Kafka + TDengine + PostgreSQL
- 设备直连 EMQX
- Go 接入服务订阅 MQTT 后写 Kafka
- Kafka 按业务域拆 topic，分区键优先 `deviceId`
- TDengine 使用超级表 + 标签
- 业务库选 PostgreSQL
- 采用逻辑多租户隔离
- `admin` 负责元数据与命令管理，`ingress` 负责 MQTT -> Kafka，`worker` 负责 Kafka -> TDengine / EMQX

## 文档位置
- 合并版：`/Users/lyc/codex/go/iot/物联网平台技术方案.html`
- 旧版草图和正式版已删除

## 已实现代码
- `go.mod`
- `README.md`
- `cmd/demo/main.go`
- `internal/platform/interfaces.go`
- `internal/platform/postgres_store.go`
- `internal/platform/kafka_publisher.go`
- `internal/platform/mqtt_bridge.go`
- `internal/platform/tdengine_writer.go`
- `internal/platform/worker.go`
- `internal/demo/service.go`
- `internal/demo/runtime.go`
- `internal/demo/simulator_test.go`
- `cmd/admin/main.go`
- `cmd/ingress/main.go`
- `cmd/worker/main.go`
- `internal/contracts/topic.go`
- `internal/contracts/envelope.go`
- `internal/contracts/command_state.go`
- `internal/platform/platform.go`
- `internal/server/health.go`
- `internal/bootstrap/run.go`
- `cmd/ingress/main.go`
- `cmd/admin/main.go`
- `cmd/worker/main.go`
- `docs/openapi.json`
- `docs/mqtt-envelope.schema.json`
- `migrations/001_init.sql`
- `docs/物联网平台技术方案.html`
- 对应测试覆盖：Topic 生成、Envelope 解析、命令状态机、HTTP 健康检查、设备注册、遥测写入、命令 ACK

## 当前可运行能力
- 三个可启动入口：`ingress`、`admin`、`worker`
- 新增第四个可启动入口：`demo`
- 默认暴露完整 HTTP API 和 `GET /healthz`
- 默认暴露 `GET /openapi.json` 和 `GET /schemas/mqtt-envelope.json`
- 支持通过 `PORT` 或 `LISTEN_ADDR` 指定监听地址
- 默认可直接连接本机 Docker 的 PostgreSQL / Kafka / EMQX / TDengine
- 已支持租户、设备、遥测、命令、ACK、状态查询和列表接口
- telemetry 事件会进入 Kafka，并由 worker 落 TDengine / PostgreSQL
- command 事件会进入 Kafka，并由 worker 下发到 EMQX
- MQTT 命令 topic：`tenant/{tenantId}/device/{deviceId}/command`
- MQTT ACK topic：`tenant/{tenantId}/device/{deviceId}/ack`
- `demo` 会自动造流：随机 telemetry、随机 command、自动 ACK
- 已提供 PostgreSQL 初始化迁移脚本
- `go test ./...` 已通过
- 已新增并通过端到端验证测试：`internal/platform/e2e_test.go`
- 已新增并通过负载级端到端验证测试：`internal/platform/e2e_load_test.go`
- 已新增并通过 demo 服务单元测试：`internal/demo/simulator_test.go`
- 当前 Kubernetes 部署已收敛为 apps-only，namespace 为 `iot`
- 当前本地 k8s 仅部署 `admin`、`ingress`、`worker`
- PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana、demo 均不进入业务 Helm release
- 已用 `kubectl` 验证 `admin`、`ingress`、`worker` 均为 `Running`/`Ready`
- 已通过外部依赖和本地监控完成 smoke test，`admin` 查询、`worker` ACK 回写、`demo` 造流均正常
- 已补 Prometheus 指标采集，所有服务都暴露 `/metrics`
- 已将 Prometheus 调整为本地 Docker 部署，默认地址为 `http://localhost:9090`
- 已在 Docker 中部署 Grafana，默认地址为 `http://localhost:3000`
- 已预置 3 个 Grafana 仪表盘：`IoT Overview`、`IoT Admin API`、`IoT Pipeline`
- Grafana 默认账号是 `admin` / `admin`
- 已新增 Helm Chart：`charts/iot`
- `helm lint charts/iot` 通过
- `helm template iot charts/iot` 通过
- Helm 安装命令：`helm upgrade --install iot charts/iot -n iot --create-namespace`
- 已按用户要求调整本地部署拓扑：
  - Docker 运行：PostgreSQL / Kafka / EMQX / TDengine / Prometheus / Grafana / demo
  - Helm 部署到本地 k8s：`admin` / `ingress` / `worker`
  - Helm 默认通过 `host.docker.internal` 连接 Docker 依赖
  - Helm 默认不再渲染 Postgres/Kafka/EMQX/TDengine/Prometheus/demo 的 k8s 资源
  - Prometheus 和 Grafana 不进 k8s，继续由 `monitoring/docker-compose.yml` 本地 Docker 启动
- 已新增本地一键 Helm 部署脚本：`scripts/helm-deploy-local.sh`
  - 默认检查 k8s Pod 到外部 PostgreSQL/Kafka/EMQX/TDengine 的端口连通性
  - 执行 `helm upgrade --install iot charts/iot -n iot --create-namespace --wait`
  - 强制 apps-only：只安装 `admin`、`ingress`、`worker` 和共享配置
  - 强制关闭 PostgreSQL/Kafka/EMQX/TDengine/Prometheus/demo 的 Helm 渲染
  - 等待 `admin`、`ingress`、`worker` rollout 完成
  - 云服务或 CI 环境可用 `CHECK_EXTERNAL_DEPS=0 scripts/helm-deploy-local.sh` 跳过本地端口检查
- 已新增本地监控转发脚本：`scripts/port-forward-local-monitoring.sh`
  - `admin` -> `localhost:18080`
  - `admin-metrics` -> `localhost:18090`
  - `ingress` -> `localhost:18081`
  - `worker` -> `localhost:18082`
- 已新增 Docker Prometheus 配置：`monitoring/prometheus/prometheus.yml`
- Grafana 数据源改为 Docker Compose 内部地址：`http://prometheus:9090`
- 已重建本地 Docker Kafka，将 `KAFKA_CFG_ADVERTISED_LISTENERS` 从 `localhost:9092` 改为 `host.docker.internal:9092`，否则 k8s Pod 会被 Kafka 元数据引导去连接 Pod 自己的 localhost
- 已按用户要求清理 k8s：
  - `helm uninstall iot -n iot`
  - 删除旧 `postgres-data`、`kafka-data`、`emqx-data`、`tdengine-data` PVC
  - 重新执行 `scripts/helm-deploy-local.sh` 验证从空 release 可一键安装
- 当前 Helm release：`iot`，namespace：`iot`，revision：`1`
- 当前 Helm 管理资源仅包含：
  - Deployment：`admin`、`ingress`、`worker`
  - Service：`admin`、`ingress`、`worker`
  - ConfigMap：`iot-common-config`
- 当前 k8s 中没有业务 PVC、业务 Secret、PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana、demo 资源
- 当前 Docker demo 容器：`iot-demo`，HTTP 暴露端口：`18084`
- 当前 Prometheus 容器：`iot-prometheus`，地址 `http://localhost:9090`
- 当前 Grafana 容器：`iot-grafana`，地址 `http://localhost:3000`，账号 `admin` / `admin`
- 当前需要保持的本地端口转发：
  - `kubectl port-forward --address 0.0.0.0 svc/admin 18080:8080 -n iot`
  - `kubectl port-forward --address 0.0.0.0 svc/admin 18090:9100 -n iot`
  - `kubectl port-forward --address 0.0.0.0 svc/ingress 18081:8080 -n iot`
  - `kubectl port-forward --address 0.0.0.0 svc/worker 18082:8080 -n iot`
- 已验证：
  - k8s Pod 可以连接 Docker 的 `5432/9092/1883/6041`
  - `admin` / `ingress` / `worker` 均 Ready
  - Docker demo 健康检查可用，并已创建 5 个 demo 租户、50 台 demo 设备
  - Prometheus targets：`admin`、`ingress`、`worker`、`demo-docker` 均为 `up`
  - Grafana datasource 代理查询 Prometheus 成功
  - worker 日志已持续出现 command consumed / command ack consumed，证明 Docker demo -> EMQX -> ingress -> Kafka -> worker -> admin ACK 链路可用

## 本地数据库
- 容器名：`postgres-local`
- 镜像：`postgres:15-alpine`
- 端口：`5432`
- 数据库：`iot`
- 用户：`iot`
- 密码：`iot123`
- 连接串：`postgres://iot:iot123@localhost:5432/iot?sslmode=disable`
- 启动命令：`docker run -d --name postgres-local --restart unless-stopped -e POSTGRES_USER=iot -e POSTGRES_PASSWORD=iot123 -e POSTGRES_DB=iot -p 5432:5432 -v postgres_data:/var/lib/postgresql/data postgres:15-alpine`
- Kafka 默认：`localhost:9092`
- Kafka 面向 k8s Pod 的 advertised listener 必须是：`PLAINTEXT://host.docker.internal:9092`
- EMQX 默认：`tcp://127.0.0.1:1883`
- TDengine 默认：`root:taosdata@http(127.0.0.1:6041)/iot`

## 下一步建议
- 继续补 `设备注册/鉴权`
- 继续补 command ack 的 MQTT 反向闭环
- 给 Kafka 消费和 MQTT 订阅补更完整的重连/监控日志
- 如果要继续扩展，再把 `internal/platform` 再细拆成 adapter + repository + service 三层

## 2026-06-06 1.0 开源发布准备
- 已补齐开源项目基础文件：`LICENSE`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md`、`SECURITY.md`、`CHANGELOG.md`
- 已新增 GitHub 协作配置：CI workflow、Issue 模板、PR 模板
- 已新增 `.env.example`、`.gitignore`、`.dockerignore`、`Makefile`
- README 已按开源项目结构补充版本、快速开始、开发命令、贡献、安全和许可证说明
- 已删除 PPT 产物相关入口，README 不再引用技术方案 PPT
- 已将 `ai_handover.md` 加入 `.gitignore`，计划从 Git 跟踪中移除，仅保留本地交接用途
- 已执行并通过：
  - `set -a; . ./.env.example; set +a`
  - `make fmt-check`
  - `make test`
  - `make build`

## 2026-06-06 1.0 apps-only Helm 修正
- README 已进一步明确：`scripts/helm-deploy-local.sh` 只部署 `admin`、`ingress`、`worker` 和共享配置
- README 已明确 PostgreSQL、Kafka、EMQX、TDengine、Prometheus、Grafana、demo 均作为外部依赖或 Docker 服务，不进入业务 Helm release
- `scripts/helm-deploy-local.sh` 已强制传入 apps-only Helm 参数，避免误装依赖资源
- `charts/iot/templates/secret.yaml` 已改为仅在 `postgres.enabled=true` 时渲染，避免 apps-only 模式残留无用 Secret
- 已执行并通过：
  - `helm template iot charts/iot ... apps-only 参数`
  - `helm lint charts/iot`
