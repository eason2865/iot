# AI Handover

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
