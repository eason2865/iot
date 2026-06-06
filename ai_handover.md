# AI Handover

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

## 关键方案
- 技术栈：Go + EMQX + Kafka + TDengine + PostgreSQL
- 设备直连 EMQX
- Go 接入服务订阅 MQTT 后写 Kafka
- Kafka 按业务域拆 topic，分区键优先 `deviceId`
- TDengine 使用超级表 + 标签
- 业务库选 PostgreSQL
- 第一版采用逻辑多租户隔离

## 文档位置
- 合并版：`/Users/lyc/codex/go/mqtt/物联网平台技术方案.html`
- 旧版草图和正式版已删除

## 已实现代码
- `go.mod`
- `README.md`
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
- 对应测试覆盖：Topic 生成、Envelope 解析、命令状态机、HTTP 健康检查、设备注册、遥测写入、命令 ACK

## 当前可运行能力
- 三个可启动入口：`ingress`、`admin`、`worker`
- 默认暴露完整 HTTP API 和 `GET /healthz`
- 默认暴露 `GET /openapi.json` 和 `GET /schemas/mqtt-envelope.json`
- 支持通过 `PORT` 或 `LISTEN_ADDR` 指定监听地址
- 默认使用内存实现，直接可跑
- 已支持租户、设备、遥测、命令、ACK、状态查询和列表接口
- 已提供 PostgreSQL 初始化迁移脚本

## 下一步建议
- 继续补 `设备注册/鉴权`
- 继续补 `Kafka producer/consumer` 抽象
- 继续补 `TDengine` 与 `PostgreSQL` repository 接口
- 如果要接真实外部系统，再把 `internal/platform` 拆成 adapter + repository + service 三层
