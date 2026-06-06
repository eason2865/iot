# MQTT IoT Platform

Go + MQTT + EMQX + Kafka + TDengine + PostgreSQL 的物联网平台骨架。

## 运行

```bash
go run ./cmd/admin
go run ./cmd/ingress
go run ./cmd/worker
```

默认监听 `:8080`，可通过下面环境变量修改：

- `PORT=8081`
- `LISTEN_ADDR=:8081`

## 可用接口

- `GET /healthz`
- `GET /openapi.json`
- `GET /schemas/mqtt-envelope.json`
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

## 说明

- 当前默认使用内存实现，直接可跑。
- 已保留后续接入 EMQX、Kafka、TDengine 和 PostgreSQL 的结构位置。
- `migrations/001_init.sql` 提供 PostgreSQL 初始化表结构。
