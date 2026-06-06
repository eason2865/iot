CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  secret TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, device_id),
  CONSTRAINT fk_devices_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS device_state (
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  connected BOOLEAN NOT NULL DEFAULT FALSE,
  last_seen_at TIMESTAMPTZ,
  last_msg_id TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS telemetry_records (
  id BIGSERIAL PRIMARY KEY,
  msg_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  ts BIGINT NOT NULL,
  type TEXT NOT NULL,
  version TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (msg_id, tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS command_ack (
  id BIGSERIAL PRIMARY KEY,
  command_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  ack_status TEXT NOT NULL,
  ack_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alarms (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  alarm_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS message_dedup (
  msg_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (msg_id, tenant_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_devices_tenant ON devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_commands_tenant_device ON commands(tenant_id, device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device ON telemetry_records(tenant_id, device_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_alarms_tenant_device ON alarms(tenant_id, device_id);
