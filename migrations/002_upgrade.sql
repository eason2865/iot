-- Run before 001_init.sql so indexes can be created on older installations.
-- IF EXISTS also makes this safe on an empty database.
ALTER TABLE IF EXISTS devices ADD COLUMN IF NOT EXISTS secret_hash TEXT;
ALTER TABLE IF EXISTS commands ADD COLUMN IF NOT EXISTS next_dispatch_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE IF EXISTS commands ADD COLUMN IF NOT EXISTS dispatch_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS commands ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;
ALTER TABLE IF EXISTS commands ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE IF EXISTS command_ack ADD COLUMN IF NOT EXISTS ack_status TEXT NOT NULL DEFAULT 'acked';
ALTER TABLE IF EXISTS command_ack ADD COLUMN IF NOT EXISTS ack_payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE IF EXISTS command_ack ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE IF EXISTS telemetry_records ADD COLUMN IF NOT EXISTS tdengine_written BOOLEAN NOT NULL DEFAULT FALSE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='devices' AND column_name='secret') THEN
    ALTER TABLE devices ALTER COLUMN secret DROP NOT NULL;
  END IF;
END $$;
