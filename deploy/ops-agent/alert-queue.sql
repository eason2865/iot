-- Install in a dedicated database using its owner, before enabling the webhook.
CREATE TABLE IF NOT EXISTS ops_alert_jobs (
  event_id text PRIMARY KEY,
  event jsonb NOT NULL,
  message text NOT NULL DEFAULT '',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  delivered boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS ops_alert_jobs_pending ON ops_alert_jobs (available_at, event_id) WHERE finished_at IS NULL;
CREATE INDEX IF NOT EXISTS ops_alert_jobs_finished ON ops_alert_jobs (finished_at) WHERE finished_at IS NOT NULL;
