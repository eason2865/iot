#!/usr/bin/env sh
set -eu

# Compose owns the forwards so Prometheus, demo, and the host see one stable path.
docker compose -f monitoring/docker-compose.yml up -d \
  k8s-forward-management-api \
  k8s-forward-management-api-metrics \
  k8s-forward-telemetry-ingestor \
  k8s-forward-device-worker \
  k8s-forward-iot-core

echo "Local monitoring forwards are ready: 18080, 18081, 18082, 18090, 18091."
