#!/usr/bin/env sh
set -eu

# Compose owns the forwards so Prometheus, demo, and the host see one stable path.
# Metrics scraping no longer uses port-forwards: Prometheus discovers every Pod
# via the apiserver pod proxy (monitoring/prometheus/prometheus.yml), which
# needs the prometheus-scraper ServiceAccount token mounted into the container.
kubectl apply -f monitoring/prometheus/k8s-rbac.yaml

# Wait for the token controller to populate the ServiceAccount token Secret.
TOKEN_FILE="monitoring/prometheus/k8s-token"
for _ in $(seq 1 30); do
  token="$(kubectl -n iot get secret prometheus-scraper-token -o jsonpath='{.data.token}' 2>/dev/null || true)"
  if [ -n "$token" ]; then
    printf '%s' "$token" | base64 -d >"$TOKEN_FILE"
    chmod 644 "$TOKEN_FILE" # Prometheus runs as nobody and mounts the file read-only.
    kubectl -n iot get secret prometheus-scraper-token -o jsonpath='{.data.ca\.crt}' | base64 -d > monitoring/prometheus/k8s-ca.crt
    break
  fi
  sleep 1
done
if [ ! -s "$TOKEN_FILE" ]; then
  echo "failed to provision $TOKEN_FILE from secret prometheus-scraper-token" >&2
  exit 1
fi

docker compose -f monitoring/docker-compose.yml up -d \
  k8s-forward-management-api \
  k8s-forward-emqx-listeners \
  prometheus

echo "Local monitoring is ready: management-api forward on 18080; Prometheus scrapes each iot Pod via the apiserver pod proxy."
