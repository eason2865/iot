#!/usr/bin/env sh
set -eu

RELEASE="${RELEASE:-iot}"
NAMESPACE="${NAMESPACE:-iot}"
CHART="${CHART:-charts/iot}"
TIMEOUT="${TIMEOUT:-180s}"
CHECK_EXTERNAL_DEPS="${CHECK_EXTERNAL_DEPS:-1}"
APP_IMAGE="${APP_IMAGE:-iot-app:2.0}"
DOCKER_GATEWAY_HOST="${DOCKER_GATEWAY_HOST:-192.168.65.254}"
COMMON_HELM_ARGS="
  --set externalDependencies.enabled=true
  --set admin.enabled=true
  --set coreRpc.enabled=true
  --set ingress.enabled=true
  --set worker.enabled=true
  --set postgres.enabled=false
  --set kafka.enabled=false
  --set emqx.enabled=false
  --set tdengine.enabled=false
  --set demo.enabled=false
  --set prometheus.enabled=false
"

wait_for_docker_deps() {
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl run iot-docker-netcheck \
    --rm \
    -i \
    --restart=Never \
    --image=busybox:1.36 \
    -n "$NAMESPACE" \
    --env="DOCKER_GATEWAY_HOST=$DOCKER_GATEWAY_HOST" \
    -- sh -c 'set -e
      nc -z "$DOCKER_GATEWAY_HOST" 5432
      nc -z "$DOCKER_GATEWAY_HOST" 9092
      nc -z "$DOCKER_GATEWAY_HOST" 1883
      nc -z "$DOCKER_GATEWAY_HOST" 6041
      nc -z "$DOCKER_GATEWAY_HOST" 2379
      echo external-deps-ok'
}

load_local_image() {
  if command -v kind >/dev/null 2>&1; then
    kind load docker-image "$APP_IMAGE"
  fi
}

wait_for_deployment() {
  name="$1"
  if kubectl get deployment "$name" -n "$NAMESPACE" >/dev/null 2>&1; then
    kubectl rollout status "deployment/$name" -n "$NAMESPACE" --timeout="$TIMEOUT"
  fi
}

if [ "$CHECK_EXTERNAL_DEPS" = "1" ]; then
  wait_for_docker_deps
fi

load_local_image

helm upgrade --install "$RELEASE" "$CHART" \
  -n "$NAMESPACE" \
  --create-namespace \
  --wait \
  --timeout "$TIMEOUT" \
  $COMMON_HELM_ARGS

kubectl rollout restart deployment/admin deployment/core-rpc deployment/ingress deployment/worker -n "$NAMESPACE"

wait_for_deployment admin
wait_for_deployment core-rpc
wait_for_deployment ingress
wait_for_deployment worker

kubectl get pods -n "$NAMESPACE"

cat <<EOF

Helm deployment is ready.

This script deploys application services only:
  - admin
  - core-rpc
  - ingress
  - worker

Useful local forwards:
  scripts/port-forward-local-monitoring.sh

Prometheus, Grafana, and demo remain external/local. If using the repo Docker Compose:
  docker compose -f monitoring/docker-compose.yml up -d
EOF
