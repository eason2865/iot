#!/usr/bin/env sh
set -eu

RELEASE="${RELEASE:-iot}"
NAMESPACE="${NAMESPACE:-iot}"
CHART="${CHART:-charts/iot}"
TIMEOUT="${TIMEOUT:-180s}"
CHECK_EXTERNAL_DEPS="${CHECK_EXTERNAL_DEPS:-1}"
COMMON_HELM_ARGS="
  --set externalDependencies.enabled=true
  --set admin.enabled=true
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
    -- sh -c 'set -e
      nc -z host.docker.internal 5432
      nc -z host.docker.internal 9092
      nc -z host.docker.internal 1883
      nc -z host.docker.internal 6041
      echo external-deps-ok'
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

helm upgrade --install "$RELEASE" "$CHART" \
  -n "$NAMESPACE" \
  --create-namespace \
  --wait \
  --timeout "$TIMEOUT" \
  $COMMON_HELM_ARGS

wait_for_deployment admin
wait_for_deployment ingress
wait_for_deployment worker

kubectl get pods -n "$NAMESPACE"

cat <<EOF

Helm deployment is ready.

This script deploys application services only:
  - admin
  - ingress
  - worker

Useful local forwards:
  scripts/port-forward-local-monitoring.sh

Prometheus, Grafana, and demo remain external/local. If using the repo Docker Compose:
  docker compose -f monitoring/docker-compose.yml up -d
EOF
