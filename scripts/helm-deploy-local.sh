#!/usr/bin/env sh
set -eu

RELEASE="${RELEASE:-iot}"
NAMESPACE="${NAMESPACE:-iot}"
CHART="${CHART:-charts/iot}"
TIMEOUT="${TIMEOUT:-180s}"
CHECK_EXTERNAL_DEPS="${CHECK_EXTERNAL_DEPS:-1}"
APP_IMAGE="${APP_IMAGE:-iot-app:2.0}"
DEPLOY_APP_IMAGE="$APP_IMAGE"
DOCKER_GATEWAY_HOST="${DOCKER_GATEWAY_HOST:-192.168.65.254}"
DOCKER_GATEWAY_KAFKA_PORT="${DOCKER_GATEWAY_KAFKA_PORT:-29092}"
EMQX_HOST="${EMQX_HOST:-emqx-listeners.emqx.svc.cluster.local}"
EMQX_PORT="${EMQX_PORT:-1883}"
MANAGEMENT_API_TOKEN="${IOT_MANAGEMENT_API_TOKEN:-local-development-token}"
EMQX_INTERNAL_PASSWORD="${IOT_EMQX_INTERNAL_PASSWORD:-local-mqtt-service-password}"
IOT_CORE_MQTT_AUTH_TOKEN="${IOT_CORE_MQTT_AUTH_TOKEN:-local-mqtt-auth-token}"
POSTGRES_DSN="${IOT_POSTGRES_DSN:-postgres://iot:iot123@${DOCKER_GATEWAY_HOST}:5432/iot?sslmode=disable}"
TDENGINE_DSN="${IOT_TDENGINE_DSN:-root:taosdata@http(${DOCKER_GATEWAY_HOST}:6041)/iot}"
GRPC_TLS_ENABLED="${GRPC_TLS_ENABLED:-1}"
GRPC_TLS_SECRET="${GRPC_TLS_SECRET:-iot-grpc-tls}"
GRPC_TLS_DIR="${GRPC_TLS_DIR:-deploy/grpc-certs}"

wait_for_docker_deps() {
  kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl run iot-docker-netcheck \
    --rm \
    -i \
    --restart=Never \
    --image=busybox:1.36 \
    -n "$NAMESPACE" \
    --env="DOCKER_GATEWAY_HOST=$DOCKER_GATEWAY_HOST" \
    --env="DOCKER_GATEWAY_KAFKA_PORT=$DOCKER_GATEWAY_KAFKA_PORT" \
    --env="EMQX_HOST=$EMQX_HOST" \
    --env="EMQX_PORT=$EMQX_PORT" \
    -- sh -c 'set -e
      nc -z "$DOCKER_GATEWAY_HOST" 5432
      nc -z "$DOCKER_GATEWAY_HOST" "$DOCKER_GATEWAY_KAFKA_PORT"
      nc -z "$EMQX_HOST" "$EMQX_PORT"
      nc -z "$DOCKER_GATEWAY_HOST" 6041
      echo external-deps-ok'
}

load_local_image() {
  if command -v kind >/dev/null 2>&1; then
    kind load docker-image "$APP_IMAGE"
    return
  fi

  # Docker Desktop Kubernetes has its own containerd image store. Import an
  # immutable tag so a reused local tag cannot resolve to an older cached image.
  if command -v docker >/dev/null 2>&1 \
    && docker inspect desktop-control-plane >/dev/null 2>&1 \
    && docker exec desktop-control-plane ctr version >/dev/null 2>&1; then
    image_id="$(docker image inspect --format '{{.Id}}' "$APP_IMAGE")"
    image_name="${APP_IMAGE%@*}"
    case "${image_name##*/}" in
      *:*) image_name="${image_name%:*}" ;;
    esac
    image_suffix="$(printf '%s' "${image_id#sha256:}" | cut -c1-12)"
    local_image="${image_name}:local-${image_suffix}"
    docker tag "$APP_IMAGE" "$local_image"
    docker save "$local_image" | docker exec -i desktop-control-plane ctr -n k8s.io images import -
    docker image rm "$local_image" >/dev/null
    DEPLOY_APP_IMAGE="$local_image"
  fi
}

prepare_local_app_image() {
  if command -v docker >/dev/null 2>&1; then
    image_id="$(docker image inspect --format '{{.Id}}' "$APP_IMAGE" 2>/dev/null || true)"
    if [ -n "$image_id" ]; then
      image_name="${APP_IMAGE%@*}"
      case "${image_name##*/}" in
        *:*) image_name="${image_name%:*}" ;;
      esac
      # Use the manifest digest, not the image config ID used by classic Docker.
      image_digests="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$APP_IMAGE")"
      for image_digest in $image_digests; do
        case "$image_digest" in
          "$image_name"@sha256:*)
            DEPLOY_APP_IMAGE="$image_digest"
            return
            ;;
        esac
      done
      echo "No repository digest for $APP_IMAGE; pull or publish this image before deploying." >&2
      exit 1
    fi
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

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic iot-runtime-secrets \
  --from-literal=MANAGEMENT_API_TOKEN="$MANAGEMENT_API_TOKEN" \
  --from-literal=EMQX_PASSWORD="$EMQX_INTERNAL_PASSWORD" \
  --from-literal=EMQX_INTERNAL_PASSWORD="$EMQX_INTERNAL_PASSWORD" \
  --from-literal=IOT_CORE_MQTT_AUTH_TOKEN="$IOT_CORE_MQTT_AUTH_TOKEN" \
  --from-literal=POSTGRES_DSN="$POSTGRES_DSN" \
  --from-literal=TDENGINE_DSN="$TDENGINE_DSN" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# EMQX runs in its own namespace and cannot read secrets across namespaces, so
# mirror the auth token there for the authentication callback header. EMQX 6
# env overrides do not accept per-key header overrides (lowercase/hyphenated
# names are rejected as unknown_env_vars), but do accept overriding the whole
# headers map with a JSON value, so the secret carries the full JSON document.
kubectl create namespace emqx --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n emqx create secret generic iot-runtime-secrets \
  --from-literal=IOT_CORE_MQTT_AUTH_TOKEN="$IOT_CORE_MQTT_AUTH_TOKEN" \
  --from-literal=EMQX_AUTHN_HEADERS_JSON="{\"content-type\":\"application/json\",\"x_iot_auth_token\":\"$IOT_CORE_MQTT_AUTH_TOKEN\"}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

if [ "$GRPC_TLS_ENABLED" = "1" ]; then
  sh scripts/gen-grpc-certs.sh "$GRPC_TLS_DIR"
  kubectl -n "$NAMESPACE" create secret generic "$GRPC_TLS_SECRET" \
    --from-file=ca.crt="$GRPC_TLS_DIR/ca.crt" \
    --from-file=server.crt="$GRPC_TLS_DIR/server.crt" \
    --from-file=server.key="$GRPC_TLS_DIR/server.key" \
    --from-file=client.crt="$GRPC_TLS_DIR/client.crt" \
    --from-file=client.key="$GRPC_TLS_DIR/client.key" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

prepare_local_app_image
load_local_image

COMMON_HELM_ARGS="
  --set images.app=${DEPLOY_APP_IMAGE}
  --set externalDependencies.kafkaBrokers=${DOCKER_GATEWAY_HOST}:${DOCKER_GATEWAY_KAFKA_PORT}
  --set externalDependencies.emqxUrl=tcp://${EMQX_HOST}:${EMQX_PORT}
  --set externalDependencies.wait.kafkaHost=${DOCKER_GATEWAY_HOST}
  --set externalDependencies.wait.kafkaPort=${DOCKER_GATEWAY_KAFKA_PORT}
  --set externalDependencies.wait.emqxHost=${EMQX_HOST}
  --set externalDependencies.wait.emqxPort=${EMQX_PORT}
"

if [ "$GRPC_TLS_ENABLED" = "1" ]; then
  COMMON_HELM_ARGS="$COMMON_HELM_ARGS
    --set grpcTLS.enabled=true
    --set grpcTLS.secretName=${GRPC_TLS_SECRET}
  "
fi

helm upgrade --install "$RELEASE" "$CHART" \
  -n "$NAMESPACE" \
  --create-namespace \
  --wait \
  --timeout "$TIMEOUT" \
  $COMMON_HELM_ARGS

kubectl rollout restart deployment/management-api deployment/iot-core deployment/telemetry-ingestor deployment/device-worker -n "$NAMESPACE"

wait_for_deployment management-api
wait_for_deployment iot-core
wait_for_deployment telemetry-ingestor
wait_for_deployment device-worker

kubectl get pods -n "$NAMESPACE"

cat <<EOF

Helm deployment is ready.

This script deploys application services only:
  - management-api
  - iot-core
  - telemetry-ingestor
  - device-worker

Useful local forwards:
  scripts/port-forward-local-monitoring.sh

Prometheus, Grafana, and demo remain external/local. If using the repo Docker Compose:
  docker compose -f monitoring/docker-compose.yml up -d
EOF
