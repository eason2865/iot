#!/usr/bin/env sh
set -eu

NAMESPACE="${NAMESPACE:-iot}"

start_forward() {
  label="$1"
  service="$2"
  local_port="$3"
  remote_port="$4"
  log_file="/tmp/iot-${label}-port-forward.log"

  if pgrep -f "kubectl port-forward --address 0.0.0.0 svc/${service} ${local_port}:${remote_port} -n ${NAMESPACE}" >/dev/null 2>&1; then
    echo "${label} port-forward already running on ${local_port}"
    return
  fi

  nohup kubectl port-forward --address 0.0.0.0 "svc/${service}" "${local_port}:${remote_port}" -n "$NAMESPACE" >"$log_file" 2>&1 </dev/null &
  echo "${label} -> localhost:${local_port} started, log: ${log_file}"
}

start_forward management-api management-api 18080 8080
start_forward management-api-metrics management-api 18090 9100
start_forward telemetry-ingestor telemetry-ingestor 18081 8080
start_forward device-worker device-worker 18082 8080
start_forward iot-core iot-core 18091 9101

echo
echo "Local monitoring forwards are ready."
echo "Prometheus can scrape:"
echo "  host.docker.internal:18080"
echo "  host.docker.internal:18090"
echo "  host.docker.internal:18081"
echo "  host.docker.internal:18082"
echo "  host.docker.internal:18091"
