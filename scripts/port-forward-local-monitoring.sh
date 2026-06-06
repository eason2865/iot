#!/usr/bin/env sh
set -eu

NAMESPACE="${NAMESPACE:-iot}"

start_forward() {
  service="$1"
  local_port="$2"
  remote_port="$3"
  log_file="/tmp/iot-${service}-port-forward.log"

  if pgrep -f "kubectl port-forward --address 0.0.0.0 svc/${service} ${local_port}:${remote_port} -n ${NAMESPACE}" >/dev/null 2>&1; then
    echo "${service} port-forward already running on ${local_port}"
    return
  fi

  nohup kubectl port-forward --address 0.0.0.0 "svc/${service}" "${local_port}:${remote_port}" -n "$NAMESPACE" >"$log_file" 2>&1 </dev/null &
  echo "${service} -> localhost:${local_port} started, log: ${log_file}"
}

start_forward admin 18080 8080
start_forward ingress 18081 8080
start_forward worker 18082 8080

echo
echo "Local monitoring forwards are ready."
echo "Prometheus can scrape:"
echo "  host.docker.internal:18080"
echo "  host.docker.internal:18081"
echo "  host.docker.internal:18082"
