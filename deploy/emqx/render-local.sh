#!/usr/bin/env sh
# Render the local EMQX manifest with the MQTT auth callback token substituted,
# then apply it. EMQX's config.data treats `${...}` inside authenticator
# headers/body as RUNTIME placeholders (only connection variables such as
# ${username} are allowed) and does NOT expand environment variables there, so
# the shared callback token must be rendered into the manifest BEFORE apply.
set -eu

NAMESPACE="${NAMESPACE:-emqx}"
MANIFEST="$(dirname "$0")/cluster.local.yaml"
SECRET="${SECRET:-iot-runtime-secrets}"
KEY="${KEY:-IOT_CORE_MQTT_AUTH_TOKEN}"

if ! command -v envsubst >/dev/null 2>&1; then
  echo "envsubst is required (brew install gettext)" >&2
  exit 1
fi

TOKEN="$(kubectl get secret -n "$NAMESPACE" "$SECRET" -o "jsonpath={.data.$KEY}" 2>/dev/null | base64 -d || true)"
if [ -z "$TOKEN" ]; then
  # Fall back to the local default so a fresh cluster can be rendered before the
  # secret exists; helm-deploy-local.sh uses the same default.
  TOKEN="${IOT_CORE_MQTT_AUTH_TOKEN:-local-mqtt-auth-token}"
fi

export IOT_CORE_MQTT_AUTH_TOKEN="$TOKEN"
# Only substitute the callback token; leave EMQX runtime placeholders such as
# ${username}/${password}/${clientid} untouched.
envsubst '${IOT_CORE_MQTT_AUTH_TOKEN}' < "$MANIFEST" | kubectl apply -f -
echo "Applied EMQX manifest with auth callback token from secret $NAMESPACE/$SECRET[$KEY]."
