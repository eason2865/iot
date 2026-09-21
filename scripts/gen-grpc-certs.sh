#!/usr/bin/env sh
# Generates a local self-signed CA plus server/client certificates for the
# iot-core gRPC mTLS channel. Local development only; production should issue
# these from a real CA / cert-manager.
set -eu

OUT_DIR="${1:-deploy/grpc-certs}"
DAYS="${DAYS:-3650}"

CA_KEY="$OUT_DIR/ca.key"
CA_CRT="$OUT_DIR/ca.crt"
SRV_KEY="$OUT_DIR/server.key"
SRV_CRT="$OUT_DIR/server.crt"
CLI_KEY="$OUT_DIR/client.key"
CLI_CRT="$OUT_DIR/client.crt"

if [ -f "$CA_CRT" ] && [ -f "$SRV_CRT" ] && [ -f "$SRV_KEY" ] && [ -f "$CLI_CRT" ] && [ -f "$CLI_KEY" ]; then
  echo "gRPC TLS certificates already exist in $OUT_DIR; skipping generation."
  exit 0
fi

mkdir -p "$OUT_DIR"
chmod 700 "$OUT_DIR"

# 1. Local CA
openssl genrsa -out "$CA_KEY" 3072 2>/dev/null
openssl req -x509 -new -nodes -key "$CA_KEY" -sha256 -days "$DAYS" \
  -subj "/CN=iot-grpc-local-ca" -out "$CA_CRT"

# 2. Server certificate (iot-core). SANs cover in-cluster DNS and local access.
openssl genrsa -out "$SRV_KEY" 2048 2>/dev/null
openssl req -new -key "$SRV_KEY" -subj "/CN=iot-core" -out "$OUT_DIR/server.csr"
cat > "$OUT_DIR/server.ext" <<'EOF'
subjectAltName=DNS:iot-core,DNS:iot-core.iot,DNS:iot-core.iot.svc,DNS:iot-core.iot.svc.cluster.local,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -in "$OUT_DIR/server.csr" -CA "$CA_CRT" -CAkey "$CA_KEY" \
  -CAcreateserial -days "$DAYS" -sha256 -extfile "$OUT_DIR/server.ext" -out "$SRV_CRT" 2>/dev/null

# 3. Client certificate (management-api)
openssl genrsa -out "$CLI_KEY" 2048 2>/dev/null
openssl req -new -key "$CLI_KEY" -subj "/CN=management-api" -out "$OUT_DIR/client.csr"
cat > "$OUT_DIR/client.ext" <<'EOF'
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in "$OUT_DIR/client.csr" -CA "$CA_CRT" -CAkey "$CA_KEY" \
  -CAcreateserial -days "$DAYS" -sha256 -extfile "$OUT_DIR/client.ext" -out "$CLI_CRT" 2>/dev/null

rm -f "$OUT_DIR"/*.csr "$OUT_DIR"/*.ext "$OUT_DIR"/*.srl
chmod 600 "$OUT_DIR"/*.key

echo "gRPC TLS certificates generated in $OUT_DIR"
