FROM alpine:3.22@sha256:d406446bc05b82480756ce8f1287964d28768c23955fa9480d2a74a2eae7a654 AS trust
RUN apk add --no-cache ca-certificates

FROM scratch

COPY --from=trust /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY bin/management-api /usr/local/bin/management-api
COPY bin/iot-core /usr/local/bin/iot-core
COPY bin/telemetry-ingestor /usr/local/bin/telemetry-ingestor
COPY bin/device-worker /usr/local/bin/device-worker
COPY bin/demo /usr/local/bin/demo
COPY bin/dlq-replay /usr/local/bin/dlq-replay

COPY bin/migrate /usr/local/bin/migrate

EXPOSE 8080
