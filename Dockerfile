FROM scratch

COPY bin/management-api /usr/local/bin/management-api
COPY bin/iot-core /usr/local/bin/iot-core
COPY bin/telemetry-ingestor /usr/local/bin/telemetry-ingestor
COPY bin/device-worker /usr/local/bin/device-worker
COPY bin/demo /usr/local/bin/demo

EXPOSE 8080
