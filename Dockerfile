FROM scratch

COPY bin/admin /usr/local/bin/admin
COPY bin/core-rpc /usr/local/bin/core-rpc
COPY bin/ingress /usr/local/bin/ingress
COPY bin/worker /usr/local/bin/worker
COPY bin/demo /usr/local/bin/demo

EXPOSE 8080
