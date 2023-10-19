FROM debian:12-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY genesis /etc/genesis

ARG TARGETARCH
COPY bin/dolos-Linux-${TARGETARCH} /bin/dolos
RUN chmod +x /bin/dolos
RUN ln -s /bin/dolos /dolos

ENTRYPOINT [ "dolos" ]
