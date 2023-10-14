FROM debian:buster-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY dolos-Linux-GNU-${TARGETARCH} /bin/dolos
RUN chmod +x /bin/dolos
RUN ln -s /bin/dolos /dolos

ENTRYPOINT [ "dolos" ]
