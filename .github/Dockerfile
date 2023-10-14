FROM debian:12-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
COPY binaries/Linux-GNU-${TARGETARCH}-dolos /bin/dolos
RUN chmod +x /bin/dolos
RUN ln -s /bin/dolos /dolos

ENTRYPOINT [ "dolos" ]
