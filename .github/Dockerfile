FROM alpine:latest

ARG TARGETARCH
COPY dolos-Linux-GNU-${TARGETARCH}/dolos-Linux-GNU-${TARGETARCH} /bin/dolos
RUN ln -s /bin/dolos /dolos

ENTRYPOINT ["dolos"]