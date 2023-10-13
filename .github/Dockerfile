FROM alpine:latest

ARG TARGETARCH
COPY Linux-GNU-${TARGETARCH}-dolos /bin/dolos
RUN ln -s /bin/dolos /dolos

ENTRYPOINT ["dolos"]