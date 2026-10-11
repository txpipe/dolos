#!/bin/sh
# Offline checks for the dolos-soak chart: lint, the required values, and
# the labels that keep a soak instance out of canary-release's survey.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CHART="$ROOT/chart"
VALUES="$CHART/values.example.yaml"
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

helm lint "$CHART" --values "$VALUES"
helm template soak "$CHART" --values "$VALUES" >"$OUT/example.yaml"

# The StatefulSet's own metadata ends at `spec:`; the network label may only
# appear after it, on the pod template.
sed -n '/^kind: StatefulSet$/,/^spec:$/p' "$OUT/example.yaml" >"$OUT/sts-metadata.yaml"
if grep -Fq 'cardano.demeter.run/network' "$OUT/sts-metadata.yaml"; then
    echo "StatefulSet metadata carries the network label" >&2
    exit 1
fi
grep -Fx '        cardano.demeter.run/network: cardano-mainnet' "$OUT/example.yaml"
grep -F 'image: ghcr.io/txpipe/dolos@sha256:' "$OUT/example.yaml"
if grep -Eq 'apt-get|curl|jq' "$OUT/example.yaml"; then
    echo "probes install or call tools the image does not ship" >&2
    exit 1
fi

for missing in image.digest name network stele.source storage.size dolosToml; do
    if helm template soak "$CHART" --values "$VALUES" \
        --set-string "$missing=" >"$OUT/missing.txt" 2>&1; then
        echo "rendered without $missing" >&2
        exit 1
    fi
    grep -F "$missing is required" "$OUT/missing.txt"
done

for invalid in image.digest=sha-4866b97 'labels.cardano\.demeter\.run/network=cardano-mainnet'; do
    if helm template soak "$CHART" --values "$VALUES" \
        --set-string "$invalid" >"$OUT/invalid.txt" 2>&1; then
        echo "invalid configuration accepted: $invalid" >&2
        exit 1
    fi
    cat "$OUT/invalid.txt"
done

helm template soak "$CHART" --values "$VALUES" \
    --set stele.insecure=false >"$OUT/tls.yaml"
if grep -Fq -- '--insecure' "$OUT/tls.yaml"; then
    echo "TLS configuration passed --insecure" >&2
    exit 1
fi
