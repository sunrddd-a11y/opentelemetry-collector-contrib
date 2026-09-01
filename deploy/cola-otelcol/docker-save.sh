#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${IMAGE:-otel-collector-contrib:cola-v0.159.0}"
TAR="${TAR:-$DIR/otel-collector-contrib-cola-v0.159.0.tar}"
BIN="$DIR/otelcol-contrib"

if [[ ! -f "$BIN" ]]; then
  echo "missing linux binary: $BIN" >&2
  echo "compile it first (GOOS=linux GOARCH=amd64) into this path" >&2
  exit 1
fi

file "$BIN" || true
ls -lh "$BIN"

echo "==> docker build ${IMAGE}"
docker build -t "$IMAGE" "$DIR"

echo "==> docker save ${TAR}"
rm -f "$TAR"
docker save "$IMAGE" -o "$TAR"
ls -lh "$TAR" "$BIN"
echo
echo "image: ${IMAGE}"
echo "tar:   ${TAR}"
