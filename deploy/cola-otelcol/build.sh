#!/usr/bin/env bash
# Build a linux/amd64 collector image compatible with
# /opt/otel-collector (ENTRYPOINT /otelcol-contrib, config /etc/otelcol-contrib/config.yaml).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${IMAGE:-otel-collector-contrib:cola-v0.159.0}"
TAR="${TAR:-$DIR/otel-collector-contrib-cola-v0.159.0.tar}"
BUILDER_VERSION="${BUILDER_VERSION:-v0.159.0}"
GOLANG_IMAGE="${GOLANG_IMAGE:-golang:1.25-bookworm}"
GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

cd "$ROOT"

echo "==> generating builder replaces"
CONFIG_IN="$DIR/builder-config.yaml"
CONFIG_OUT="$DIR/builder-config-replaced.yaml"
cp "$CONFIG_IN" "$CONFIG_OUT"
find . -type f -name go.mod \
  -not -path './.git/*' \
  -not -path './deploy/*' \
  -exec dirname {} \; | sort | while read -r mod_path; do
  mod="${mod_path#.}"
  echo "  - github.com/open-telemetry/opentelemetry-collector-contrib${mod} => ../../..${mod}" >> "$CONFIG_OUT"
done
echo "    wrote $(grep -c ' => ' "$CONFIG_OUT") replace lines"

echo "==> compiling linux/amd64 binary in ${GOLANG_IMAGE}"
docker run --rm \
  -e GOPROXY="$GOPROXY" \
  -e GOOS=linux \
  -e GOARCH=amd64 \
  -e CGO_ENABLED=0 \
  -e GOTOOLCHAIN=local \
  -v "$ROOT":/src \
  -v cola-otelcol-gomod:/go/pkg/mod \
  -v cola-otelcol-gocache:/root/.cache/go-build \
  -w /src \
  "$GOLANG_IMAGE" \
  bash -c "
    set -euo pipefail
    go install go.opentelemetry.io/collector/cmd/builder@${BUILDER_VERSION}
    \"\$(go env GOPATH)/bin/builder\" --skip-compilation --config deploy/cola-otelcol/builder-config-replaced.yaml
    cd deploy/cola-otelcol/out
    go build -buildvcs=false -trimpath -ldflags='-s -w' -o ../otelcol-contrib .
  "

test -x "$DIR/otelcol-contrib" || test -f "$DIR/otelcol-contrib"
echo "==> binary: $(ls -lh "$DIR/otelcol-contrib" | awk '{print $5, $9}')"

echo "==> docker build ${IMAGE}"
docker build -t "$IMAGE" "$DIR"

echo "==> docker save ${TAR}"
docker save "$IMAGE" -o "$TAR"
ls -lh "$TAR"

echo
echo "Done."
echo "  image: ${IMAGE}"
echo "  tar:   ${TAR}"
echo "Upload the tar to 10.2.17.16 and run:"
echo "  docker load -i otel-collector-contrib-cola-v0.159.0.tar"
echo "  # then point /opt/otel-collector/docker-compose.yaml image to ${IMAGE}"
