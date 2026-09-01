#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
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
echo "replace_lines=$(grep -c ' => ' "$CONFIG_OUT")"
wc -l "$CONFIG_OUT"
