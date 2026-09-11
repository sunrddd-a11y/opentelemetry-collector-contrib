// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package clickhouseexportercola wraps the official clickhouse exporter
// and adds Cola-specific schema (otel_agents, snapshots, otel_spans, Distributed, otel_agent_mv, otel_span_mv)
// plus writes for SkyWalking instance properties and profile snapshots.
package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"
