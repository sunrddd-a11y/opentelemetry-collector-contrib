// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type      = component.MustNewType("clickhouse_cola")
	ScopeName = "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"
)

const (
	MetricsStability = component.StabilityLevelAlpha
	TracesStability  = component.StabilityLevelBeta
	LogsStability    = component.StabilityLevelBeta
)
