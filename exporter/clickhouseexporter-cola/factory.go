// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

var extras = sharedcomponent.NewSharedComponents()

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func officialSettings(set exporter.Settings) exporter.Settings {
	set.ID = component.NewID(component.MustNewType("clickhouse"))
	return set
}

func getExtra(cfg *Config, set exporter.Settings) *colaExtra {
	sc := extras.GetOrAdd(cfg, func() component.Component {
		return newColaExtra(cfg, set.Logger)
	})
	return sc.Unwrap().(*colaExtra)
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	prepareClusterHTTPSchema(c)
	inner, err := clickhouseexporter.NewFactory().CreateTraces(ctx, officialSettings(set), &c.Config)
	if err != nil {
		return nil, err
	}
	return &tracesWrapper{extra: getExtra(c, set), inner: inner}, nil
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	prepareClusterHTTPSchema(c)
	inner, err := clickhouseexporter.NewFactory().CreateMetrics(ctx, officialSettings(set), &c.Config)
	if err != nil {
		return nil, err
	}
	return &metricsWrapper{extra: getExtra(c, set), inner: inner}, nil
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	prepareClusterHTTPSchema(c)
	inner, err := clickhouseexporter.NewFactory().CreateLogs(ctx, officialSettings(set), &c.Config)
	if err != nil {
		return nil, err
	}
	return &logsWrapper{extra: getExtra(c, set), inner: inner}, nil
}
