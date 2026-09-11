// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesWrapper struct {
	extra *colaExtra
	inner exporter.Traces
}

type metricsWrapper struct {
	extra *colaExtra
	inner exporter.Metrics
}

type logsWrapper struct {
	extra *colaExtra
	inner exporter.Logs
}

func (w *tracesWrapper) Start(ctx context.Context, host component.Host) error {
	if err := w.extra.holdStart(ctx); err != nil {
		return err
	}
	if err := w.inner.Start(ctx, host); err != nil {
		return err
	}
	return w.extra.ensureDependentSchema(ctx)
}

func (w *tracesWrapper) Shutdown(ctx context.Context) error {
	err := w.inner.Shutdown(ctx)
	if serr := w.extra.release(); serr != nil && err == nil {
		err = serr
	}
	return err
}

func (w *tracesWrapper) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	td = w.extra.filterTraces(td)
	if td.ResourceSpans().Len() == 0 {
		return nil
	}
	return w.inner.ConsumeTraces(ctx, td)
}

func (w *tracesWrapper) Capabilities() consumer.Capabilities {
	return w.inner.Capabilities()
}

func (w *metricsWrapper) Start(ctx context.Context, host component.Host) error {
	if err := w.extra.holdStart(ctx); err != nil {
		return err
	}
	if err := w.inner.Start(ctx, host); err != nil {
		return err
	}
	return w.extra.ensureDependentSchema(ctx)
}

func (w *metricsWrapper) Shutdown(ctx context.Context) error {
	err := w.inner.Shutdown(ctx)
	if serr := w.extra.release(); serr != nil && err == nil {
		err = serr
	}
	return err
}

func (w *metricsWrapper) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	md = w.extra.filterMetrics(md)
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return w.inner.ConsumeMetrics(ctx, md)
}

func (w *metricsWrapper) Capabilities() consumer.Capabilities {
	return w.inner.Capabilities()
}

func (w *logsWrapper) Start(ctx context.Context, host component.Host) error {
	if err := w.extra.holdStart(ctx); err != nil {
		return err
	}
	if err := w.inner.Start(ctx, host); err != nil {
		return err
	}
	return w.extra.ensureDependentSchema(ctx)
}

func (w *logsWrapper) Shutdown(ctx context.Context) error {
	err := w.inner.Shutdown(ctx)
	if serr := w.extra.release(); serr != nil && err == nil {
		err = serr
	}
	return err
}

func (w *logsWrapper) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rest, err := w.extra.consumeColaLogs(ctx, ld)
	if err != nil {
		return err
	}
	rest = w.extra.filterLogs(rest)
	if rest.LogRecordCount() == 0 {
		return nil
	}
	return w.inner.ConsumeLogs(ctx, rest)
}

func (w *logsWrapper) Capabilities() consumer.Capabilities {
	return w.inner.Capabilities()
}
