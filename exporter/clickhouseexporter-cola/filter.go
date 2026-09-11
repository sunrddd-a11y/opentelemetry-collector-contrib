// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func (e *colaExtra) allowAgent(agentType, service, instance string) bool {
	if e == nil {
		return true
	}
	return e.license.allow(agentType, service, instance)
}

func resolveAgentIdentity(attrs pcommon.Map) (agentType, service, instance string) {
	service = strAttr(attrs, attrServiceName)
	instance = strAttr(attrs, attrServiceInstanceID)
	if at := strAttr(attrs, attrColaAgentType); at != "" {
		return at, service, instance
	}
	if strAttr(attrs, attrColaSignal) != "" {
		return agentTypeSkyWalking, service, instance
	}
	if _, ok := attrs.Get(attrSWTraceID); ok {
		return agentTypeSkyWalking, service, instance
	}
	if strAttr(attrs, attrTelemetrySDKName) == agentTypeSkyWalking {
		return agentTypeSkyWalking, service, instance
	}
	return agentTypeOTel, service, instance
}

func (e *colaExtra) allowResource(attrs pcommon.Map, signal string) bool {
	agentType, service, instance := resolveAgentIdentity(attrs)
	if e.allowAgent(agentType, service, instance) {
		return true
	}
	if e.logger != nil {
		e.logger.Debug("drop unauthorized telemetry",
			zap.String("signal", signal),
			zap.String("agent_type", agentType),
			zap.String("service", service),
			zap.String("instance", instance),
		)
	}
	return false
}

func (e *colaExtra) filterTraces(td ptrace.Traces) ptrace.Traces {
	if e.license == nil || !e.license.cfg.Enabled {
		return td
	}
	out := ptrace.NewTraces()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if e.allowResource(rs.Resource().Attributes(), "traces") {
			rs.CopyTo(out.ResourceSpans().AppendEmpty())
		}
	}
	return out
}

func (e *colaExtra) filterMetrics(md pmetric.Metrics) pmetric.Metrics {
	if e.license == nil || !e.license.cfg.Enabled {
		return md
	}
	out := pmetric.NewMetrics()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if e.allowResource(rm.Resource().Attributes(), "metrics") {
			rm.CopyTo(out.ResourceMetrics().AppendEmpty())
		}
	}
	return out
}

func (e *colaExtra) filterLogs(ld plog.Logs) plog.Logs {
	if e.license == nil || !e.license.cfg.Enabled {
		return ld
	}
	out := plog.NewLogs()
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		if e.allowResource(rl.Resource().Attributes(), "logs") {
			rl.CopyTo(out.ResourceLogs().AppendEmpty())
		}
	}
	return out
}

func (e *colaExtra) filterSnapshots(rows []snapshotRow) []snapshotRow {
	if len(rows) == 0 || e.license == nil || !e.license.cfg.Enabled {
		return rows
	}
	out := make([]snapshotRow, 0, len(rows))
	for _, row := range rows {
		if e.allowAgent(agentTypeSkyWalking, row.Service, row.ServiceInstance) {
			out = append(out, row)
			continue
		}
		if e.logger != nil {
			e.logger.Debug("drop unauthorized telemetry",
				zap.String("signal", "profile_snapshot"),
				zap.String("agent_type", agentTypeSkyWalking),
				zap.String("service", row.Service),
				zap.String("instance", row.ServiceInstance),
			)
		}
	}
	return out
}
