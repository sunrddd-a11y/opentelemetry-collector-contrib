// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func (e *colaExtra) consumeColaLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	rest := plog.NewLogs()
	var snaps []snapshotRow
	now := time.Now()

	rss := ld.ResourceLogs()
	for i := 0; i < rss.Len(); i++ {
		rl := rss.At(i)
		res := rl.Resource()
		signal := resourceSignal(res.Attributes())
		service := strAttr(res.Attributes(), attrServiceName)
		instance := strAttr(res.Attributes(), attrServiceInstanceID)

		switch signal {
		case signalProfileSnapshot:
			snaps = append(snaps, e.filterSnapshots(snapshotRowsFromResource(rl, service, instance))...)
		case signalAgentProperties:
			if err := e.consumeAgentProperties(ctx, rl, service, instance, now); err != nil {
				return plog.Logs{}, err
			}
		case signalAgentKeepAlive:
			if err := e.consumeAgentKeepAlive(ctx, rl, service, instance, now); err != nil {
				return plog.Logs{}, err
			}
		default:
			rl.CopyTo(rest.ResourceLogs().AppendEmpty())
		}
	}

	if err := e.insertSnapshots(ctx, snaps); err != nil {
		return plog.Logs{}, err
	}
	return rest, nil
}

func (e *colaExtra) consumeAgentProperties(ctx context.Context, rl plog.ResourceLogs, service, instance string, now time.Time) error {
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		sl := rl.ScopeLogs().At(i)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			lr := sl.LogRecords().At(j)
			attrs := lr.Attributes()
			if svc := strAttr(attrs, "service_name"); svc != "" {
				service = svc
			}
			if inst := strAttr(attrs, "instance_name"); inst != "" {
				instance = inst
			}
			layer := strAttr(attrs, "layer")
			props := mapFromAttr(attrs, "properties")
			if ts := lr.Timestamp(); ts != 0 {
				now = ts.AsTime()
			}
			if err := e.upsertAgentProperties(ctx, service, instance, layer, props, now); err != nil {
				if e.logger != nil {
					e.logger.Warn("insert skywalking agent properties failed",
						zap.Error(err),
						zap.String("service", service),
						zap.String("instance", instance),
					)
				}
				return err
			}
		}
	}
	return nil
}

func (e *colaExtra) consumeAgentKeepAlive(ctx context.Context, rl plog.ResourceLogs, service, instance string, now time.Time) error {
	layer := ""
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		sl := rl.ScopeLogs().At(i)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			lr := sl.LogRecords().At(j)
			attrs := lr.Attributes()
			if svc := strAttr(attrs, "service_name"); svc != "" {
				service = svc
			}
			if inst := strAttr(attrs, "instance_name"); inst != "" {
				instance = inst
			}
			if v := strAttr(attrs, "layer"); v != "" {
				layer = v
			}
			if ts := lr.Timestamp(); ts != 0 {
				now = ts.AsTime()
			}
		}
	}
	if err := e.upsertAgentKeepAlive(ctx, service, instance, layer, now); err != nil {
		if e.logger != nil {
			e.logger.Warn("insert skywalking agent keepalive failed",
				zap.Error(err),
				zap.String("service", service),
				zap.String("instance", instance),
			)
		}
		return err
	}
	return nil
}

func snapshotRowsFromResource(rl plog.ResourceLogs, service, instance string) []snapshotRow {
	var out []snapshotRow
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		sl := rl.ScopeLogs().At(i)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			lr := sl.LogRecords().At(j)
			attrs := lr.Attributes()
			row := snapshotRow{
				Timestamp:       lr.Timestamp().AsTime(),
				TaskID:          strAttr(attrs, "task_id"),
				Service:         service,
				ServiceInstance: instance,
				EndpointName:    strAttr(attrs, "endpoint_name"),
				OTelTraceID:     strAttr(attrs, "otel_trace_id"),
				SWTraceID:       strAttr(attrs, "sw_trace_id"),
				SegmentID:       strAttr(attrs, "segment_id"),
				Sequence:        uint32(intAttr(attrs, "sequence")),
				StackLeafFirst:  sliceAttr(attrs, "stack_leaf_first"),
			}
			if svc := strAttr(attrs, "service"); svc != "" {
				row.Service = svc
			}
			if inst := strAttr(attrs, "service_instance"); inst != "" {
				row.ServiceInstance = inst
			}
			if row.Timestamp.IsZero() {
				row.Timestamp = time.Now()
			}
			out = append(out, row)
		}
	}
	return out
}

func resourceSignal(attrs pcommon.Map) string {
	return strAttr(attrs, attrColaSignal)
}

func strAttr(attrs pcommon.Map, key string) string {
	v, ok := attrs.Get(key)
	if !ok {
		return ""
	}
	return v.AsString()
}

func intAttr(attrs pcommon.Map, key string) int64 {
	v, ok := attrs.Get(key)
	if !ok {
		return 0
	}
	switch v.Type() {
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return int64(v.Double())
	default:
		return 0
	}
}

func sliceAttr(attrs pcommon.Map, key string) []string {
	v, ok := attrs.Get(key)
	if !ok || v.Type() != pcommon.ValueTypeSlice {
		return []string{}
	}
	s := v.Slice()
	out := make([]string, 0, s.Len())
	for i := 0; i < s.Len(); i++ {
		out = append(out, s.At(i).AsString())
	}
	return out
}

func mapFromAttr(attrs pcommon.Map, key string) map[string]string {
	v, ok := attrs.Get(key)
	if !ok || v.Type() != pcommon.ValueTypeMap {
		return map[string]string{}
	}
	m := v.Map()
	out := make(map[string]string, m.Len())
	m.Range(func(k string, val pcommon.Value) bool {
		out[k] = val.AsString()
		return true
	})
	return out
}
