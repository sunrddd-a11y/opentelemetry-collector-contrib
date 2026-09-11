// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	AttrColaSignal        = "sw.cola.signal"
	AttrColaAgentType     = "sw.cola.agent_type"
	SignalProfileSnapshot = "profile_snapshot"
	SignalAgentProperties = "agent_properties"
	SignalAgentKeepAlive  = "agent_keepalive"
)

func stampSkyWalkingAgentType(attrs pcommon.Map) {
	attrs.PutStr(AttrColaAgentType, AgentTypeSkyWalking)
}

func SnapshotsToLogs(rows []SnapshotRow) plog.Logs {
	ld := plog.NewLogs()
	byKey := map[string]plog.ResourceLogs{}
	for _, row := range rows {
		key := row.Service + "\x00" + row.ServiceInstance
		rl, ok := byKey[key]
		if !ok {
			rl = ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", row.Service)
			rl.Resource().Attributes().PutStr("service.instance.id", row.ServiceInstance)
			rl.Resource().Attributes().PutStr(AttrColaSignal, SignalProfileSnapshot)
			stampSkyWalkingAgentType(rl.Resource().Attributes())
			rl.ScopeLogs().AppendEmpty()
			byKey[key] = rl
		}
		lr := rl.ScopeLogs().At(0).LogRecords().AppendEmpty()
		if row.Timestamp.IsZero() {
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		} else {
			lr.SetTimestamp(pcommon.NewTimestampFromTime(row.Timestamp))
		}
		attrs := lr.Attributes()
		attrs.PutStr("task_id", row.TaskID)
		attrs.PutStr("service", row.Service)
		attrs.PutStr("service_instance", row.ServiceInstance)
		attrs.PutStr("endpoint_name", row.EndpointName)
		attrs.PutStr("otel_trace_id", row.OTelTraceID)
		attrs.PutStr("sw_trace_id", row.SWTraceID)
		attrs.PutStr("segment_id", row.SegmentID)
		attrs.PutInt("sequence", int64(row.Sequence))
		s := attrs.PutEmptySlice("stack_leaf_first")
		for _, frame := range row.StackLeafFirst {
			s.AppendEmpty().SetStr(frame)
		}
	}
	return ld
}

func AgentPropertiesToLogs(service, instance, layer string, props map[string]string, now time.Time) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	rl.Resource().Attributes().PutStr("service.instance.id", instance)
	rl.Resource().Attributes().PutStr(AttrColaSignal, SignalAgentProperties)
	stampSkyWalkingAgentType(rl.Resource().Attributes())
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(now))
	attrs := lr.Attributes()
	attrs.PutStr("service_name", service)
	attrs.PutStr("instance_name", instance)
	if layer != "" {
		attrs.PutStr("layer", layer)
	}
	m := attrs.PutEmptyMap("properties")
	for k, v := range props {
		m.PutStr(k, v)
	}
	return ld
}

func AgentKeepAliveToLogs(service, instance, layer string, now time.Time) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	rl.Resource().Attributes().PutStr("service.instance.id", instance)
	rl.Resource().Attributes().PutStr(AttrColaSignal, SignalAgentKeepAlive)
	stampSkyWalkingAgentType(rl.Resource().Attributes())
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(now))
	attrs := lr.Attributes()
	attrs.PutStr("service_name", service)
	attrs.PutStr("instance_name", instance)
	if layer != "" {
		attrs.PutStr("layer", layer)
	}
	return ld
}
