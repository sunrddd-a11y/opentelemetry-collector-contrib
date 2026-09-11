// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"strings"
	"time"
)

const (
	agentTypeSkyWalking = "skywalking"
	agentTypeOTel       = "otel"

	attrColaSignal        = "sw.cola.signal"
	attrColaAgentType     = "sw.cola.agent_type"
	attrSWTraceID         = "sw8.trace_id"
	attrTelemetrySDKName  = "telemetry.sdk.name"
	signalProfileSnapshot = "profile_snapshot"
	signalAgentProperties = "agent_properties"
	signalAgentKeepAlive  = "agent_keepalive"
	attrServiceName       = "service.name"
	attrServiceInstanceID = "service.instance.id"
)

type agentRow struct {
	AgentType    string
	ServiceName  string
	InstanceName string
	IP           string
	Environment  string
	Language     string
	Version      string
	Properties   map[string]string
	LastTime     time.Time
}

func cloneProperties(props map[string]string) map[string]string {
	if len(props) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(props))
	for k, v := range props {
		out[k] = v
	}
	return out
}

func resolveAgentIP(instanceName string, props map[string]string) string {
	if i := strings.Index(instanceName, "@"); i >= 0 {
		if ip := strings.TrimSpace(instanceName[i+1:]); ip != "" {
			return ip
		}
	}
	if ip := strings.TrimSpace(props["ipv4"]); ip != "" {
		return ip
	}
	return ""
}

func newSkyWalkingAgent(service, instance, layer string, props map[string]string, now time.Time) agentRow {
	props = cloneProperties(props)
	if layer != "" {
		props["layer"] = layer
	}
	return agentRow{
		AgentType:    agentTypeSkyWalking,
		ServiceName:  service,
		InstanceName: instance,
		IP:           resolveAgentIP(instance, props),
		Environment:  strings.ToLower(props["OS Name"]),
		Language:     props["language"],
		Version:      props["version"],
		Properties:   props,
		LastTime:     now,
	}
}

func keepAliveAgent(existing *agentRow, service, instance, layer string, now time.Time) agentRow {
	if existing != nil {
		row := *existing
		row.LastTime = now
		row.Properties = cloneProperties(row.Properties)
		return row
	}
	props := map[string]string{}
	if layer != "" {
		props["layer"] = layer
	}
	return agentRow{
		AgentType:    agentTypeSkyWalking,
		ServiceName:  service,
		InstanceName: instance,
		IP:           resolveAgentIP(instance, props),
		LastTime:     now,
		Properties:   props,
	}
}
