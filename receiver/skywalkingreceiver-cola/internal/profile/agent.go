// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"strings"
	"time"
)

const AgentTypeSkyWalking = "skywalking"

// AgentRow is one row in otel_agents after FINAL / latest-row collapse.
type AgentRow struct {
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

// ResolveAgentIP prefers the substring after the first '@' in instance_name,
// then the ipv4 property, otherwise empty.
func ResolveAgentIP(instanceName string, props map[string]string) string {
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

// NewSkyWalkingAgent maps ReportInstanceProperties onto an otel_agents row.
func NewSkyWalkingAgent(service, instance, layer string, props map[string]string, now time.Time) AgentRow {
	props = cloneProperties(props)
	if layer != "" {
		props["layer"] = layer
	}
	return AgentRow{
		AgentType:    AgentTypeSkyWalking,
		ServiceName:  service,
		InstanceName: instance,
		IP:           ResolveAgentIP(instance, props),
		Environment:  strings.ToLower(props["OS Name"]),
		Language:     props["language"],
		Version:      props["version"],
		Properties:   props,
		LastTime:     now,
	}
}

// KeepAliveAgent reuses a FINAL row when present; otherwise writes a minimal skywalking row.
func KeepAliveAgent(existing *AgentRow, service, instance, layer string, now time.Time) AgentRow {
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
	return AgentRow{
		AgentType:    AgentTypeSkyWalking,
		ServiceName:  service,
		InstanceName: instance,
		IP:           ResolveAgentIP(instance, props),
		LastTime:     now,
		Properties:   props,
	}
}
