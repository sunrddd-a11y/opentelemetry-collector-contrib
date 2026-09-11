// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResolveAgentIP(t *testing.T) {
	assert.Equal(t, "10.2.120.42", resolveAgentIP("sk_alarm@10.2.120.42", map[string]string{"ipv4": "1.1.1.1"}))
	assert.Equal(t, "1.2.3.4", resolveAgentIP("host@", map[string]string{"ipv4": "1.2.3.4"}))
	assert.Equal(t, "10.0.0.8", resolveAgentIP("worker-1", map[string]string{"ipv4": "10.0.0.8"}))
	assert.Equal(t, "", resolveAgentIP("worker-1", nil))
}

func TestNewSkyWalkingAgentAndKeepAlive(t *testing.T) {
	now := time.Date(2026, 9, 10, 3, 0, 0, 0, time.UTC)
	row := newSkyWalkingAgent("svc", "inst@10.0.0.1", "GENERAL", map[string]string{
		"hostname": "host-a",
		"OS Name":  "Linux",
		"language": "java",
		"version":  "9.3.0",
	}, now)
	assert.Equal(t, agentTypeSkyWalking, row.AgentType)
	assert.Equal(t, "10.0.0.1", row.IP)
	assert.Equal(t, "linux", row.Environment)
	assert.Equal(t, "GENERAL", row.Properties["layer"])

	later := now.Add(time.Minute)
	hit := keepAliveAgent(&row, "svc", "inst@10.0.0.1", "GENERAL", later)
	assert.Equal(t, "10.0.0.1", hit.IP)
	assert.Equal(t, later, hit.LastTime)
	hit.Properties["hostname"] = "mutated"
	assert.Equal(t, "host-a", row.Properties["hostname"])

	miss := keepAliveAgent(nil, "svc", "inst@10.0.0.1", "GENERAL", later)
	assert.Equal(t, "10.0.0.1", miss.IP)
	assert.Equal(t, "GENERAL", miss.Properties["layer"])
}
