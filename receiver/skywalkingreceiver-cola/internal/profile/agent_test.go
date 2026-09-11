// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveAgentIP(t *testing.T) {
	t.Run("from instance after at", func(t *testing.T) {
		assert.Equal(t, "10.2.120.42", ResolveAgentIP("sk_alarm@10.2.120.42", map[string]string{"ipv4": "1.1.1.1"}))
	})
	t.Run("empty after at falls back to ipv4", func(t *testing.T) {
		assert.Equal(t, "1.2.3.4", ResolveAgentIP("host@", map[string]string{"ipv4": "1.2.3.4"}))
	})
	t.Run("ipv4 property", func(t *testing.T) {
		assert.Equal(t, "10.0.0.8", ResolveAgentIP("worker-1", map[string]string{"ipv4": "10.0.0.8"}))
	})
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "", ResolveAgentIP("worker-1", nil))
	})
}

func TestReadQualified(t *testing.T) {
	assert.Equal(t, "`csotel`.`sw_profile_tasks`", readQualified("csotel", "", "", "sw_profile_tasks"))
	assert.Equal(t, "`dvotel`.`csotel_sw_profile_tasks`", readQualified("csotel", "cas_cluster", "dvotel", "sw_profile_tasks"))
	assert.Equal(t, "`dvotel`.`csotel_otel_agents`", readQualified("csotel", "cas_cluster", "dvotel", "otel_agents"))
}

func TestNewSkyWalkingAgent(t *testing.T) {
	now := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
	row := NewSkyWalkingAgent(
		"sk.colasoft.cas.alarm",
		"sk_alarm@10.2.120.42",
		"GENERAL",
		map[string]string{
			"hostname": "alarm-1",
			"OS Name":  "Linux",
			"language": "java",
			"version":  "9.3.0",
			"ipv4":     "10.2.120.42",
		},
		now,
	)
	assert.Equal(t, AgentTypeSkyWalking, row.AgentType)
	assert.Equal(t, "sk.colasoft.cas.alarm", row.ServiceName)
	assert.Equal(t, "sk_alarm@10.2.120.42", row.InstanceName)
	assert.Equal(t, "10.2.120.42", row.IP)
	assert.Equal(t, "linux", row.Environment)
	assert.Equal(t, "java", row.Language)
	assert.Equal(t, "9.3.0", row.Version)
	assert.Equal(t, "GENERAL", row.Properties["layer"])
	assert.Equal(t, "java", row.Properties["language"])
	assert.Equal(t, now, row.LastTime)
}

func TestKeepAliveAgent(t *testing.T) {
	now := time.Date(2026, 9, 10, 3, 0, 0, 0, time.UTC)
	existing := &AgentRow{
		AgentType:    AgentTypeSkyWalking,
		ServiceName:  "svc",
		InstanceName: "inst@10.0.0.1",
		IP:           "10.0.0.1",
		Environment:  "host-a",
		Language:     "java",
		Version:      "9.3.0",
		Properties:   map[string]string{"hostname": "host-a", "layer": "GENERAL"},
		LastTime:     now.Add(-time.Minute),
	}

	hit := KeepAliveAgent(existing, "svc", "inst@10.0.0.1", "GENERAL", now)
	assert.Equal(t, existing.IP, hit.IP)
	assert.Equal(t, existing.Environment, hit.Environment)
	assert.Equal(t, existing.Properties["hostname"], hit.Properties["hostname"])
	assert.Equal(t, now, hit.LastTime)
	hit.Properties["hostname"] = "mutated"
	assert.Equal(t, "host-a", existing.Properties["hostname"])

	miss := KeepAliveAgent(nil, "svc", "inst@10.0.0.1", "GENERAL", now)
	assert.Equal(t, AgentTypeSkyWalking, miss.AgentType)
	assert.Equal(t, "svc", miss.ServiceName)
	assert.Equal(t, "inst@10.0.0.1", miss.InstanceName)
	assert.Equal(t, "10.0.0.1", miss.IP)
	assert.Equal(t, "GENERAL", miss.Properties["layer"])
	assert.Equal(t, now, miss.LastTime)
}

func TestMemoryStoreAgentRoundTrip(t *testing.T) {
	store := &MemoryStore{}
	now := time.Date(2026, 9, 10, 4, 0, 0, 0, time.UTC)
	require.NoError(t, store.InsertAgent(t.Context(), AgentRow{
		AgentType:    AgentTypeSkyWalking,
		ServiceName:  "svc",
		InstanceName: "i1",
		IP:           "1.1.1.1",
		LastTime:     now,
		Properties:   map[string]string{"k": "v"},
	}))
	got, err := store.GetAgent(t.Context(), AgentTypeSkyWalking, "svc", "i1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "1.1.1.1", got.IP)
	assert.Equal(t, "v", got.Properties["k"])

	missing, err := store.GetAgent(t.Context(), AgentTypeSkyWalking, "svc", "other")
	require.NoError(t, err)
	assert.Nil(t, missing)
}
