// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package skywalkingreceivercola

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	management "skywalking.apache.org/repo/goapi/collect/management/v3"

	swprofile "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"
)

func TestReportInstancePropertiesInsertsAgent(t *testing.T) {
	store := &swprofile.MemoryStore{}
	svc := &dummyReportService{agents: store}

	cmds, err := svc.ReportInstanceProperties(t.Context(), &management.InstanceProperties{
		Service:         "sk.colasoft.cas.alarm",
		ServiceInstance: "sk_alarm@10.2.120.42",
		Layer:           "GENERAL",
		Properties: []*common.KeyStringValuePair{
			{Key: "hostname", Value: "alarm-1"},
			{Key: "OS Name", Value: "Linux"},
			{Key: "language", Value: "java"},
			{Key: "ipv4", Value: "10.2.120.42"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, cmds.GetCommands())
	require.Len(t, store.Agents, 1)
	row := store.Agents[0]
	assert.Equal(t, swprofile.AgentTypeSkyWalking, row.AgentType)
	assert.Equal(t, "sk.colasoft.cas.alarm", row.ServiceName)
	assert.Equal(t, "10.2.120.42", row.IP)
	assert.Equal(t, "linux", row.Environment)
	assert.Equal(t, "java", row.Language)
	assert.Equal(t, "GENERAL", row.Properties["layer"])
}

func TestKeepAliveReusesExistingThenInserts(t *testing.T) {
	store := &swprofile.MemoryStore{}
	svc := &dummyReportService{agents: store}
	first := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	require.NoError(t, store.InsertAgent(t.Context(), swprofile.AgentRow{
		AgentType:    swprofile.AgentTypeSkyWalking,
		ServiceName:  "svc",
		InstanceName: "inst@10.0.0.1",
		IP:           "10.0.0.1",
		Environment:  "host-a",
		Language:     "java",
		Properties:   map[string]string{"hostname": "host-a"},
		LastTime:     first,
	}))

	cmds, err := svc.KeepAlive(t.Context(), &management.InstancePingPkg{
		Service:         "svc",
		ServiceInstance: "inst@10.0.0.1",
	})
	require.NoError(t, err)
	require.Empty(t, cmds.GetCommands())
	require.Len(t, store.Agents, 2)
	got := store.Agents[1]
	assert.Equal(t, "10.0.0.1", got.IP)
	assert.Equal(t, "host-a", got.Environment)
	assert.Equal(t, "java", got.Language)
	assert.True(t, got.LastTime.After(first))
}

func TestKeepAliveMissInsertsMinimalRow(t *testing.T) {
	store := &swprofile.MemoryStore{}
	svc := &dummyReportService{agents: store}

	_, err := svc.KeepAlive(t.Context(), &management.InstancePingPkg{
		Service:         "svc",
		ServiceInstance: "sky-http@10.2.120.42",
		Layer:           "GENERAL",
	})
	require.NoError(t, err)
	require.Len(t, store.Agents, 1)
	got := store.Agents[0]
	assert.Equal(t, swprofile.AgentTypeSkyWalking, got.AgentType)
	assert.Equal(t, "svc", got.ServiceName)
	assert.Equal(t, "sky-http@10.2.120.42", got.InstanceName)
	assert.Equal(t, "10.2.120.42", got.IP)
	assert.Equal(t, "GENERAL", got.Properties["layer"])
}

func TestReportInstancePropertiesPrefersLogs(t *testing.T) {
	sink := &consumertest.LogsSink{}
	store := &swprofile.MemoryStore{}
	svc := &dummyReportService{agents: store, logs: sink}

	_, err := svc.ReportInstanceProperties(t.Context(), &management.InstanceProperties{
		Service:         "svc",
		ServiceInstance: "inst@10.0.0.1",
		Layer:           "GENERAL",
	})
	require.NoError(t, err)
	assert.Empty(t, store.Agents)
	require.Equal(t, 1, sink.LogRecordCount())
	v, ok := sink.AllLogs()[0].ResourceLogs().At(0).Resource().Attributes().Get(swprofile.AttrColaSignal)
	require.True(t, ok)
	assert.Equal(t, swprofile.SignalAgentProperties, v.AsString())
}

func TestManagementRPCsNoStoreStillSucceed(t *testing.T) {
	svc := &dummyReportService{}
	cmds, err := svc.ReportInstanceProperties(t.Context(), &management.InstanceProperties{Service: "svc"})
	require.NoError(t, err)
	require.Empty(t, cmds.GetCommands())
	cmds, err = svc.KeepAlive(t.Context(), &management.InstancePingPkg{Service: "svc"})
	require.NoError(t, err)
	require.Empty(t, cmds.GetCommands())
}
