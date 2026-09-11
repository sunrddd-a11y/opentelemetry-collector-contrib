// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func testLicenseExtra(states map[agentKey]bool) *colaExtra {
	return &colaExtra{
		cfg: createDefaultConfig().(*Config),
		license: &licenseGate{
			cfg:    LicenseConfig{Enabled: true},
			states: states,
			loaded: true,
		},
	}
}

func resourceWith(attrs map[string]string) pcommon.Map {
	m := pcommon.NewMap()
	for k, v := range attrs {
		m.PutStr(k, v)
	}
	return m
}

func TestResolveAgentIdentity(t *testing.T) {
	at, svc, inst := resolveAgentIdentity(resourceWith(map[string]string{
		attrColaAgentType:     agentTypeSkyWalking,
		attrServiceName:       "sw-svc",
		attrServiceInstanceID: "sw-inst",
	}))
	assert.Equal(t, agentTypeSkyWalking, at)
	assert.Equal(t, "sw-svc", svc)
	assert.Equal(t, "sw-inst", inst)

	at, _, _ = resolveAgentIdentity(resourceWith(map[string]string{
		attrColaSignal:        signalAgentProperties,
		attrServiceName:       "sw-svc",
		attrServiceInstanceID: "sw-inst",
	}))
	assert.Equal(t, agentTypeSkyWalking, at)

	at, _, _ = resolveAgentIdentity(resourceWith(map[string]string{
		attrSWTraceID:         "abc",
		attrServiceName:       "sw-svc",
		attrServiceInstanceID: "sw-inst",
	}))
	assert.Equal(t, agentTypeSkyWalking, at)

	at, _, _ = resolveAgentIdentity(resourceWith(map[string]string{
		attrTelemetrySDKName:  agentTypeSkyWalking,
		attrServiceName:       "sw-svc",
		attrServiceInstanceID: "sw-inst",
	}))
	assert.Equal(t, agentTypeSkyWalking, at)

	at, svc, inst = resolveAgentIdentity(resourceWith(map[string]string{
		attrServiceName:       "otel-svc",
		attrServiceInstanceID: "otel-inst",
	}))
	assert.Equal(t, agentTypeOTel, at)
	assert.Equal(t, "otel-svc", svc)
	assert.Equal(t, "otel-inst", inst)
}

func TestFilterTracesByLicense(t *testing.T) {
	e := testLicenseExtra(map[agentKey]bool{
		{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "denied"}: false,
		{AgentType: agentTypeOTel, ServiceName: "svc", InstanceName: "ok"}:           true,
	})

	td := ptrace.NewTraces()
	denied := td.ResourceSpans().AppendEmpty()
	denied.Resource().Attributes().PutStr(attrColaAgentType, agentTypeSkyWalking)
	denied.Resource().Attributes().PutStr(attrServiceName, "svc")
	denied.Resource().Attributes().PutStr(attrServiceInstanceID, "denied")
	denied.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("denied-span")

	allowed := td.ResourceSpans().AppendEmpty()
	allowed.Resource().Attributes().PutStr(attrServiceName, "svc")
	allowed.Resource().Attributes().PutStr(attrServiceInstanceID, "ok")
	allowed.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("ok-span")

	unknown := td.ResourceSpans().AppendEmpty()
	unknown.Resource().Attributes().PutStr(attrServiceName, "svc")
	unknown.Resource().Attributes().PutStr(attrServiceInstanceID, "new")
	unknown.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("new-span")

	missing := td.ResourceSpans().AppendEmpty()
	missing.Resource().Attributes().PutStr(attrServiceName, "svc")
	missing.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("missing-span")

	got := e.filterTraces(td)
	require.Equal(t, 3, got.ResourceSpans().Len())
	names := []string{
		got.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name(),
		got.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0).Name(),
		got.ResourceSpans().At(2).ScopeSpans().At(0).Spans().At(0).Name(),
	}
	assert.Equal(t, []string{"ok-span", "new-span", "missing-span"}, names)
}

func TestFilterMetricsAndLogs(t *testing.T) {
	e := testLicenseExtra(map[agentKey]bool{
		{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "denied"}: false,
	})

	md := pmetric.NewMetrics()
	denied := md.ResourceMetrics().AppendEmpty()
	denied.Resource().Attributes().PutStr(attrColaAgentType, agentTypeSkyWalking)
	denied.Resource().Attributes().PutStr(attrServiceName, "svc")
	denied.Resource().Attributes().PutStr(attrServiceInstanceID, "denied")
	denied.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("denied")

	ok := md.ResourceMetrics().AppendEmpty()
	ok.Resource().Attributes().PutStr(attrColaAgentType, agentTypeSkyWalking)
	ok.Resource().Attributes().PutStr(attrServiceName, "svc")
	ok.Resource().Attributes().PutStr(attrServiceInstanceID, "other")
	ok.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("ok")

	gotM := e.filterMetrics(md)
	require.Equal(t, 1, gotM.ResourceMetrics().Len())
	assert.Equal(t, "ok", gotM.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name())

	ld := plog.NewLogs()
	deniedL := ld.ResourceLogs().AppendEmpty()
	deniedL.Resource().Attributes().PutStr(attrColaAgentType, agentTypeSkyWalking)
	deniedL.Resource().Attributes().PutStr(attrServiceName, "svc")
	deniedL.Resource().Attributes().PutStr(attrServiceInstanceID, "denied")
	deniedL.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("denied")

	okL := ld.ResourceLogs().AppendEmpty()
	okL.Resource().Attributes().PutStr(attrServiceName, "svc")
	okL.Resource().Attributes().PutStr(attrServiceInstanceID, "other")
	okL.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("ok")

	gotL := e.filterLogs(ld)
	require.Equal(t, 1, gotL.LogRecordCount())
	assert.Equal(t, "ok", gotL.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString())
}

func TestFilterSnapshots(t *testing.T) {
	e := testLicenseExtra(map[agentKey]bool{
		{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "denied"}: false,
	})
	got := e.filterSnapshots([]snapshotRow{
		{Service: "svc", ServiceInstance: "denied"},
		{Service: "svc", ServiceInstance: "ok"},
		{Service: "", ServiceInstance: "denied"},
	})
	require.Len(t, got, 2)
	assert.Equal(t, "ok", got[0].ServiceInstance)
	assert.Equal(t, "", got[1].Service)
}

func TestFilterDisabledSkipsCopy(t *testing.T) {
	e := &colaExtra{
		license: &licenseGate{cfg: LicenseConfig{Enabled: false}, loaded: true},
	}
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty()
	assert.Equal(t, 1, e.filterTraces(td).ResourceSpans().Len())
}
