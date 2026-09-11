// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLicenseAllowRules(t *testing.T) {
	g := &licenseGate{
		cfg: LicenseConfig{Enabled: true},
		states: map[agentKey]bool{
			{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "inst"}: false,
			{AgentType: agentTypeOTel, ServiceName: "svc", InstanceName: "inst"}:       true,
		},
		loaded: true,
	}

	assert.True(t, g.allow(agentTypeSkyWalking, "svc", "new-inst"), "not in list")
	assert.True(t, g.allow(agentTypeOTel, "svc", "inst"), "authorized")
	assert.False(t, g.allow(agentTypeSkyWalking, "svc", "inst"), "unauthorized")
	assert.True(t, g.allow(agentTypeSkyWalking, "", "inst"), "missing service")
	assert.True(t, g.allow(agentTypeSkyWalking, "svc", ""), "missing instance")
}

func TestLicenseAllowBeforeLoadAndDisabled(t *testing.T) {
	unloaded := &licenseGate{
		cfg: LicenseConfig{Enabled: true},
		states: map[agentKey]bool{
			{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "inst"}: false,
		},
	}
	assert.True(t, unloaded.allow(agentTypeSkyWalking, "svc", "inst"), "fail-open before first success")

	disabled := &licenseGate{
		cfg: LicenseConfig{Enabled: false},
		states: map[agentKey]bool{
			{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "inst"}: false,
		},
		loaded: true,
	}
	assert.True(t, disabled.allow(agentTypeSkyWalking, "svc", "inst"))
	assert.True(t, (*licenseGate)(nil).allow(agentTypeSkyWalking, "svc", "inst"))
}

func TestLicenseQuerySQL(t *testing.T) {
	q, err := licenseQuery(defaultLicenseAgentsTable, defaultLicenseTable, defaultLicenseDictType)
	require.NoError(t, err)
	assert.Contains(t, q, "`dvotel`.`csotel_otel_agents`")
	assert.Contains(t, q, "`dvutl`.`csutl_tconfig_public`")
	assert.Contains(t, q, "dict_type = 30")
	assert.Contains(t, q, "apm_agent_license:")
	assert.Contains(t, q, "char(0)")
	assert.NotContains(t, q, "%!")

	_, err = licenseQuery("bad-name", defaultLicenseTable, 30)
	require.Error(t, err)
}

func TestLicenseApplyDefaults(t *testing.T) {
	var c LicenseConfig
	c.applyDefaults()
	assert.Equal(t, defaultLicenseRefresh, c.RefreshInterval)
	assert.Equal(t, defaultLicenseAgentsTable, c.AgentsTable)
	assert.Equal(t, defaultLicenseTable, c.LicenseTable)
	assert.Equal(t, uint64(defaultLicenseDictType), c.DictType)

	c = LicenseConfig{RefreshInterval: time.Minute, AgentsTable: "a.b", LicenseTable: "c.d", DictType: 7}
	c.applyDefaults()
	assert.Equal(t, time.Minute, c.RefreshInterval)
	assert.Equal(t, "a.b", c.AgentsTable)
	assert.Equal(t, "c.d", c.LicenseTable)
	assert.Equal(t, uint64(7), c.DictType)
}

func TestLicenseGateStartShutdown(t *testing.T) {
	g := newLicenseGate(LicenseConfig{Enabled: true, RefreshInterval: time.Hour}, nil, nil)
	g.start()
	g.shutdown()
}

func TestQuoteQualifiedTable(t *testing.T) {
	got, err := quoteQualifiedTable("dvotel.csotel_otel_agents", "license.agents_table")
	require.NoError(t, err)
	assert.Equal(t, "`dvotel`.`csotel_otel_agents`", got)
	require.Error(t, validateQualifiedTable("onlytable", "license.agents_table"))
	require.Error(t, validateQualifiedTable("a.b-c", "license.agents_table"))
}
