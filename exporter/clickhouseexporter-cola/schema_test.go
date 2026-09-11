// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColaSchemaStatementsLocal(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database = "otel"
	stmts, err := colaSchemaStatements(cfg)
	require.NoError(t, err)
	require.Len(t, stmts, 6)
	joined := strings.Join(stmts, "\n")
	assert.Contains(t, stmts[0], "CREATE DATABASE IF NOT EXISTS `otel`")
	assert.Contains(t, stmts[1], "CREATE TABLE IF NOT EXISTS `otel`.`otel_agents`")
	assert.Contains(t, stmts[1], "ORDER BY (agent_type, service_name, instance_name)")
	assert.Contains(t, stmts[2], "CREATE TABLE IF NOT EXISTS `otel`.`sw_profile_snapshots`")
	assert.Contains(t, stmts[3], "CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_agent_mv`")
	assert.Contains(t, stmts[3], "TO `otel`.`otel_agents`")
	assert.Contains(t, stmts[3], "FROM `otel`.`otel_traces`")
	assert.Contains(t, stmts[3], "telemetry.sdk.name")
	assert.Contains(t, stmts[3], "nullIf(ResourceAttributes['os.type'], ''),\n\t\tnullIf(ResourceAttributes['k8s.pod.name'], ''),")
	osType := strings.Index(stmts[3], "ResourceAttributes['os.type']")
	k8sPod := strings.Index(stmts[3], "ResourceAttributes['k8s.pod.name']")
	hostName := strings.Index(stmts[3], "ResourceAttributes['host.name']")
	require.Greater(t, osType, 0)
	require.Greater(t, k8sPod, 0)
	require.Greater(t, hostName, 0)
	assert.Less(t, osType, k8sPod)
	assert.Less(t, k8sPod, hostName)
	assert.Contains(t, stmts[4], "CREATE TABLE IF NOT EXISTS `otel`.`otel_spans`")
	assert.Contains(t, stmts[4], "PRIMARY KEY (SpanKind, Timestamp)")
	assert.Contains(t, stmts[5], "CREATE MATERIALIZED VIEW IF NOT EXISTS `otel`.`otel_span_mv`")
	assert.Contains(t, stmts[5], "TO `otel`.`otel_spans`")
	assert.Contains(t, stmts[5], "FROM `otel`.`otel_traces`")
	assert.Contains(t, stmts[5], "AS mw_name")
	assert.NotContains(t, joined, "sw_profile_tasks")
	assert.NotContains(t, joined, "ON CLUSTER")
}

func TestColaSchemaStatementsDistributed(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database = "csotel"
	cfg.ClusterName = "cas_cluster"
	cfg.DistributedDatabase = "dvotel"
	stmts, err := colaSchemaStatements(cfg)
	require.NoError(t, err)
	joined := strings.Join(stmts, "\n")
	assert.Contains(t, stmts[0], "CREATE DATABASE IF NOT EXISTS `csotel` ON CLUSTER `cas_cluster`")
	assert.Contains(t, joined, "`csotel`.`otel_agents` ON CLUSTER `cas_cluster`")
	assert.Contains(t, joined, "CREATE MATERIALIZED VIEW IF NOT EXISTS `csotel`.`otel_agent_mv` ON CLUSTER `cas_cluster`")
	assert.Contains(t, joined, "CREATE DATABASE IF NOT EXISTS `dvotel` ON CLUSTER `cas_cluster`")
	assert.Contains(t, joined, "`dvotel`.`csotel_otel_traces`")
	assert.Contains(t, joined, "`dvotel`.`csotel_otel_agents`")
	assert.Contains(t, joined, "`dvotel`.`csotel_sw_profile_snapshots`")
	assert.Contains(t, joined, "`dvotel`.`csotel_otel_spans`")
	assert.Contains(t, joined, "CREATE MATERIALIZED VIEW IF NOT EXISTS `csotel`.`otel_span_mv` ON CLUSTER `cas_cluster`")
	assert.NotContains(t, joined, "sw_profile_tasks")
}

func TestIndependentSchemaHasNoOtelTableDependency(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database = "csotel"
	cfg.ClusterName = "cas_cluster"
	cfg.DistributedDatabase = "dvotel"
	stmts, err := independentColaStatements(cfg)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	joined := strings.Join(stmts, "\n")
	assert.Contains(t, joined, "`csotel`.`otel_agents`")
	assert.Contains(t, joined, "`csotel`.`sw_profile_snapshots`")
	assert.NotContains(t, joined, "otel_traces")
	assert.NotContains(t, joined, "otel_logs")
	assert.NotContains(t, joined, "otel_spans")
	assert.NotContains(t, joined, "MATERIALIZED VIEW")
	assert.NotContains(t, joined, "ENGINE = Distributed")
}

func TestDependentSchemaSkipsMissingLocals(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database = "csotel"
	cfg.ClusterName = "cas_cluster"
	cfg.DistributedDatabase = "dvotel"
	stmts, err := dependentColaStatements(cfg, map[string]struct{}{
		"otel_agents":          {},
		"sw_profile_snapshots": {},
	})
	require.NoError(t, err)
	joined := strings.Join(stmts, "\n")
	assert.NotContains(t, joined, "MATERIALIZED VIEW")
	assert.NotContains(t, joined, "`dvotel`.`csotel_otel_traces`")
	assert.NotContains(t, joined, "`dvotel`.`csotel_otel_logs`")
	assert.NotContains(t, joined, "`dvotel`.`csotel_otel_spans`")
	assert.NotContains(t, joined, "otel_span_mv")
	assert.Contains(t, joined, "CREATE DATABASE IF NOT EXISTS `dvotel`")
	assert.Contains(t, joined, "`dvotel`.`csotel_otel_agents`")
	assert.Contains(t, joined, "`dvotel`.`csotel_sw_profile_snapshots`")
}

func TestDependentSchemaEmitsMVWhenTracesExist(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Database = "csotel"
	stmts, err := dependentColaStatements(cfg, map[string]struct{}{"otel_traces": {}})
	require.NoError(t, err)
	require.NotEmpty(t, stmts)
	assert.Contains(t, stmts[0], "CREATE MATERIALIZED VIEW IF NOT EXISTS `csotel`.`otel_agent_mv`")
	assert.Contains(t, stmts[0], "FROM `csotel`.`otel_traces`")
	require.GreaterOrEqual(t, len(stmts), 3)
	assert.Contains(t, stmts[1], "CREATE TABLE IF NOT EXISTS `csotel`.`otel_spans`")
	assert.Contains(t, stmts[2], "CREATE MATERIALIZED VIEW IF NOT EXISTS `csotel`.`otel_span_mv`")
	assert.Contains(t, stmts[2], "FROM `csotel`.`otel_traces`")
}

func TestReadQualified(t *testing.T) {
	assert.Equal(t, "`csotel`.`otel_agents`", readQualified("csotel", "", "", "otel_agents"))
	assert.Equal(t, "`dvotel`.`csotel_otel_agents`", readQualified("csotel", "cas_cluster", "dvotel", "otel_agents"))
}
