// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaStatements(t *testing.T) {
	stmts, err := schemaStatements(CHConfig{
		Database:   "otel",
		TasksTable: "sw_profile_tasks",
	})
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "CREATE DATABASE IF NOT EXISTS `otel`")
	assert.Contains(t, stmts[1], "CREATE TABLE IF NOT EXISTS `otel`.`sw_profile_tasks`")
	assert.Contains(t, stmts[1], "ENGINE = ReplacingMergeTree")
	assert.Contains(t, stmts[1], "ORDER BY task_id")
	assert.Contains(t, stmts[1], "`delete`                  UInt8 DEFAULT 0")
	joined := strings.Join(stmts, "\n")
	assert.NotContains(t, joined, "sw_profile_snapshots")
	assert.NotContains(t, joined, "otel_agents")
	assert.NotContains(t, joined, "otel_agent_mv")
	assert.NotContains(t, joined, "ON CLUSTER")
}

func TestSchemaStatementsDistributed(t *testing.T) {
	stmts, err := schemaStatements(CHConfig{
		Database:            "csotel",
		ClusterName:         "cas_cluster",
		DistributedDatabase: "dvotel",
		TasksTable:          "sw_profile_tasks",
	})
	require.NoError(t, err)
	require.Len(t, stmts, 2+1+1)
	joined := strings.Join(stmts, "\n")
	assert.Contains(t, stmts[2], "CREATE DATABASE IF NOT EXISTS `dvotel` ON CLUSTER `cas_cluster`")
	assert.Contains(t, joined, "`dvotel`.`csotel_sw_profile_tasks`")
	assert.NotContains(t, joined, "otel_traces")
	assert.NotContains(t, joined, "otel_agents")
	assert.NotContains(t, joined, "sw_profile_snapshots")
	assert.NotContains(t, joined, "MATERIALIZED VIEW")
}

func TestSchemaStatementsRejectsBadIdent(t *testing.T) {
	_, err := schemaStatements(CHConfig{
		Database:   "otel",
		TasksTable: "bad-name",
	})
	require.Error(t, err)
}
