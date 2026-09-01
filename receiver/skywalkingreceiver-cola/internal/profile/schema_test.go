// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaStatements(t *testing.T) {
	stmts, err := schemaStatements(CHConfig{
		Database:       "otel",
		TasksTable:     "sw_profile_tasks",
		SnapshotsTable: "sw_profile_snapshots",
	})
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	assert.Contains(t, stmts[0], "CREATE DATABASE IF NOT EXISTS `otel`")
	assert.Contains(t, stmts[1], "CREATE TABLE IF NOT EXISTS `otel`.`sw_profile_tasks`")
	assert.Contains(t, stmts[1], "ENGINE = ReplacingMergeTree")
	assert.Contains(t, stmts[1], "ORDER BY (service, task_id, service_instance)")
	assert.Contains(t, stmts[1], "status                    LowCardinality(String) DEFAULT 'running'")
	assert.NotContains(t, stmts[1], "ReplacingMergeTree(")
	assert.Contains(t, stmts[2], "CREATE TABLE IF NOT EXISTS `otel`.`sw_profile_snapshots`")
	assert.Contains(t, stmts[2], "ENGINE = MergeTree")
	assert.Contains(t, stmts[2], "ORDER BY (segment_id, sequence)")
	assert.Contains(t, stmts[2], "INDEX idx_ts         timestamp     TYPE minmax GRANULARITY 1")
	assert.NotContains(t, stmts[2], "sw_profile_flame_edges")
	assert.NotContains(t, stmts[2], "sw_profile_task_finish")
}

func TestSchemaStatementsRejectsBadIdent(t *testing.T) {
	_, err := schemaStatements(CHConfig{
		Database:       "otel",
		TasksTable:     "sw_profile_tasks",
		SnapshotsTable: "bad-name",
	})
	require.Error(t, err)
}
