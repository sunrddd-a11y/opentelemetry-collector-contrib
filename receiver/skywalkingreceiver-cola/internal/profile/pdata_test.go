// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotsToLogs(t *testing.T) {
	now := time.Date(2026, 9, 10, 5, 0, 0, 0, time.UTC)
	ld := SnapshotsToLogs([]SnapshotRow{{
		Timestamp:       now,
		TaskID:          "t1",
		Service:         "svc",
		ServiceInstance: "i1",
		SegmentID:       "seg",
		Sequence:        2,
		StackLeafFirst:  []string{"a()"},
	}})
	require.Equal(t, 1, ld.LogRecordCount())
	rl := ld.ResourceLogs().At(0)
	assert.Equal(t, SignalProfileSnapshot, func() string {
		v, _ := rl.Resource().Attributes().Get(AttrColaSignal)
		return v.AsString()
	}())
	v, ok := rl.Resource().Attributes().Get(AttrColaAgentType)
	require.True(t, ok)
	assert.Equal(t, AgentTypeSkyWalking, v.AsString())
	lr := rl.ScopeLogs().At(0).LogRecords().At(0)
	v, _ = lr.Attributes().Get("task_id")
	assert.Equal(t, "t1", v.AsString())
}

func TestAgentLogs(t *testing.T) {
	now := time.Date(2026, 9, 10, 5, 0, 0, 0, time.UTC)
	ld := AgentPropertiesToLogs("svc", "inst", "GENERAL", map[string]string{"hostname": "h"}, now)
	rl := ld.ResourceLogs().At(0)
	v, _ := rl.Resource().Attributes().Get(AttrColaSignal)
	assert.Equal(t, SignalAgentProperties, v.AsString())
	at, ok := rl.Resource().Attributes().Get(AttrColaAgentType)
	require.True(t, ok)
	assert.Equal(t, AgentTypeSkyWalking, at.AsString())

	ld = AgentKeepAliveToLogs("svc", "inst", "GENERAL", now)
	v, _ = ld.ResourceLogs().At(0).Resource().Attributes().Get(AttrColaSignal)
	assert.Equal(t, SignalAgentKeepAlive, v.AsString())
	at, ok = ld.ResourceLogs().At(0).Resource().Attributes().Get(AttrColaAgentType)
	require.True(t, ok)
	assert.Equal(t, AgentTypeSkyWalking, at.AsString())
}
