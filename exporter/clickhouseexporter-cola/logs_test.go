// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestSnapshotRowsFromResource(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(attrServiceName, "order-service")
	rl.Resource().Attributes().PutStr(attrServiceInstanceID, "i1")
	rl.Resource().Attributes().PutStr(attrColaSignal, signalProfileSnapshot)
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(1_000_000_000)
	lr.Attributes().PutStr("task_id", "t1")
	lr.Attributes().PutStr("segment_id", "seg-1")
	lr.Attributes().PutInt("sequence", 3)
	lr.Attributes().PutStr("endpoint_name", "/checkout")
	s := lr.Attributes().PutEmptySlice("stack_leaf_first")
	s.AppendEmpty().SetStr("a()")
	s.AppendEmpty().SetStr("b()")

	rows := snapshotRowsFromResource(rl, "order-service", "i1")
	require.Len(t, rows, 1)
	assert.Equal(t, "t1", rows[0].TaskID)
	assert.Equal(t, "seg-1", rows[0].SegmentID)
	assert.Equal(t, uint32(3), rows[0].Sequence)
	assert.Equal(t, []string{"a()", "b()"}, rows[0].StackLeafFirst)
	assert.Equal(t, time.Unix(0, 1_000_000_000).UTC(), rows[0].Timestamp.UTC())
}

func TestConsumeColaLogsFiltersUnauthorizedSnapshots(t *testing.T) {
	e := testLicenseExtra(map[agentKey]bool{
		{AgentType: agentTypeSkyWalking, ServiceName: "svc", InstanceName: "denied"}: false,
	})
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(attrServiceName, "svc")
	rl.Resource().Attributes().PutStr(attrServiceInstanceID, "denied")
	rl.Resource().Attributes().PutStr(attrColaSignal, signalProfileSnapshot)
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes().PutStr("task_id", "t1")

	rest, err := e.consumeColaLogs(t.Context(), ld)
	require.NoError(t, err)
	assert.Equal(t, 0, rest.LogRecordCount())
}

func TestConsumeColaLogsLeavesOrdinaryLogs(t *testing.T) {
	e := &colaExtra{cfg: createDefaultConfig().(*Config)}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(attrServiceName, "svc")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("hello")

	rest, err := e.consumeColaLogs(t.Context(), ld)
	require.NoError(t, err)
	require.Equal(t, 1, rest.LogRecordCount())
	assert.Equal(t, "hello", rest.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString())
}
