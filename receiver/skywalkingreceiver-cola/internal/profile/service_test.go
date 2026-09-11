// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	swprofile "skywalking.apache.org/repo/goapi/collect/language/profile/v3"
)

type mockSnapStream struct {
	grpc.ServerStream
	inbox []*swprofile.ThreadSnapshot
	idx   int
	got   *common.Commands
}

func (m *mockSnapStream) Context() context.Context { return context.Background() }

func (m *mockSnapStream) Recv() (*swprofile.ThreadSnapshot, error) {
	if m.idx >= len(m.inbox) {
		return nil, io.EOF
	}
	s := m.inbox[m.idx]
	m.idx++
	return s, nil
}

func (m *mockSnapStream) SendAndClose(c *common.Commands) error {
	m.got = c
	return nil
}

func TestGetProfileTaskCommands(t *testing.T) {
	now := time.Now()
	store := &MemoryStore{Tasks: []Task{{
		TaskID:          "checkout-slow",
		Service:         "order-service",
		EndpointName:    "/checkout",
		DurationSeconds: 600,
		StartTime:       now.Add(-time.Minute),
		CreateTime:      now.Add(-time.Minute),
		SerialNumber:    "sn-1",
		Enabled:         1,
	}}}
	rt := NewRuntime(store, nil, zap.NewNop(), time.Hour, 10, time.Hour)
	require.NoError(t, rt.Start(context.Background()))
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	cmds, err := rt.Service().GetProfileTaskCommands(context.Background(), &swprofile.ProfileTaskCommandQuery{
		Service:         "order-service",
		ServiceInstance: "i1",
		LastCommandTime: 0,
	})
	require.NoError(t, err)
	require.Len(t, cmds.Commands, 1)
	assert.Equal(t, "ProfileTaskQuery", cmds.Commands[0].Command)

	cmds, err = rt.Service().GetProfileTaskCommands(context.Background(), &swprofile.ProfileTaskCommandQuery{
		Service:         "other",
		LastCommandTime: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, cmds.Commands)
}

func TestCollectSnapshotAndFinish(t *testing.T) {
	now := time.Now()
	store := &MemoryStore{Tasks: []Task{{
		TaskID:          "t1",
		Service:         "order-service",
		EndpointName:    "/checkout",
		DurationSeconds: 600,
		StartTime:       now.Add(-time.Minute),
		CreateTime:      now.Add(-time.Minute),
		Enabled:         1,
	}}}
	rt := NewRuntime(store, nil, zap.NewNop(), time.Hour, 1, time.Hour)
	require.NoError(t, rt.Start(context.Background()))
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	rt.SegmentCache().Put("seg-1", SegmentIdent{
		OTelTraceID:     "aabbcc",
		SWTraceID:       "sw-trace",
		Service:         "order-service",
		ServiceInstance: "i1",
	})

	stream := &mockSnapStream{inbox: []*swprofile.ThreadSnapshot{{
		TaskId:         "t1",
		TraceSegmentId: "seg-1",
		Time:           now.UnixMilli(),
		Sequence:       0,
		Stack:          &swprofile.ThreadStack{CodeSignatures: []string{"a()", "b()"}},
	}}}
	require.NoError(t, rt.Service().CollectSnapshot(stream))
	require.NotNil(t, stream.got)

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.Snapshots) == 1
	}, 2*time.Second, 20*time.Millisecond)

	row := store.Snapshots[0]
	assert.Equal(t, "t1", row.TaskID)
	assert.Equal(t, "seg-1", row.SegmentID)
	assert.Equal(t, "aabbcc", row.OTelTraceID)
	assert.Equal(t, "sw-trace", row.SWTraceID)
	assert.Equal(t, "order-service", row.Service)
	assert.Equal(t, "/checkout", row.EndpointName)
	assert.Equal(t, []string{"a()", "b()"}, row.StackLeafFirst)

	_, err := rt.Service().ReportTaskFinish(context.Background(), &swprofile.ProfileTaskFinishReport{
		TaskId:          "t1",
		Service:         "order-service",
		ServiceInstance: "i1",
	})
	require.NoError(t, err)

	store.mu.Lock()
	var finished Task
	for _, task := range store.Tasks {
		if task.TaskID == "t1" && task.Status == TaskStatusFinished {
			finished = task
		}
	}
	store.mu.Unlock()
	require.Equal(t, TaskStatusFinished, finished.Status)
	assert.Equal(t, "order-service", finished.Service)
	assert.Equal(t, "/checkout", finished.EndpointName)

	cmds, err := rt.Service().GetProfileTaskCommands(context.Background(), &swprofile.ProfileTaskCommandQuery{
		Service:         "order-service",
		ServiceInstance: "i1",
		LastCommandTime: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, cmds.Commands)
}

func TestGetProfileTaskCommandsWithoutStore(t *testing.T) {
	rt := NewRuntime(nil, nil, zap.NewNop(), time.Second, 1, time.Second)
	cmds, err := rt.Service().GetProfileTaskCommands(context.Background(), &swprofile.ProfileTaskCommandQuery{
		Service: "order-service",
	})
	require.NoError(t, err)
	assert.Empty(t, cmds.Commands)
}
