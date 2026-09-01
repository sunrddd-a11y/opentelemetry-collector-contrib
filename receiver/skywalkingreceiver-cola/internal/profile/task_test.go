// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskMatches(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	base := Task{
		TaskID:          "t1",
		Service:         "order-service",
		EndpointName:    "/checkout",
		DurationSeconds: 300,
		StartTime:       now.Add(-time.Minute),
		CreateTime:      now.Add(-time.Minute),
		Enabled:         1,
	}

	t.Run("match", func(t *testing.T) {
		assert.True(t, base.Matches("order-service", "i1", 0, now))
	})
	t.Run("wrong service", func(t *testing.T) {
		assert.False(t, base.Matches("other", "i1", 0, now))
	})
	t.Run("instance filter", func(t *testing.T) {
		task := base
		task.ServiceInstance = "i1"
		assert.True(t, task.Matches("order-service", "i1", 0, now))
		assert.False(t, task.Matches("order-service", "i2", 0, now))
	})
	t.Run("already delivered", func(t *testing.T) {
		assert.False(t, base.Matches("order-service", "i1", now.UnixMilli(), now))
	})
	t.Run("future start still dispatched", func(t *testing.T) {
		task := base
		task.StartTime = now.Add(time.Minute)
		assert.True(t, task.Matches("order-service", "i1", 0, now))
	})
	t.Run("past duration still dispatched", func(t *testing.T) {
		task := base
		task.StartTime = now.Add(-10 * time.Minute)
		task.DurationSeconds = 60
		assert.True(t, task.Matches("order-service", "i1", 0, now))
	})
	t.Run("disabled", func(t *testing.T) {
		task := base
		task.Enabled = 0
		assert.False(t, task.Matches("order-service", "i1", 0, now))
	})
	t.Run("finished", func(t *testing.T) {
		task := base
		task.Status = TaskStatusFinished
		assert.False(t, task.Matches("order-service", "i1", 0, now))
		assert.Equal(t, "already_finished", task.skipReason("order-service", "i1", 0))
	})
}

func TestMatchingSkipsFinishedInstanceAndDedupes(t *testing.T) {
	now := time.Now()
	cache := NewTaskCache()
	cache.Replace(dedupeLatestTasks([]Task{
		{
			TaskID:     "t1",
			Service:    "order-service",
			Enabled:    1,
			CreateTime: now.Add(-time.Minute),
			Status:     TaskStatusRunning,
			UpdatedAt:  now.Add(-time.Minute),
		},
		{
			TaskID:          "t1",
			Service:         "order-service",
			ServiceInstance: "i1",
			Enabled:         1,
			CreateTime:      now.Add(-time.Minute),
			Status:          TaskStatusFinished,
			UpdatedAt:       now,
		},
		{
			TaskID:     "t1",
			Service:    "order-service",
			Enabled:    1,
			CreateTime: now.Add(-time.Minute),
			Status:     TaskStatusRunning,
			UpdatedAt:  now.Add(-2 * time.Minute),
		},
	}))

	assert.Empty(t, cache.Matching("order-service", "i1", 0, now))
	assert.Equal(t, "already_finished", cache.SkipReason(Task{
		TaskID:     "t1",
		Service:    "order-service",
		Enabled:    1,
		CreateTime: now.Add(-time.Minute),
	}, "order-service", "i1", 0))

	got := cache.Matching("order-service", "i2", 0, now)
	require.Len(t, got, 1)
	assert.Equal(t, "t1", got[0].TaskID)
	assert.Equal(t, "", got[0].ServiceInstance)
}

func TestTaskToCommandDurationMinutes(t *testing.T) {
	cmd := taskToCommand(Task{
		TaskID:          "t1",
		SerialNumber:    "sn",
		EndpointName:    "/x",
		DurationSeconds: 300,
		StartTime:       time.UnixMilli(1000),
		CreateTime:      time.UnixMilli(2000),
	})
	require.Equal(t, profileTaskQueryCommand, cmd.Command)
	got := map[string]string{}
	for _, a := range cmd.Args {
		got[a.Key] = a.Value
	}
	assert.Equal(t, "5", got["Duration"])
	assert.Equal(t, "t1", got["TaskId"])
	assert.Equal(t, "1000", got["StartTime"])
}

func TestTaskToCommandClampsAgentLimits(t *testing.T) {
	cmd := taskToCommand(Task{
		TaskID:           "t1",
		DumpPeriodMs:     1,
		MaxSamplingCount: 20,
		DurationSeconds:  0,
	})
	got := map[string]string{}
	for _, a := range cmd.Args {
		got[a.Key] = a.Value
	}
	assert.Equal(t, "1", got["Duration"])
	assert.Equal(t, "10", got["DumpPeriod"])
	assert.Equal(t, "9", got["MaxSamplingCount"])
}

func TestSegmentCacheTTL(t *testing.T) {
	c := NewSegmentCache(2, 50*time.Millisecond)
	c.Put("s1", SegmentIdent{SWTraceID: "a"})
	id, ok := c.Get("s1")
	require.True(t, ok)
	assert.Equal(t, "a", id.SWTraceID)
	time.Sleep(60 * time.Millisecond)
	_, ok = c.Get("s1")
	assert.False(t, ok)
}
