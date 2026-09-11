// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"sync"
	"time"
)

const (
	TaskStatusRunning  = "running"
	TaskStatusFinished = "finished"
)

// Task is one row from sw_profile_tasks after FINAL / latest-row collapse.
// task_id is unique. service_instance is reserved and ignored at dispatch.
type Task struct {
	TaskID                 string
	Service                string
	ServiceInstance        string
	EndpointName           string
	DurationSeconds        uint32
	MinDurationThresholdMs uint32
	DumpPeriodMs           uint32
	MaxSamplingCount       uint32
	StartTime              time.Time
	CreateTime             time.Time
	SerialNumber           string
	Enabled                uint8
	Status                 string
	Extra                  string
	Deleted                uint8
	Tts                    time.Time
}

func (t Task) isFinished() bool {
	return t.Status == TaskStatusFinished
}

func (t Task) statusOrRunning() string {
	if t.Status == "" {
		return TaskStatusRunning
	}
	return t.Status
}

// skipReason is empty when this task row itself is eligible.
// Matching follows SkyWalking OAP: service name + createTime > lastCommandTime.
// StartTime/Duration are sent to the agent; OAP does not filter them at dispatch.
// service_instance is ignored.
func (t Task) skipReason(service, _ string, lastCommandTime int64) string {
	if t.Deleted != 0 {
		return "deleted"
	}
	if t.isFinished() {
		return "already_finished"
	}
	if t.Enabled == 0 {
		return "disabled"
	}
	if t.Service != service {
		return "service_mismatch"
	}
	if !t.CreateTime.After(time.UnixMilli(lastCommandTime)) {
		return "already_delivered"
	}
	return ""
}

// Matches reports whether this task row should be sent to the querying agent.
func (t Task) Matches(service, serviceInstance string, lastCommandTime int64, _ time.Time) bool {
	return t.skipReason(service, serviceInstance, lastCommandTime) == ""
}

// TaskCache holds the last successfully loaded task list.
type TaskCache struct {
	mu    sync.RWMutex
	tasks []Task
}

func NewTaskCache() *TaskCache {
	return &TaskCache{}
}

func (c *TaskCache) Replace(tasks []Task) {
	copied := make([]Task, len(tasks))
	copy(copied, tasks)
	c.mu.Lock()
	c.tasks = copied
	c.mu.Unlock()
}

func (c *TaskCache) All() []Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Task, len(c.tasks))
	copy(out, c.tasks)
	return out
}

func (c *TaskCache) Upsert(t Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, cur := range c.tasks {
		if cur.TaskID == t.TaskID {
			c.tasks[i] = t
			return
		}
	}
	c.tasks = append(c.tasks, t)
}

func (c *TaskCache) ByID(id string) (Task, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.tasks {
		if t.TaskID == id {
			return t, true
		}
	}
	return Task{}, false
}

func (c *TaskCache) templateForFinish(taskID, service string) (Task, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.tasks {
		if t.TaskID != taskID {
			continue
		}
		if service != "" && t.Service != service {
			continue
		}
		return t, true
	}
	return Task{}, false
}

func (c *TaskCache) SkipReason(t Task, service, serviceInstance string, lastCommandTime int64) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return t.skipReason(service, serviceInstance, lastCommandTime)
}

func (c *TaskCache) Matching(service, serviceInstance string, lastCommandTime int64, _ time.Time) []Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	best := make(map[string]Task, len(c.tasks))
	for _, t := range c.tasks {
		if t.skipReason(service, serviceInstance, lastCommandTime) != "" {
			continue
		}
		best[t.TaskID] = t
	}
	if len(best) == 0 {
		return nil
	}
	out := make([]Task, 0, len(best))
	for _, t := range best {
		out = append(out, t)
	}
	return out
}

// dedupeLatestTasks keeps one row per task_id. FINAL can still leave
// duplicates before parts merge; latest tts wins. Soft-deleted rows are dropped.
func dedupeLatestTasks(tasks []Task) []Task {
	best := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		if t.Status == "" {
			t.Status = TaskStatusRunning
		}
		if prev, ok := best[t.TaskID]; ok && !t.Tts.After(prev.Tts) {
			continue
		}
		best[t.TaskID] = t
	}
	if len(best) == 0 {
		return nil
	}
	out := make([]Task, 0, len(best))
	for _, t := range best {
		if t.Deleted != 0 {
			continue
		}
		out = append(out, t)
	}
	return out
}
