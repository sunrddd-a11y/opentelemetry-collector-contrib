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
	UpdatedAt              time.Time
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
func (t Task) skipReason(service, serviceInstance string, lastCommandTime int64) string {
	if t.isFinished() {
		return "already_finished"
	}
	if t.Enabled == 0 {
		return "disabled"
	}
	if t.Service != service {
		return "service_mismatch"
	}
	if t.ServiceInstance != "" && t.ServiceInstance != serviceInstance {
		return "instance_mismatch"
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
		if cur.Service == t.Service && cur.TaskID == t.TaskID && cur.ServiceInstance == t.ServiceInstance {
			c.tasks[i] = t
			return
		}
	}
	c.tasks = append(c.tasks, t)
}

func (c *TaskCache) ByID(id string) (Task, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var fallback Task
	var found bool
	for _, t := range c.tasks {
		if t.TaskID != id {
			continue
		}
		if !t.isFinished() && t.Enabled == 1 {
			return t, true
		}
		if !found {
			fallback = t
			found = true
		}
	}
	return fallback, found
}

func (c *TaskCache) templateForFinish(taskID, service, instance string) (Task, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var broadcast Task
	var hasBroadcast bool
	var any Task
	var hasAny bool
	for _, t := range c.tasks {
		if t.TaskID != taskID {
			continue
		}
		if service != "" && t.Service != service {
			continue
		}
		if t.ServiceInstance == instance {
			return t, true
		}
		if t.ServiceInstance == "" {
			broadcast = t
			hasBroadcast = true
			continue
		}
		if !hasAny {
			any = t
			hasAny = true
		}
	}
	if hasBroadcast {
		return broadcast, true
	}
	return any, hasAny
}

func (c *TaskCache) instanceFinished(taskID, serviceInstance string) bool {
	for _, t := range c.tasks {
		if t.TaskID == taskID && t.ServiceInstance == serviceInstance && t.isFinished() {
			return true
		}
	}
	return false
}

func (c *TaskCache) SkipReason(t Task, service, serviceInstance string, lastCommandTime int64) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dispatchReason(t, service, serviceInstance, lastCommandTime)
}

func (c *TaskCache) dispatchReason(t Task, service, serviceInstance string, lastCommandTime int64) string {
	if reason := t.skipReason(service, serviceInstance, lastCommandTime); reason != "" {
		return reason
	}
	if c.instanceFinished(t.TaskID, serviceInstance) {
		return "already_finished"
	}
	return ""
}

func (c *TaskCache) Matching(service, serviceInstance string, lastCommandTime int64, now time.Time) []Task {
	c.mu.RLock()
	defer c.mu.RUnlock()
	best := make(map[string]Task, len(c.tasks))
	for _, t := range c.tasks {
		if c.dispatchReason(t, service, serviceInstance, lastCommandTime) != "" {
			continue
		}
		if prev, ok := best[t.TaskID]; ok {
			// Prefer the instance-specific row over a broadcast row.
			if prev.ServiceInstance == "" && t.ServiceInstance != "" {
				best[t.TaskID] = t
			}
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

// dedupeLatestTasks keeps one row per (service, task_id, service_instance).
// FINAL can still leave duplicates before parts merge; latest updated_at wins.
func dedupeLatestTasks(tasks []Task) []Task {
	type key struct {
		service, taskID, instance string
	}
	best := make(map[key]Task, len(tasks))
	for _, t := range tasks {
		if t.Status == "" {
			t.Status = TaskStatusRunning
		}
		k := key{t.Service, t.TaskID, t.ServiceInstance}
		if prev, ok := best[k]; ok && !t.UpdatedAt.After(prev.UpdatedAt) {
			continue
		}
		best[k] = t
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
