// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"sync"
	"time"
)

// SnapshotRow is one ThreadSnapshot prepared for ClickHouse.
type SnapshotRow struct {
	Timestamp       time.Time
	TaskID          string
	Service         string
	ServiceInstance string
	EndpointName    string
	OTelTraceID     string
	SWTraceID       string
	SegmentID       string
	Sequence        uint32
	StackLeafFirst  []string
}

// Store is the ClickHouse-backed (or test) persistence for profile data and agents.
type Store interface {
	LoadTasks(ctx context.Context) ([]Task, error)
	InsertTask(ctx context.Context, task Task) error
	InsertSnapshots(ctx context.Context, rows []SnapshotRow) error
	GetAgent(ctx context.Context, agentType, service, instance string) (*AgentRow, error)
	InsertAgent(ctx context.Context, row AgentRow) error
	Close() error
}

// MemoryStore is used by unit tests.
type MemoryStore struct {
	mu        sync.Mutex
	Tasks     []Task
	Snapshots []SnapshotRow
	Agents    []AgentRow
	LoadErr   error
	InsertErr error
}

func (m *MemoryStore) LoadTasks(context.Context) ([]Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LoadErr != nil {
		return nil, m.LoadErr
	}
	out := make([]Task, len(m.Tasks))
	copy(out, m.Tasks)
	return dedupeLatestTasks(out), nil
}

func (m *MemoryStore) InsertTask(_ context.Context, task Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InsertErr != nil {
		return m.InsertErr
	}
	if task.Tts.IsZero() {
		task.Tts = time.Now()
	}
	if task.Status == "" {
		task.Status = TaskStatusRunning
	}
	for i, cur := range m.Tasks {
		if cur.TaskID == task.TaskID {
			m.Tasks[i] = task
			return nil
		}
	}
	m.Tasks = append(m.Tasks, task)
	return nil
}

func (m *MemoryStore) InsertSnapshots(_ context.Context, rows []SnapshotRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InsertErr != nil {
		return m.InsertErr
	}
	m.Snapshots = append(m.Snapshots, rows...)
	return nil
}

func (m *MemoryStore) Close() error { return nil }
func (m *MemoryStore) GetAgent(_ context.Context, agentType, service, instance string) (*AgentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LoadErr != nil {
		return nil, m.LoadErr
	}
	var best *AgentRow
	for i := range m.Agents {
		row := m.Agents[i]
		if row.AgentType != agentType || row.ServiceName != service || row.InstanceName != instance {
			continue
		}
		if best == nil || row.LastTime.After(best.LastTime) {
			cp := row
			cp.Properties = cloneProperties(row.Properties)
			best = &cp
		}
	}
	return best, nil
}

func (m *MemoryStore) InsertAgent(_ context.Context, row AgentRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.InsertErr != nil {
		return m.InsertErr
	}
	if row.LastTime.IsZero() {
		row.LastTime = time.Now()
	}
	row.Properties = cloneProperties(row.Properties)
	m.Agents = append(m.Agents, row)
	return nil
}

