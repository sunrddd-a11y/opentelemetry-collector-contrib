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

// Store is the ClickHouse-backed (or test) persistence for profile data.
type Store interface {
	LoadTasks(ctx context.Context) ([]Task, error)
	InsertTask(ctx context.Context, task Task) error
	InsertSnapshots(ctx context.Context, rows []SnapshotRow) error
	Close() error
}

// MemoryStore is used by unit tests.
type MemoryStore struct {
	mu        sync.Mutex
	Tasks     []Task
	Snapshots []SnapshotRow
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
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = time.Now()
	}
	if task.Status == "" {
		task.Status = TaskStatusRunning
	}
	for i, cur := range m.Tasks {
		if cur.Service == task.Service && cur.TaskID == task.TaskID && cur.ServiceInstance == task.ServiceInstance {
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
