// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Batcher accumulates snapshots and flushes them in ClickHouse-friendly batches.
type Batcher struct {
	store    Store
	logger   *zap.Logger
	maxSize  int
	interval time.Duration

	mu     sync.Mutex
	buf    []SnapshotRow
	cancel context.CancelFunc
	done   chan struct{}
}

func NewBatcher(store Store, logger *zap.Logger, maxSize int, interval time.Duration) *Batcher {
	if maxSize <= 0 {
		maxSize = 5000
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Batcher{
		store:    store,
		logger:   logger,
		maxSize:  maxSize,
		interval: interval,
		done:     make(chan struct{}),
	}
}

func (b *Batcher) Start() {
	if b == nil || b.store == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.loop(ctx)
}

func (b *Batcher) loop(ctx context.Context) {
	defer close(b.done)
	t := time.NewTicker(b.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.flush(ctx)
		}
	}
}

func (b *Batcher) Add(row SnapshotRow) {
	if b == nil || b.store == nil {
		return
	}
	var overflow []SnapshotRow
	b.mu.Lock()
	b.buf = append(b.buf, row)
	if len(b.buf) >= b.maxSize {
		overflow = b.buf
		b.buf = nil
	}
	b.mu.Unlock()
	if len(overflow) > 0 {
		b.send(context.Background(), overflow)
	}
}

func (b *Batcher) flush(ctx context.Context) {
	b.mu.Lock()
	rows := b.buf
	b.buf = nil
	b.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	b.send(ctx, rows)
}

func (b *Batcher) send(ctx context.Context, rows []SnapshotRow) {
	if err := b.store.InsertSnapshots(ctx, rows); err != nil && b.logger != nil {
		b.logger.Error("insert profile snapshots failed", zap.Error(err), zap.Int("rows", len(rows)))
	}
}

func (b *Batcher) Shutdown(ctx context.Context) {
	if b == nil {
		return
	}
	if b.cancel != nil {
		b.cancel()
		select {
		case <-b.done:
		case <-ctx.Done():
		}
	}
	b.flush(ctx)
}
