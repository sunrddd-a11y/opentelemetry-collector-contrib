// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

// Runtime owns task refresh, snapshot batching and the gRPC service.
type Runtime struct {
	store    Store
	tasks    *TaskCache
	segs     *SegmentCache
	batcher  *Batcher
	svc      *Service
	logger   *zap.Logger
	interval time.Duration

	cancel context.CancelFunc
	done   chan struct{}
}

func NewRuntime(store Store, segs *SegmentCache, logger *zap.Logger, refresh time.Duration, batchSize int, flush time.Duration) *Runtime {
	tasks := NewTaskCache()
	if segs == nil {
		segs = NewSegmentCache(100_000, 10*time.Minute)
	}
	if refresh <= 0 {
		refresh = 15 * time.Second
	}
	var batcher *Batcher
	if store != nil {
		batcher = NewBatcher(store, logger, batchSize, flush)
	}
	return &Runtime{
		store:    store,
		tasks:    tasks,
		segs:     segs,
		batcher:  batcher,
		svc:      NewService(tasks, segs, batcher, store, logger),
		logger:   logger,
		interval: refresh,
		done:     make(chan struct{}),
	}
}

func (r *Runtime) SetLogsConsumer(lc consumer.Logs) {
	if r.batcher != nil {
		r.batcher.SetLogsConsumer(lc)
	}
}

func (r *Runtime) Service() *Service { return r.svc }

func (r *Runtime) SegmentCache() *SegmentCache { return r.segs }

func (r *Runtime) Start(ctx context.Context) error {
	if r.store != nil {
		if err := r.refresh(ctx); err != nil {
			return fmt.Errorf("initial profile task load: %w", err)
		}
		r.batcher.Start()
		runCtx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		go r.loop(runCtx)
	}
	return nil
}

func (r *Runtime) loop(ctx context.Context) {
	defer close(r.done)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.refresh(ctx); err != nil && r.logger != nil {
				r.logger.Error("refresh profile tasks failed, keeping last cache", zap.Error(err))
			}
		}
	}
}

func (r *Runtime) refresh(ctx context.Context) error {
	tasks, err := r.store.LoadTasks(ctx)
	if err != nil {
		return err
	}
	r.tasks.Replace(dedupeLatestTasks(tasks))
	if r.logger != nil {
		r.logger.Info("loaded profile tasks", zap.Int("count", len(tasks)))
	}
	return nil
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
		select {
		case <-r.done:
		case <-ctx.Done():
		}
	}
	if r.batcher != nil {
		r.batcher.Shutdown(ctx)
	}
	if r.store != nil {
		return r.store.Close()
	}
	return nil
}
