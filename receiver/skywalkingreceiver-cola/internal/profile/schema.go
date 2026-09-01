// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func ensureSchema(ctx context.Context, conn driver.Conn, cfg CHConfig) error {
	stmts, err := schemaStatements(cfg)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if err = conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create profile schema: %w", err)
		}
	}
	return nil
}

func schemaStatements(cfg CHConfig) ([]string, error) {
	for _, pair := range []struct {
		name  string
		value string
	}{
		{"database", cfg.Database},
		{"tasks_table", cfg.TasksTable},
		{"snapshots_table", cfg.SnapshotsTable},
	} {
		if err := validateIdent(pair.value, pair.name); err != nil {
			return nil, err
		}
	}

	db := quoteIdent(cfg.Database)
	tasks := qualified(cfg.Database, cfg.TasksTable)
	snapshots := qualified(cfg.Database, cfg.SnapshotsTable)

	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
(
	task_id                   String,
	service                   String,
	service_instance          String DEFAULT '',
	endpoint_name             String,
	duration_seconds          UInt32,
	min_duration_threshold_ms UInt32,
	dump_period_ms            UInt32,
	max_sampling_count        UInt32,
	start_time                DateTime64(3),
	create_time               DateTime64(3),
	serial_number             String,
	enabled                   UInt8 DEFAULT 1,
	status                    LowCardinality(String) DEFAULT 'running',
	extra                     String DEFAULT '',
	updated_at                DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (service, task_id, service_instance)
SETTINGS index_granularity = 8192`, tasks),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
(
	timestamp        DateTime64(3) CODEC(Delta, ZSTD(1)),
	task_id          LowCardinality(String) CODEC(ZSTD(1)),
	service          LowCardinality(String) CODEC(ZSTD(1)),
	service_instance String CODEC(ZSTD(1)),
	endpoint_name    LowCardinality(String) CODEC(ZSTD(1)),
	otel_trace_id    String CODEC(ZSTD(1)),
	sw_trace_id      String CODEC(ZSTD(1)),
	segment_id       String CODEC(ZSTD(1)),
	sequence         UInt32 CODEC(Delta, ZSTD(1)),
	stack_leaf_first Array(String) CODEC(ZSTD(1)),
	stack_root_first Array(String) MATERIALIZED arrayReverse(stack_leaf_first) CODEC(ZSTD(1)),
	stack_hash       UInt64 MATERIALIZED xxHash64(arrayStringConcat(stack_root_first, '\n')) CODEC(ZSTD(1)),
	stack_depth      UInt16 MATERIALIZED length(stack_root_first),
	INDEX idx_otel_trace otel_trace_id TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_sw_trace   sw_trace_id   TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_task       task_id       TYPE bloom_filter(0.01)  GRANULARITY 1,
	INDEX idx_ts         timestamp     TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (segment_id, sequence)
TTL toDate(timestamp) + INTERVAL 14 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`, snapshots),
	}, nil
}
