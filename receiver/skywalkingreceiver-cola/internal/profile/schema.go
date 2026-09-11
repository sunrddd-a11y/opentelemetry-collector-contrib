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
	} {
		if err := validateIdent(pair.value, pair.name); err != nil {
			return nil, err
		}
	}

	db := quoteIdent(cfg.Database)
	tasks := qualified(cfg.Database, cfg.TasksTable)

	stmts := []string{
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
	` + "`delete`" + `                  UInt8 DEFAULT 0,
	tts                       DateTime DEFAULT now() CODEC(DoubleDelta, LZ4)
)
ENGINE = ReplacingMergeTree
ORDER BY task_id
SETTINGS index_granularity = 8192`, tasks),
	}

	dist, err := distributedStatements(cfg)
	if err != nil {
		return nil, err
	}
	return append(stmts, dist...), nil
}

func distributedStatements(cfg CHConfig) ([]string, error) {
	if cfg.ClusterName == "" {
		return nil, nil
	}
	if err := validateIdent(cfg.ClusterName, "cluster_name"); err != nil {
		return nil, err
	}
	if err := validateIdent(cfg.DistributedDatabase, "distributed_database"); err != nil {
		return nil, err
	}

	cluster := quoteIdent(cfg.ClusterName)
	localDB := quoteIdent(cfg.Database)
	distDB := quoteIdent(cfg.DistributedDatabase)

	tables := []string{cfg.TasksTable}

	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s", distDB, cluster),
	}
	for _, table := range tables {
		if err := validateIdent(table, "distributed_local_table"); err != nil {
			return nil, err
		}
		distTable := cfg.Database + "_" + table
		if err := validateIdent(distTable, "distributed_table"); err != nil {
			return nil, err
		}
		stmts = append(stmts, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s AS %s ENGINE = Distributed(%s, %s, %s)",
			qualified(cfg.DistributedDatabase, distTable),
			cluster,
			qualified(cfg.Database, table),
			cluster,
			localDB,
			quoteIdent(table),
		))
	}
	return stmts, nil
}
