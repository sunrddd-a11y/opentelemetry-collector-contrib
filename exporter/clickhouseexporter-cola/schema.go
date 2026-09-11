// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func ensureIndependentColaSchema(ctx context.Context, conn driver.Conn, cfg *Config) error {
	stmts, err := independentColaStatements(cfg)
	if err != nil {
		return err
	}
	return execSchemaStatements(ctx, conn, "independent", stmts)
}

func ensureDependentColaSchema(ctx context.Context, conn driver.Conn, cfg *Config) error {
	existing, err := listLocalTables(ctx, conn, cfg.localDatabase())
	if err != nil {
		return err
	}
	stmts, err := dependentColaStatements(cfg, existing)
	if err != nil {
		return err
	}
	return execSchemaStatements(ctx, conn, "dependent", stmts)
}

func execSchemaStatements(ctx context.Context, conn driver.Conn, phase string, stmts []string) error {
	for _, stmt := range stmts {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create clickhouse_cola schema (%s): %w; sql=%s", phase, err, truncateSQL(stmt))
		}
	}
	return nil
}

func truncateSQL(stmt string) string {
	const max = 180
	if len(stmt) <= max {
		return stmt
	}
	return stmt[:max] + "..."
}

func listLocalTables(ctx context.Context, conn driver.Conn, database string) (map[string]struct{}, error) {
	rows, err := conn.Query(ctx, "SELECT name FROM system.tables WHERE database = ?", database)
	if err != nil {
		return nil, fmt.Errorf("list clickhouse tables in %s: %w", database, err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan clickhouse table name: %w", err)
		}
		out[name] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clickhouse tables: %w", err)
	}
	return out, nil
}

func colaSchemaStatements(cfg *Config) ([]string, error) {
	ind, err := independentColaStatements(cfg)
	if err != nil {
		return nil, err
	}
	dep, err := dependentColaStatements(cfg, nil)
	if err != nil {
		return nil, err
	}
	return append(ind, dep...), nil
}

func independentColaStatements(cfg *Config) ([]string, error) {
	onCluster, dbName, err := schemaIdents(cfg)
	if err != nil {
		return nil, err
	}

	db := quoteIdent(dbName)
	agents := qualified(dbName, cfg.AgentsTable)
	snapshots := qualified(dbName, cfg.SnapshotsTable)

	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s%s", db, onCluster),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s
(
	agent_type    LowCardinality(String) CODEC(ZSTD(1)),
	service_name  String CODEC(ZSTD(1)),
	instance_name String CODEC(ZSTD(1)),
	ip            String CODEC(ZSTD(1)),
	environment   String CODEC(ZSTD(1)),
	language      String CODEC(ZSTD(1)),
	version       String CODEC(ZSTD(1)),
	properties    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	last_time     DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	INDEX idx_last_time last_time TYPE minmax GRANULARITY 3
)
ENGINE = ReplacingMergeTree
ORDER BY (agent_type, service_name, instance_name)
TTL toDateTime(last_time) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`, agents, onCluster),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s
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
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1`, snapshots, onCluster),
	}, nil
}

func dependentColaStatements(cfg *Config, existing map[string]struct{}) ([]string, error) {
	onCluster, dbName, err := schemaIdents(cfg)
	if err != nil {
		return nil, err
	}
	emitAll := existing == nil
	has := func(name string) bool {
		if emitAll {
			return true
		}
		_, ok := existing[name]
		return ok
	}

	agents := qualified(dbName, cfg.AgentsTable)
	tracesName := cfg.TracesTableName
	traces := qualified(dbName, tracesName)
	mv := qualified(dbName, "otel_agent_mv")
	spansTable := defaultSpansTable("")

	var stmts []string
	if has(tracesName) {
		stmts = append(stmts, fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %s%s TO %s AS
SELECT
	'otel' AS agent_type,
	ServiceName AS service_name,
	ResourceAttributes['service.instance.id'] AS instance_name,
	if(position(ResourceAttributes['service.instance.id'], '@') > 0,
		substring(ResourceAttributes['service.instance.id'], position(ResourceAttributes['service.instance.id'], '@') + 1),
		coalesce(
			nullIf(ResourceAttributes['host.ip'], ''),
			nullIf(ResourceAttributes['net.host.ip'], ''),
			nullIf(ResourceAttributes['net.sock.host.addr'], ''),
			''
		)
	) AS ip,
	coalesce(
		nullIf(ResourceAttributes['os.type'], ''),
		nullIf(ResourceAttributes['k8s.pod.name'], ''),
		nullIf(ResourceAttributes['net.host.name'], ''),
		nullIf(ResourceAttributes['container.name'], ''),
		nullIf(ResourceAttributes['host.name'], ''),
		''
	) AS environment,
	ifNull(ResourceAttributes['telemetry.sdk.language'], '') AS language,
	coalesce(
		nullIf(ResourceAttributes['telemetry.distro.version'], ''),
		nullIf(ResourceAttributes['telemetry.sdk.version'], ''),
		''
	) AS version,
	Timestamp AS last_time,
	ResourceAttributes AS properties
FROM %s
WHERE ParentSpanId = ''
  AND ServiceName != ''
  AND ResourceAttributes['telemetry.sdk.name'] != 'skywalking'
  AND NOT mapContains(ResourceAttributes, 'sw8.trace_id')`, mv, onCluster, agents, traces))
		stmts = append(stmts, spanSchemaStatements(onCluster, dbName, tracesName)...)
	}

	distHas := has
	if has(tracesName) {
		orig := has
		distHas = func(name string) bool {
			if name == spansTable {
				return true
			}
			return orig(name)
		}
	}
	dist, err := distributedStatements(cfg, distHas)
	if err != nil {
		return nil, err
	}
	return append(stmts, dist...), nil
}

func schemaIdents(cfg *Config) (onCluster, dbName string, err error) {
	cfg.AgentsTable = defaultAgentsTable(cfg.AgentsTable)
	cfg.SnapshotsTable = defaultSnapshotsTable(cfg.SnapshotsTable)
	if cfg.LogsTableName == "" {
		cfg.LogsTableName = "otel_logs"
	}
	if cfg.TracesTableName == "" {
		cfg.TracesTableName = "otel_traces"
	}

	dbName = cfg.localDatabase()
	if err = validateIdent(dbName, "database"); err != nil {
		return "", "", err
	}
	if err = validateIdent(cfg.AgentsTable, "agents_table"); err != nil {
		return "", "", err
	}
	if err = validateIdent(cfg.SnapshotsTable, "snapshots_table"); err != nil {
		return "", "", err
	}
	if err = validateIdent(cfg.TracesTableName, "traces_table_name"); err != nil {
		return "", "", err
	}
	if cfg.ClusterName != "" {
		if err = validateIdent(cfg.ClusterName, "cluster_name"); err != nil {
			return "", "", err
		}
		onCluster = " ON CLUSTER " + quoteIdent(cfg.ClusterName)
	}
	return onCluster, dbName, nil
}

func distributedLocalTables(cfg *Config) []string {
	logs := cfg.LogsTableName
	if logs == "" {
		logs = "otel_logs"
	}
	traces := cfg.TracesTableName
	if traces == "" {
		traces = "otel_traces"
	}
	gauge := cfg.MetricsTables.Gauge.Name
	if gauge == "" {
		gauge = "otel_metrics_gauge"
	}
	sum := cfg.MetricsTables.Sum.Name
	if sum == "" {
		sum = "otel_metrics_sum"
	}
	summary := cfg.MetricsTables.Summary.Name
	if summary == "" {
		summary = "otel_metrics_summary"
	}
	histogram := cfg.MetricsTables.Histogram.Name
	if histogram == "" {
		histogram = "otel_metrics_histogram"
	}
	exp := cfg.MetricsTables.ExponentialHistogram.Name
	if exp == "" {
		exp = "otel_metrics_exponential_histogram"
	}
	return []string{
		logs,
		exp,
		gauge,
		histogram,
		sum,
		summary,
		traces,
		traces + "_trace_id_ts",
		defaultAgentsTable(cfg.AgentsTable),
		defaultSnapshotsTable(cfg.SnapshotsTable),
		defaultSpansTable(""),
	}
}

func distributedStatements(cfg *Config, has func(string) bool) ([]string, error) {
	if cfg.ClusterName == "" {
		return nil, nil
	}
	if has == nil {
		has = func(string) bool { return true }
	}
	if err := validateIdent(cfg.ClusterName, "cluster_name"); err != nil {
		return nil, err
	}
	if err := validateIdent(cfg.DistributedDatabase, "distributed_database"); err != nil {
		return nil, err
	}

	cluster := quoteIdent(cfg.ClusterName)
	localDBName := cfg.localDatabase()
	if err := validateIdent(localDBName, "database"); err != nil {
		return nil, err
	}
	localDB := quoteIdent(localDBName)
	distDB := quoteIdent(cfg.DistributedDatabase)

	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s", distDB, cluster),
	}
	for _, table := range distributedLocalTables(cfg) {
		if !has(table) {
			continue
		}
		if err := validateIdent(table, "distributed_local_table"); err != nil {
			return nil, err
		}
		distTable := localDBName + "_" + table
		if err := validateIdent(distTable, "distributed_table"); err != nil {
			return nil, err
		}
		stmts = append(stmts, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s AS %s ENGINE = Distributed(%s, %s, %s)",
			qualified(cfg.DistributedDatabase, distTable),
			cluster,
			qualified(localDBName, table),
			cluster,
			localDB,
			quoteIdent(table),
		))
	}
	return stmts, nil
}
