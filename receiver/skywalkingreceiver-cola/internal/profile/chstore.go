// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// CHConfig is the subset of receiver ClickHouse settings the store needs.
type CHConfig struct {
	DSN                 string
	Database            string
	ClusterName         string
	DistributedDatabase string
	TasksTable          string
	SnapshotsTable      string
	AgentsTable         string
	CreateSchema        bool
}

type chStore struct {
	conn      driver.Conn
	database  string
	cluster   string
	distDB    string
	tasks     string
	snapshots string
	agents    string
}

func validateIdent(name, field string) error {
	if !identRE.MatchString(name) {
		return fmt.Errorf("invalid clickhouse %s %q", field, name)
	}
	return nil
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

func qualified(database, table string) string {
	return quoteIdent(database) + "." + quoteIdent(table)
}

func defaultAgentsTable(name string) string {
	if name == "" {
		return "otel_agents"
	}
	return name
}

// readQualified is the FINAL read target: Distributed when cluster is set, else local.
func readQualified(database, cluster, distDB, table string) string {
	if cluster != "" {
		return qualified(distDB, database+"_"+table)
	}
	return qualified(database, table)
}

// NewCHStore opens a ClickHouse connection from DSN (tcp:// or http://).
func NewCHStore(cfg CHConfig) (Store, error) {
	if err := validateIdent(cfg.Database, "database"); err != nil {
		return nil, err
	}
	if err := validateIdent(cfg.TasksTable, "tasks_table"); err != nil {
		return nil, err
	}
	if err := validateIdent(cfg.SnapshotsTable, "snapshots_table"); err != nil {
		return nil, err
	}
	cfg.AgentsTable = defaultAgentsTable(cfg.AgentsTable)
	if err := validateIdent(cfg.AgentsTable, "agents_table"); err != nil {
		return nil, err
	}
	if cfg.ClusterName != "" {
		if err := validateIdent(cfg.ClusterName, "cluster_name"); err != nil {
			return nil, err
		}
		if err := validateIdent(cfg.DistributedDatabase, "distributed_database"); err != nil {
			return nil, err
		}
	}

	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	targetDB := cfg.Database
	if targetDB == "" {
		targetDB = opts.Auth.Database
	}
	if cfg.CreateSchema {
		opts.Auth.Database = "default"
	} else if targetDB != "" {
		opts.Auth.Database = targetDB
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	ctx := context.Background()
	if err = conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if cfg.CreateSchema {
		if err = ensureSchema(ctx, conn, cfg); err != nil {
			_ = conn.Close()
			return nil, err
		}
		if targetDB != "" && targetDB != "default" {
			_ = conn.Close()
			opts.Auth.Database = targetDB
			conn, err = clickhouse.Open(opts)
			if err != nil {
				return nil, fmt.Errorf("open clickhouse database: %w", err)
			}
			if err = conn.Ping(ctx); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("ping clickhouse database: %w", err)
			}
		}
	}

	return &chStore{
		conn:      conn,
		database:  cfg.Database,
		cluster:   cfg.ClusterName,
		distDB:    cfg.DistributedDatabase,
		tasks:     cfg.TasksTable,
		snapshots: cfg.SnapshotsTable,
		agents:    cfg.AgentsTable,
	}, nil
}

func (s *chStore) LoadTasks(ctx context.Context) ([]Task, error) {
	q := fmt.Sprintf(`
SELECT
	task_id,
	service,
	service_instance,
	endpoint_name,
	duration_seconds,
	min_duration_threshold_ms,
	dump_period_ms,
	max_sampling_count,
	start_time,
	create_time,
	serial_number,
	enabled,
	status,
	extra,
	`+"`delete`"+`,
	tts
FROM %s FINAL
WHERE `+"`delete`"+` = 0`, readQualified(s.database, s.cluster, s.distDB, s.tasks))

	rows, err := s.conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query profile tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err = rows.Scan(
			&t.TaskID,
			&t.Service,
			&t.ServiceInstance,
			&t.EndpointName,
			&t.DurationSeconds,
			&t.MinDurationThresholdMs,
			&t.DumpPeriodMs,
			&t.MaxSamplingCount,
			&t.StartTime,
			&t.CreateTime,
			&t.SerialNumber,
			&t.Enabled,
			&t.Status,
			&t.Extra,
			&t.Deleted,
			&t.Tts,
		); err != nil {
			return nil, fmt.Errorf("scan profile task: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return dedupeLatestTasks(tasks), nil
}

func (s *chStore) InsertTask(ctx context.Context, task Task) error {
	if task.Tts.IsZero() {
		task.Tts = time.Now()
	}
	if task.Status == "" {
		task.Status = TaskStatusRunning
	}
	q := fmt.Sprintf(`INSERT INTO %s (
		task_id, service, service_instance, endpoint_name,
		duration_seconds, min_duration_threshold_ms, dump_period_ms, max_sampling_count,
		start_time, create_time, serial_number, enabled, status, extra, `+"`delete`"+`, tts
	)`, qualified(s.database, s.tasks))
	batch, err := s.conn.PrepareBatch(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare task insert: %w", err)
	}
	if err = batch.Append(
		task.TaskID,
		task.Service,
		task.ServiceInstance,
		task.EndpointName,
		task.DurationSeconds,
		task.MinDurationThresholdMs,
		task.DumpPeriodMs,
		task.MaxSamplingCount,
		task.StartTime,
		task.CreateTime,
		task.SerialNumber,
		task.Enabled,
		task.Status,
		task.Extra,
		task.Deleted,
		task.Tts,
	); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("append task: %w", err)
	}
	if err = batch.Send(); err != nil {
		return fmt.Errorf("send task: %w", err)
	}
	return nil
}

func (s *chStore) InsertSnapshots(ctx context.Context, rows []SnapshotRow) error {
	if len(rows) == 0 {
		return nil
	}
	q := fmt.Sprintf(`INSERT INTO %s (
		timestamp, task_id, service, service_instance, endpoint_name,
		otel_trace_id, sw_trace_id, segment_id, sequence, stack_leaf_first
	)`, qualified(s.database, s.snapshots))

	batch, err := s.conn.PrepareBatch(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare snapshot batch: %w", err)
	}
	for _, row := range rows {
		stack := row.StackLeafFirst
		if stack == nil {
			stack = []string{}
		}
		if err = batch.Append(
			row.Timestamp,
			row.TaskID,
			row.Service,
			row.ServiceInstance,
			row.EndpointName,
			row.OTelTraceID,
			row.SWTraceID,
			row.SegmentID,
			row.Sequence,
			stack,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append snapshot: %w", err)
		}
	}
	if err = batch.Send(); err != nil {
		return fmt.Errorf("send snapshot batch: %w", err)
	}
	return nil
}

func (s *chStore) GetAgent(ctx context.Context, agentType, service, instance string) (*AgentRow, error) {
	q := fmt.Sprintf(`
SELECT
	agent_type,
	service_name,
	instance_name,
	ip,
	environment,
	language,
	version,
	properties,
	last_time
FROM %s FINAL
WHERE agent_type = ? AND service_name = ? AND instance_name = ?
LIMIT 1`, readQualified(s.database, s.cluster, s.distDB, s.agents))

	row := s.conn.QueryRow(ctx, q, agentType, service, instance)
	var out AgentRow
	if err := row.Scan(
		&out.AgentType,
		&out.ServiceName,
		&out.InstanceName,
		&out.IP,
		&out.Environment,
		&out.Language,
		&out.Version,
		&out.Properties,
		&out.LastTime,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query agent: %w", err)
	}
	if out.Properties == nil {
		out.Properties = map[string]string{}
	}
	return &out, nil
}

func (s *chStore) InsertAgent(ctx context.Context, row AgentRow) error {
	if row.LastTime.IsZero() {
		row.LastTime = time.Now()
	}
	if row.Properties == nil {
		row.Properties = map[string]string{}
	}
	q := fmt.Sprintf(`INSERT INTO %s (
		agent_type, service_name, instance_name, ip, environment, language, version, properties, last_time
	)`, qualified(s.database, s.agents))
	batch, err := s.conn.PrepareBatch(ctx, q)
	if err != nil {
		return fmt.Errorf("prepare agent insert: %w", err)
	}
	if err = batch.Append(
		row.AgentType,
		row.ServiceName,
		row.InstanceName,
		row.IP,
		row.Environment,
		row.Language,
		row.Version,
		row.Properties,
		row.LastTime,
	); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("append agent: %w", err)
	}
	if err = batch.Send(); err != nil {
		return fmt.Errorf("send agent: %w", err)
	}
	return nil
}

func (s *chStore) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}
