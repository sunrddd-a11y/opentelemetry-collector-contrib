// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type snapshotRow struct {
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

func (e *colaExtra) insertSnapshots(ctx context.Context, rows []snapshotRow) error {
	if e.conn == nil || len(rows) == 0 {
		return nil
	}
	q := fmt.Sprintf(`INSERT INTO %s (
		timestamp, task_id, service, service_instance, endpoint_name,
		otel_trace_id, sw_trace_id, segment_id, sequence, stack_leaf_first
	)`, qualified(e.cfg.localDatabase(), defaultSnapshotsTable(e.cfg.SnapshotsTable)))

	batch, err := e.conn.PrepareBatch(ctx, q)
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

func (e *colaExtra) getAgent(ctx context.Context, agentType, service, instance string) (*agentRow, error) {
	if e.conn == nil {
		return nil, errors.New("clickhouse_cola is not started")
	}
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
LIMIT 1`, readQualified(e.cfg.localDatabase(), e.cfg.ClusterName, e.cfg.DistributedDatabase, defaultAgentsTable(e.cfg.AgentsTable)))

	var out agentRow
	err := e.conn.QueryRow(ctx, q, agentType, service, instance).Scan(
		&out.AgentType,
		&out.ServiceName,
		&out.InstanceName,
		&out.IP,
		&out.Environment,
		&out.Language,
		&out.Version,
		&out.Properties,
		&out.LastTime,
	)
	if err != nil {
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

func (e *colaExtra) insertAgent(ctx context.Context, row agentRow) error {
	if e.conn == nil {
		return errors.New("clickhouse_cola is not started")
	}
	if row.LastTime.IsZero() {
		row.LastTime = time.Now()
	}
	if row.Properties == nil {
		row.Properties = map[string]string{}
	}
	q := fmt.Sprintf(`INSERT INTO %s (
		agent_type, service_name, instance_name, ip, environment, language, version, properties, last_time
	)`, qualified(e.cfg.localDatabase(), defaultAgentsTable(e.cfg.AgentsTable)))
	batch, err := e.conn.PrepareBatch(ctx, q)
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

func (e *colaExtra) upsertAgentProperties(ctx context.Context, service, instance, layer string, props map[string]string, now time.Time) error {
	return e.insertAgent(ctx, newSkyWalkingAgent(service, instance, layer, props, now))
}

func (e *colaExtra) upsertAgentKeepAlive(ctx context.Context, service, instance, layer string, now time.Time) error {
	existing, err := e.getAgent(ctx, agentTypeSkyWalking, service, instance)
	if err != nil {
		return err
	}
	return e.insertAgent(ctx, keepAliveAgent(existing, service, instance, layer, now))
}
