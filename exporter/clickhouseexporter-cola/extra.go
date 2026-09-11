// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

type colaExtra struct {
	cfg    *Config
	logger *zap.Logger
	conn   driver.Conn

	license *licenseGate

	mu       sync.Mutex
	refs     int
	started  bool
	startErr error
}

var _ component.Component = (*colaExtra)(nil)

func newColaExtra(cfg *Config, logger *zap.Logger) *colaExtra {
	return &colaExtra{cfg: cfg, logger: logger}
}

func (e *colaExtra) Start(ctx context.Context, _ component.Host) error {
	return e.holdStart(ctx)
}

func (e *colaExtra) Shutdown(_ context.Context) error {
	return e.release()
}

func (e *colaExtra) holdStart(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refs++
	if e.started {
		return e.startErr
	}
	e.started = true
	e.startErr = e.open(ctx)
	return e.startErr
}

func (e *colaExtra) release() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.refs > 0 {
		e.refs--
	}
	if e.refs > 0 || e.conn == nil {
		return nil
	}
	if e.license != nil {
		e.license.shutdown()
		e.license = nil
	}
	err := e.conn.Close()
	e.conn = nil
	e.started = false
	e.startErr = nil
	return err
}

func (e *colaExtra) open(ctx context.Context) error {
	opts, err := clickhouseOptions(&e.cfg.Config)
	if err != nil {
		return err
	}
	targetDB := e.cfg.localDatabase()
	if e.cfg.CreateSchema {
		if schemaErr := e.withSchemaConn(ctx, func(conn driver.Conn) error {
			return ensureIndependentColaSchema(ctx, conn, e.cfg)
		}); schemaErr != nil {
			return schemaErr
		}
	}

	opts.Auth.Database = targetDB
	e.conn, err = clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("open clickhouse_cola: %w", err)
	}
	if err = e.conn.Ping(ctx); err != nil {
		_ = e.conn.Close()
		e.conn = nil
		return fmt.Errorf("ping clickhouse_cola: %w", err)
	}
	e.license = newLicenseGate(e.cfg.License, e.conn, e.logger)
	e.license.start()
	return nil
}

func clickhouseOptions(cfg *clickhouseexporter.Config) (*clickhouse.Options, error) {
	dsnURL, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse endpoint: %w", err)
	}
	queryParams := dsnURL.Query()
	for k, v := range cfg.ConnectionParams {
		queryParams.Set(k, v)
	}
	if dsnURL.Scheme == "https" {
		queryParams.Set("secure", "true")
	}
	if !queryParams.Has("async_insert") {
		queryParams.Set("async_insert", fmt.Sprintf("%t", cfg.AsyncInsert))
	}
	if !queryParams.Has("compress") && (cfg.Compress == "" || cfg.Compress == "true") {
		queryParams.Set("compress", "lz4")
	} else if !queryParams.Has("compress") {
		queryParams.Set("compress", cfg.Compress)
	}
	if cfg.Username != "" {
		dsnURL.User = url.UserPassword(cfg.Username, string(cfg.Password))
	}
	dsnURL.RawQuery = queryParams.Encode()

	opts, err := clickhouse.ParseDSN(dsnURL.String())
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	if cfg.Database != "" {
		opts.Auth.Database = cfg.Database
	}
	return opts, nil
}

func schemaClickhouseOptions(cfg *clickhouseexporter.Config) (*clickhouse.Options, error) {
	clone := *cfg
	params := make(map[string]string, len(cfg.ConnectionParams)+2)
	for k, v := range cfg.ConnectionParams {
		params[k] = v
	}
	params["compress"] = "none"
	params["wait_end_of_query"] = "1"
	clone.ConnectionParams = params
	clone.Compress = "none"
	return clickhouseOptions(&clone)
}

func (e *colaExtra) withSchemaConn(ctx context.Context, fn func(driver.Conn) error) error {
	opts, err := schemaClickhouseOptions(&e.cfg.Config)
	if err != nil {
		return err
	}
	opts.Auth.Database = "default"
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("open clickhouse for cola schema: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err = conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse for cola schema: %w", err)
	}
	return fn(conn)
}

func (e *colaExtra) ensureDependentSchema(ctx context.Context) error {
	if e == nil || e.cfg == nil || !e.cfg.CreateSchema {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.withSchemaConn(ctx, func(conn driver.Conn) error {
		return ensureDependentColaSchema(ctx, conn, e.cfg)
	})
}

func prepareClusterHTTPSchema(c *Config) {
	if c == nil || c.ClusterName == "" {
		return
	}
	ep := strings.ToLower(c.Endpoint)
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		return
	}
	switch c.Compress {
	case "", "true", "lz4":
		c.Compress = "none"
	}
}
