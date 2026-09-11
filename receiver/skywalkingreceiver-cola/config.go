// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package skywalkingreceivercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola"

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap"
)

const (
	// The config field id to load the protocol map from
	protocolsFieldName = "protocols"
)

var clickhouseIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Protocols is the configuration for the supported protocols.
type Protocols struct {
	GRPC *configgrpc.ServerConfig `mapstructure:"grpc"`
	HTTP *confighttp.ServerConfig `mapstructure:"http"`
}

// ClickHouseConfig controls profile task loading and snapshot persistence.
type ClickHouseConfig struct {
	DSN                   string        `mapstructure:"dsn"`
	Database              string        `mapstructure:"database"`
	ClusterName           string        `mapstructure:"cluster_name"`
	DistributedDatabase   string        `mapstructure:"distributed_database"`
	TasksTable            string        `mapstructure:"tasks_table"`
	SnapshotsTable        string        `mapstructure:"snapshots_table"`
	AgentsTable           string        `mapstructure:"agents_table"`
	CreateSchema          bool          `mapstructure:"create_schema"`
	TaskRefreshInterval time.Duration `mapstructure:"task_refresh_interval"`
	InsertBatchSize     int           `mapstructure:"insert_batch_size"`
	InsertFlushInterval time.Duration `mapstructure:"insert_flush_interval"`
}

func defaultClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Database:            "otel",
		TasksTable:          "sw_profile_tasks",
		SnapshotsTable:      "sw_profile_snapshots",
		AgentsTable:         "otel_agents",
		CreateSchema:        true,
		TaskRefreshInterval: 15 * time.Second,
		InsertBatchSize:     5000,
		InsertFlushInterval: time.Second,
	}
}

func (c ClickHouseConfig) Enabled() bool {
	return c.DSN != ""
}

func (c ClickHouseConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}
	if _, err := clickhouse.ParseDSN(c.DSN); err != nil {
		return fmt.Errorf("clickhouse.dsn: %w", err)
	}
	for _, pair := range []struct {
		name  string
		value string
	}{
		{"database", c.Database},
		{"tasks_table", c.TasksTable},
		{"snapshots_table", c.SnapshotsTable},
		{"agents_table", c.AgentsTable},
	} {
		if !clickhouseIdentRE.MatchString(pair.value) {
			return fmt.Errorf("clickhouse.%s is not a valid identifier", pair.name)
		}
	}
	if c.ClusterName != "" || c.DistributedDatabase != "" {
		if c.ClusterName == "" || c.DistributedDatabase == "" {
			return errors.New("clickhouse.cluster_name and clickhouse.distributed_database must be set together")
		}
		for _, pair := range []struct {
			name  string
			value string
		}{
			{"cluster_name", c.ClusterName},
			{"distributed_database", c.DistributedDatabase},
		} {
			if !clickhouseIdentRE.MatchString(pair.value) {
				return fmt.Errorf("clickhouse.%s is not a valid identifier", pair.name)
			}
		}
	}
	if c.TaskRefreshInterval <= 0 {
		return errors.New("clickhouse.task_refresh_interval must be > 0")
	}
	if c.InsertBatchSize <= 0 {
		return errors.New("clickhouse.insert_batch_size must be > 0")
	}
	if c.InsertFlushInterval <= 0 {
		return errors.New("clickhouse.insert_flush_interval must be > 0")
	}
	return nil
}

// Config defines configuration for skywalking receiver.
type Config struct {
	Protocols  Protocols        `mapstructure:"protocols"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	// prevent unkeyed literal initialization
	_ struct{}
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Protocols.GRPC == nil && cfg.Protocols.HTTP == nil {
		return errors.New("must specify at least one protocol when using the Skywalking receiver")
	}

	if cfg.Protocols.GRPC != nil {
		var err error
		if _, err = extractPortFromEndpoint(cfg.Protocols.GRPC.NetAddr.Endpoint); err != nil {
			return fmt.Errorf("unable to extract port for the gRPC endpoint: %w", err)
		}
	}

	if cfg.Protocols.HTTP != nil {
		if _, err := extractPortFromEndpoint(cfg.Protocols.HTTP.NetAddr.Endpoint); err != nil {
			return fmt.Errorf("unable to extract port for the HTTP endpoint: %w", err)
		}
	}

	if err := cfg.ClickHouse.Validate(); err != nil {
		return err
	}
	if cfg.ClickHouse.Enabled() && cfg.Protocols.GRPC == nil {
		return errors.New("clickhouse profile support requires the grpc protocol")
	}

	return nil
}

// Unmarshal a config.Parser into the config struct.
func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil || len(componentParser.AllKeys()) == 0 {
		return errors.New("empty config for Skywalking receiver")
	}

	// UnmarshalExact will not set struct properties to nil even if no key is provided,
	// so set the protocol structs to nil where the keys were omitted.
	err := componentParser.Unmarshal(cfg)
	if err != nil {
		return err
	}

	protocols, err := componentParser.Sub(protocolsFieldName)
	if err != nil {
		return err
	}

	if !protocols.IsSet(protoGRPC) {
		cfg.Protocols.GRPC = nil
	}

	if !protocols.IsSet(protoHTTP) {
		cfg.Protocols.HTTP = nil
	}

	return nil
}
