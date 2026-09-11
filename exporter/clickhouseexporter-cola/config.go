// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"errors"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
)

// Config embeds the official clickhouse exporter settings and adds Cola extras.
type Config struct {
	clickhouseexporter.Config `mapstructure:",squash"`

	DistributedDatabase string        `mapstructure:"distributed_database"`
	AgentsTable         string        `mapstructure:"agents_table"`
	SnapshotsTable      string        `mapstructure:"snapshots_table"`
	License             LicenseConfig `mapstructure:"license"`
}

// LicenseConfig controls agent license cache and ingest gating.
type LicenseConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`
	AgentsTable     string        `mapstructure:"agents_table"`
	LicenseTable    string        `mapstructure:"license_table"`
	DictType        uint64        `mapstructure:"dict_type"`
}

var _ component.Config = (*Config)(nil)

func createDefaultConfig() component.Config {
	inner := clickhouseexporter.NewFactory().CreateDefaultConfig().(*clickhouseexporter.Config)
	return &Config{
		Config:         *inner,
		AgentsTable:    "otel_agents",
		SnapshotsTable: "sw_profile_snapshots",
		License:        defaultLicenseConfig(),
	}
}

func defaultLicenseConfig() LicenseConfig {
	return LicenseConfig{
		Enabled:         true,
		RefreshInterval: 30 * time.Second,
		AgentsTable:     defaultLicenseAgentsTable,
		LicenseTable:    defaultLicenseTable,
		DictType:        defaultLicenseDictType,
	}
}

func (c *Config) Validate() error {
	if c.AgentsTable == "" {
		c.AgentsTable = "otel_agents"
	}
	if c.SnapshotsTable == "" {
		c.SnapshotsTable = "sw_profile_snapshots"
	}
	c.License.applyDefaults()
	if err := c.Config.Validate(); err != nil {
		return err
	}
	for _, pair := range []struct {
		name  string
		value string
	}{
		{"agents_table", c.AgentsTable},
		{"snapshots_table", c.SnapshotsTable},
	} {
		if err := validateIdent(pair.value, pair.name); err != nil {
			return err
		}
	}
	if c.Database != "" {
		if err := validateIdent(c.Database, "database"); err != nil {
			return err
		}
	}
	if c.ClusterName != "" || c.DistributedDatabase != "" {
		if c.ClusterName == "" || c.DistributedDatabase == "" {
			return errors.New("cluster_name and distributed_database must be set together")
		}
		if err := validateIdent(c.ClusterName, "cluster_name"); err != nil {
			return err
		}
		if err := validateIdent(c.DistributedDatabase, "distributed_database"); err != nil {
			return err
		}
	}
	if c.LogsTableName != "" {
		if err := validateIdent(c.LogsTableName, "logs_table_name"); err != nil {
			return err
		}
	}
	if c.TracesTableName != "" {
		if err := validateIdent(c.TracesTableName, "traces_table_name"); err != nil {
			return err
		}
	}
	for _, pair := range []struct {
		name  string
		value string
	}{
		{"metrics_tables.gauge", c.MetricsTables.Gauge.Name},
		{"metrics_tables.sum", c.MetricsTables.Sum.Name},
		{"metrics_tables.summary", c.MetricsTables.Summary.Name},
		{"metrics_tables.histogram", c.MetricsTables.Histogram.Name},
		{"metrics_tables.exponential_histogram", c.MetricsTables.ExponentialHistogram.Name},
	} {
		if pair.value == "" {
			continue
		}
		if err := validateIdent(pair.value, pair.name); err != nil {
			return fmt.Errorf("clickhouse.%s: %w", pair.name, err)
		}
	}
	if c.License.Enabled {
		if err := validateQualifiedTable(c.License.AgentsTable, "license.agents_table"); err != nil {
			return err
		}
		if err := validateQualifiedTable(c.License.LicenseTable, "license.license_table"); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) localDatabase() string {
	if c.Database != "" {
		return c.Database
	}
	return "default"
}
