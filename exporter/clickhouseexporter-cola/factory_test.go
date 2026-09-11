// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	c := cfg.(*Config)
	assert.Equal(t, "otel_agents", c.AgentsTable)
	assert.Equal(t, "sw_profile_snapshots", c.SnapshotsTable)
	assert.True(t, c.License.Enabled)
	assert.Equal(t, defaultLicenseRefresh, c.License.RefreshInterval)
	assert.Equal(t, defaultLicenseAgentsTable, c.License.AgentsTable)
	assert.Equal(t, defaultLicenseTable, c.License.LicenseTable)
	assert.Equal(t, uint64(defaultLicenseDictType), c.License.DictType)
}

func TestFactoryCreateExporters(t *testing.T) {
	factory := NewFactory()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "clickhouse://127.0.0.1:9000"
	params := exportertest.NewNopSettings(metadata.Type)

	traces, err := factory.CreateTraces(t.Context(), params, cfg)
	require.NoError(t, err)
	require.NoError(t, traces.Shutdown(t.Context()))

	metrics, err := factory.CreateMetrics(t.Context(), params, cfg)
	require.NoError(t, err)
	require.NoError(t, metrics.Shutdown(t.Context()))

	logs, err := factory.CreateLogs(t.Context(), params, cfg)
	require.NoError(t, err)
	require.NoError(t, logs.Shutdown(t.Context()))
}

func TestConfigValidateClusterPair(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "clickhouse://127.0.0.1:9000"
	cfg.ClusterName = "cas_cluster"
	assert.EqualError(t, cfg.Validate(), "cluster_name and distributed_database must be set together")
	cfg.DistributedDatabase = "dvotel"
	assert.NoError(t, cfg.Validate())
	cfg.License.AgentsTable = "not-qualified"
	assert.Error(t, cfg.Validate())
}

func TestPrepareClusterHTTPSchema(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "http://10.2.120.42:8123"
	cfg.ClusterName = "cas_cluster"
	cfg.DistributedDatabase = "dvotel"
	prepareClusterHTTPSchema(cfg)
	assert.Equal(t, "none", cfg.Compress)

	tcp := createDefaultConfig().(*Config)
	tcp.Endpoint = "tcp://10.2.120.42:9000"
	tcp.ClusterName = "cas_cluster"
	tcp.DistributedDatabase = "dvotel"
	before := tcp.Compress
	prepareClusterHTTPSchema(tcp)
	assert.Equal(t, before, tcp.Compress)
}

func TestFactoryType(t *testing.T) {
	assert.Equal(t, metadata.Type, NewFactory().Type())
}
