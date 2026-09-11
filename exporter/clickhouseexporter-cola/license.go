// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

const (
	defaultLicenseAgentsTable = "dvotel.csotel_otel_agents"
	defaultLicenseTable       = "dvutl.csutl_tconfig_public"
	defaultLicenseDictType    = 30
	defaultLicenseRefresh     = 30 * time.Second
	licenseQueryTimeout       = 15 * time.Second
)

type agentKey struct {
	AgentType    string
	ServiceName  string
	InstanceName string
}

type licenseGate struct {
	cfg    LicenseConfig
	conn   driver.Conn
	logger *zap.Logger

	mu     sync.RWMutex
	states map[agentKey]bool
	loaded bool

	stopCh chan struct{}
	doneCh chan struct{}
}

func (c *LicenseConfig) applyDefaults() {
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = defaultLicenseRefresh
	}
	if c.AgentsTable == "" {
		c.AgentsTable = defaultLicenseAgentsTable
	}
	if c.LicenseTable == "" {
		c.LicenseTable = defaultLicenseTable
	}
	if c.DictType == 0 {
		c.DictType = defaultLicenseDictType
	}
}

func newLicenseGate(cfg LicenseConfig, conn driver.Conn, logger *zap.Logger) *licenseGate {
	cfg.applyDefaults()
	return &licenseGate{
		cfg:    cfg,
		conn:   conn,
		logger: logger,
		states: map[agentKey]bool{},
	}
}

func (g *licenseGate) allow(agentType, service, instance string) bool {
	if g == nil || !g.cfg.Enabled {
		return true
	}
	if service == "" || instance == "" {
		return true
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.loaded {
		return true
	}
	auth, ok := g.states[agentKey{AgentType: agentType, ServiceName: service, InstanceName: instance}]
	if !ok {
		return true
	}
	return auth
}

func (g *licenseGate) start() {
	if g == nil || !g.cfg.Enabled || g.stopCh != nil {
		return
	}
	g.stopCh = make(chan struct{})
	g.doneCh = make(chan struct{})
	go g.loop()
}

func (g *licenseGate) shutdown() {
	if g == nil || g.stopCh == nil {
		return
	}
	close(g.stopCh)
	<-g.doneCh
	g.stopCh = nil
	g.doneCh = nil
}

func (g *licenseGate) loop() {
	defer close(g.doneCh)
	g.refreshOnce()
	ticker := time.NewTicker(g.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.refreshOnce()
		}
	}
}

func (g *licenseGate) refreshOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), licenseQueryTimeout)
	defer cancel()
	if err := g.refresh(ctx); err != nil && g.logger != nil {
		g.logger.Warn("refresh agent license cache failed, keep previous cache", zap.Error(err))
	}
}

func (g *licenseGate) refresh(ctx context.Context) error {
	if g.conn == nil {
		return fmt.Errorf("clickhouse connection is not ready")
	}
	query, err := licenseQuery(g.cfg.AgentsTable, g.cfg.LicenseTable, g.cfg.DictType)
	if err != nil {
		return err
	}
	rows, err := g.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query agent license: %w", err)
	}
	defer rows.Close()

	next := make(map[agentKey]bool)
	for rows.Next() {
		var key agentKey
		var authorized uint8
		if err = rows.Scan(&key.AgentType, &key.ServiceName, &key.InstanceName, &authorized); err != nil {
			return fmt.Errorf("scan agent license: %w", err)
		}
		next[key] = authorized == 1
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterate agent license: %w", err)
	}

	g.mu.Lock()
	g.states = next
	g.loaded = true
	g.mu.Unlock()
	if g.logger != nil {
		g.logger.Debug("refreshed agent license cache", zap.Int("agents", len(next)))
	}
	return nil
}

func licenseQuery(agentsTable, licenseTable string, dictType uint64) (string, error) {
	agents, err := quoteQualifiedTable(agentsTable, "license.agents_table")
	if err != nil {
		return "", err
	}
	licenses, err := quoteQualifiedTable(licenseTable, "license.license_table")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`SELECT
	a.agent_type, a.service_name, a.instance_name, if(g.dict_value_uint64 = 1, 1, 0) AS authorized
FROM (
	SELECT
		agent_type, service_name, instance_name
	FROM %s
	GROUP BY agent_type, service_name, instance_name
) AS a
GLOBAL LEFT JOIN (
	SELECT
		dict_type, dict_key, argMax(dict_value_uint64, ttime) AS dict_value_uint64
	FROM %s
	WHERE dict_type = %d AND dict_key LIKE 'apm_agent_license:%%'
	GROUP BY dict_type, dict_key
) AS g ON g.dict_key = concat('apm_agent_license:', lower(hex(MD5(concat(a.agent_type, char(0), a.service_name, char(0), a.instance_name)))))`, agents, licenses, dictType), nil
}
