// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexportercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter-cola"

import (
	"fmt"
	"regexp"
	"strings"
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateIdent(name, field string) error {
	if !identRE.MatchString(name) {
		return fmt.Errorf("invalid clickhouse %s %q", field, name)
	}
	return nil
}

func validateQualifiedTable(name, field string) error {
	_, err := quoteQualifiedTable(name, field)
	return err
}

func quoteQualifiedTable(name, field string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid clickhouse %s %q, want database.table", field, name)
	}
	if err := validateIdent(parts[0], field); err != nil {
		return "", err
	}
	if err := validateIdent(parts[1], field); err != nil {
		return "", err
	}
	return quoteIdent(parts[0]) + "." + quoteIdent(parts[1]), nil
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

func qualified(database, table string) string {
	return quoteIdent(database) + "." + quoteIdent(table)
}

func readQualified(database, cluster, distDB, table string) string {
	if cluster != "" {
		return qualified(distDB, database+"_"+table)
	}
	return qualified(database, table)
}

func defaultAgentsTable(name string) string {
	if name == "" {
		return "otel_agents"
	}
	return name
}

func defaultSnapshotsTable(name string) string {
	if name == "" {
		return "sw_profile_snapshots"
	}
	return name
}

func defaultSpansTable(name string) string {
	if name == "" {
		return "otel_spans"
	}
	return name
}
