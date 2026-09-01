// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package skywalkingreceivercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola"

import (
	"context"

	v3c "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	event "skywalking.apache.org/repo/goapi/collect/event/v3"
	agent "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	management "skywalking.apache.org/repo/goapi/collect/management/v3"
)

type dummyReportService struct {
	management.UnimplementedManagementServiceServer
	v3c.UnimplementedConfigurationDiscoveryServiceServer
	agent.UnimplementedJVMMetricReportServiceServer
	agent.UnimplementedBrowserPerfServiceServer
	event.UnimplementedEventServiceServer
}

// for sw InstanceProperties
func (*dummyReportService) ReportInstanceProperties(context.Context, *management.InstanceProperties) (*common.Commands, error) {
	return &common.Commands{}, nil
}

// for sw InstancePingPkg
func (*dummyReportService) KeepAlive(context.Context, *management.InstancePingPkg) (*common.Commands, error) {
	return &common.Commands{}, nil
}

// for sw JVMMetric
func (*dummyReportService) Collect(context.Context, *agent.JVMMetricCollection) (*common.Commands, error) {
	return &common.Commands{}, nil
}

// for sw agent cds
func (*dummyReportService) FetchConfigurations(context.Context, *v3c.ConfigurationSyncRequest) (*common.Commands, error) {
	return &common.Commands{}, nil
}
