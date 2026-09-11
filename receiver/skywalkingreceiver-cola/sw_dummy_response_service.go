// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package skywalkingreceivercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	v3c "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	event "skywalking.apache.org/repo/goapi/collect/event/v3"
	agent "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	management "skywalking.apache.org/repo/goapi/collect/management/v3"

	swprofile "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"
)

type dummyReportService struct {
	management.UnimplementedManagementServiceServer
	v3c.UnimplementedConfigurationDiscoveryServiceServer
	agent.UnimplementedJVMMetricReportServiceServer
	agent.UnimplementedBrowserPerfServiceServer
	event.UnimplementedEventServiceServer

	agents swprofile.Store
	logs   consumer.Logs
	logger *zap.Logger
}

func kvPairsToMap(pairs []*common.KeyStringValuePair) map[string]string {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		if p == nil || p.GetKey() == "" {
			continue
		}
		out[p.GetKey()] = p.GetValue()
	}
	return out
}

// for sw InstanceProperties
func (s *dummyReportService) ReportInstanceProperties(ctx context.Context, in *management.InstanceProperties) (*common.Commands, error) {
	if s == nil || in == nil || (s.logs == nil && s.agents == nil) {
		return &common.Commands{}, nil
	}
	now := time.Now()
	props := kvPairsToMap(in.GetProperties())
	if s.logs != nil {
		if err := s.logs.ConsumeLogs(ctx, swprofile.AgentPropertiesToLogs(
			in.GetService(), in.GetServiceInstance(), in.GetLayer(), props, now,
		)); err != nil && s.logger != nil {
			s.logger.Warn("export skywalking agent properties failed",
				zap.Error(err),
				zap.String("service", in.GetService()),
				zap.String("instance", in.GetServiceInstance()),
			)
		}
		return &common.Commands{}, nil
	}
	row := swprofile.NewSkyWalkingAgent(
		in.GetService(),
		in.GetServiceInstance(),
		in.GetLayer(),
		props,
		now,
	)
	if err := s.agents.InsertAgent(ctx, row); err != nil && s.logger != nil {
		s.logger.Warn("insert skywalking agent properties failed",
			zap.Error(err),
			zap.String("service", row.ServiceName),
			zap.String("instance", row.InstanceName),
		)
	}
	return &common.Commands{}, nil
}

// for sw InstancePingPkg
func (s *dummyReportService) KeepAlive(ctx context.Context, in *management.InstancePingPkg) (*common.Commands, error) {
	if s == nil || in == nil || (s.logs == nil && s.agents == nil) {
		return &common.Commands{}, nil
	}
	service := in.GetService()
	instance := in.GetServiceInstance()
	if s.logs != nil {
		if err := s.logs.ConsumeLogs(ctx, swprofile.AgentKeepAliveToLogs(
			service, instance, in.GetLayer(), time.Now(),
		)); err != nil && s.logger != nil {
			s.logger.Warn("export skywalking agent keepalive failed",
				zap.Error(err),
				zap.String("service", service),
				zap.String("instance", instance),
			)
		}
		return &common.Commands{}, nil
	}
	existing, err := s.agents.GetAgent(ctx, swprofile.AgentTypeSkyWalking, service, instance)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("load skywalking agent for keepalive failed",
				zap.Error(err),
				zap.String("service", service),
				zap.String("instance", instance),
			)
		}
		return &common.Commands{}, nil
	}
	row := swprofile.KeepAliveAgent(existing, service, instance, in.GetLayer(), time.Now())
	if err = s.agents.InsertAgent(ctx, row); err != nil && s.logger != nil {
		s.logger.Warn("insert skywalking agent keepalive failed",
			zap.Error(err),
			zap.String("service", service),
			zap.String("instance", instance),
		)
	}
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
