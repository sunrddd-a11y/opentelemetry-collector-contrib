// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package skywalkingreceivercola // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	cds "skywalking.apache.org/repo/goapi/collect/agent/configuration/v3"
	event "skywalking.apache.org/repo/goapi/collect/event/v3"
	v3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	profile "skywalking.apache.org/repo/goapi/collect/language/profile/v3"
	management "skywalking.apache.org/repo/goapi/collect/management/v3"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/metrics"
	swprofile "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/trace"
)

// configuration defines the behavior and the ports that
// the Skywalking receiver will use.
type configuration struct {
	CollectorHTTPPort           int
	CollectorHTTPSettings       confighttp.ServerConfig
	CollectorGRPCPort           int
	CollectorGRPCServerSettings configgrpc.ServerConfig
	ClickHouse                  ClickHouseConfig
}

// Receiver type is used to receive spans that were originally intended to be sent to Skywalking.
// This receiver is basically a Skywalking collector.
type swReceiver struct {
	config *configuration

	grpc            *grpc.Server
	collectorServer *http.Server

	goroutines sync.WaitGroup

	settings receiver.Settings

	traceReceiver *trace.Receiver

	metricsReceiver *metrics.Receiver

	dummyReportService *dummyReportService
	profileRuntime     *swprofile.Runtime
	segmentCache       *swprofile.SegmentCache
}

// newSkywalkingReceiver creates a TracesReceiver that receives traffic as a Skywalking collector
func newSkywalkingReceiver(
	config *configuration,
	set receiver.Settings,
) *swReceiver {
	return &swReceiver{
		config:       config,
		settings:     set,
		segmentCache: swprofile.NewSegmentCache(100_000, 10*time.Minute),
	}
}

// registerTraceConsumer register a TracesReceiver that receives trace
func (sr *swReceiver) registerTraceConsumer(tc consumer.Traces) error {
	var err error
	sr.traceReceiver, err = trace.NewReceiver(tc, sr.settings)
	if err != nil {
		return err
	}
	if sr.segmentCache != nil {
		sr.traceReceiver.SetSegmentObserver(sr.segmentCache)
	}
	return nil
}

// registerTraceConsumer register a TracesReceiver that receives trace
func (sr *swReceiver) registerMetricsConsumer(mc consumer.Metrics) error {
	var err error
	sr.metricsReceiver, err = metrics.NewReceiver(mc, sr.settings)
	if err != nil {
		return err
	}
	return nil
}

func (sr *swReceiver) collectorGRPCAddr() string {
	var port int
	if sr.config != nil {
		port = sr.config.CollectorGRPCPort
	}
	return fmt.Sprintf(":%d", port)
}

func (sr *swReceiver) collectorGRPCEnabled() bool {
	return sr.config != nil && sr.config.CollectorGRPCPort > 0
}

func (sr *swReceiver) collectorHTTPEnabled() bool {
	return sr.config != nil && sr.config.CollectorHTTPPort > 0
}

func (sr *swReceiver) Start(ctx context.Context, host component.Host) error {
	if err := sr.startProfile(ctx); err != nil {
		return err
	}
	return sr.startCollector(host)
}

func (sr *swReceiver) Shutdown(ctx context.Context) error {
	var errs error

	if sr.collectorServer != nil {
		if cerr := sr.collectorServer.Shutdown(ctx); cerr != nil {
			errs = multierr.Append(errs, cerr)
		}
	}
	if sr.grpc != nil {
		sr.grpc.GracefulStop()
	}

	sr.goroutines.Wait()
	if sr.profileRuntime != nil {
		if err := sr.profileRuntime.Shutdown(ctx); err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	return errs
}

func (sr *swReceiver) startCollector(host component.Host) error {
	if !sr.collectorGRPCEnabled() && !sr.collectorHTTPEnabled() {
		return nil
	}

	ctx := context.Background()

	if sr.collectorHTTPEnabled() {
		cln, cerr := sr.config.CollectorHTTPSettings.ToListener(ctx)
		if cerr != nil {
			return fmt.Errorf("failed to bind to Collector address %q: %w",
				sr.config.CollectorHTTPSettings.NetAddr.Endpoint, cerr)
		}

		nr := mux.NewRouter()
		nr.HandleFunc("/v3/segments", sr.traceReceiver.HTTPHandler).Methods(http.MethodPost)
		sr.collectorServer, cerr = sr.config.CollectorHTTPSettings.ToServer(ctx, host.GetExtensions(), sr.settings.TelemetrySettings, nr)
		if cerr != nil {
			return cerr
		}

		sr.goroutines.Go(func() {
			if errHTTP := sr.collectorServer.Serve(cln); !errors.Is(errHTTP, http.ErrServerClosed) && errHTTP != nil {
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
			}
		})
	}

	if sr.collectorGRPCEnabled() {
		var err error
		sr.grpc, err = sr.config.CollectorGRPCServerSettings.ToServer(ctx, host.GetExtensions(), sr.settings.TelemetrySettings)
		if err != nil {
			return fmt.Errorf("failed to build the options for the Skywalking gRPC Collector: %w", err)
		}
		gaddr := sr.collectorGRPCAddr()
		gln, gerr := net.Listen("tcp", gaddr)
		if gerr != nil {
			return fmt.Errorf("failed to bind to gRPC address %q: %w", gaddr, gerr)
		}
		if sr.traceReceiver != nil {
			v3.RegisterTraceSegmentReportServiceServer(sr.grpc, sr.traceReceiver)
		}
		if sr.metricsReceiver != nil {
			v3.RegisterJVMMetricReportServiceServer(sr.grpc, sr.metricsReceiver)
		}
		sr.dummyReportService = &dummyReportService{}
		management.RegisterManagementServiceServer(sr.grpc, sr.dummyReportService)
		cds.RegisterConfigurationDiscoveryServiceServer(sr.grpc, sr.dummyReportService)
		event.RegisterEventServiceServer(sr.grpc, &eventService{})
		if sr.profileRuntime != nil {
			profile.RegisterProfileTaskServer(sr.grpc, sr.profileRuntime.Service())
		}
		v3.RegisterMeterReportServiceServer(sr.grpc, &meterService{})
		v3.RegisterCLRMetricReportServiceServer(sr.grpc, &clrService{})
		v3.RegisterBrowserPerfServiceServer(sr.grpc, sr.dummyReportService)

		sr.goroutines.Go(func() {
			if errGrpc := sr.grpc.Serve(gln); !errors.Is(errGrpc, grpc.ErrServerStopped) && errGrpc != nil {
				componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errGrpc))
			}
		})
	}

	return nil
}

func (sr *swReceiver) startProfile(ctx context.Context) error {
	var store swprofile.Store
	if sr.config != nil && sr.config.ClickHouse.Enabled() {
		var err error
		store, err = swprofile.NewCHStore(swprofile.CHConfig{
			DSN:             sr.config.ClickHouse.DSN,
			Database:        sr.config.ClickHouse.Database,
			TasksTable:     sr.config.ClickHouse.TasksTable,
			SnapshotsTable: sr.config.ClickHouse.SnapshotsTable,
			CreateSchema:   sr.config.ClickHouse.CreateSchema,
		})
		if err != nil {
			return err
		}
	}

	ch := defaultClickHouseConfig()
	if sr.config != nil {
		ch = sr.config.ClickHouse
		if ch.TaskRefreshInterval == 0 {
			ch.TaskRefreshInterval = defaultClickHouseConfig().TaskRefreshInterval
		}
		if ch.InsertBatchSize == 0 {
			ch.InsertBatchSize = defaultClickHouseConfig().InsertBatchSize
		}
		if ch.InsertFlushInterval == 0 {
			ch.InsertFlushInterval = defaultClickHouseConfig().InsertFlushInterval
		}
	}
	sr.profileRuntime = swprofile.NewRuntime(
		store,
		sr.segmentCache,
		sr.settings.Logger,
		ch.TaskRefreshInterval,
		ch.InsertBatchSize,
		ch.InsertFlushInterval,
	)
	if err := sr.profileRuntime.Start(ctx); err != nil {
		_ = sr.profileRuntime.Shutdown(ctx)
		sr.profileRuntime = nil
		return err
	}
	if sr.traceReceiver != nil && sr.segmentCache != nil {
		sr.traceReceiver.SetSegmentObserver(sr.segmentCache)
	}
	return nil
}
