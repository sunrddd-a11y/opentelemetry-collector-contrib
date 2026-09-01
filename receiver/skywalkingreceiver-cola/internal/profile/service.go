// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"context"
	"errors"
	"io"
	"time"

	"go.uber.org/zap"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	swprofile "skywalking.apache.org/repo/goapi/collect/language/profile/v3"
)

// Service implements skywalking.v3.ProfileTask.
type Service struct {
	swprofile.UnimplementedProfileTaskServer
	tasks   *TaskCache
	segs    *SegmentCache
	batcher *Batcher
	store   Store
	logger  *zap.Logger
}

func NewService(tasks *TaskCache, segs *SegmentCache, batcher *Batcher, store Store, logger *zap.Logger) *Service {
	return &Service{
		tasks:   tasks,
		segs:    segs,
		batcher: batcher,
		store:   store,
		logger:  logger,
	}
}

func (s *Service) GetProfileTaskCommands(_ context.Context, q *swprofile.ProfileTaskCommandQuery) (*common.Commands, error) {
	out := &common.Commands{}
	if s.tasks == nil || q == nil {
		return out, nil
	}
	service := q.GetService()
	instance := q.GetServiceInstance()
	last := q.GetLastCommandTime()
	now := time.Now()
	matched := s.tasks.Matching(service, instance, last, now)
	for _, t := range matched {
		out.Commands = append(out.Commands, taskToCommand(t))
	}
	if s.logger != nil {
		s.logger.Info("profile task query",
			zap.String("service", service),
			zap.String("service_instance", instance),
			zap.Int64("last_command_time", last),
			zap.Int("cached", len(s.tasks.All())),
			zap.Int("matched", len(matched)),
		)
		if len(matched) == 0 {
			for _, t := range s.tasks.All() {
				s.logger.Info("profile task not matched",
					zap.String("task_id", t.TaskID),
					zap.String("task_service", t.Service),
					zap.String("task_instance", t.ServiceInstance),
					zap.Time("create_time", t.CreateTime),
					zap.String("reason", s.tasks.SkipReason(t, service, instance, last)),
				)
			}
		}
	}
	return out, nil
}

func (s *Service) CollectSnapshot(stream swprofile.ProfileTask_CollectSnapshotServer) error {
	for {
		snap, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&common.Commands{})
			}
			return err
		}
		if snap == nil {
			continue
		}
		s.enqueueSnapshot(snap)
	}
}

func (s *Service) enqueueSnapshot(snap *swprofile.ThreadSnapshot) {
	if s.batcher == nil {
		return
	}
	row := SnapshotRow{
		Timestamp: time.UnixMilli(snap.GetTime()),
		TaskID:    snap.GetTaskId(),
		SegmentID: snap.GetTraceSegmentId(),
		Sequence:  uint32(snap.GetSequence()),
	}
	if snap.GetStack() != nil {
		row.StackLeafFirst = append([]string(nil), snap.GetStack().GetCodeSignatures()...)
	}
	if s.tasks != nil {
		if t, ok := s.tasks.ByID(row.TaskID); ok {
			row.Service = t.Service
			row.EndpointName = t.EndpointName
			if t.ServiceInstance != "" {
				row.ServiceInstance = t.ServiceInstance
			}
		}
	}
	if ident, ok := s.segs.Get(row.SegmentID); ok {
		row.OTelTraceID = ident.OTelTraceID
		row.SWTraceID = ident.SWTraceID
		if row.Service == "" {
			row.Service = ident.Service
		}
		if row.ServiceInstance == "" {
			row.ServiceInstance = ident.ServiceInstance
		}
	}
	if s.logger != nil && (row.Sequence == 0 || row.Sequence%50 == 0) {
		s.logger.Info("profile snapshot received",
			zap.String("task_id", row.TaskID),
			zap.String("segment_id", row.SegmentID),
			zap.Uint32("sequence", row.Sequence),
			zap.Int("stack_depth", len(row.StackLeafFirst)),
		)
	}
	s.batcher.Add(row)
}

func (s *Service) ReportTaskFinish(ctx context.Context, in *swprofile.ProfileTaskFinishReport) (*common.Commands, error) {
	if in == nil {
		return &common.Commands{}, nil
	}
	if s.store != nil && s.tasks != nil {
		row, ok := s.tasks.templateForFinish(in.GetTaskId(), in.GetService(), in.GetServiceInstance())
		if !ok {
			row = Task{
				TaskID:     in.GetTaskId(),
				Service:    in.GetService(),
				Enabled:    1,
				CreateTime: time.Now(),
			}
		}
		if in.GetService() != "" {
			row.Service = in.GetService()
		}
		row.ServiceInstance = in.GetServiceInstance()
		row.Status = TaskStatusFinished
		row.UpdatedAt = time.Now()
		if err := s.store.InsertTask(ctx, row); err != nil {
			if s.logger != nil {
				s.logger.Error("insert profile task finish failed", zap.Error(err), zap.String("task_id", in.GetTaskId()))
			}
		} else {
			s.tasks.Upsert(row)
		}
	}
	if s.logger != nil {
		s.logger.Info("profile task finish",
			zap.String("task_id", in.GetTaskId()),
			zap.String("service", in.GetService()),
			zap.String("service_instance", in.GetServiceInstance()),
		)
	}
	return &common.Commands{}, nil
}
