// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package profile // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"

import (
	"strconv"

	common "skywalking.apache.org/repo/goapi/collect/common/v3"
)

const profileTaskQueryCommand = "ProfileTaskQuery"

// SkyWalking Java agent ProfileConstants — values outside these ranges
// are discarded after lastCommandTime is already updated.
const (
	agentDurationMinMinutes   = 1
	agentDurationMaxMinutes   = 15
	agentDumpPeriodMinMillis  = 10
	agentMaxSamplingCountMax  = 9 // agent requires maxSamplingCount < 10
)

func taskToCommand(t Task) *common.Command {
	durationMinutes := (t.DurationSeconds + 59) / 60
	if durationMinutes < agentDurationMinMinutes {
		durationMinutes = agentDurationMinMinutes
	}
	if durationMinutes > agentDurationMaxMinutes {
		durationMinutes = agentDurationMaxMinutes
	}
	dumpPeriod := t.DumpPeriodMs
	if dumpPeriod < agentDumpPeriodMinMillis {
		dumpPeriod = agentDumpPeriodMinMillis
	}
	maxSampling := t.MaxSamplingCount
	if maxSampling == 0 {
		maxSampling = 5
	}
	if maxSampling > agentMaxSamplingCountMax {
		maxSampling = agentMaxSamplingCountMax
	}
	return &common.Command{
		Command: profileTaskQueryCommand,
		Args: []*common.KeyStringValuePair{
			kv("SerialNumber", t.SerialNumber),
			kv("TaskId", t.TaskID),
			kv("EndpointName", t.EndpointName),
			kv("Duration", strconv.FormatUint(uint64(durationMinutes), 10)),
			kv("MinDurationThreshold", strconv.FormatUint(uint64(t.MinDurationThresholdMs), 10)),
			kv("DumpPeriod", strconv.FormatUint(uint64(dumpPeriod), 10)),
			kv("MaxSamplingCount", strconv.FormatUint(uint64(maxSampling), 10)),
			kv("StartTime", strconv.FormatInt(t.StartTime.UnixMilli(), 10)),
			kv("CreateTime", strconv.FormatInt(t.CreateTime.UnixMilli(), 10)),
		},
	}
}

func kv(key, value string) *common.KeyStringValuePair {
	return &common.KeyStringValuePair{Key: key, Value: value}
}
