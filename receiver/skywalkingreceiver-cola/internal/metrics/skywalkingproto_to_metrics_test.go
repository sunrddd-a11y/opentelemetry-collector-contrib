// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	agent "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
)

func TestSwMetricsStampsAgentType(t *testing.T) {
	md := SwMetricsToMetrics(&agent.JVMMetricCollection{
		Service:         "svc",
		ServiceInstance: "inst",
		Metrics: []*agent.JVMMetric{{
			Time:   1,
			Thread: &agent.Thread{},
			Cpu:    &common.CPU{},
		}},
	})
	require.Equal(t, 1, md.ResourceMetrics().Len())
	v, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("sw.cola.agent_type")
	require.True(t, ok)
	assert.Equal(t, "skywalking", v.AsString())
}
