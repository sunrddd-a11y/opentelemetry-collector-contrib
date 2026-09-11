// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver-cola/internal/profile"
)

func TestStampSkyWalkingAgentType(t *testing.T) {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty()
	stampSkyWalkingAgentType(td)
	v, ok := td.ResourceSpans().At(0).Resource().Attributes().Get(profile.AttrColaAgentType)
	require.True(t, ok)
	assert.Equal(t, profile.AgentTypeSkyWalking, v.AsString())
}
