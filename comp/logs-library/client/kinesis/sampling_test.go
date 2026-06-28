// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package kinesis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
)

func TestParseSamplingRules(t *testing.T) {
	rules, err := ParseSamplingRules("source:nginx=0.01,service:payments=10,tag:env:prod=50")
	require.NoError(t, err)
	require.Len(t, rules, 3)

	assert.Equal(t, "source", rules[0].key)
	assert.Equal(t, "nginx", rules[0].value)
	assert.InDelta(t, 0.0001, rules[0].rate, 1e-9)

	assert.Equal(t, "service", rules[1].key)
	assert.Equal(t, "payments", rules[1].value)
	assert.InDelta(t, 0.1, rules[1].rate, 1e-9)

	assert.Equal(t, "tag", rules[2].key)
	assert.Equal(t, "env:prod", rules[2].value)
	assert.InDelta(t, 0.5, rules[2].rate, 1e-9)
}

func TestParseSamplingRulesEmpty(t *testing.T) {
	rules, err := ParseSamplingRules("")
	require.NoError(t, err)
	assert.Nil(t, rules)
}

func TestParseSamplingRulesInvalidRate(t *testing.T) {
	_, err := ParseSamplingRules("source:nginx=abc")
	assert.Error(t, err)

	_, err = ParseSamplingRules("source:nginx=101")
	assert.Error(t, err)

	_, err = ParseSamplingRules("source:nginx=-1")
	assert.Error(t, err)
}

func TestParseSamplingRulesInvalidPattern(t *testing.T) {
	_, err := ParseSamplingRules("host:nginx=10")
	assert.Error(t, err)

	_, err = ParseSamplingRules("nginx=10")
	assert.Error(t, err)
}

func TestRateForPayloadSourceMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx=10")

	origin := &message.Origin{}
	origin.SetSource("nginx")

	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: origin}},
	}
	assert.InDelta(t, 0.1, rateForPayload(payload, rules), 1e-9)
}

func TestRateForPayloadServiceMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("service:payments=5")

	origin := &message.Origin{}
	origin.SetService("payments")

	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: origin}},
	}
	assert.InDelta(t, 0.05, rateForPayload(payload, rules), 1e-9)
}

func TestRateForPayloadTagMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("tag:env:prod=25")

	origin := &message.Origin{}
	origin.SetTags([]string{"env:prod", "region:eu"})

	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: origin}},
	}
	assert.InDelta(t, 0.25, rateForPayload(payload, rules), 1e-9)
}

func TestRateForPayloadNoMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx=1")

	origin := &message.Origin{}
	origin.SetSource("postgres")

	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: origin}},
	}
	assert.Equal(t, 1.0, rateForPayload(payload, rules))
}

func TestRateForPayloadNoRules(t *testing.T) {
	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{},
	}
	assert.Equal(t, 1.0, rateForPayload(payload, nil))
}

func TestRateForPayloadCaseInsensitive(t *testing.T) {
	rules, _ := ParseSamplingRules("source:NGINX=10")

	origin := &message.Origin{}
	origin.SetSource("nginx")

	payload := &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: origin}},
	}
	assert.InDelta(t, 0.1, rateForPayload(payload, rules), 1e-9)
}
