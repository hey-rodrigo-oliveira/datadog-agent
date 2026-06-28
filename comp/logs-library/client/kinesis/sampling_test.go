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

// ── ParseSamplingRules ──────────────────────────────────────────────────────

func TestParseSamplingRulesEmpty(t *testing.T) {
	rules, err := ParseSamplingRules("")
	require.NoError(t, err)
	assert.Nil(t, rules)
}

func TestParseSamplingRulesCount(t *testing.T) {
	rules, err := ParseSamplingRules("source:nginx=0.01,service:payments=10,tag:env:prod=50")
	require.NoError(t, err)
	assert.Len(t, rules, 3)
}

func TestParseSamplingRulesInvalidRate(t *testing.T) {
	for _, bad := range []string{
		"source:nginx=abc",
		"source:nginx=101",
		"source:nginx=-1",
	} {
		_, err := ParseSamplingRules(bad)
		assert.Error(t, err, "expected error for %q", bad)
	}
}

func TestParseSamplingRulesInvalidPattern(t *testing.T) {
	// No field prefix at all.
	_, err := ParseSamplingRules("nginx=10")
	assert.Error(t, err)

	// Unknown field.
	_, err = ParseSamplingRules("datacenter:eu=10")
	assert.Error(t, err)
}

func TestParseSamplingRulesMissingEquals(t *testing.T) {
	_, err := ParseSamplingRules("source:nginx")
	assert.Error(t, err)
}

// ── rateForPayload — simple term matching ───────────────────────────────────

func originWith(src, svc string, tags []string) *message.Origin {
	o := &message.Origin{}
	o.SetSource(src)
	o.SetService(svc)
	if tags != nil {
		o.SetTags(tags)
	}
	return o
}

func payload(o *message.Origin) *message.Payload {
	return &message.Payload{
		MessageMetas: []*message.MessageMetadata{{Origin: o}},
	}
}

func TestRateForPayloadSourceMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx=10")
	assert.InDelta(t, 0.1, rateForPayload(payload(originWith("nginx", "", nil)), rules), 1e-9)
}

func TestRateForPayloadServiceMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("service:payments=5")
	assert.InDelta(t, 0.05, rateForPayload(payload(originWith("", "payments", nil)), rules), 1e-9)
}

func TestRateForPayloadTagMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("tag:env:prod=25")
	assert.InDelta(t, 0.25, rateForPayload(payload(originWith("", "", []string{"env:prod", "region:eu"})), rules), 1e-9)
}

func TestRateForPayloadEnvShorthand(t *testing.T) {
	rules, _ := ParseSamplingRules("env:prod=20")
	assert.InDelta(t, 0.2, rateForPayload(payload(originWith("", "", []string{"env:prod"})), rules), 1e-9)
}

func TestRateForPayloadHostShorthand(t *testing.T) {
	rules, _ := ParseSamplingRules("host:web-01=30")
	assert.InDelta(t, 0.3, rateForPayload(payload(originWith("", "", []string{"host:web-01"})), rules), 1e-9)
}

func TestRateForPayloadNoMatch(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx=1")
	assert.Equal(t, 1.0, rateForPayload(payload(originWith("postgres", "", nil)), rules))
}

func TestRateForPayloadNoRules(t *testing.T) {
	assert.Equal(t, 1.0, rateForPayload(&message.Payload{MessageMetas: nil}, nil))
}

func TestRateForPayloadCaseInsensitive(t *testing.T) {
	rules, _ := ParseSamplingRules("source:NGINX=10")
	assert.InDelta(t, 0.1, rateForPayload(payload(originWith("nginx", "", nil)), rules), 1e-9)
}

func TestRateForPayloadFirstRuleWins(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx=10,source:nginx=50")
	// First matching rule wins → 10%
	assert.InDelta(t, 0.1, rateForPayload(payload(originWith("nginx", "", nil)), rules), 1e-9)
}

// ── wildcard matching ───────────────────────────────────────────────────────

func TestMatchGlobExact(t *testing.T) {
	assert.True(t, matchGlob("nginx", "nginx"))
	assert.False(t, matchGlob("nginx-access", "nginx"))
}

func TestMatchGlobPrefix(t *testing.T) {
	assert.True(t, matchGlob("nginx-access", "nginx*"))
	assert.True(t, matchGlob("nginx", "nginx*"))
	assert.False(t, matchGlob("access-nginx", "nginx*"))
}

func TestMatchGlobSuffix(t *testing.T) {
	assert.True(t, matchGlob("access-nginx", "*nginx"))
	assert.False(t, matchGlob("nginx-access", "*nginx"))
}

func TestMatchGlobContains(t *testing.T) {
	assert.True(t, matchGlob("my-nginx-access", "*nginx*"))
	assert.False(t, matchGlob("apache-access", "*nginx*"))
}

func TestMatchGlobStar(t *testing.T) {
	assert.True(t, matchGlob("anything", "*"))
}

func TestRateForPayloadWildcard(t *testing.T) {
	rules, _ := ParseSamplingRules("source:nginx*=5")
	assert.InDelta(t, 0.05, rateForPayload(payload(originWith("nginx-access", "", nil)), rules), 1e-9)
	assert.Equal(t, 1.0, rateForPayload(payload(originWith("apache", "", nil)), rules))
}

// ── boolean expressions ─────────────────────────────────────────────────────

func TestRateForPayloadAND(t *testing.T) {
	rules, err := ParseSamplingRules("source:nginx AND tag:env:prod=10")
	require.NoError(t, err)

	// Both conditions met → 10%
	o1 := originWith("nginx", "", []string{"env:prod"})
	assert.InDelta(t, 0.1, rateForPayload(payload(o1), rules), 1e-9)

	// Only source matches → fallback 100%
	o2 := originWith("nginx", "", []string{"env:staging"})
	assert.Equal(t, 1.0, rateForPayload(payload(o2), rules))
}

func TestRateForPayloadOR(t *testing.T) {
	rules, err := ParseSamplingRules("source:nginx OR source:apache=5")
	require.NoError(t, err)

	assert.InDelta(t, 0.05, rateForPayload(payload(originWith("nginx", "", nil)), rules), 1e-9)
	assert.InDelta(t, 0.05, rateForPayload(payload(originWith("apache", "", nil)), rules), 1e-9)
	assert.Equal(t, 1.0, rateForPayload(payload(originWith("iis", "", nil)), rules))
}

func TestRateForPayloadNOT(t *testing.T) {
	rules, err := ParseSamplingRules("NOT source:debug=50")
	require.NoError(t, err)

	// NOT debug → matches
	assert.InDelta(t, 0.5, rateForPayload(payload(originWith("nginx", "", nil)), rules), 1e-9)
	// debug itself → no match → 100%
	assert.Equal(t, 1.0, rateForPayload(payload(originWith("debug", "", nil)), rules))
}

func TestRateForPayloadParentheses(t *testing.T) {
	rules, err := ParseSamplingRules("(source:nginx OR source:apache) AND tag:env:prod=10")
	require.NoError(t, err)

	o1 := originWith("nginx", "", []string{"env:prod"})
	assert.InDelta(t, 0.1, rateForPayload(payload(o1), rules), 1e-9)

	o2 := originWith("apache", "", []string{"env:prod"})
	assert.InDelta(t, 0.1, rateForPayload(payload(o2), rules), 1e-9)

	// Right source but wrong env → 100%
	o3 := originWith("nginx", "", []string{"env:staging"})
	assert.Equal(t, 1.0, rateForPayload(payload(o3), rules))
}

func TestRateForPayloadComplex(t *testing.T) {
	// Multiple rules: first match wins
	rules, err := ParseSamplingRules("source:nginx AND env:prod=1,(source:nginx OR source:apache) AND NOT env:prod=10")
	require.NoError(t, err)

	// nginx+prod → 1%
	o1 := originWith("nginx", "", []string{"env:prod"})
	assert.InDelta(t, 0.01, rateForPayload(payload(o1), rules), 1e-9)

	// nginx+staging → second rule matches → 10%
	o2 := originWith("nginx", "", []string{"env:staging"})
	assert.InDelta(t, 0.1, rateForPayload(payload(o2), rules), 1e-9)

	// apache+prod → first rule misses (source is apache), second rule: apache AND NOT prod → prod fails NOT → no match for rule 2; 100%
	o3 := originWith("apache", "", []string{"env:prod"})
	assert.Equal(t, 1.0, rateForPayload(payload(o3), rules))
}
