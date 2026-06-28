// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package kinesis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

// SamplingRule pairs a pattern with a sample rate.
//
// Pattern format: "source:<name>", "service:<name>", or "tag:<key>:<value>".
// Rate is a percentage (0.01–100). Logs matching this rule are sent to Firehose
// at that percentage; e.g. rate=10 means 1-in-10 payloads are shipped.
// Logs not matching any rule are sent at 100%.
type SamplingRule struct {
	key   string // "source", "service", "tag"
	value string // source name, service name, or full tag "env:prod"
	rate  float64 // stored as fraction [0, 1]
}

// ParseSamplingRules parses a comma-separated list of "pattern=rate%" pairs.
//
// Example: "source:nginx=0.01,service:payments=10,tag:env:prod=50"
func ParseSamplingRules(raw string) ([]SamplingRule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var rules []SamplingRule
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		// Split on last "=" to separate pattern from rate.
		idx := strings.LastIndex(item, "=")
		if idx < 0 {
			return nil, fmt.Errorf("kinesis sampling: missing '=' in rule %q", item)
		}
		pattern := strings.TrimSpace(item[:idx])
		rateStr := strings.TrimSpace(item[idx+1:])

		pct, err := strconv.ParseFloat(rateStr, 64)
		if err != nil || pct < 0 || pct > 100 {
			return nil, fmt.Errorf("kinesis sampling: rate %q must be a number 0–100", rateStr)
		}

		rule, err := parsePattern(pattern, pct/100.0)
		if err != nil {
			return nil, err
		}
		log.Infof("kinesis sampling: rule %q → %.4f%%", pattern, pct)
		rules = append(rules, rule)
	}
	return rules, nil
}

func parsePattern(pattern string, rate float64) (SamplingRule, error) {
	if strings.HasPrefix(pattern, "source:") {
		return SamplingRule{key: "source", value: strings.TrimPrefix(pattern, "source:"), rate: rate}, nil
	}
	if strings.HasPrefix(pattern, "service:") {
		return SamplingRule{key: "service", value: strings.TrimPrefix(pattern, "service:"), rate: rate}, nil
	}
	if strings.HasPrefix(pattern, "tag:") {
		tagVal := strings.TrimPrefix(pattern, "tag:")
		if tagVal == "" {
			return SamplingRule{}, fmt.Errorf("kinesis sampling: tag pattern %q has no tag value", pattern)
		}
		return SamplingRule{key: "tag", value: tagVal, rate: rate}, nil
	}
	return SamplingRule{}, fmt.Errorf("kinesis sampling: unknown pattern %q (must start with source:, service:, or tag:)", pattern)
}

// rateForPayload returns the sample rate for the payload, based on the first
// matching rule. Returns 1.0 (send all) if no rule matches.
func rateForPayload(payload *message.Payload, rules []SamplingRule) float64 {
	if len(rules) == 0 {
		return 1.0
	}

	// Use the first message's origin for matching. Payloads within a pipeline
	// instance are always from the same log source, so the first meta is representative.
	var origin *message.Origin
	for _, meta := range payload.MessageMetas {
		if meta != nil && meta.Origin != nil {
			origin = meta.Origin
			break
		}
	}
	if origin == nil {
		return 1.0
	}

	src := origin.Source()
	svc := origin.Service()
	tags := origin.Tags()

	for _, rule := range rules {
		switch rule.key {
		case "source":
			if strings.EqualFold(src, rule.value) {
				return rule.rate
			}
		case "service":
			if strings.EqualFold(svc, rule.value) {
				return rule.rate
			}
		case "tag":
			for _, t := range tags {
				if strings.EqualFold(t, rule.value) {
					return rule.rate
				}
			}
		}
	}
	return 1.0
}
