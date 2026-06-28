// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// Package kinesis — sampling rules for Kinesis Firehose log routing.
//
// Rule format (comma-separated):  <filter>=<rate%>
//
//	source:nginx=0.01
//	service:payments AND tag:env:prod=10
//	(source:nginx OR source:apache) AND NOT tag:env:dev=50
//	source:nginx*=5      (prefix wildcard)
//	source:*nginx=5      (suffix wildcard)
//	source:*access*=5    (contains wildcard)
//
// Supported fields: source, service, tag, env (alias for tag:env:<v>), host.
// Operators: AND  OR  NOT  ( )
// Rate: percentage 0.01–100. First matching rule wins. No match → 100%%.
package kinesis

import (
	"fmt"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

// SamplingRule pairs a parsed filter expression with a sample rate.
type SamplingRule struct {
	expr FilterExpr
	rate float64 // [0, 1]
	raw  string  // original pattern string for logging
}

// ParseSamplingRules parses a comma-separated list of "<filter>=<rate%>" pairs.
//
// Example:
//
//	source:nginx=0.01,service:payments AND tag:env:prod=10,(source:nginx OR source:apache)=50
func ParseSamplingRules(raw string) ([]SamplingRule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var rules []SamplingRule
	for _, item := range splitTopLevel(raw, ',') {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		idx := strings.LastIndex(item, "=")
		if idx < 0 {
			return nil, fmt.Errorf("kinesis sampling: missing '=' in rule %q", item)
		}
		pattern := strings.TrimSpace(item[:idx])
		rateStr := strings.TrimSpace(item[idx+1:])

		pct, err := parseRate(rateStr)
		if err != nil {
			return nil, fmt.Errorf("kinesis sampling: %w in rule %q", err, item)
		}

		expr, err := parseExpr(pattern)
		if err != nil {
			return nil, fmt.Errorf("kinesis sampling: %w", err)
		}
		log.Infof("kinesis sampling: rule %q → %.4f%%", pattern, pct)
		rules = append(rules, SamplingRule{expr: expr, rate: pct / 100.0, raw: pattern})
	}
	return rules, nil
}

// rateForPayload returns the sample rate for the payload based on the first matching rule.
// Returns 1.0 (send all) when no rule matches.
func rateForPayload(payload *message.Payload, rules []SamplingRule) float64 {
	if len(rules) == 0 {
		return 1.0
	}

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

	for _, rule := range rules {
		if rule.expr.match(origin) {
			return rule.rate
		}
	}
	return 1.0
}

// ─── expression types ────────────────────────────────────────────────────────

// FilterExpr evaluates a log origin against a filter expression.
type FilterExpr interface {
	match(o *message.Origin) bool
}

type andExpr struct{ left, right FilterExpr }
type orExpr struct{ left, right FilterExpr }
type notExpr struct{ inner FilterExpr }

type termExpr struct {
	field   string // "source", "service", "tag", "env", "host"
	pattern string // value to match, may contain * wildcards
}

func (e andExpr) match(o *message.Origin) bool { return e.left.match(o) && e.right.match(o) }
func (e orExpr) match(o *message.Origin) bool  { return e.left.match(o) || e.right.match(o) }
func (e notExpr) match(o *message.Origin) bool { return !e.inner.match(o) }

func (e termExpr) match(o *message.Origin) bool {
	switch e.field {
	case "source":
		return matchGlob(o.Source(), e.pattern)
	case "service":
		return matchGlob(o.Service(), e.pattern)
	case "tag":
		for _, t := range o.Tags() {
			if matchGlob(t, e.pattern) {
				return true
			}
		}
		return false
	case "env":
		// shorthand: env:prod → tag:env:prod
		for _, t := range o.Tags() {
			if matchGlob(t, "env:"+e.pattern) {
				return true
			}
		}
		return false
	case "host":
		// shorthand: host:<name> → tag:host:<name>
		for _, t := range o.Tags() {
			if matchGlob(t, "host:"+e.pattern) {
				return true
			}
		}
		return false
	}
	return false
}

// ─── parser ───────────────────────────────────────────────────────────────────
//
// Grammar (EBNF):
//   expr    = or_expr
//   or_expr = and_expr  { "OR"  and_expr }
//   and_expr= not_expr  { "AND" not_expr }
//   not_expr= "NOT" not_expr | atom
//   atom    = "(" expr ")" | term
//   term    = field ":" value

type parser struct {
	tokens []string
	pos    int
}

func parseExpr(input string) (FilterExpr, error) {
	tokens := tokenize(input)
	p := &parser{tokens: tokens}
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.tokens) {
		return nil, fmt.Errorf("unexpected token %q in filter %q", p.tokens[p.pos], input)
	}
	return expr, nil
}

func (p *parser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	return p.tokens[p.pos]
}

func (p *parser) consume() string {
	t := p.peek()
	p.pos++
	return t
}

func (p *parser) parseOr() (FilterExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "OR") {
		p.consume()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orExpr{left, right}
	}
	return left, nil
}

func (p *parser) parseAnd() (FilterExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "AND") {
		p.consume()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = andExpr{left, right}
	}
	return left, nil
}

func (p *parser) parseNot() (FilterExpr, error) {
	if strings.EqualFold(p.peek(), "NOT") {
		p.consume()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notExpr{inner}, nil
	}
	return p.parseAtom()
}

func (p *parser) parseAtom() (FilterExpr, error) {
	if p.peek() == "(" {
		p.consume()
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, fmt.Errorf("expected ')' but got %q", p.peek())
		}
		p.consume()
		return expr, nil
	}
	return p.parseTerm()
}

func (p *parser) parseTerm() (FilterExpr, error) {
	tok := p.consume()
	if tok == "" {
		return nil, fmt.Errorf("unexpected end of filter expression")
	}

	// tok must be "field:value"
	colon := strings.Index(tok, ":")
	if colon < 0 {
		return nil, fmt.Errorf("term %q must be field:value (supported fields: source, service, tag, env, host)", tok)
	}
	field := strings.ToLower(tok[:colon])
	value := tok[colon+1:]

	if value == "" {
		return nil, fmt.Errorf("term %q has empty value", tok)
	}

	switch field {
	case "source", "service", "tag", "env", "host":
	default:
		return nil, fmt.Errorf("unknown field %q in term %q (supported: source, service, tag, env, host)", field, tok)
	}
	return termExpr{field: field, pattern: value}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// tokenize splits the filter string into tokens: operators (AND, OR, NOT),
// parentheses, and term strings (field:value).
func tokenize(input string) []string {
	var tokens []string
	buf := strings.Builder{}

	flush := func() {
		if s := strings.TrimSpace(buf.String()); s != "" {
			// A buffered segment may itself be space-separated tokens (AND/OR/NOT between terms)
			for _, word := range splitWords(s) {
				tokens = append(tokens, word)
			}
		}
		buf.Reset()
	}

	for _, ch := range input {
		switch ch {
		case '(':
			flush()
			tokens = append(tokens, "(")
		case ')':
			flush()
			tokens = append(tokens, ")")
		default:
			buf.WriteRune(ch)
		}
	}
	flush()
	return tokens
}

// splitWords splits "source:nginx AND NOT service:debug" into individual tokens,
// treating AND/OR/NOT as delimiters while preserving field:value as single tokens.
func splitWords(s string) []string {
	// Normalise whitespace.
	parts := strings.Fields(s)
	var out []string
	for i := 0; i < len(parts); i++ {
		upper := strings.ToUpper(parts[i])
		if upper == "AND" || upper == "OR" || upper == "NOT" {
			out = append(out, upper)
		} else {
			out = append(out, parts[i])
		}
	}
	return out
}

// splitTopLevel splits s on sep, ignoring sep inside parentheses.
func splitTopLevel(s string, sep rune) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case sep:
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// matchGlob matches value against pattern which may contain * wildcards.
// Case-insensitive.
func matchGlob(value, pattern string) bool {
	value = strings.ToLower(value)
	pattern = strings.ToLower(pattern)

	if !strings.Contains(pattern, "*") {
		return value == pattern
	}
	// Split on * and match each segment in order.
	parts := strings.Split(pattern, "*")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(value[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			// First segment must be a prefix match.
			return false
		}
		pos += idx + len(part)
	}
	// Last segment must end at the end of value (suffix match).
	if last := parts[len(parts)-1]; last != "" {
		return strings.HasSuffix(value, last)
	}
	return true
}

func parseRate(s string) (float64, error) {
	var pct float64
	_, err := fmt.Sscanf(s, "%f", &pct)
	if err != nil || pct < 0 || pct > 100 {
		return 0, fmt.Errorf("rate %q must be a number 0–100", s)
	}
	return pct, nil
}
