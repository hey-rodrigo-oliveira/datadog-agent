// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package cspm

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oscap-io is the patched OpenSCAP engine the agent shells out to; we drive it directly
// for the cross-check.
const oscapIO = "/opt/datadog-agent/embedded/bin/oscap-io"

//go:embed testdata/golden
var goldenFS embed.FS

// oscapResults maps oscap-io's numeric result codes to the agent's result strings.
var oscapResults = map[string]string{
	"1": "passed", "2": "failed", "3": "error", "4": "error",
	"5": "skipped", "6": "notchecked", "7": "notselected",
}

// logGolden (below) is informational and never asserts — it logs a per-rule snapshot and,
// when a baseline is committed, its diff. TestCrossCheck is a real assertion: the agent
// and oscap-io share an engine, so any divergence is an agent Go-layer bug.

// snapshot renders a rule->result map as sorted "rule result" lines.
func snapshot(results map[string]string) string {
	lines := make([]string, 0, len(results))
	for rule, res := range results {
		lines = append(lines, rule+" "+res)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// goldenBaseline returns the committed rule->result baseline for a framework, if present.
func goldenBaseline(frameworkID string) (map[string]string, bool) {
	data, err := goldenFS.ReadFile("testdata/golden/" + frameworkID + ".txt")
	if err != nil {
		return nil, false
	}
	m := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if f := strings.Fields(line); len(f) == 2 {
			m[f[0]] = f[1]
		}
	}
	return m, true
}

// diffResults renders per-rule differences (- removed, + added, ~ changed).
func diffResults(want, got map[string]string) string {
	var lines []string
	for rule, w := range want {
		switch g, ok := got[rule]; {
		case !ok:
			lines = append(lines, "- "+rule+" "+w)
		case g != w:
			lines = append(lines, "~ "+rule+" "+w+" -> "+g)
		}
	}
	for rule, g := range got {
		if _, ok := want[rule]; !ok {
			lines = append(lines, "+ "+rule+" "+g)
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// writeArtifact saves diagnostic output to the suite output dir for CI to upload.
func writeArtifact(t *testing.T, outDir, name, content string) {
	if err := os.WriteFile(filepath.Join(outDir, name), []byte(content+"\n"), 0o644); err != nil {
		t.Logf("could not write %s: %v", name, err)
	}
}

// logGolden logs the per-rule result snapshot and, if a baseline is committed for this
// framework+environment, its diff. The host and container variants of a framework keep
// separate baselines (cis-x.txt vs cis-x-container.txt) since their results differ.
func logGolden(t *testing.T, d distro, env, outDir string, results map[string]string) {
	key := d.frameworkID
	if env != "host" {
		key += "-" + env
	}
	snap := snapshot(results)
	t.Logf("%s (%s) golden snapshot (%d rules):\n%s", d.frameworkID, env, len(results), snap)
	writeArtifact(t, outDir, "golden-"+key+".txt", snap)
	base, ok := goldenBaseline(key)
	if !ok {
		t.Logf("%s (%s) golden: no committed baseline yet (snapshot saved as artifact)", d.frameworkID, env)
		return
	}
	if diff := diffResults(base, results); diff != "" {
		t.Logf("%s (%s) golden diff vs baseline (informational; - removed, + added, ~ changed):\n%s", d.frameworkID, env, diff)
	} else {
		t.Logf("%s (%s) golden: identical to baseline", d.frameworkID, env)
	}
}

// benchmarkInput extracts the datastream path and XCCDF profile the agent uses for a
// framework, by reading the bundled benchmark YAML.
func benchmarkInput(run func(string) string, frameworkID string) (datastream, profile string) {
	const dir = "/etc/datadog-agent/compliance.d"
	yaml := run(fmt.Sprintf("sudo cat %s/%s*.yaml 2>/dev/null", dir, frameworkID))
	if name := regexp.MustCompile(`ssg-[a-z0-9._-]+-ds\.xml(\.bz2)?`).FindString(yaml); name != "" {
		datastream = dir + "/" + name
	}
	profile = regexp.MustCompile(`xccdf_org\.ssgproject\.content_profile_[a-z0-9_]+`).FindString(yaml)
	return datastream, profile
}

// runOscapIO evaluates rules with the bundled oscap-io directly (bypassing the agent's Go
// layer) and returns rule->result. Returns nil if oscap-io cannot be driven.
func runOscapIO(run func(string) string, datastream, profile string, rules []string) map[string]string {
	if datastream == "" || profile == "" || len(rules) == 0 {
		return nil
	}
	var stdin strings.Builder
	for _, r := range rules {
		fmt.Fprintf(&stdin, "%s %s\n", profile, r)
	}
	out := run(fmt.Sprintf("sudo %s %s 2>/dev/null <<'OSCAPEOF'\n%sOSCAPEOF", oscapIO, datastream, stdin.String()))
	m := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 2 {
			if res, ok := oscapResults[f[1]]; ok {
				m[f[0]] = res
			}
		}
	}
	return m
}

// TestGolden logs an informational per-rule result snapshot and baseline diff.
func (s *hostBenchmarksSuite) TestGolden() {
	logGolden(s.T(), s.distro, "host", s.SessionOutputDir(), resultsByRule(s.distro, s.check))
}

// TestGolden logs an informational per-rule result snapshot and baseline diff.
func (s *containerBenchmarksSuite) TestGolden() {
	logGolden(s.T(), s.distro, "container", s.SessionOutputDir(), resultsByRule(s.distro, s.check))
}

// TestCrossCheck asserts the agent's results match the bundled oscap-io engine run
// directly. They share an engine, so any divergence is an agent Go-layer bug (rule
// selection, result mapping, probe-root handling) and is fatal. Failing to drive oscap-io
// at all is also fatal — it ships with the agent, so that signals a real breakage, not an
// environment quirk to skip silently. Host-only (the container variant already exercises
// the agent's own oscap-io path).
func (s *hostBenchmarksSuite) TestCrossCheck() {
	t := s.T()
	run := func(cmd string) string { out, _ := s.Env().RemoteHost.Execute(cmd); return out }
	agent := resultsByRule(s.distro, s.check)
	rules := make([]string, 0, len(agent))
	for rule := range agent {
		rules = append(rules, rule)
	}
	datastream, profile := benchmarkInput(run, s.distro.frameworkID)
	oscap := runOscapIO(run, datastream, profile, rules)
	require.GreaterOrEqualf(t, len(oscap), s.distro.minRules,
		"oscap-io cross-check could not run: evaluated %d rules (datastream=%q profile=%q)",
		len(oscap), datastream, profile)

	var diffs []string
	for rule, a := range agent {
		if o, ok := oscap[rule]; ok && o != a {
			diffs = append(diffs, fmt.Sprintf("%s agent=%s oscap=%s", rule, a, o))
		}
	}
	sort.Strings(diffs)
	writeArtifact(t, s.SessionOutputDir(), "oscap-crosscheck-"+s.distro.frameworkID+".txt", strings.Join(diffs, "\n"))
	assert.Emptyf(t, diffs, "%s: agent disagrees with oscap-io on %d rules (same engine, so this is an agent Go-layer bug):\n%s",
		s.distro.frameworkID, len(diffs), strings.Join(diffs, "\n"))
	if len(diffs) == 0 {
		t.Logf("%s oscap cross-check: agent matches oscap-io on all %d rules", s.distro.frameworkID, len(oscap))
	}
}
