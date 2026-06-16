// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2024-present Datadog, Inc.

//go:build test

package snmpscanimpl

import (
	"testing"

	snmpscan "github.com/DataDog/datadog-agent/comp/snmpscan/def"

	"github.com/gosnmp/gosnmp"
	"github.com/stretchr/testify/assert"
)

func TestResolveUseBulk(t *testing.T) {
	tests := []struct {
		name    string
		method  snmpscan.ScanMethod
		version gosnmp.SnmpVersion
		want    bool
	}{
		{"default uses getbulk", "", gosnmp.Version2c, true},
		{"explicit getbulk", snmpscan.ScanMethodGetBulk, gosnmp.Version3, true},
		{"explicit getnext", snmpscan.ScanMethodGetNext, gosnmp.Version2c, false},
		{"v1 falls back to getnext", snmpscan.ScanMethodGetBulk, gosnmp.Version1, false},
		{"v1 default falls back to getnext", "", gosnmp.Version1, false},
		{"v1 explicit getnext stays getnext", snmpscan.ScanMethodGetNext, gosnmp.Version1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveUseBulk(tt.method, tt.version))
		})
	}
}
