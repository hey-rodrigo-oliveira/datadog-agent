// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2024-present Datadog, Inc.

//go:build test

package snmpscanimpl

import (
	"testing"

	snmpscan "github.com/DataDog/datadog-agent/comp/snmpscan/def"
	"github.com/DataDog/datadog-agent/pkg/networkdevice/metadata"

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

// collectOIDs sums the DeviceOIDs across the payloads a flusher sent.
func collectOIDs(payloads []metadata.NetworkDevicesMetadata) int {
	total := 0
	for _, p := range payloads {
		total += len(p.DeviceOIDs)
	}
	return total
}

func TestOIDFlusherCountBased(t *testing.T) {
	var sent []metadata.NetworkDevicesMetadata
	send := func(p metadata.NetworkDevicesMetadata) error {
		sent = append(sent, p)
		return nil
	}

	flusher := newOIDFlusher("default", 100, 0, send)
	for i := 0; i < 250; i++ {
		assert.NoError(t, flusher.add(&metadata.DeviceOID{OID: "1.2.3"}))
	}
	// Two flushes happened mid-walk (at 100 and 200), so results stream out
	// before the scan finishes.
	assert.Len(t, sent, 2)
	assert.NoError(t, flusher.flush())
	// The final flush reports the remaining 50 OIDs; nothing is dropped.
	assert.Len(t, sent, 3)
	assert.Equal(t, 250, collectOIDs(sent))
}

func TestOIDFlusherEndOnly(t *testing.T) {
	var sent []metadata.NetworkDevicesMetadata
	send := func(p metadata.NetworkDevicesMetadata) error {
		sent = append(sent, p)
		return nil
	}

	// A threshold larger than the OID count means no mid-walk flush.
	flusher := newOIDFlusher("default", 1000, 0, send)
	for i := 0; i < 50; i++ {
		assert.NoError(t, flusher.add(&metadata.DeviceOID{OID: "1.2.3"}))
	}
	assert.Empty(t, sent)
	assert.NoError(t, flusher.flush())
	assert.Len(t, sent, 1)
	assert.Equal(t, 50, collectOIDs(sent))
}

func TestOIDFlusherEmptyFlushNoOp(t *testing.T) {
	called := false
	flusher := newOIDFlusher("default", 100, 0, func(metadata.NetworkDevicesMetadata) error {
		called = true
		return nil
	})
	assert.NoError(t, flusher.flush())
	assert.False(t, called)
}
