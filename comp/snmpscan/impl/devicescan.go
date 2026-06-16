// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2024-present Datadog, Inc.

package snmpscanimpl

import (
	"context"
	"errors"
	"fmt"
	"time"

	snmpscan "github.com/DataDog/datadog-agent/comp/snmpscan/def"
	"github.com/DataDog/datadog-agent/pkg/networkdevice/metadata"
	"github.com/DataDog/datadog-agent/pkg/snmp/gosnmplib"
	"github.com/DataDog/datadog-agent/pkg/snmp/snmpparse"

	"github.com/gosnmp/gosnmp"
)

func (s snmpScannerImpl) ScanDeviceAndSendData(ctx context.Context, connParams *snmpparse.SNMPConfig, namespace string, scanParams snmpscan.ScanParams) error {
	// Establish connection
	snmp, err := snmpparse.NewSNMP(connParams, s.log)
	if err != nil {
		return err
	}
	deviceID := namespace + ":" + connParams.IPAddress
	// Since the snmp connection can take a while, start by sending an in progress status for the start of the scan
	// before connecting to the agent
	inProgressStatusPayload := metadata.NetworkDevicesMetadata{
		DeviceScanStatus: &metadata.ScanStatusMetadata{
			DeviceID:   deviceID,
			ScanStatus: metadata.ScanStatusInProgress,
			ScanType:   scanParams.ScanType,
		},
		CollectTimestamp: time.Now().Unix(),
		Namespace:        namespace,
	}
	if err = s.sendPayload(inProgressStatusPayload); err != nil {
		return fmt.Errorf("unable to send in progress status: %w", err)
	}
	if err = snmp.Connect(); err != nil {
		errs := []error{err}

		// Send an error status if we can't connect to the agent
		errorStatusPayload := metadata.NetworkDevicesMetadata{
			DeviceScanStatus: &metadata.ScanStatusMetadata{
				DeviceID:   deviceID,
				ScanStatus: metadata.ScanStatusError,
				ScanType:   scanParams.ScanType,
			},
			CollectTimestamp: time.Now().Unix(),
			Namespace:        namespace,
		}
		if sendErr := s.sendPayload(errorStatusPayload); sendErr != nil {
			errs = append(errs, fmt.Errorf("unable to send error status: %w", sendErr))
		}
		return gosnmplib.NewConnectionError(
			fmt.Errorf("unable to connect to SNMP agent on %s:%d: %w",
				snmp.LocalAddr, snmp.Port, errors.Join(errs...)),
		)
	}
	// GetBulk is the default walk method, but it does not exist in SNMPv1, so
	// fall back to GetNext for v1 devices.
	useBulk := resolveUseBulk(scanParams.ScanMethod, snmp.Version)
	if !useBulk && scanParams.ScanMethod != snmpscan.ScanMethodGetNext {
		s.log.Infof("device %s is SNMPv1, falling back to GetNext for the scan", deviceID)
	}
	if scanParams.BulkMaxRepetitions > 0 {
		snmp.MaxRepetitions = scanParams.BulkMaxRepetitions
	}

	flushEveryNOIDs := scanParams.FlushEveryNOIDs
	if flushEveryNOIDs <= 0 {
		flushEveryNOIDs = metadata.PayloadMetadataBatchSize
	}

	err = s.runDeviceScan(ctx, snmp, namespace, deviceID, useBulk,
		scanParams.CallInterval, scanParams.MaxCallCount,
		flushEveryNOIDs, scanParams.FlushInterval)
	if err != nil {
		errs := []error{err}

		// Send an error status if we can't scan the device
		errorStatusPayload := metadata.NetworkDevicesMetadata{
			DeviceScanStatus: &metadata.ScanStatusMetadata{
				DeviceID:   deviceID,
				ScanStatus: metadata.ScanStatusError,
				ScanType:   scanParams.ScanType,
			},
			CollectTimestamp: time.Now().Unix(),
			Namespace:        namespace,
		}
		if sendErr := s.sendPayload(errorStatusPayload); sendErr != nil {
			errs = append(errs, fmt.Errorf("unable to send error status: %w", sendErr))
		}
		return fmt.Errorf("unable to perform device scan: %w", errors.Join(errs...))
	}
	// Send a completed status if the scan was successful
	completedStatusPayload := metadata.NetworkDevicesMetadata{
		DeviceScanStatus: &metadata.ScanStatusMetadata{
			DeviceID:   deviceID,
			ScanStatus: metadata.ScanStatusCompleted,
			ScanType:   scanParams.ScanType,
		},
		CollectTimestamp: time.Now().Unix(),
		Namespace:        namespace,
	}
	if err = s.sendPayload(completedStatusPayload); err != nil {
		return fmt.Errorf("unable to send completed status: %w", err)
	}
	return nil
}

// resolveUseBulk returns whether a scan should walk the device with GetBulk.
// GetBulk is used by default and only disabled when explicitly requesting
// GetNext or when the device speaks SNMPv1, which has no GetBulk.
func resolveUseBulk(method snmpscan.ScanMethod, version gosnmp.SnmpVersion) bool {
	if method == snmpscan.ScanMethodGetNext {
		return false
	}
	return version != gosnmp.Version1
}

func (s snmpScannerImpl) runDeviceScan(
	ctx context.Context,
	snmpConnection *gosnmp.GoSNMP,
	deviceNamespace string,
	deviceID string,
	useBulk bool,
	callInterval time.Duration,
	maxCallCount int,
	flushEveryNOIDs int,
	flushInterval time.Duration,
) error {
	flusher := newOIDFlusher(deviceNamespace, flushEveryNOIDs, flushInterval, s.sendPayload)

	err := gosnmplib.ConditionalWalk(
		ctx,
		snmpConnection,
		"",
		useBulk,
		callInterval,
		maxCallCount,
		func(dataUnit gosnmp.SnmpPDU) (string, error) {
			record, err := metadata.DeviceOIDFromPDU(deviceID, &dataUnit)
			if err != nil {
				s.log.Warnf("PDU parsing error: %v", err)
			} else if err := flusher.add(record); err != nil {
				return "", err
			}
			return gosnmplib.SkipOIDRowsNaive(dataUnit.Name), nil
		})
	if err != nil {
		return err
	}

	// Report whatever is left after the walk completes.
	return flusher.flush()
}

// oidFlusher accumulates scanned OIDs and reports them as partial scan results
// once a threshold (count or elapsed time) is reached, so large devices surface
// results before the whole scan completes.
type oidFlusher struct {
	namespace       string
	flushEveryNOIDs int
	flushInterval   time.Duration
	send            func(metadata.NetworkDevicesMetadata) error

	oids      []*metadata.DeviceOID
	lastFlush time.Time
}

func newOIDFlusher(namespace string, flushEveryNOIDs int, flushInterval time.Duration, send func(metadata.NetworkDevicesMetadata) error) *oidFlusher {
	return &oidFlusher{
		namespace:       namespace,
		flushEveryNOIDs: flushEveryNOIDs,
		flushInterval:   flushInterval,
		send:            send,
		lastFlush:       time.Now(),
	}
}

// add buffers a record and flushes when a threshold is reached.
func (f *oidFlusher) add(record *metadata.DeviceOID) error {
	f.oids = append(f.oids, record)
	if len(f.oids) >= f.flushEveryNOIDs ||
		(f.flushInterval > 0 && time.Since(f.lastFlush) >= f.flushInterval) {
		return f.flush()
	}
	return nil
}

// flush reports the buffered OIDs and resets the buffer.
func (f *oidFlusher) flush() error {
	if len(f.oids) == 0 {
		return nil
	}
	payloads := metadata.BatchDeviceScan(f.namespace, time.Now(), metadata.PayloadMetadataBatchSize, f.oids)
	for _, payload := range payloads {
		if err := f.send(payload); err != nil {
			return err
		}
	}
	f.oids = nil
	f.lastFlush = time.Now()
	return nil
}
