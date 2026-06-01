// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build linux

// Package rules holds rules related files
package rules

import (
	"errors"

	"github.com/DataDog/datadog-agent/pkg/security/ebpf/probes/rawpacket"
)

// validateBPFFilter checks that the provided BPF filter expression can be compiled
func validateBPFFilter(bpfFilter string) error {
	filter := rawpacket.Filter{
		BPFFilter: bpfFilter,
		Policy:    rawpacket.PolicyDrop,
	}
	if _, err := rawpacket.FilterToInsts(0, filter, rawpacket.DefaultProgOpts()); err != nil {
		return errors.New("a valid BPF filter must be specified to the 'network_filter' action, error: " + err.Error())
	}
	return nil
}
