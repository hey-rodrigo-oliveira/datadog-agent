// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build !linux

// Package rules holds rules related files
package rules

// validateBPFFilter is a no-op on non-Linux platforms where the network_filter
// action (and the eBPF raw packet compiler) is not supported.
func validateBPFFilter(_ string) error {
	return nil
}
