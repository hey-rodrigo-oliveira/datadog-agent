// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// Package kinesis provides a log sender that ships payloads to Amazon Kinesis
// Data Firehose. Enable it by setting the following in datadog.yaml:
//
//	logs_config:
//	  kinesis_firehose_delivery_stream: my-delivery-stream
//	  kinesis_firehose_region: us-east-1   # optional, defaults to AWS_DEFAULT_REGION / instance metadata
package kinesis

import (
	"strconv"

	"github.com/DataDog/datadog-agent/comp/logs-library/client"
	kinesisclient "github.com/DataDog/datadog-agent/comp/logs-library/client/kinesis"
	"github.com/DataDog/datadog-agent/comp/logs-library/metrics"
	"github.com/DataDog/datadog-agent/comp/logs-library/sender"
	pkgconfigmodel "github.com/DataDog/datadog-agent/pkg/config/model"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

const (
	configKeyStream      = "logs_config.kinesis_firehose_delivery_stream"
	configKeyRegion      = "logs_config.kinesis_firehose_region"
	configKeyEndpointURL = "logs_config.kinesis_firehose_endpoint_url"
)

// IsEnabled reports whether the Kinesis Firehose destination is configured.
func IsEnabled(cfg pkgconfigmodel.Reader) bool {
	return cfg.GetString(configKeyStream) != ""
}

// NewKinesisSender returns a Sender that forwards log payloads to Kinesis Data
// Firehose. Returns nil and an error if AWS config cannot be loaded.
//
// The caller should check IsEnabled before calling this function.
func NewKinesisSender(
	cfg pkgconfigmodel.Reader,
	sink sender.Sink,
	bufferSize int,
	serverlessMeta sender.ServerlessMeta,
	componentName string,
	queueCount int,
	workersPerQueue int,
	pipelineMonitor metrics.PipelineMonitor,
) (*sender.Sender, error) {
	streamName := cfg.GetString(configKeyStream)
	region := cfg.GetString(configKeyRegion)
	endpointURL := cfg.GetString(configKeyEndpointURL)

	log.Infof("kinesis sender: routing logs to Firehose stream %q (region=%q, endpoint=%q)", streamName, region, endpointURL)

	factory := kinesisDestinationFactory(streamName, region, endpointURL, componentName, pipelineMonitor)

	return sender.NewSender(
		cfg,
		sink,
		factory,
		bufferSize,
		serverlessMeta,
		queueCount,
		workersPerQueue,
		pipelineMonitor,
	), nil
}

func kinesisDestinationFactory(
	streamName, region, endpointURL, componentName string,
	pipelineMonitor metrics.PipelineMonitor,
) sender.DestinationFactory {
	return func(instanceID string) *client.Destinations {
		destMeta := client.NewDestinationMetadata(componentName, instanceID, "reliable", strconv.Itoa(0), "")

		dest, err := kinesisclient.NewDestination(streamName, region, endpointURL, destMeta)
		if err != nil {
			log.Errorf("kinesis sender: failed to create Firehose destination for stream %q: %v", streamName, err)
			return client.NewDestinations(nil, nil)
		}

		return client.NewDestinations([]client.Destination{dest}, nil)
	}
}
