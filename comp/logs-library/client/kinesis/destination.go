// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// Package kinesis implements a log destination that ships payloads to Amazon
// Kinesis Data Firehose via PutRecord.
package kinesis

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	"github.com/DataDog/datadog-agent/comp/logs-library/client"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/util/backoff"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

const (
	// maxRecordBytes is the Firehose PutRecord limit per record.
	maxRecordBytes = 1000 * 1024

	minBackoffFactor    = 2
	baseBackoffSeconds  = 2.0
	maxBackoffSeconds   = 120.0
	recoveryInterval    = 2
)

// firehoseClient is the subset of the Firehose API we use, allowing test injection.
type firehoseClient interface {
	PutRecord(ctx context.Context, input *firehose.PutRecordInput, opts ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

// Destination ships log payloads to a Kinesis Data Firehose delivery stream.
type Destination struct {
	streamName    string
	fh            firehoseClient
	destMeta      *client.DestinationMetadata
	isMRF         bool
	backoff       backoff.Policy
	samplingRules []SamplingRule
	mu            sync.Mutex
	nbErrors      int
	wg            sync.WaitGroup
}

// NewDestination creates a Firehose destination using the default AWS credential chain.
// endpointURL overrides the Firehose endpoint (e.g. for LocalStack or VPC endpoints);
// leave empty to use the standard regional endpoint.
// Maps to config key logs_config.kinesis_firehose_endpoint_url /
// env var DD_LOGS_CONFIG_KINESIS_FIREHOSE_ENDPOINT_URL.
// NewDestination creates a Firehose destination using the default AWS credential chain.
// endpointURL overrides the Firehose endpoint (leave empty for the standard regional endpoint).
// samplingRules controls per-pattern sample rates; nil sends everything at 100%.
func NewDestination(streamName, region, endpointURL string, samplingRules []SamplingRule, meta *client.DestinationMetadata) (*Destination, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("kinesis destination: failed to load AWS config: %w", err)
	}

	var clientOpts []func(*firehose.Options)
	if endpointURL != "" {
		clientOpts = append(clientOpts, func(o *firehose.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
		})
	}
	return newDestinationWithClient(streamName, firehose.NewFromConfig(cfg, clientOpts...), samplingRules, meta), nil
}

// newDestinationWithClient is used in tests to inject a mock Firehose client.
func newDestinationWithClient(streamName string, fh firehoseClient, samplingRules []SamplingRule, meta *client.DestinationMetadata) *Destination {
	return &Destination{
		streamName:    streamName,
		fh:            fh,
		samplingRules: samplingRules,
		destMeta:      meta,
		backoff:       backoff.NewExpBackoffPolicy(minBackoffFactor, baseBackoffSeconds, maxBackoffSeconds, recoveryInterval, false),
	}
}

// IsMRF reports whether this destination is used for Multi-Region Failover.
func (d *Destination) IsMRF() bool { return d.isMRF }

// Target returns a human-readable identifier for this destination.
func (d *Destination) Target() string {
	return "kinesis-firehose://" + d.streamName
}

// Metadata returns the destination metadata.
func (d *Destination) Metadata() *client.DestinationMetadata { return d.destMeta }

// Start begins consuming payloads from input and forwarding them to Firehose.
// It closes stopChan when the input channel is drained and all in-flight sends finish.
func (d *Destination) Start(
	input chan *message.Payload,
	output chan *message.Payload,
	isRetrying chan bool,
) (stopChan <-chan struct{}) {
	stop := make(chan struct{})
	go d.run(input, output, stop, isRetrying)
	return stop
}

func (d *Destination) run(
	input chan *message.Payload,
	output chan *message.Payload,
	stopChan chan struct{},
	isRetrying chan bool,
) {
	for payload := range input {
		d.wg.Add(1)
		go func(p *message.Payload) {
			defer d.wg.Done()
			d.sendWithRetry(p, output, isRetrying)
		}(payload)
	}
	d.wg.Wait()
	d.signalRetrying(false, isRetrying)
	stopChan <- struct{}{}
}

func (d *Destination) sendWithRetry(
	payload *message.Payload,
	output chan *message.Payload,
	isRetrying chan bool,
) {
	if rate := rateForPayload(payload, d.samplingRules); rate < 1.0 && rand.Float64() >= rate {
		if output != nil {
			output <- payload
		}
		return
	}
	for {
		d.mu.Lock()
		n := d.nbErrors
		d.mu.Unlock()

		if dur := d.backoff.GetBackoffDuration(n); dur > 0 {
			log.Warnf("kinesis firehose: backing off %s before retry (errors=%d, stream=%s)", dur, n, d.streamName)
			time.Sleep(dur)
		}

		if err := d.send(payload); err != nil {
			log.Warnf("kinesis firehose: failed to send to %s: %v", d.streamName, err)
			d.mu.Lock()
			d.nbErrors++
			d.mu.Unlock()
			d.signalRetrying(true, isRetrying)
			continue
		}

		d.mu.Lock()
		d.nbErrors = 0
		d.mu.Unlock()
		d.signalRetrying(false, isRetrying)
		if output != nil {
			output <- payload
		}
		return
	}
}

func (d *Destination) send(payload *message.Payload) error {
	data := payload.Encoded
	if len(data) > maxRecordBytes {
		log.Warnf("kinesis firehose: payload %d bytes exceeds 1000 KB record limit for stream %s, truncating",
			len(data), d.streamName)
		data = data[:maxRecordBytes]
	}
	_, err := d.fh.PutRecord(context.Background(), &firehose.PutRecordInput{
		DeliveryStreamName: aws.String(d.streamName),
		Record:             &types.Record{Data: data},
	})
	return err
}

func (d *Destination) signalRetrying(retrying bool, ch chan bool) {
	if ch == nil {
		return
	}
	select {
	case ch <- retrying:
	default:
	}
}
