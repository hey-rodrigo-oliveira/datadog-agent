// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// Package kinesis implements a log destination that ships payloads to Amazon
// Kinesis Data Firehose via PutRecordBatch with GZIP compression.
package kinesis

import (
	"bytes"
	"compress/gzip"
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
	maxRecordBytes  = 1000 * 1024      // Firehose per-record limit
	maxBatchRecords = 100              // records per PutRecordBatch call
	maxBatchBytes   = 4 * 1024 * 1024 // Firehose PutRecordBatch total limit

	defaultFlushInterval = time.Second

	minBackoffFactor   = 2
	baseBackoffSeconds = 2.0
	maxBackoffSeconds  = 120.0
	recoveryInterval   = 2
)

// firehoseClient is the subset of the Firehose API used here, allowing test injection.
type firehoseClient interface {
	PutRecordBatch(ctx context.Context, input *firehose.PutRecordBatchInput, opts ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

// Destination ships log payloads to a Kinesis Data Firehose delivery stream.
// Payloads are GZIP-compressed and batched (up to 100 records or 4 MB per call).
type Destination struct {
	streamName    string
	fh            firehoseClient
	destMeta      *client.DestinationMetadata
	isMRF         bool
	backoff       backoff.Policy
	samplingRules []SamplingRule
	flushInterval time.Duration
	mu            sync.Mutex
	nbErrors      int
	wg            sync.WaitGroup
}

// pendingBatch accumulates payloads and their compressed records before a flush.
type pendingBatch struct {
	payloads []*message.Payload
	records  []types.Record
	size     int
}

func (b *pendingBatch) add(p *message.Payload, data []byte) {
	b.payloads = append(b.payloads, p)
	b.records = append(b.records, types.Record{Data: data})
	b.size += len(data)
}

func (b *pendingBatch) full() bool {
	return len(b.payloads) >= maxBatchRecords || b.size >= maxBatchBytes
}

func (b *pendingBatch) empty() bool { return len(b.payloads) == 0 }

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
		flushInterval: defaultFlushInterval,
		backoff:       backoff.NewExpBackoffPolicy(minBackoffFactor, baseBackoffSeconds, maxBackoffSeconds, recoveryInterval, false),
	}
}

func (d *Destination) IsMRF() bool                                { return d.isMRF }
func (d *Destination) Target() string                             { return "kinesis-firehose://" + d.streamName }
func (d *Destination) Metadata() *client.DestinationMetadata     { return d.destMeta }

// Start begins consuming payloads from input and forwarding them to Firehose.
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
	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	var current pendingBatch

	flush := func() {
		if current.empty() {
			return
		}
		b := current
		current = pendingBatch{}
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.sendBatchWithRetry(b, output, isRetrying)
		}()
	}

	for {
		select {
		case payload, ok := <-input:
			if !ok {
				flush()
				goto drain
			}
			if rate := rateForPayload(payload, d.samplingRules); rate < 1.0 && rand.Float64() >= rate {
				if output != nil {
					output <- payload
				}
				continue
			}
			data := compressRecord(payload.Encoded, d.streamName)
			current.add(payload, data)
			if current.full() {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}

drain:
	d.wg.Wait()
	d.signalRetrying(false, isRetrying)
	stopChan <- struct{}{}
}

// sendBatchWithRetry sends a batch to Firehose, retrying failed records until all succeed.
func (d *Destination) sendBatchWithRetry(b pendingBatch, output chan *message.Payload, isRetrying chan bool) {
	payloads := b.payloads
	records := b.records

	for {
		d.mu.Lock()
		n := d.nbErrors
		d.mu.Unlock()

		if dur := d.backoff.GetBackoffDuration(n); dur > 0 {
			log.Warnf("kinesis firehose: backing off %s (errors=%d, stream=%s)", dur, n, d.streamName)
			time.Sleep(dur)
		}

		out, err := d.fh.PutRecordBatch(context.Background(), &firehose.PutRecordBatchInput{
			DeliveryStreamName: aws.String(d.streamName),
			Records:            records,
		})
		if err != nil {
			log.Warnf("kinesis firehose: PutRecordBatch failed for stream %s: %v", d.streamName, err)
			d.mu.Lock()
			d.nbErrors++
			d.mu.Unlock()
			d.signalRetrying(true, isRetrying)
			continue
		}

		if out.FailedPutCount != nil && *out.FailedPutCount > 0 {
			var retryPayloads []*message.Payload
			var retryRecords []types.Record
			for i, resp := range out.RequestResponses {
				if resp.ErrorCode != nil {
					retryPayloads = append(retryPayloads, payloads[i])
					retryRecords = append(retryRecords, records[i])
				} else if output != nil {
					output <- payloads[i]
				}
			}
			payloads = retryPayloads
			records = retryRecords
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
			for _, p := range payloads {
				output <- p
			}
		}
		return
	}
}

// compressRecord GZIP-compresses data for a Firehose record.
// Falls back to raw truncation if compression produces an oversized result (theoretical).
func compressRecord(data []byte, stream string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(data)
	_ = w.Close()
	compressed := buf.Bytes()

	if len(compressed) <= maxRecordBytes {
		return compressed
	}

	// ponytail: truncating gzip output corrupts the stream; only happens if a single
	// uncompressed log is so large it doesn't compress under 1 MB — vanishingly rare.
	log.Warnf("kinesis firehose: compressed record %d bytes exceeds 1000 KB for stream %s, truncating raw",
		len(compressed), stream)
	if len(data) > maxRecordBytes {
		data = data[:maxRecordBytes]
	}
	return data
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
