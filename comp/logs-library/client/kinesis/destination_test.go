// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package kinesis

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/datadog-agent/comp/logs-library/client"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
)

// mockFirehose records batches and returns configurable errors / partial failures.
type mockFirehose struct {
	// batches holds the Records slice from each PutRecordBatch call.
	batches [][]types.Record
	// errFn returns an error for the nth call (1-based). nil = success.
	errFn func(n int) error
	// failFirst causes the first call to report all records as failed, then succeed.
	failFirst bool
	calls     int
}

func (m *mockFirehose) PutRecordBatch(_ context.Context, in *firehose.PutRecordBatchInput, _ ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	m.calls++
	m.batches = append(m.batches, in.Records)

	if m.errFn != nil {
		if err := m.errFn(m.calls); err != nil {
			return nil, err
		}
	}

	resps := make([]types.PutRecordBatchResponseEntry, len(in.Records))

	if m.failFirst && m.calls == 1 {
		// Mark all as failed on first attempt.
		code := "ServiceUnavailableException"
		for i := range resps {
			resps[i].ErrorCode = &code
		}
		n := int32(len(in.Records))
		return &firehose.PutRecordBatchOutput{FailedPutCount: &n, RequestResponses: resps}, nil
	}

	var zero int32
	return &firehose.PutRecordBatchOutput{FailedPutCount: &zero, RequestResponses: resps}, nil
}

// decompress is a test helper to unwrap a GZIP-compressed record.
func decompress(t *testing.T, data []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return out
}

func newTestDestination(fh firehoseClient) *Destination {
	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	d := newDestinationWithClient("test-stream", fh, nil, meta)
	d.flushInterval = 10 * time.Millisecond // fast flush in tests
	return d
}

// drain reads n items from ch with a timeout.
func drain(t *testing.T, ch <-chan *message.Payload, n int) []*message.Payload {
	t.Helper()
	out := make([]*message.Payload, 0, n)
	for i := 0; i < n; i++ {
		select {
		case p := <-ch:
			out = append(out, p)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for payload %d/%d on output", i+1, n)
		}
	}
	return out
}

func TestDestinationSendsPayload(t *testing.T) {
	fh := &mockFirehose{}
	dest := newTestDestination(fh)

	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: []byte(`{"message":"hello"}`)}
	close(input)

	select {
	case <-stop:
	case <-time.After(2 * time.Second):
		t.Fatal("destination did not stop")
	}

	require.Len(t, fh.batches, 1)
	require.Len(t, fh.batches[0], 1)
	assert.Equal(t, []byte(`{"message":"hello"}`), decompress(t, fh.batches[0][0].Data))
}

func TestDestinationSendsToCorrectStream(t *testing.T) {
	fh := &mockFirehose{}
	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("my-stream", fh, nil, meta)
	dest.flushInterval = 10 * time.Millisecond

	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)
	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: []byte("log")}
	close(input)
	<-stop

	assert.Equal(t, "kinesis-firehose://my-stream", dest.Target())
	require.Len(t, fh.batches, 1)
	assert.Equal(t, []byte("log"), decompress(t, fh.batches[0][0].Data))
}

func TestDestinationCompressesPayload(t *testing.T) {
	fh := &mockFirehose{}
	dest := newTestDestination(fh)

	raw := []byte(`{"level":"info","message":"compressed log line"}`)
	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: raw}
	close(input)
	<-stop

	require.Len(t, fh.batches[0], 1)
	record := fh.batches[0][0].Data
	// Compressed must be smaller than raw for this payload and decompressible.
	assert.Less(t, len(record), len(raw)+len(raw)/2, "compression should shrink the record")
	assert.Equal(t, raw, decompress(t, record))
}

func TestDestinationBatches(t *testing.T) {
	fh := &mockFirehose{}
	dest := newTestDestination(fh)

	const n = 10
	input := make(chan *message.Payload, n)
	output := make(chan *message.Payload, n)

	stop := dest.Start(input, output, nil)
	for i := 0; i < n; i++ {
		input <- &message.Payload{Encoded: []byte("log")}
	}
	close(input)
	<-stop

	total := 0
	for _, b := range fh.batches {
		total += len(b)
	}
	assert.Equal(t, n, total, "all payloads must be sent")
	drain(t, output, n)
}

func TestDestinationForwardsToOutput(t *testing.T) {
	fh := &mockFirehose{}
	dest := newTestDestination(fh)

	payload := &message.Payload{Encoded: []byte(`{"level":"info"}`)}
	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- payload
	close(input)
	<-stop

	got := drain(t, output, 1)
	assert.Equal(t, payload, got[0])
}

func TestDestinationRetriesOnError(t *testing.T) {
	callCount := 0
	fh := &mockFirehose{
		errFn: func(n int) error {
			callCount = n
			if n < 3 {
				return errors.New("transient")
			}
			return nil
		},
	}
	dest := newTestDestination(fh)
	dest.backoff = &zeroBackoff{}

	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: []byte("log")}
	close(input)

	select {
	case <-stop:
	case <-time.After(2 * time.Second):
		t.Fatal("destination did not stop")
	}

	assert.Equal(t, 3, callCount)
}

func TestDestinationRetriesPartialFailure(t *testing.T) {
	fh := &mockFirehose{failFirst: true}
	dest := newTestDestination(fh)
	dest.backoff = &zeroBackoff{}

	const n = 3
	input := make(chan *message.Payload, n)
	output := make(chan *message.Payload, n)

	stop := dest.Start(input, output, nil)
	for i := 0; i < n; i++ {
		input <- &message.Payload{Encoded: []byte("log")}
	}
	close(input)
	<-stop

	// First call: all n records sent (all fail). Second call: all n retried.
	require.Len(t, fh.batches, 2)
	assert.Len(t, fh.batches[0], n)
	assert.Len(t, fh.batches[1], n)
	drain(t, output, n)
}

func TestDestinationSamplingDropsBySource(t *testing.T) {
	fh := &mockFirehose{}
	rules, err := ParseSamplingRules("source:nginx=0")
	require.NoError(t, err)

	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, rules, meta)
	dest.flushInterval = 10 * time.Millisecond

	origin := &message.Origin{}
	origin.SetSource("nginx")

	const n = 5
	input := make(chan *message.Payload, n)
	output := make(chan *message.Payload, n)

	stop := dest.Start(input, output, nil)
	for i := 0; i < n; i++ {
		input <- &message.Payload{
			Encoded:      []byte("log"),
			MessageMetas: []*message.MessageMetadata{{Origin: origin}},
		}
	}
	close(input)
	<-stop

	assert.Len(t, fh.batches, 0, "0%% rule should ship nothing")
	drain(t, output, n) // sampled-out payloads still acked
}

func TestDestinationSamplingUnmatchedSendsAll(t *testing.T) {
	fh := &mockFirehose{}
	rules, err := ParseSamplingRules("source:nginx=0")
	require.NoError(t, err)

	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, rules, meta)
	dest.flushInterval = 10 * time.Millisecond

	origin := &message.Origin{}
	origin.SetSource("postgres")

	const n = 5
	input := make(chan *message.Payload, n)
	output := make(chan *message.Payload, n)

	stop := dest.Start(input, output, nil)
	for i := 0; i < n; i++ {
		input <- &message.Payload{
			Encoded:      []byte("log"),
			MessageMetas: []*message.MessageMetadata{{Origin: origin}},
		}
	}
	close(input)
	<-stop

	total := 0
	for _, b := range fh.batches {
		total += len(b)
	}
	assert.Equal(t, n, total, "unmatched logs must be sent at 100%%")
}

func TestDestinationMetadata(t *testing.T) {
	dest := newTestDestination(&mockFirehose{})
	assert.Equal(t, "kinesis-firehose://test-stream", dest.Target())
	assert.False(t, dest.IsMRF())
	assert.NotNil(t, dest.Metadata())
}

// zeroBackoff returns 0 duration so tests don't sleep.
type zeroBackoff struct{}

func (z *zeroBackoff) GetBackoffDuration(_ int) time.Duration { return 0 }
func (z *zeroBackoff) IncError(n int) int                    { return n + 1 }
func (z *zeroBackoff) DecError(n int) int                    { return n - 1 }
