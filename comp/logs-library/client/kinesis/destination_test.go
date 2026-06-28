// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package kinesis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/datadog-agent/comp/logs-library/client"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
)

// mockFirehose records calls and returns a configurable error.
type mockFirehose struct {
	calls  []*firehose.PutRecordInput
	errFn  func(n int) error
}

func (m *mockFirehose) PutRecord(_ context.Context, in *firehose.PutRecordInput, _ ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	m.calls = append(m.calls, in)
	if m.errFn != nil {
		return nil, m.errFn(len(m.calls))
	}
	return &firehose.PutRecordOutput{}, nil
}

func newTestDestination(fh firehoseClient) *Destination {
	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	return newDestinationWithClient("test-stream", fh, 1.0, meta)
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

	require.Len(t, fh.calls, 1)
	assert.Equal(t, []byte(`{"message":"hello"}`), fh.calls[0].Record.Data)
	assert.Equal(t, "test-stream", *fh.calls[0].DeliveryStreamName)
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

	select {
	case got := <-output:
		assert.Equal(t, payload, got)
	default:
		t.Fatal("payload not forwarded to output")
	}
}

func TestDestinationRetriesOnError(t *testing.T) {
	callCount := 0
	fh := &mockFirehose{
		errFn: func(n int) error {
			callCount = n
			if n < 3 {
				return errors.New("transient error")
			}
			return nil
		},
	}
	// Override backoff to zero so the test doesn't sleep.
	dest := newTestDestination(fh)
	dest.backoff = &zeroBackoff{}

	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: []byte("log line")}
	close(input)

	select {
	case <-stop:
	case <-time.After(2 * time.Second):
		t.Fatal("destination did not stop")
	}

	assert.Equal(t, 3, callCount)
}

func TestDestinationTruncatesOversizedPayload(t *testing.T) {
	fh := &mockFirehose{}
	dest := newTestDestination(fh)

	big := make([]byte, maxRecordBytes+100)
	for i := range big {
		big[i] = 'x'
	}

	input := make(chan *message.Payload, 1)
	output := make(chan *message.Payload, 1)

	stop := dest.Start(input, output, nil)
	input <- &message.Payload{Encoded: big}
	close(input)
	<-stop

	require.Len(t, fh.calls, 1)
	assert.Len(t, fh.calls[0].Record.Data, maxRecordBytes)
}

func TestDestinationSamplingDropsMostPayloads(t *testing.T) {
	fh := &mockFirehose{}
	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, 0.0, meta) // 0% sample rate → ship nothing

	input := make(chan *message.Payload, 10)
	output := make(chan *message.Payload, 10)

	stop := dest.Start(input, output, nil)
	for i := 0; i < 10; i++ {
		input <- &message.Payload{Encoded: []byte("log")}
	}
	close(input)
	<-stop

	assert.Len(t, fh.calls, 0, "0%% sample rate should ship nothing")
	assert.Len(t, output, 10, "sampled-out payloads must still be acked on output")
}

func TestDestinationSamplingShipsAll(t *testing.T) {
	fh := &mockFirehose{}
	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, 1.0, meta) // 100% → ship all

	input := make(chan *message.Payload, 5)
	output := make(chan *message.Payload, 5)

	stop := dest.Start(input, output, nil)
	for i := 0; i < 5; i++ {
		input <- &message.Payload{Encoded: []byte("log")}
	}
	close(input)
	<-stop

	assert.Len(t, fh.calls, 5, "100%% sample rate should ship everything")
}

func TestDestinationMetadata(t *testing.T) {
	dest := newTestDestination(&mockFirehose{})
	assert.Equal(t, "kinesis-firehose://test-stream", dest.Target())
	assert.False(t, dest.IsMRF())
	assert.NotNil(t, dest.Metadata())
}

// zeroBackoff returns 0 duration so tests don't sleep waiting for retry.
type zeroBackoff struct{}

func (z *zeroBackoff) GetBackoffDuration(_ int) time.Duration { return 0 }
func (z *zeroBackoff) IncError(n int) int                    { return n + 1 }
func (z *zeroBackoff) DecError(n int) int                    { return n - 1 }
