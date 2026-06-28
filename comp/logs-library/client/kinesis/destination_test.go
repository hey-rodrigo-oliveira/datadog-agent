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
	return newDestinationWithClient("test-stream", fh, nil, meta)
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

func TestDestinationSamplingDropsBySource(t *testing.T) {
	fh := &mockFirehose{}
	// 0% for "nginx" source → nothing shipped
	rules, err := ParseSamplingRules("source:nginx=0")
	require.NoError(t, err)

	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, rules, meta)

	nginxOrigin := &message.Origin{}
	nginxOrigin.SetSource("nginx")

	input := make(chan *message.Payload, 5)
	output := make(chan *message.Payload, 5)

	stop := dest.Start(input, output, nil)
	for i := 0; i < 5; i++ {
		input <- &message.Payload{
			Encoded:      []byte("log"),
			MessageMetas: []*message.MessageMetadata{{Origin: nginxOrigin}},
		}
	}
	close(input)
	<-stop

	assert.Len(t, fh.calls, 0, "0%% rule should ship nothing")
	assert.Len(t, output, 5, "sampled-out payloads must still be acked")
}

func TestDestinationSamplingUnmatchedSendsAll(t *testing.T) {
	fh := &mockFirehose{}
	// Rule only covers nginx; these payloads have source=postgres → 100%
	rules, err := ParseSamplingRules("source:nginx=0")
	require.NoError(t, err)

	meta := client.NewDestinationMetadata("test", "0", "reliable", "0", "")
	dest := newDestinationWithClient("test-stream", fh, rules, meta)

	pgOrigin := &message.Origin{}
	pgOrigin.SetSource("postgres")

	input := make(chan *message.Payload, 5)
	output := make(chan *message.Payload, 5)

	stop := dest.Start(input, output, nil)
	for i := 0; i < 5; i++ {
		input <- &message.Payload{
			Encoded:      []byte("log"),
			MessageMetas: []*message.MessageMetadata{{Origin: pgOrigin}},
		}
	}
	close(input)
	<-stop

	assert.Len(t, fh.calls, 5, "unmatched logs must be sent at 100%%")
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
