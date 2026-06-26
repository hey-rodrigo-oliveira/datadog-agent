// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build darwin

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func withMockKickstart(t *testing.T, mock func(string) error) {
	t.Helper()
	orig := kickstart
	kickstart = mock
	t.Cleanup(func() { kickstart = orig })
}

func TestHandleAgentRestart_Returns200Immediately(t *testing.T) {
	withMockKickstart(t, func(string) error { return nil })

	req := httptest.NewRequest(http.MethodPost, "/agent-restart", nil)
	rr := httptest.NewRecorder()

	handleAgentRestart(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestHandleAgentRestart_KickstartsServicesAsynchronously(t *testing.T) {
	var called []string
	done := make(chan struct{})

	withMockKickstart(t, func(svc string) error {
		called = append(called, svc)
		if len(called) == 2 {
			close(done)
		}
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/agent-restart", nil)
	rr := httptest.NewRecorder()

	handleAgentRestart(rr, req)

	// Response must be 200 before the goroutine fires.
	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("kickstart was not called within timeout")
	}

	assert.Equal(t, []string{"system/com.datadoghq.agent", "system/com.datadoghq.sysprobe"}, called)
}

func TestHandleAgentRestart_KickstartsAgentBeforeSysprobe(t *testing.T) {
	var order []string
	done := make(chan struct{})

	withMockKickstart(t, func(svc string) error {
		order = append(order, svc)
		if len(order) == 2 {
			close(done)
		}
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/agent-restart", nil)
	handleAgentRestart(httptest.NewRecorder(), req)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("kickstart was not called within timeout")
	}

	assert.Equal(t, "system/com.datadoghq.agent", order[0], "agent should be restarted before sysprobe")
	assert.Equal(t, "system/com.datadoghq.sysprobe", order[1])
}
