// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package guiimpl

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestGUI(t *testing.T) *gui {
	t.Helper()
	return &gui{
		address:      "localhost:5002",
		auth:         newAuthenticator("test-secret", 5*time.Minute),
		intentTokens: make(map[string]bool),
	}
}

func TestGetAccessToken_CookieHasSameSiteStrict(t *testing.T) {
	g := newTestGUI(t)
	g.intentTokens["test-intent"] = true

	req := httptest.NewRequest(http.MethodGet, "/auth?intent=test-intent", nil)
	rr := httptest.NewRecorder()

	g.getAccessToken(rr, req)

	var accessCookie *http.Cookie
	for _, c := range rr.Result().Cookies() {
		if c.Name == "accessToken" {
			accessCookie = c
			break
		}
	}
	require.NotNil(t, accessCookie, "accessToken cookie must be set")
	assert.Equal(t, http.SameSiteStrictMode, accessCookie.SameSite)
	assert.True(t, accessCookie.HttpOnly)
}

func TestAuthMiddleware_OriginCheck(t *testing.T) {
	g := newTestGUI(t)
	token := g.auth.GenerateAccessToken()

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name           string
		method         string
		origin         string
		expectedStatus int
	}{
		{
			name:           "POST without Origin is allowed (same-origin browser request)",
			method:         http.MethodPost,
			origin:         "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "POST with matching Origin is allowed",
			method:         http.MethodPost,
			origin:         "http://localhost:5002",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "POST with cross-origin Origin is rejected",
			method:         http.MethodPost,
			origin:         "http://evil.com",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:           "GET with cross-origin Origin is allowed (safe method)",
			method:         http.MethodGet,
			origin:         "http://evil.com",
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/agent/restart", nil)
			req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			rr := httptest.NewRecorder()
			g.authMiddleware(okHandler).ServeHTTP(rr, req)

			assert.Equal(t, tt.expectedStatus, rr.Code)
		})
	}
}
