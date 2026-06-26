// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build darwin

package api

import (
	"net/http"
	"os/exec"
)

var agentServices = []string{
	"system/com.datadoghq.agent",
	"system/com.datadoghq.sysprobe",
}

func handleAgentRestart(w http.ResponseWriter, r *http.Request) {
	for _, service := range agentServices {
		cmd := exec.Command("/bin/launchctl", "kickstart", "-k", service)
		out, err := cmd.CombinedOutput()
		if err != nil {
			http.Error(w, string(out), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}
