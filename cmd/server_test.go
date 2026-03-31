package cmd

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"tm1cli/internal/config"
	"tm1cli/internal/model"
)

// ============================================================
// whoami integration tests — runWhoami with httptest.NewServer
// ============================================================

func TestRunWhoami_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(activeUserJSON("admin"))
	})

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if capturedPath != "/api/v1/ActiveUser" {
		t.Errorf("expected path /api/v1/ActiveUser, got %s", capturedPath)
	}
	if !strings.Contains(captured.Stdout, "User") {
		t.Errorf("output missing header 'User', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "admin") {
		t.Errorf("output missing 'admin', got:\n%s", captured.Stdout)
	}
}

func TestRunWhoami_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(activeUserJSON("admin"))
	})

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result model.ActiveUser
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if result.Name != "admin" {
		t.Errorf("Name = %q, want %q", result.Name, "admin")
	}
}

func TestRunWhoami_NoConfig(t *testing.T) {
	resetCmdFlags(t)

	// Set HOME to empty temp dir — no config file
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	// Prevent local config resolution from finding config in cwd ancestors
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	t.Cleanup(func() { os.Chdir(origDir) })

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "No connection configured") {
		t.Errorf("stderr should contain 'No connection configured', got:\n%s", captured.Stderr)
	}
}

func TestRunWhoami_ServerOverride(t *testing.T) {
	resetCmdFlags(t)

	var requestReceived bool
	ts := setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		w.Header().Set("Content-Type", "application/json")
		w.Write(activeUserJSON("admin"))
	})

	// Reconfigure: default points to a non-existent server, "override" points to mock
	cfg := &config.Config{
		Default:  "broken",
		Settings: config.DefaultSettings(),
		Servers: map[string]config.ServerConfig{
			"broken": {
				URL:      "http://127.0.0.1:1/api/v1",
				User:     "admin",
				Password: config.EncodePassword("pass"),
				AuthMode: "basic",
			},
			"override": {
				URL:      ts.URL + "/api/v1",
				User:     "admin",
				Password: config.EncodePassword("testpass"),
				AuthMode: "basic",
			},
		},
	}
	writeTestConfig(t, cfg)

	flagServer = "override"

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !requestReceived {
		t.Error("mock server should have received a request via --server override")
	}
	if !strings.Contains(captured.Stdout, "admin") {
		t.Errorf("output missing 'admin', got:\n%s", captured.Stdout)
	}
}

func TestRunWhoami_AuthError(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"Unauthorized"}`))
	})

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Authentication failed") {
		t.Errorf("stderr should contain 'Authentication failed', got:\n%s", captured.Stderr)
	}
}

func TestRunWhoami_MalformedJSON(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{bad json`))
	})

	captured := captureAll(t, func() {
		err := runWhoami(whoamiCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Cannot parse server response") {
		t.Errorf("stderr should contain 'Cannot parse server response', got:\n%s", captured.Stderr)
	}
}

// ============================================================
// server-info integration tests — runServerInfo with httptest.NewServer
// ============================================================

func TestRunServerInfo_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(serverConfigJSON("MyTM1", "2.0.9.4", "tm1.example.com", 9001))
	})

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if capturedPath != "/api/v1/Configuration" {
		t.Errorf("expected path /api/v1/Configuration, got %s", capturedPath)
	}
	if !strings.Contains(captured.Stdout, "Server Name") {
		t.Errorf("output missing 'Server Name', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Version") {
		t.Errorf("output missing 'Version', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Admin Host") {
		t.Errorf("output missing 'Admin Host', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "HTTP Port") {
		t.Errorf("output missing 'HTTP Port', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "MyTM1") {
		t.Errorf("output missing 'MyTM1', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "2.0.9.4") {
		t.Errorf("output missing '2.0.9.4', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "tm1.example.com") {
		t.Errorf("output missing 'tm1.example.com', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "9001") {
		t.Errorf("output missing '9001', got:\n%s", captured.Stdout)
	}
}

func TestRunServerInfo_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(serverConfigJSON("MyTM1", "2.0.9.4", "tm1.example.com", 9001))
	})

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result model.ServerConfiguration
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if result.ServerName != "MyTM1" {
		t.Errorf("ServerName = %q, want %q", result.ServerName, "MyTM1")
	}
	if result.ProductVersion != "2.0.9.4" {
		t.Errorf("ProductVersion = %q, want %q", result.ProductVersion, "2.0.9.4")
	}
	if result.AdminHost != "tm1.example.com" {
		t.Errorf("AdminHost = %q, want %q", result.AdminHost, "tm1.example.com")
	}
	if result.HTTPPortNumber != 9001 {
		t.Errorf("HTTPPortNumber = %d, want %d", result.HTTPPortNumber, 9001)
	}
}

func TestRunServerInfo_NoConfig(t *testing.T) {
	resetCmdFlags(t)

	// Set HOME to empty temp dir — no config file
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	// Prevent local config resolution from finding config in cwd ancestors
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	t.Cleanup(func() { os.Chdir(origDir) })

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "No connection configured") {
		t.Errorf("stderr should contain 'No connection configured', got:\n%s", captured.Stderr)
	}
}

func TestRunServerInfo_ServerOverride(t *testing.T) {
	resetCmdFlags(t)

	var requestReceived bool
	ts := setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		w.Header().Set("Content-Type", "application/json")
		w.Write(serverConfigJSON("OverrideTM1", "2.0.9.4", "override.example.com", 9001))
	})

	// Reconfigure: default points to a non-existent server, "override" points to mock
	cfg := &config.Config{
		Default:  "broken",
		Settings: config.DefaultSettings(),
		Servers: map[string]config.ServerConfig{
			"broken": {
				URL:      "http://127.0.0.1:1/api/v1",
				User:     "admin",
				Password: config.EncodePassword("pass"),
				AuthMode: "basic",
			},
			"override": {
				URL:      ts.URL + "/api/v1",
				User:     "admin",
				Password: config.EncodePassword("testpass"),
				AuthMode: "basic",
			},
		},
	}
	writeTestConfig(t, cfg)

	flagServer = "override"

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !requestReceived {
		t.Error("mock server should have received a request via --server override")
	}
	if !strings.Contains(captured.Stdout, "OverrideTM1") {
		t.Errorf("output missing 'OverrideTM1', got:\n%s", captured.Stdout)
	}
}

func TestRunServerInfo_AuthError(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"Unauthorized"}`))
	})

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Authentication failed") {
		t.Errorf("stderr should contain 'Authentication failed', got:\n%s", captured.Stderr)
	}
}

func TestRunServerInfo_MalformedJSON(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{bad json`))
	})

	captured := captureAll(t, func() {
		err := runServerInfo(serverInfoCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Cannot parse") {
		t.Errorf("stderr should contain 'Cannot parse', got:\n%s", captured.Stderr)
	}
}

// ============================================================
// displayWhoami unit tests
// ============================================================

func TestDisplayWhoami_Table(t *testing.T) {
	user := model.ActiveUser{Name: "admin"}

	out := captureStdout(t, func() {
		displayWhoami(user, false)
	})

	if !strings.Contains(out, "PROPERTY") {
		t.Errorf("output missing header 'PROPERTY', got:\n%s", out)
	}
	if !strings.Contains(out, "VALUE") {
		t.Errorf("output missing header 'VALUE', got:\n%s", out)
	}
	if !strings.Contains(out, "User") {
		t.Errorf("output missing row label 'User', got:\n%s", out)
	}
	if !strings.Contains(out, "admin") {
		t.Errorf("output missing value 'admin', got:\n%s", out)
	}
}

func TestDisplayWhoami_JSON(t *testing.T) {
	user := model.ActiveUser{Name: "admin"}

	out := captureStdout(t, func() {
		displayWhoami(user, true)
	})

	var result model.ActiveUser
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if result.Name != "admin" {
		t.Errorf("Name = %q, want %q", result.Name, "admin")
	}
}

// ============================================================
// displayServerInfo unit tests
// ============================================================

func TestDisplayServerInfo_Table(t *testing.T) {
	serverCfg := model.ServerConfiguration{
		ServerName:     "MyTM1",
		ProductVersion: "2.0.9.4",
		AdminHost:      "tm1.example.com",
		HTTPPortNumber: 9001,
	}

	out := captureStdout(t, func() {
		displayServerInfo(serverCfg, false)
	})

	expectedRows := []struct {
		label string
		value string
	}{
		{"Server Name", "MyTM1"},
		{"Version", "2.0.9.4"},
		{"Admin Host", "tm1.example.com"},
		{"HTTP Port", "9001"},
	}

	for _, row := range expectedRows {
		if !strings.Contains(out, row.label) {
			t.Errorf("output missing label %q, got:\n%s", row.label, out)
		}
		if !strings.Contains(out, row.value) {
			t.Errorf("output missing value %q, got:\n%s", row.value, out)
		}
	}
}

func TestDisplayServerInfo_JSON(t *testing.T) {
	serverCfg := model.ServerConfiguration{
		ServerName:     "MyTM1",
		ProductVersion: "2.0.9.4",
		AdminHost:      "tm1.example.com",
		HTTPPortNumber: 9001,
	}

	out := captureStdout(t, func() {
		displayServerInfo(serverCfg, true)
	})

	var result model.ServerConfiguration
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if result.ServerName != "MyTM1" {
		t.Errorf("ServerName = %q, want %q", result.ServerName, "MyTM1")
	}
	if result.ProductVersion != "2.0.9.4" {
		t.Errorf("ProductVersion = %q, want %q", result.ProductVersion, "2.0.9.4")
	}
	if result.AdminHost != "tm1.example.com" {
		t.Errorf("AdminHost = %q, want %q", result.AdminHost, "tm1.example.com")
	}
	if result.HTTPPortNumber != 9001 {
		t.Errorf("HTTPPortNumber = %d, want %d", result.HTTPPortNumber, 9001)
	}
}
