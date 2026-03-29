package client

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"tm1cli/internal/config"
)

// newTestServer creates a test HTTP server with the given handler.
func newTestServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(handler)
}

// newTestClient creates a Client pointing at the test server.
func newTestClient(t *testing.T, serverURL string, opts ...func(*config.ServerConfig)) *Client {
	t.Helper()
	srv := config.ServerConfig{
		URL:      serverURL,
		User:     "admin",
		AuthMode: "basic",
	}
	for _, opt := range opts {
		opt(&srv)
	}
	c, err := NewClient(srv, "testpass", false, false)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	return c
}

func TestNewClient(t *testing.T) {
	srv := config.ServerConfig{
		URL:       "https://localhost:8010/api/v1/",
		User:      "admin",
		AuthMode:  "cam",
		Namespace: "LDAP",
	}

	c, err := NewClient(srv, "secret", false, true)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	if c.baseURL != "https://localhost:8010/api/v1" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", c.baseURL)
	}
	if c.user != "admin" {
		t.Errorf("user = %q, want %q", c.user, "admin")
	}
	if c.password != "secret" {
		t.Errorf("password = %q, want %q", c.password, "secret")
	}
	if c.authMode != "cam" {
		t.Errorf("authMode = %q, want %q", c.authMode, "cam")
	}
	if c.namespace != "LDAP" {
		t.Errorf("namespace = %q, want %q", c.namespace, "LDAP")
	}
	if !c.verbose {
		t.Error("verbose should be true")
	}
	if c.httpClient == nil {
		t.Error("httpClient should not be nil")
	}
	if c.httpClient.Jar == nil {
		t.Error("httpClient.Jar should not be nil (cookie jar)")
	}
	if c.httpClient.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", c.httpClient.Timeout)
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name        string
		handler     http.HandlerFunc
		wantErr     bool
		errContains string
		wantBody    string
	}{
		{
			name: "successful GET returns body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("method = %q, want GET", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"value":[{"Name":"Sales"}]}`))
			},
			wantBody: `{"value":[{"Name":"Sales"}]}`,
		},
		{
			name: "401 returns authentication error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`Unauthorized`))
			},
			wantErr:     true,
			errContains: "Authentication failed",
		},
		{
			name: "404 returns not found error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`Not found`))
			},
			wantErr:     true,
			errContains: "Not found",
		},
		{
			name: "500 returns HTTP error with body excerpt",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`Internal Server Error`))
			},
			wantErr:     true,
			errContains: "HTTP 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newTestServer(tt.handler)
			defer ts.Close()

			c := newTestClient(t, ts.URL)
			body, err := c.Get("Cubes")

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(body) != tt.wantBody {
				t.Errorf("body = %q, want %q", string(body), tt.wantBody)
			}
		})
	}
}

func TestGetTimeout(t *testing.T) {
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slow server that exceeds the client timeout
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	srv := config.ServerConfig{
		URL:      ts.URL,
		User:     "admin",
		AuthMode: "basic",
	}
	c, err := NewClient(srv, "testpass", false, false)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	// Override timeout to a very short duration for testing
	c.httpClient.Timeout = 100 * time.Millisecond

	_, getErr := c.Get("slow-endpoint")
	if getErr == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(getErr.Error(), "timed out") {
		t.Errorf("error = %q, want it to contain 'timed out'", getErr.Error())
	}
}

func TestGetConnectionRefused(t *testing.T) {
	// Use a server that was already closed to simulate connection refused
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	closedURL := ts.URL
	ts.Close()

	c := newTestClient(t, closedURL)
	_, err := c.Get("Cubes")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
	// Should get a connection error (the exact message depends on OS)
	if !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Connection error") && !strings.Contains(err.Error(), "Cannot connect") {
		t.Errorf("error = %q, want a connection-related error", err.Error())
	}
}

func TestPost(t *testing.T) {
	tests := []struct {
		name     string
		payload  interface{}
		handler  http.HandlerFunc
		wantErr  bool
		wantBody string
	}{
		{
			name:    "successful POST with body",
			payload: map[string]interface{}{"Parameters": []interface{}{}},
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("method = %q, want POST", r.Method)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"success":true}`))
			},
			wantBody: `{"success":true}`,
		},
		{
			name:    "POST with nil body",
			payload: nil,
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("method = %q, want POST", r.Method)
				}
				w.WriteHeader(http.StatusNoContent)
			},
			wantBody: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newTestServer(tt.handler)
			defer ts.Close()

			c := newTestClient(t, ts.URL)
			body, err := c.Post("Processes('test')/tm1.Execute", tt.payload)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(body) != tt.wantBody {
				t.Errorf("body = %q, want %q", string(body), tt.wantBody)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	t.Run("successful DELETE", func(t *testing.T) {
		ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("method = %q, want DELETE", r.Method)
			}
			w.WriteHeader(http.StatusNoContent)
		})
		defer ts.Close()

		c := newTestClient(t, ts.URL)
		err := c.Delete("Cubes('TestCube')")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestSetAuth(t *testing.T) {
	t.Run("basic auth sets Authorization header", func(t *testing.T) {
		var capturedAuth string
		ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
			capturedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		})
		defer ts.Close()

		c := newTestClient(t, ts.URL, func(srv *config.ServerConfig) {
			srv.AuthMode = "basic"
			srv.User = "admin"
		})
		c.password = "secret"

		_, _ = c.Get("Cubes")

		if !strings.HasPrefix(capturedAuth, "Basic ") {
			t.Errorf("Authorization = %q, want it to start with 'Basic '", capturedAuth)
		}
		// Decode and verify
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(capturedAuth, "Basic "))
		if err != nil {
			t.Fatalf("cannot decode Basic auth: %v", err)
		}
		if string(decoded) != "admin:secret" {
			t.Errorf("decoded auth = %q, want %q", string(decoded), "admin:secret")
		}
	})

	t.Run("CAM auth sets CAMNamespace header with base64", func(t *testing.T) {
		var capturedAuth string
		ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
			capturedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		})
		defer ts.Close()

		c := newTestClient(t, ts.URL, func(srv *config.ServerConfig) {
			srv.AuthMode = "cam"
			srv.User = "admin"
			srv.Namespace = "LDAP"
		})
		c.password = "secret"

		_, _ = c.Get("Cubes")

		if !strings.HasPrefix(capturedAuth, "CAMNamespace ") {
			t.Errorf("Authorization = %q, want it to start with 'CAMNamespace '", capturedAuth)
		}
		// Decode and verify the format is user:password:namespace
		encoded := strings.TrimPrefix(capturedAuth, "CAMNamespace ")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("cannot decode CAM auth: %v", err)
		}
		expected := "admin:secret:LDAP"
		if string(decoded) != expected {
			t.Errorf("decoded auth = %q, want %q", string(decoded), expected)
		}
	})
}

func TestVerboseMode(t *testing.T) {
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	defer ts.Close()

	srv := config.ServerConfig{
		URL:      ts.URL,
		User:     "admin",
		AuthMode: "basic",
	}
	c, err := NewClient(srv, "testpass", false, true) // verbose=true
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	var stderr bytes.Buffer
	c.SetStderr(&stderr)

	_, getErr := c.Get("Cubes")
	if getErr != nil {
		t.Fatalf("unexpected error: %v", getErr)
	}

	output := stderr.String()

	// Should contain the request URL
	if !strings.Contains(output, "GET") {
		t.Errorf("verbose output missing 'GET', got: %q", output)
	}
	if !strings.Contains(output, "/Cubes") {
		t.Errorf("verbose output missing '/Cubes', got: %q", output)
	}
	// Should contain the response status
	if !strings.Contains(output, "200") {
		t.Errorf("verbose output missing '200', got: %q", output)
	}
	// Should contain [verbose] prefix
	if !strings.Contains(output, "[verbose]") {
		t.Errorf("verbose output missing '[verbose]' prefix, got: %q", output)
	}
}

func TestCookieJarPersistence(t *testing.T) {
	requestCount := 0
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if requestCount == 1 {
			// First request: set a session cookie
			http.SetCookie(w, &http.Cookie{
				Name:  "TM1SessionId",
				Value: "abc123session",
				Path:  "/",
			})
		} else {
			// Subsequent requests: verify cookie is sent
			cookie, err := r.Cookie("TM1SessionId")
			if err != nil {
				t.Error("expected TM1SessionId cookie on second request, not found")
			} else if cookie.Value != "abc123session" {
				t.Errorf("cookie value = %q, want %q", cookie.Value, "abc123session")
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	defer ts.Close()

	c := newTestClient(t, ts.URL)

	// First request — server sets cookie
	_, err := c.Get("Cubes?$top=1")
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	// Second request — cookie should be sent back
	_, err = c.Get("Cubes")
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}

	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2", requestCount)
	}
}

func TestGetSetsCorrectHeaders(t *testing.T) {
	var contentType, accept string
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		accept = r.Header.Get("Accept")
		w.WriteHeader(http.StatusOK)
	})
	defer ts.Close()

	c := newTestClient(t, ts.URL)
	_, _ = c.Get("Cubes")

	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
	if accept != "application/json; charset=utf-8" {
		t.Errorf("Accept = %q, want %q", accept, "application/json; charset=utf-8")
	}
}

func TestHTTPErrorBodyTruncation(t *testing.T) {
	// Test that very long error bodies are truncated to 200 chars
	longBody := strings.Repeat("x", 500)
	ts := newTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(longBody))
	})
	defer ts.Close()

	c := newTestClient(t, ts.URL)
	_, err := c.Get("BadEndpoint")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Error message should contain "HTTP 400" and be truncated
	errStr := err.Error()
	if !strings.Contains(errStr, "HTTP 400") {
		t.Errorf("error should contain 'HTTP 400', got: %q", errStr)
	}
	// The truncated body should be at most ~200 chars of x's
	// The full error is "HTTP 400: " + 200 x's
	if strings.Count(errStr, "x") > 200 {
		t.Errorf("error body should be truncated to 200 chars, but contains %d x's", strings.Count(errStr, "x"))
	}
}
