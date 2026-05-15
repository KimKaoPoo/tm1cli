package client

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"strings"
	"time"
	"tm1cli/internal/config"
)

// ErrNotFound is returned when the TM1 server responds with HTTP 404.
var ErrNotFound = errors.New("not found")

// ErrTimeout is wrapped into timeout errors so callers can dispatch with errors.Is.
var ErrTimeout = errors.New("timeout")

// ErrAsyncNoLocation is returned when an async POST succeeds but the server
// did not include a parseable "Threads('<id>')" Location header — i.e. it
// likely ignored the Prefer: respond-async preference.
var ErrAsyncNoLocation = errors.New("async response missing Location header")

const defaultTimeout = 30 * time.Second

type Client struct {
	httpClient *http.Client
	baseURL    string
	user       string
	password   string
	authMode   string
	namespace  string
	verbose    bool
	stderr     io.Writer
}

func NewClient(server config.ServerConfig, password string, tlsVerify bool, verbose bool) (*Client, error) {
	if strings.EqualFold(server.AuthMode, "cam") && server.Namespace == "" {
		return nil, fmt.Errorf("CAM auth requires a namespace; set it with --namespace or 'tm1cli config edit'")
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("cannot create cookie jar: %w", err)
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: !tlsVerify,
		},
	}

	httpClient := &http.Client{
		Jar:       jar,
		Transport: transport,
		Timeout:   defaultTimeout,
	}

	baseURL := strings.TrimRight(server.URL, "/")
	if !strings.HasSuffix(baseURL, "/api/v1") {
		baseURL += "/api/v1"
	}

	return &Client{
		httpClient: httpClient,
		baseURL:    baseURL,
		user:       server.User,
		password:   password,
		authMode:   server.AuthMode,
		namespace:  server.Namespace,
		verbose:    verbose,
		stderr:     os.Stderr,
	}, nil
}

// SetTimeout overrides the per-request HTTP timeout. Non-positive values are
// ignored. Long-running operations (e.g. SaveDataAll) extend past the 30s default.
func (c *Client) SetTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	c.httpClient.Timeout = d
}

// SetStderr overrides stderr output (for testing).
func (c *Client) SetStderr(w io.Writer) {
	c.stderr = w
}

func (c *Client) setAuth(req *http.Request) {
	switch strings.ToLower(c.authMode) {
	case "cam":
		encoded := base64.StdEncoding.EncodeToString(
			[]byte(c.user + ":" + c.password + ":" + c.namespace),
		)
		req.Header.Set("Authorization", "CAMNamespace "+encoded)
	default:
		req.SetBasicAuth(c.user, c.password)
	}
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json; charset=utf-8")
	c.setAuth(req)
}

func (c *Client) do(req *http.Request) (http.Header, []byte, int, error) {
	c.setHeaders(req)

	start := time.Now()
	if c.verbose {
		fmt.Fprintf(c.stderr, "[verbose] %s %s\n", req.Method, req.URL.String())
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, 0, c.wrapError(err)
	}
	defer resp.Body.Close()

	if c.verbose {
		elapsed := time.Since(start)
		fmt.Fprintf(c.stderr, "[verbose] %d %s (%dms)\n", resp.StatusCode, resp.Status, elapsed.Milliseconds())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.Header, nil, resp.StatusCode, fmt.Errorf("cannot read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return resp.Header, body, resp.StatusCode, c.httpError(resp.StatusCode, body, req.URL.Path)
	}

	return resp.Header, body, resp.StatusCode, nil
}

func (c *Client) Get(endpoint string) ([]byte, error) {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}
	_, body, _, err := c.do(req)
	return body, err
}

func (c *Client) doWithPayload(method, endpoint string, payload interface{}) ([]byte, error) {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}
	_, body, _, err := c.do(req)
	return body, err
}

func (c *Client) Post(endpoint string, payload interface{}) ([]byte, error) {
	return c.doWithPayload("POST", endpoint, payload)
}

func (c *Client) Patch(endpoint string, payload interface{}) ([]byte, error) {
	return c.doWithPayload("PATCH", endpoint, payload)
}

// PostAsync sends a POST with Prefer: respond-async. TM1 responds 202 Accepted
// with Location: Threads('<id>'); the parsed numeric thread id is returned.
// If the server omits or malforms the Location header (including the case
// where it ignored the async preference and ran synchronously), the call
// returns ErrAsyncNoLocation. HTTP errors >= 400 propagate from the
// underlying do() path (ErrNotFound, ErrTimeout, etc.).
func (c *Client) PostAsync(endpoint string, payload interface{}) (string, error) {
	fullURL := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("cannot marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest("POST", fullURL, bodyReader)
	if err != nil {
		return "", fmt.Errorf("cannot create request: %w", err)
	}
	req.Header.Set("Prefer", "respond-async")

	headers, _, _, err := c.do(req)
	if err != nil {
		return "", err
	}
	id, ok := parseThreadIDFromLocation(headers.Get("Location"))
	if !ok {
		return "", ErrAsyncNoLocation
	}
	return id, nil
}

// parseThreadIDFromLocation extracts the thread id from a TM1 async Location
// header. Accepts "Threads('1234')" alone or as a suffix of an absolute URL.
// Returns ("", false) when missing, empty-id, malformed, or wrong entity.
func parseThreadIDFromLocation(loc string) (string, bool) {
	if loc == "" {
		return "", false
	}
	i := strings.LastIndex(loc, "Threads('")
	if i < 0 {
		return "", false
	}
	rest := loc[i+len("Threads('"):]
	j := strings.Index(rest, "'")
	if j <= 0 {
		return "", false
	}
	return rest[:j], true
}

func (c *Client) Delete(endpoint string) error {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}
	_, _, _, err = c.do(req)
	return err
}

func (c *Client) wrapError(err error) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()

	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return fmt.Errorf("Request timed out after %s: %w", c.httpClient.Timeout, ErrTimeout)
	}
	if strings.Contains(errStr, "connection refused") {
		return fmt.Errorf("Cannot connect to %s. Is TM1 server running?", c.baseURL)
	}
	if strings.Contains(errStr, "no such host") {
		return fmt.Errorf("Cannot connect to %s. Is TM1 server running?", c.baseURL)
	}
	return fmt.Errorf("Connection error: %s", errStr)
}

func (c *Client) httpError(status int, body []byte, endpoint string) error {
	switch status {
	case 401:
		if strings.EqualFold(c.authMode, "cam") {
			return fmt.Errorf("Authentication failed. Check username, password, and CAM namespace (%s).", c.namespace)
		}
		return fmt.Errorf("Authentication failed. Check credentials.")
	case 404:
		return fmt.Errorf("Not found: %s: %w", endpoint, ErrNotFound)
	default:
		short := string(body)
		if len(short) > 200 {
			short = short[:200]
		}
		return fmt.Errorf("HTTP %d: %s", status, short)
	}
}
