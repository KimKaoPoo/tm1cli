package client

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
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
		Timeout:   30 * time.Second,
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

func (c *Client) do(req *http.Request) ([]byte, int, error) {
	c.setHeaders(req)

	start := time.Now()
	if c.verbose {
		fmt.Fprintf(c.stderr, "[verbose] %s %s\n", req.Method, req.URL.String())
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, c.wrapError(err)
	}
	defer resp.Body.Close()

	if c.verbose {
		elapsed := time.Since(start)
		fmt.Fprintf(c.stderr, "[verbose] %d %s (%dms)\n", resp.StatusCode, resp.Status, elapsed.Milliseconds())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("cannot read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return body, resp.StatusCode, c.httpError(resp.StatusCode, body, req.URL.Path)
	}

	return body, resp.StatusCode, nil
}

func (c *Client) Get(endpoint string) ([]byte, error) {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}
	body, _, err := c.do(req)
	return body, err
}

func (c *Client) Post(endpoint string, payload interface{}) ([]byte, error) {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}
	body, _, err := c.do(req)
	return body, err
}

func (c *Client) Delete(endpoint string) error {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("cannot create request: %w", err)
	}
	_, _, err = c.do(req)
	return err
}

func (c *Client) Patch(endpoint string, payload interface{}) ([]byte, error) {
	url := c.baseURL + "/" + strings.TrimLeft(endpoint, "/")
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("cannot marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest("PATCH", url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("cannot create request: %w", err)
	}
	body, _, err := c.do(req)
	return body, err
}

func (c *Client) wrapError(err error) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()

	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return fmt.Errorf("Request timed out after 30s.")
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
		return fmt.Errorf("Authentication failed. Check credentials.")
	case 404:
		return fmt.Errorf("Not found: %s", endpoint)
	default:
		short := string(body)
		if len(short) > 200 {
			short = short[:200]
		}
		return fmt.Errorf("HTTP %d: %s", status, short)
	}
}
