package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// saveDataHandlerOpts controls the per-test behavior of newSaveDataHandler.
type saveDataHandlerOpts struct {
	status         int           // status returned by POST /SaveDataAll (default 200)
	delay          time.Duration // optional sleep before responding
	postsCaptured  *int32        // optional counter incremented on each POST hit
	pathsCaptured  *[]string     // optional capture of POST URL paths
	bodiesCaptured *[][]byte     // optional capture of POST request bodies
}

func newSaveDataHandler(opts saveDataHandlerOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !(r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/SaveDataAll")) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"unexpected route"}`))
			return
		}
		if opts.postsCaptured != nil {
			atomic.AddInt32(opts.postsCaptured, 1)
		}
		if opts.pathsCaptured != nil {
			*opts.pathsCaptured = append(*opts.pathsCaptured, r.URL.Path)
		}
		if opts.bodiesCaptured != nil {
			body, _ := io.ReadAll(r.Body)
			*opts.bodiesCaptured = append(*opts.bodiesCaptured, body)
		}
		if opts.delay > 0 {
			time.Sleep(opts.delay)
		}
		status := opts.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if status == http.StatusNoContent {
			return
		}
		w.Write([]byte(`{}`))
	}
}

func TestServerSaveData_Success(t *testing.T) {
	resetCmdFlags(t)
	var posts int32
	paths := []string{}
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		postsCaptured: &posts,
		pathsCaptured: &paths,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST, got %d", got)
	}
	if len(paths) != 1 || !strings.HasSuffix(paths[0], "/SaveDataAll") {
		t.Errorf("expected POST path ending /SaveDataAll, got: %v", paths)
	}
	if !strings.Contains(cap.Stdout, "Status") || !strings.Contains(cap.Stdout, "ok") {
		t.Errorf("expected Status / ok in stdout, got: %s", cap.Stdout)
	}
}

func TestServerSaveData_RequestBodyIsEmpty(t *testing.T) {
	resetCmdFlags(t)
	bodies := [][]byte{}
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		bodiesCaptured: &bodies,
	}))

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	if len(bodies) != 1 {
		t.Fatalf("expected 1 captured body, got %d", len(bodies))
	}
	if len(bodies[0]) != 0 {
		t.Errorf("expected zero-length POST body, got %d bytes: %q", len(bodies[0]), string(bodies[0]))
	}
}

func TestServerSaveData_TableOutput_Shape(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	for _, want := range []string{"PROPERTY", "VALUE", "Status", "Start", "End", "Elapsed"} {
		if !strings.Contains(cap.Stdout, want) {
			t.Errorf("table output missing %q\nstdout: %s", want, cap.Stdout)
		}
	}
}

func TestServerSaveData_JSONOutputContainsTimestamps(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes", "--output", "json"})
		rootCmd.Execute()
	})

	var got struct {
		Status    string    `json:"status"`
		Start     time.Time `json:"start"`
		End       time.Time `json:"end"`
		ElapsedMS int64     `json:"elapsed_ms"`
		Elapsed   string    `json:"elapsed"`
	}
	if err := json.Unmarshal([]byte(cap.Stdout), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\nstdout: %s", err, cap.Stdout)
	}
	if got.Status != "ok" {
		t.Errorf("status = %q, want %q", got.Status, "ok")
	}
	if got.Start.IsZero() || got.End.IsZero() {
		t.Errorf("expected non-zero timestamps, start=%v end=%v", got.Start, got.End)
	}
	if got.ElapsedMS < 0 {
		t.Errorf("elapsed_ms = %d, want >= 0", got.ElapsedMS)
	}
	if got.Elapsed == "" {
		t.Errorf("expected non-empty elapsed string")
	}
}

func TestServerSaveData_DryRunMakesNoHTTPCalls(t *testing.T) {
	resetCmdFlags(t)
	httpCalls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		httpCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--dry-run"})
		rootCmd.Execute()
	})

	if httpCalls != 0 {
		t.Errorf("expected 0 HTTP calls on --dry-run, got %d", httpCalls)
	}
	if !strings.Contains(cap.Stdout, "[dry-run] Would POST /SaveDataAll.") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestServerSaveData_PromptDecline(t *testing.T) {
	resetCmdFlags(t)
	var posts int32
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		postsCaptured: &posts,
	}))
	injectStdin(t, "n\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data"})
		rootCmd.Execute()
	})

	if got := atomic.LoadInt32(&posts); got != 0 {
		t.Errorf("expected zero POSTs after decline, got %d", got)
	}
	if !strings.Contains(cap.Stderr, "pause user activity") {
		t.Errorf("expected pause warning in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_PromptAccept(t *testing.T) {
	resetCmdFlags(t)
	var posts int32
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		postsCaptured: &posts,
	}))
	injectStdin(t, "y\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data"})
		rootCmd.Execute()
	})

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST after accept, got %d", got)
	}
	if !strings.Contains(cap.Stdout, "Status") {
		t.Errorf("expected success row in stdout, got: %s", cap.Stdout)
	}
}

func TestServerSaveData_PromptOnClosedStdin_Declines(t *testing.T) {
	resetCmdFlags(t)
	var posts int32
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		postsCaptured: &posts,
	}))
	injectStdin(t, "")

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data"})
		rootCmd.Execute()
	})

	if got := atomic.LoadInt32(&posts); got != 0 {
		t.Errorf("expected zero POSTs on closed stdin, got %d", got)
	}
}

func TestServerSaveData_PermissionDenied(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		status: http.StatusForbidden,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Permission denied") || !strings.Contains(cap.Stderr, "admin privileges") {
		t.Errorf("expected permission denied message in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_AuthFailureBubbles(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		status: http.StatusUnauthorized,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Authentication failed") {
		t.Errorf("expected 'Authentication failed' in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_Timeout(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		delay: 200 * time.Millisecond,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes", "--timeout", "50ms"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "did not complete within 50ms") {
		t.Errorf("expected timeout message in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_RejectsZeroTimeout(t *testing.T) {
	resetCmdFlags(t)
	httpCalls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		httpCalls++
		w.WriteHeader(http.StatusOK)
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes", "--timeout", "0"})
		rootCmd.Execute()
	})

	if httpCalls != 0 {
		t.Errorf("expected 0 HTTP calls, got %d", httpCalls)
	}
	if !strings.Contains(cap.Stderr, "--timeout must be greater than 0") {
		t.Errorf("expected timeout-zero error in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		status: http.StatusNotFound,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "SaveDataAll endpoint not found") {
		t.Errorf("expected not-found message in stderr, got: %s", cap.Stderr)
	}
}

func TestServerSaveData_WaitFlagAccepted(t *testing.T) {
	resetCmdFlags(t)
	var posts int32
	setupMockTM1(t, newSaveDataHandler(saveDataHandlerOpts{
		postsCaptured: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"server", "save-data", "--yes", "--wait"})
		rootCmd.Execute()
	})

	if got := atomic.LoadInt32(&posts); got != 1 {
		t.Errorf("expected 1 POST with --wait set, got %d", got)
	}
	if !strings.Contains(cap.Stdout, "Status") {
		t.Errorf("expected success output, got: %s", cap.Stdout)
	}
}

func TestServerSaveData_Help_MentionsPause(t *testing.T) {
	resetCmdFlags(t)
	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetErr(&buf)
	defer func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
	}()

	rootCmd.SetArgs([]string{"server", "save-data", "--help"})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("--help returned error: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"pause", "WARNING"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help output missing %q\noutput: %s", want, out)
		}
	}
}
