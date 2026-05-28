package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// --- Unit: endpoint builders ---

func TestProcessErrorLogsEndpoint(t *testing.T) {
	tests := []struct {
		name string
		proc string
		want string
	}{
		{name: "plain", proc: "Load", want: "Processes('Load')/ErrorLogs"},
		{name: "dotted", proc: "Bedrock.Server.Wait", want: "Processes('Bedrock.Server.Wait')/ErrorLogs"},
		{name: "space escaped", proc: "Load Data", want: "Processes('Load%20Data')/ErrorLogs"},
		// url.PathEscape percent-encodes the apostrophe to %27. This documents the
		// existing codebase convention (process.go uses url.PathEscape too); OData
		// string-literal quote-doubling is intentionally not done here.
		{name: "apostrophe percent-encoded", proc: "It's", want: "Processes('It%27s')/ErrorLogs"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processErrorLogsEndpoint(tt.proc); got != tt.want {
				t.Errorf("processErrorLogsEndpoint(%q) = %q, want %q", tt.proc, got, tt.want)
			}
		})
	}
}

func TestErrorLogContentEndpoint(t *testing.T) {
	got := errorLogContentEndpoint("TM1ProcessError_20260528_Load.log")
	want := "ErrorLogFiles('TM1ProcessError_20260528_Load.log')/Content"
	if got != want {
		t.Errorf("errorLogContentEndpoint() = %q, want %q", got, want)
	}
}

// --- Unit: buildHistoryEntries ---

func TestBuildHistoryEntries(t *testing.T) {
	logs := []model.ProcessErrorLog{
		{Timestamp: "2026-05-28T09:14:02Z", File: "a.log"},
		{Timestamp: "2026-05-27T22:01:55Z", Filename: "b.log"},
		{Timestamp: "2026-05-26T10:00:00Z"}, // no filename
	}

	entries := buildHistoryEntries(logs)
	if entries == nil {
		t.Fatal("buildHistoryEntries returned nil slice; want non-nil for JSON []")
	}
	if len(entries) != 3 {
		t.Fatalf("len = %d, want 3", len(entries))
	}

	for i, e := range entries {
		if e.Status != processHistoryStatusError {
			t.Errorf("entry[%d].Status = %q, want %q", i, e.Status, processHistoryStatusError)
		}
		if e.User != "" || e.Duration != "" {
			t.Errorf("entry[%d] User/Duration = %q/%q, want empty", i, e.User, e.Duration)
		}
	}
	if entries[0].StartTime != "2026-05-28T09:14:02Z" || entries[0].ErrorLogFile != "a.log" {
		t.Errorf("entry[0] = %+v", entries[0])
	}
	if entries[1].ErrorLogFile != "b.log" {
		t.Errorf("entry[1].ErrorLogFile = %q, want b.log", entries[1].ErrorLogFile)
	}
	if entries[2].ErrorLogFile != "" {
		t.Errorf("entry[2].ErrorLogFile = %q, want empty", entries[2].ErrorLogFile)
	}
}

// --- Unit: filterHistorySince ---

func TestFilterHistorySince(t *testing.T) {
	entries := []model.ProcessHistoryEntry{
		{StartTime: "2026-05-27T00:00:00Z"},
		{StartTime: "2026-05-28T12:00:00Z"},
		{StartTime: "not-a-time"},
	}

	t.Run("empty cutoff returns unchanged", func(t *testing.T) {
		got := filterHistorySince(entries, "")
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3", len(got))
		}
	})

	t.Run("cutoff keeps newer and drops older and unparseable", func(t *testing.T) {
		got := filterHistorySince(entries, "2026-05-28T00:00:00Z")
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1", len(got))
		}
		if got[0].StartTime != "2026-05-28T12:00:00Z" {
			t.Errorf("kept %q, want 2026-05-28T12:00:00Z", got[0].StartTime)
		}
	})
}

// --- Unit: isFailureStatus / filterOnlyFailures ---

func TestIsFailureStatus(t *testing.T) {
	tests := []struct {
		status string
		want   bool
	}{
		{"Error", true},
		{"error", true},
		{"Aborted", true},
		{"Success", false},
		{"success", false},
		{"Completed", false},
		{"completed", false},
	}
	for _, tt := range tests {
		if got := isFailureStatus(tt.status); got != tt.want {
			t.Errorf("isFailureStatus(%q) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestFilterOnlyFailures(t *testing.T) {
	t.Run("drops non-failures", func(t *testing.T) {
		entries := []model.ProcessHistoryEntry{
			{Status: "Error", ErrorLogFile: "a.log"},
			{Status: "Success"},
			{Status: "Error", ErrorLogFile: "b.log"},
		}
		got := filterOnlyFailures(entries)
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})

	t.Run("all-error input unchanged", func(t *testing.T) {
		entries := []model.ProcessHistoryEntry{
			{Status: "Error"}, {Status: "Error"},
		}
		got := filterOnlyFailures(entries)
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
	})
}

// --- Unit: sortHistoryByTimeDesc ---

func TestSortHistoryByTimeDesc(t *testing.T) {
	entries := []model.ProcessHistoryEntry{
		{StartTime: "2026-05-27T00:00:00Z", ErrorLogFile: "a"},
		{StartTime: "2026-05-28T12:00:00Z", ErrorLogFile: "c"},
		{StartTime: "not-a-time", ErrorLogFile: "bad"},
		{StartTime: "2026-05-28T06:00:00Z", ErrorLogFile: "b"},
	}
	sortHistoryByTimeDesc(entries)

	wantOrder := []string{"c", "b", "a", "bad"} // newest first, unparseable last
	for i, want := range wantOrder {
		if entries[i].ErrorLogFile != want {
			t.Errorf("entries[%d] = %q, want %q (order: %v)", i, entries[i].ErrorLogFile, want, wantOrder)
		}
	}
}

// --- Unit: tailHistory ---

func TestTailHistory(t *testing.T) {
	mk := func(n int) []model.ProcessHistoryEntry {
		out := make([]model.ProcessHistoryEntry, n)
		return out
	}
	tests := []struct {
		name string
		in   int
		n    int
		want int
	}{
		{"zero returns all", 3, 0, 3},
		{"negative returns all", 3, -1, 3},
		{"n less than len", 3, 2, 2},
		{"n greater than len", 3, 5, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tailHistory(mk(tt.in), tt.n)
			if len(got) != tt.want {
				t.Errorf("len = %d, want %d", len(got), tt.want)
			}
		})
	}
}

// --- Integration helpers ---

// errorLogEntries returns three error-log records (deliberately out of order)
// spanning two days. Newest: err_c.log, then err_b.log, oldest err_a.log.
func errorLogEntries() []model.ProcessErrorLog {
	return []model.ProcessErrorLog{
		{Timestamp: "2026-05-27T00:00:00Z", ProcessName: "Load", Filename: "err_a.log"},
		{Timestamp: "2026-05-28T12:00:00Z", ProcessName: "Load", Filename: "err_c.log"},
		{Timestamp: "2026-05-28T06:00:00Z", ProcessName: "Load", Filename: "err_b.log"},
	}
}

// historyListHandler serves the ErrorLogs listing and fails the test if any
// audit/message/transaction-log endpoint is hit — proving the command works
// without audit logging enabled.
func historyListHandler(t *testing.T, body []byte) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.Contains(p, "MessageLog") || strings.Contains(p, "TransactionLog") || strings.Contains(p, "AuditLog") {
			t.Errorf("unexpected audit/message-log request: %s", p)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if strings.HasSuffix(p, "/ErrorLogs") {
			w.Write(body)
			return
		}
		t.Errorf("unexpected path: %s", p)
		w.WriteHeader(http.StatusNotFound)
	}
}

// --- Integration: listing ---

func TestProcessHistory_ListTable(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load"})
		rootCmd.Execute()
	})

	for _, header := range []string{"START TIME", "STATUS", "USER", "DURATION", "ERROR LOG FILE"} {
		if !strings.Contains(out, header) {
			t.Errorf("output missing header %q\n%s", header, out)
		}
	}
	if !strings.Contains(out, "Error") {
		t.Errorf("output missing Error status\n%s", out)
	}
	// User/Duration unavailable -> rendered as "-".
	if !strings.Contains(out, "-") {
		t.Errorf("output missing dash placeholder\n%s", out)
	}
	// Most recent first: err_c before err_b before err_a.
	ic, ib, ia := strings.Index(out, "err_c.log"), strings.Index(out, "err_b.log"), strings.Index(out, "err_a.log")
	if ic < 0 || ib < 0 || ia < 0 {
		t.Fatalf("missing a filename in output\n%s", out)
	}
	if !(ic < ib && ib < ia) {
		t.Errorf("rows not sorted most-recent-first (c=%d b=%d a=%d)\n%s", ic, ib, ia, out)
	}
}

func TestProcessHistory_JSON(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--output", "json"})
		rootCmd.Execute()
	})

	var got []model.ProcessHistoryEntry
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not a JSON array: %v\n%s", err, out)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Status != "Error" || got[0].StartTime == "" {
		t.Errorf("got[0] = %+v", got[0])
	}
	if got[0].User != "" || got[0].Duration != "" {
		t.Errorf("JSON should keep user/duration empty, got %+v", got[0])
	}
}

func TestProcessHistory_Tail(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--tail", "2"})
		rootCmd.Execute()
	})

	if !strings.Contains(res.Stdout, "err_c.log") || !strings.Contains(res.Stdout, "err_b.log") {
		t.Errorf("expected two newest entries in output\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "err_a.log") {
		t.Errorf("oldest entry should be trimmed by --tail 2\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "Showing 2 of 3") {
		t.Errorf("expected truncation summary, got stderr: %s", res.Stderr)
	}
	if !strings.Contains(res.Stderr, "--tail 0") {
		t.Errorf("summary should hint --tail 0, got stderr: %s", res.Stderr)
	}
}

func TestProcessHistory_Since(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--since", "2026-05-28T10:00:00Z"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "err_c.log") {
		t.Errorf("expected err_c.log (after cutoff)\n%s", out)
	}
	if strings.Contains(out, "err_b.log") || strings.Contains(out, "err_a.log") {
		t.Errorf("entries before cutoff should be filtered out\n%s", out)
	}
}

func TestProcessHistory_OnlyFailures(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--only-failures"})
		rootCmd.Execute()
	})

	// All error-log entries are failures, so all are retained.
	for _, f := range []string{"err_a.log", "err_b.log", "err_c.log"} {
		if !strings.Contains(out, f) {
			t.Errorf("expected %s with --only-failures\n%s", f, out)
		}
	}
}

func TestProcessHistory_EmptyHistory(t *testing.T) {
	t.Run("table prints a note to stderr", func(t *testing.T) {
		setupMockTM1(t, historyListHandler(t, processErrorLogsJSON()))
		resetCmdFlags(t)

		res := captureAll(t, func() {
			rootCmd.SetArgs([]string{"process", "history", "Load"})
			rootCmd.Execute()
		})
		if !strings.Contains(res.Stderr, "No error-log history found") {
			t.Errorf("expected empty-history note on stderr, got: %s", res.Stderr)
		}
	})

	t.Run("json emits empty array", func(t *testing.T) {
		setupMockTM1(t, historyListHandler(t, processErrorLogsJSON()))
		resetCmdFlags(t)

		out := captureStdout(t, func() {
			rootCmd.SetArgs([]string{"process", "history", "Load", "--output", "json"})
			rootCmd.Execute()
		})
		if strings.TrimSpace(out) != "[]" {
			t.Errorf("expected [], got: %q", out)
		}
	})
}

// --- Integration: --show-error ---

func TestProcessHistory_ShowError(t *testing.T) {
	const content = "line1\nline2\n"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/Content") {
			w.Write([]byte(content))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", "err_c.log"})
		rootCmd.Execute()
	})

	if out != content {
		t.Errorf("show-error output = %q, want %q", out, content)
	}
}

func TestProcessHistory_ShowErrorJSON(t *testing.T) {
	const content = "boom\n"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/Content") {
			w.Write([]byte(content))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", "err_c.log", "--output", "json"})
		rootCmd.Execute()
	})

	var got map[string]string
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not JSON object: %v\n%s", err, out)
	}
	if got["file"] != "err_c.log" || got["content"] != content {
		t.Errorf("got = %+v", got)
	}
}

// TestProcessHistory_ShowErrorPrecedence verifies --show-error takes precedence:
// the content endpoint is fetched and the ErrorLogs listing endpoint is NEVER hit.
func TestProcessHistory_ShowErrorPrecedence(t *testing.T) {
	const content = "precedence-content\n"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ErrorLogs") {
			t.Errorf("listing endpoint must not be called in --show-error mode: %s", r.URL.Path)
		}
		if strings.HasSuffix(r.URL.Path, "/Content") {
			w.Write([]byte(content))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", "err_c.log", "--tail", "5", "--since", "1h"})
		rootCmd.Execute()
	})

	if out != content {
		t.Errorf("show-error output = %q, want %q", out, content)
	}
}

// --- Integration: error/edge cases ---

func TestProcessHistory_ProcessNotFound(t *testing.T) {
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	var execErr error
	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "DoesNotExist"})
		execErr = rootCmd.Execute()
	})

	if got := exitCodeForError(execErr); got != 3 {
		t.Errorf("exit code = %d, want 3", got)
	}
	if !strings.Contains(res.Stderr, "not found") {
		t.Errorf("expected not-found message, got stderr: %s", res.Stderr)
	}
}

func TestProcessHistory_ShowErrorMissingFile(t *testing.T) {
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	var execErr error
	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", "missing.log"})
		execErr = rootCmd.Execute()
	})

	if got := exitCodeForError(execErr); got != 3 {
		t.Errorf("exit code = %d, want 3", got)
	}
	if !strings.Contains(res.Stderr, "Error log file 'missing.log' not found") {
		t.Errorf("expected missing-file message, got stderr: %s", res.Stderr)
	}
}

func TestProcessHistory_InvalidSince(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	var execErr error
	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--since", "garbage"})
		execErr = rootCmd.Execute()
	})

	if got := exitCodeForError(execErr); got == 0 {
		t.Errorf("expected non-zero exit for invalid --since")
	}
	if !strings.Contains(res.Stderr, "since") {
		t.Errorf("expected --since error message, got stderr: %s", res.Stderr)
	}
}

func TestProcessHistory_NegativeTail(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, processErrorLogsJSON(errorLogEntries()...)))
	resetCmdFlags(t)

	var execErr error
	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--tail", "-1"})
		execErr = rootCmd.Execute()
	})

	if got := exitCodeForError(execErr); got == 0 {
		t.Errorf("expected non-zero exit for negative --tail")
	}
	if !strings.Contains(res.Stderr, "--tail must be non-negative") {
		t.Errorf("expected negative-tail message, got stderr: %s", res.Stderr)
	}
}

func TestProcessHistory_MalformedJSON(t *testing.T) {
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ErrorLogs") {
			w.Write([]byte("{not valid json"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	resetCmdFlags(t)

	var execErr error
	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load"})
		execErr = rootCmd.Execute()
	})

	if got := exitCodeForError(execErr); got == 0 {
		t.Errorf("expected non-zero exit for malformed JSON")
	}
	if !strings.Contains(res.Stderr, "Cannot parse server response") {
		t.Errorf("expected parse-error message, got stderr: %s", res.Stderr)
	}
}
