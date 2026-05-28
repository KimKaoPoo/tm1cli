package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// --- Unit: endpoint builders ---

func TestErrorLogFilesEndpoint(t *testing.T) {
	if got := errorLogFilesEndpoint(); got != "ErrorLogFiles" {
		t.Errorf("errorLogFilesEndpoint() = %q, want %q", got, "ErrorLogFiles")
	}
}

func TestErrorLogContentEndpoint(t *testing.T) {
	tests := []struct {
		name string
		file string
		want string
	}{
		{name: "plain", file: "TM1ProcessError_20260528090402_Load.log", want: "ErrorLogFiles('TM1ProcessError_20260528090402_Load.log')/Content"},
		{name: "space encoded", file: "with space.log", want: "ErrorLogFiles('with%20space.log')/Content"},
		// odataKey doubles the single quote ('') then url.PathEscape percent-encodes
		// each quote to %27, yielding %27%27.
		{name: "apostrophe doubled then escaped", file: "It's.log", want: "ErrorLogFiles('It%27%27s.log')/Content"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorLogContentEndpoint(tt.file); got != tt.want {
				t.Errorf("errorLogContentEndpoint(%q) = %q, want %q", tt.file, got, tt.want)
			}
		})
	}
}

// --- Unit: parseErrorLogFilename ---

func TestParseErrorLogFilename(t *testing.T) {
	tests := []struct {
		name          string
		filename      string
		wantOK        bool
		wantProc      string
		wantStartTime string
	}{
		{
			name:          "canonical",
			filename:      "TM1ProcessError_20260528090402_Load.log",
			wantOK:        true,
			wantProc:      "Load",
			wantStartTime: "2026-05-28T09:04:02Z",
		},
		{
			name:          "process name with underscores",
			filename:      "TM1ProcessError_20260528090402_Load_Data.log",
			wantOK:        true,
			wantProc:      "Load_Data",
			wantStartTime: "2026-05-28T09:04:02Z",
		},
		{
			name:          "non-numeric timestamp slot — structurally ok, empty start time",
			filename:      "TM1ProcessError_AAAAAAAAAAAAAA_Load.log",
			wantOK:        true,
			wantProc:      "Load",
			wantStartTime: "",
		},
		{name: "missing prefix", filename: "TM1Other_20260528090402_Load.log", wantOK: false},
		{name: "missing .log suffix", filename: "TM1ProcessError_20260528090402_Load.txt", wantOK: false},
		{name: "missing process name", filename: "TM1ProcessError_20260528090402.log", wantOK: false},
		{name: "empty process name after separator", filename: "TM1ProcessError_20260528090402_.log", wantOK: false},
		{name: "timestamp slot too short", filename: "TM1ProcessError_2026_Load.log", wantOK: false},
		{name: "no separator after timestamp slot", filename: "TM1ProcessError_20260528090402ZLoad.log", wantOK: false},
		{name: "empty filename", filename: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProc, gotStart, gotOK := parseErrorLogFilename(tt.filename)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotOK {
				if gotProc != tt.wantProc {
					t.Errorf("procName = %q, want %q", gotProc, tt.wantProc)
				}
				if gotStart != tt.wantStartTime {
					t.Errorf("startTime = %q, want %q", gotStart, tt.wantStartTime)
				}
			}
		})
	}
}

// --- Unit: buildHistoryEntries ---

func TestBuildHistoryEntries(t *testing.T) {
	files := []model.ErrorLogFile{
		{Filename: "TM1ProcessError_20260528090402_Load.log"},      // match (Load)
		{Filename: "TM1ProcessError_20260527000000_Load.log"},      // match (Load)
		{File: "TM1ProcessError_20260528000000_Load.log"},          // match via File fallback
		{Filename: "TM1ProcessError_20260528090402_OtherProc.log"}, // different process — drop
		{Filename: "Other.log"},                                    // non-canonical — drop
		{Filename: ""},                                             // empty — drop
		{Filename: "TM1ProcessError_AAAAAAAAAAAAAA_Load.log"},      // canonical shape, unparseable ts
	}

	entries := buildHistoryEntries(files, "Load")
	if entries == nil {
		t.Fatal("buildHistoryEntries returned nil slice; want non-nil")
	}
	if len(entries) != 4 {
		t.Fatalf("len = %d, want 4 (3 canonical Load + 1 unparseable-ts Load)", len(entries))
	}

	for i, e := range entries {
		if e.Status != processHistoryStatusError {
			t.Errorf("entry[%d].Status = %q, want %q", i, e.Status, processHistoryStatusError)
		}
		if e.User != "" || e.Duration != "" {
			t.Errorf("entry[%d] User/Duration must be empty, got %q/%q", i, e.User, e.Duration)
		}
		if e.ErrorLogFile == "" {
			t.Errorf("entry[%d].ErrorLogFile is empty", i)
		}
	}

	// Spot-check the first row: canonical filename → parsed StartTime.
	if entries[0].StartTime != "2026-05-28T09:04:02Z" {
		t.Errorf("entries[0].StartTime = %q, want 2026-05-28T09:04:02Z", entries[0].StartTime)
	}
	// Unparseable-timestamp row keeps empty StartTime.
	var foundUnparseable bool
	for _, e := range entries {
		if e.ErrorLogFile == "TM1ProcessError_AAAAAAAAAAAAAA_Load.log" {
			foundUnparseable = true
			if e.StartTime != "" {
				t.Errorf("unparseable-ts row should have empty StartTime, got %q", e.StartTime)
			}
		}
	}
	if !foundUnparseable {
		t.Error("expected to find the unparseable-timestamp Load row in output")
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
		return make([]model.ProcessHistoryEntry, n)
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

// errorLogFileFixtures returns canonical TM1 error-log files for process "Load"
// (deliberately out of order, newest = err_c) plus a noise file belonging to a
// different process to confirm client-side filtering.
//
// "Load" entries:
//
//	err_a: 2026-05-27T00:00:00Z
//	err_b: 2026-05-28T06:00:00Z
//	err_c: 2026-05-28T12:00:00Z (newest)
//
// "OtherProc" noise must be filtered out by process filter.
const (
	errFileA     = "TM1ProcessError_20260527000000_Load.log"
	errFileB     = "TM1ProcessError_20260528060000_Load.log"
	errFileC     = "TM1ProcessError_20260528120000_Load.log"
	errFileNoise = "TM1ProcessError_20260528090000_OtherProc.log"
)

func errorLogFileFixtures() []model.ErrorLogFile {
	return []model.ErrorLogFile{
		{Filename: errFileA},
		{Filename: errFileC},
		{Filename: errFileNoise},
		{Filename: errFileB},
	}
}

// historyListHandler serves the ErrorLogFiles listing and fails the test if any
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
		if strings.HasSuffix(p, "/ErrorLogFiles") {
			w.Write(body)
			return
		}
		t.Errorf("unexpected path: %s", p)
		w.WriteHeader(http.StatusNotFound)
	}
}

// --- Integration: listing ---

func TestProcessHistory_ListTable(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
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
	// Noise file (different process) must be filtered out.
	if strings.Contains(out, errFileNoise) {
		t.Errorf("noise file for OtherProc must not appear in Load history\n%s", out)
	}
	// Most recent first: err_c before err_b before err_a.
	ic, ib, ia := strings.Index(out, errFileC), strings.Index(out, errFileB), strings.Index(out, errFileA)
	if ic < 0 || ib < 0 || ia < 0 {
		t.Fatalf("missing a Load filename in output\n%s", out)
	}
	if !(ic < ib && ib < ia) {
		t.Errorf("rows not sorted most-recent-first (c=%d b=%d a=%d)\n%s", ic, ib, ia, out)
	}
}

func TestProcessHistory_JSON(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
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
		t.Fatalf("len = %d, want 3 (noise file should be filtered)", len(got))
	}
	if got[0].Status != "Error" || got[0].StartTime == "" {
		t.Errorf("got[0] = %+v", got[0])
	}
	if got[0].User != "" || got[0].Duration != "" {
		t.Errorf("JSON should keep user/duration empty, got %+v", got[0])
	}
}

func TestProcessHistory_Tail(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
	resetCmdFlags(t)

	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--tail", "2"})
		rootCmd.Execute()
	})

	if !strings.Contains(res.Stdout, errFileC) || !strings.Contains(res.Stdout, errFileB) {
		t.Errorf("expected two newest entries in output\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, errFileA) {
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
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--since", "2026-05-28T10:00:00Z"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, errFileC) {
		t.Errorf("expected newest entry (after cutoff)\n%s", out)
	}
	if strings.Contains(out, errFileB) || strings.Contains(out, errFileA) {
		t.Errorf("entries before cutoff should be filtered out\n%s", out)
	}
}

func TestProcessHistory_OnlyFailures(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
	resetCmdFlags(t)

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--only-failures"})
		rootCmd.Execute()
	})

	// All error-log entries are failures, so all 3 Load entries are retained.
	for _, f := range []string{errFileA, errFileB, errFileC} {
		if !strings.Contains(out, f) {
			t.Errorf("expected %s with --only-failures\n%s", f, out)
		}
	}
}

func TestProcessHistory_EmptyHistory(t *testing.T) {
	t.Run("table prints a note to stderr", func(t *testing.T) {
		setupMockTM1(t, historyListHandler(t, errorLogFilesJSON()))
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
		setupMockTM1(t, historyListHandler(t, errorLogFilesJSON()))
		resetCmdFlags(t)

		out := captureStdout(t, func() {
			rootCmd.SetArgs([]string{"process", "history", "Load", "--output", "json"})
			rootCmd.Execute()
		})
		if strings.TrimSpace(out) != "[]" {
			t.Errorf("expected [], got: %q", out)
		}
	})

	t.Run("no files match this process", func(t *testing.T) {
		// Server returns files but none for "Load".
		setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(model.ErrorLogFile{Filename: errFileNoise})))
		resetCmdFlags(t)

		res := captureAll(t, func() {
			rootCmd.SetArgs([]string{"process", "history", "Load"})
			rootCmd.Execute()
		})
		if !strings.Contains(res.Stderr, "No error-log history found") {
			t.Errorf("expected empty-history note when no files match, got: %s", res.Stderr)
		}
	})
}

// TestProcessHistory_FilteredEmpty verifies that when matching files exist but
// a filter removes every row, the note distinguishes that from "no history".
func TestProcessHistory_FilteredEmpty(t *testing.T) {
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
	resetCmdFlags(t)

	res := captureAll(t, func() {
		rootCmd.SetArgs([]string{"process", "history", "Load", "--since", "2030-01-01T00:00:00Z"})
		rootCmd.Execute()
	})
	if !strings.Contains(res.Stderr, "No runs match the filter") {
		t.Errorf("expected filtered-empty note, got stderr: %s", res.Stderr)
	}
	if strings.Contains(res.Stderr, "No error-log history found") {
		t.Errorf("should not report 'no history' when entries exist but are filtered out: %s", res.Stderr)
	}
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
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", errFileC})
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
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", errFileC, "--output", "json"})
		rootCmd.Execute()
	})

	var got map[string]string
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not JSON object: %v\n%s", err, out)
	}
	if got["file"] != errFileC || got["content"] != content {
		t.Errorf("got = %+v", got)
	}
}

// TestProcessHistory_ShowErrorPrecedence verifies --show-error takes precedence:
// the content endpoint is fetched and the listing endpoint is NEVER hit.
func TestProcessHistory_ShowErrorPrecedence(t *testing.T) {
	const content = "precedence-content\n"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ErrorLogFiles") {
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
		rootCmd.SetArgs([]string{"process", "history", "Load", "--show-error", errFileC, "--tail", "5", "--since", "1h"})
		rootCmd.Execute()
	})

	if out != content {
		t.Errorf("show-error output = %q, want %q", out, content)
	}
}

// --- Integration: error/edge cases ---

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
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
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
	setupMockTM1(t, historyListHandler(t, errorLogFilesJSON(errorLogFileFixtures()...)))
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
		if strings.HasSuffix(r.URL.Path, "/ErrorLogFiles") {
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
