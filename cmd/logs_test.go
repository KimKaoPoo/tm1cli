package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// ============================================================
// Unit Tests — parseSince
// ============================================================

func TestParseSince(t *testing.T) {
	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{
			name:  "empty input returns empty",
			input: "",
			want:  "",
		},
		{
			name:  "parse 10m duration",
			input: "10m",
			want:  "2026-04-25T11:50:00Z",
		},
		{
			name:  "parse 2h duration",
			input: "2h",
			want:  "2026-04-25T10:00:00Z",
		},
		{
			name:  "parse RFC3339 absolute",
			input: "2026-04-24T10:00:00Z",
			want:  "2026-04-24T10:00:00Z",
		},
		{
			name:  "parse short timestamp without seconds",
			input: "2026-04-24T10:00",
			want:  "2026-04-24T10:00:00Z",
		},
		{
			name:  "parse date-only",
			input: "2026-04-24",
			want:  "2026-04-24T00:00:00Z",
		},
		{
			name:    "error on garbage",
			input:   "garbage",
			wantErr: "not a duration",
		},
		{
			name:    "error on negative duration",
			input:   "-5m",
			wantErr: "positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSince(tt.input, now)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("parseSince(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit Tests — validateLevel
// ============================================================

func TestValidateLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr string
	}{
		{"info", "Info", ""},
		{"INFO", "Info", ""},
		{"Info", "Info", ""},
		{"warn", "Warning", ""},
		{"warning", "Warning", ""},
		{"WARNING", "Warning", ""},
		{"error", "Error", ""},
		{"err", "Error", ""},
		{"ERROR", "Error", ""},
		{"fatal", "Fatal", ""},
		{"FATAL", "Fatal", ""},
		{"debug", "Debug", ""},
		{"DEBUG", "Debug", ""},
		{"unknown", "Unknown", ""},
		{"UNKNOWN", "Unknown", ""},
		{"off", "Off", ""},
		{"OFF", "Off", ""},
		{"", "", ""},
		{"trace", "", "info, warn, error"},
		{"verbose", "", "info, warn, error"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			got, err := validateLevel(tt.input)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("validateLevel(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit Tests — parseTimeStamp
// ============================================================

func TestParseTimeStamp(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantUTC string
		wantErr bool
	}{
		{
			name:    "RFC3339 with Z",
			input:   "2026-04-25T10:00:00Z",
			wantUTC: "2026-04-25T10:00:00Z",
		},
		{
			name:    "RFC3339 with positive offset",
			input:   "2026-04-25T18:00:00+08:00",
			wantUTC: "2026-04-25T10:00:00Z",
		},
		{
			name:    "RFC3339Nano with fractional seconds",
			input:   "2026-04-25T10:00:00.123456789Z",
			wantUTC: "2026-04-25T10:00:00.123456789Z",
		},
		{
			name:    "no-Z timestamp",
			input:   "2026-04-25T10:00:00",
			wantUTC: "2026-04-25T10:00:00Z",
		},
		{
			name:    "invalid",
			input:   "not-a-time",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimeStamp(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got time %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantT, _ := time.Parse(time.RFC3339Nano, tt.wantUTC)
			if !got.Equal(wantT) {
				t.Errorf("parseTimeStamp(%q) = %v, want %v", tt.input, got.UTC(), wantT.UTC())
			}
		})
	}
}

// ============================================================
// Unit Tests — buildMessageLogFilter
// ============================================================

func TestBuildMessageLogFilter(t *testing.T) {
	tests := []struct {
		name    string
		sinceTS string
		level   string
		want    string
	}{
		{
			name:    "empty inputs returns empty",
			sinceTS: "",
			level:   "",
			want:    "",
		},
		{
			name:    "sinceTS only",
			sinceTS: "2026-04-25T10:00:00Z",
			level:   "",
			want:    "TimeStamp ge 2026-04-25T10:00:00Z",
		},
		{
			name:    "level only",
			sinceTS: "",
			level:   "Error",
			want:    "Level eq 'Error'",
		},
		{
			name:    "both sinceTS and level",
			sinceTS: "2026-04-25T10:00:00Z",
			level:   "Error",
			want:    "TimeStamp ge 2026-04-25T10:00:00Z and Level eq 'Error'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMessageLogFilter(tt.sinceTS, tt.level)
			if got != tt.want {
				t.Errorf("buildMessageLogFilter(%q, %q) = %q, want %q", tt.sinceTS, tt.level, got, tt.want)
			}
			// Regression guard: User must NEVER appear in the filter.
			if strings.Contains(got, "User") {
				t.Errorf("filter must not include 'User', got %q", got)
			}
		})
	}
}

// ============================================================
// Unit Tests — buildMessageLogQuery
// ============================================================

func TestBuildMessageLogQuery(t *testing.T) {
	tests := []struct {
		name      string
		filter    string
		top       int
		orderDesc bool
		wantParts []string
	}{
		{
			name: "no flags returns base endpoint only",
		},
		{
			name:      "filter only",
			filter:    "TimeStamp ge 2026-04-25T10:00:00Z",
			wantParts: []string{"$filter="},
		},
		{
			name:      "all three: filter top orderDesc",
			filter:    "Level eq 'Error'",
			top:       50,
			orderDesc: true,
			wantParts: []string{"$filter=", "$top=50", "$orderby="},
		},
		{
			name:      "top only",
			top:       100,
			orderDesc: false,
			wantParts: []string{"$top=100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMessageLogQuery(tt.filter, tt.top, tt.orderDesc)
			if !strings.HasPrefix(got, "MessageLogEntries") {
				t.Errorf("query should start with 'MessageLogEntries', got %q", got)
			}
			if tt.filter == "" && tt.top == 0 && !tt.orderDesc {
				if got != "MessageLogEntries" {
					t.Errorf("got %q, want 'MessageLogEntries'", got)
				}
				return
			}
			// Decode the URL-encoded query for readable assertions.
			decoded, _ := decodedQuery(got)
			for _, part := range tt.wantParts {
				if !strings.Contains(decoded, part) {
					t.Errorf("decoded query %q should contain %q", decoded, part)
				}
			}
		})
	}
}

// ============================================================
// Unit Tests — applyClientFilters
// ============================================================

func TestApplyClientFilters(t *testing.T) {
	baseEntries := []model.MessageLogEntry{
		{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", User: "alice", Message: "foo bar"},
		{ID: "2", TimeStamp: "2026-04-25T11:00:00Z", Level: "Error", User: "bob", Message: "BAR baz"},
		{ID: "3", TimeStamp: "2026-04-25T12:00:00Z", Level: "Warning", User: "alice", Message: "barbaz qux"},
		{ID: "4", TimeStamp: "2026-04-25T09:00:00Z", Level: "Info", User: "", Message: "alice did X"},
		{ID: "5", TimeStamp: "2026-04-25T09:30:00Z", Level: "Info", User: "", Message: "bob did Y"},
	}

	t.Run("contains matches case-insensitively", func(t *testing.T) {
		got := applyClientFilters(baseEntries, "", "", "", "bar")
		if len(got) != 3 {
			t.Fatalf("got %d entries, want 3", len(got))
		}
		ids := map[string]bool{}
		for _, e := range got {
			ids[e.ID] = true
		}
		for _, id := range []string{"1", "2", "3"} {
			if !ids[id] {
				t.Errorf("expected ID %q in results", id)
			}
		}
	})

	t.Run("user matches User field when present", func(t *testing.T) {
		got := applyClientFilters(baseEntries, "", "", "alice", "")
		// Should match ID 1 (User=alice), 3 (User=alice), and 4 (User="" but Message has "alice")
		if len(got) != 3 {
			t.Fatalf("got %d entries, want 3", len(got))
		}
	})

	t.Run("user matches Message when User field is empty", func(t *testing.T) {
		// Only entries with User="" should fall through to message match
		entries := []model.MessageLogEntry{
			{ID: "4", TimeStamp: "2026-04-25T09:00:00Z", Level: "Info", User: "", Message: "alice did X"},
			{ID: "5", TimeStamp: "2026-04-25T09:30:00Z", Level: "Info", User: "", Message: "bob did Y"},
		}
		got := applyClientFilters(entries, "", "", "alice", "")
		if len(got) != 1 || got[0].ID != "4" {
			t.Errorf("expected only entry 4, got %v", got)
		}
	})

	t.Run("since (fallback) drops older entries", func(t *testing.T) {
		// entries 4 and 5 are at 09:00 and 09:30; since 10:00 should drop them
		got := applyClientFilters(baseEntries, "2026-04-25T10:00:00Z", "", "", "")
		for _, e := range got {
			if e.ID == "4" || e.ID == "5" {
				t.Errorf("entry %q should have been dropped by since filter", e.ID)
			}
		}
	})

	t.Run("level (fallback) keeps only matching", func(t *testing.T) {
		got := applyClientFilters(baseEntries, "", "Error", "", "")
		if len(got) != 1 || got[0].ID != "2" {
			t.Errorf("expected only ID=2 (Error), got %v", got)
		}
	})

	t.Run("all four together keeps only matching entries", func(t *testing.T) {
		// All four: since=10:00, level=Error, user=bob, contains=bar
		got := applyClientFilters(baseEntries, "2026-04-25T10:00:00Z", "Error", "bob", "bar")
		if len(got) != 1 || got[0].ID != "2" {
			t.Errorf("expected only ID=2, got %v", got)
		}
	})
}

// ============================================================
// Unit Tests — sortEntriesByTimeStamp
// ============================================================

func TestSortEntriesByTimeStamp(t *testing.T) {
	t.Run("sorts ascending by parsed time", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "3", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "2", TimeStamp: "2026-04-25T11:00:00Z"},
		}
		sortEntriesByTimeStamp(entries)
		wantIDs := []string{"1", "2", "3"}
		for i, id := range wantIDs {
			if entries[i].ID != id {
				t.Errorf("entries[%d].ID = %q, want %q", i, entries[i].ID, id)
			}
		}
	})

	t.Run("ties broken by ID ascending", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "b", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "a", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "c", TimeStamp: "2026-04-25T10:00:00Z"},
		}
		sortEntriesByTimeStamp(entries)
		if entries[0].ID != "a" || entries[1].ID != "b" || entries[2].ID != "c" {
			t.Errorf("expected [a, b, c], got [%s, %s, %s]", entries[0].ID, entries[1].ID, entries[2].ID)
		}
	})

	t.Run("entries with unparseable timestamps go to the end", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "bad", TimeStamp: "not-a-time"},
			{ID: "good1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "good2", TimeStamp: "2026-04-25T11:00:00Z"},
		}
		sortEntriesByTimeStamp(entries)
		if entries[0].ID != "good1" {
			t.Errorf("expected good1 first, got %q", entries[0].ID)
		}
		if entries[1].ID != "good2" {
			t.Errorf("expected good2 second, got %q", entries[1].ID)
		}
		if entries[2].ID != "bad" {
			t.Errorf("expected bad last, got %q", entries[2].ID)
		}
	})
}

// ============================================================
// Unit Tests — reverseEntries
// ============================================================

func TestReverseEntries(t *testing.T) {
	t.Run("reverses 3 entries in place", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "1"},
			{ID: "2"},
			{ID: "3"},
		}
		reverseEntries(entries)
		if entries[0].ID != "3" || entries[1].ID != "2" || entries[2].ID != "1" {
			t.Errorf("expected [3,2,1], got [%s,%s,%s]", entries[0].ID, entries[1].ID, entries[2].ID)
		}
	})

	t.Run("empty slice no-op", func(t *testing.T) {
		entries := []model.MessageLogEntry{}
		reverseEntries(entries)
		if len(entries) != 0 {
			t.Errorf("expected empty slice, got %v", entries)
		}
	})
}

// ============================================================
// Unit Tests — sanitizeRawMessage
// ============================================================

func TestSanitizeRawMessage(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"a\nb", "a b"},
		{"a\r\nb", "a b"},
		{"a\tb", "a b"},
		{"no special chars", "no special chars"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			got := sanitizeRawMessage(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeRawMessage(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit Tests — boundaryIDs
// ============================================================

func TestBoundaryIDs(t *testing.T) {
	t.Run("empty returns empty", func(t *testing.T) {
		maxTS, ids := boundaryIDs(nil)
		if maxTS != "" {
			t.Errorf("maxTS = %q, want empty", maxTS)
		}
		if ids != nil {
			t.Errorf("ids should be nil, got %v", ids)
		}
	})

	t.Run("single entry returns its TS and ID", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "abc", TimeStamp: "2026-04-25T10:00:00Z"},
		}
		maxTS, ids := boundaryIDs(entries)
		if maxTS != "2026-04-25T10:00:00Z" {
			t.Errorf("maxTS = %q, want 2026-04-25T10:00:00Z", maxTS)
		}
		if _, ok := ids["abc"]; !ok {
			t.Error("expected 'abc' in boundary IDs")
		}
	})

	t.Run("multiple distinct timestamps returns max TS and its IDs", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "2", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "3", TimeStamp: "2026-04-25T11:00:00Z"},
		}
		maxTS, ids := boundaryIDs(entries)
		if maxTS != "2026-04-25T12:00:00Z" {
			t.Errorf("maxTS = %q, want 2026-04-25T12:00:00Z", maxTS)
		}
		if _, ok := ids["2"]; !ok {
			t.Error("expected '2' in boundary IDs")
		}
		if _, ok := ids["1"]; ok {
			t.Error("'1' should NOT be in boundary IDs")
		}
	})

	t.Run("multiple entries at same max TS — all their IDs in set", func(t *testing.T) {
		entries := []model.MessageLogEntry{
			{ID: "a", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "b", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "c", TimeStamp: "2026-04-25T10:00:00Z"},
		}
		maxTS, ids := boundaryIDs(entries)
		if maxTS != "2026-04-25T12:00:00Z" {
			t.Errorf("maxTS = %q, want 2026-04-25T12:00:00Z", maxTS)
		}
		for _, id := range []string{"a", "b"} {
			if _, ok := ids[id]; !ok {
				t.Errorf("expected %q in boundary IDs", id)
			}
		}
		if _, ok := ids["c"]; ok {
			t.Error("'c' should NOT be in boundary IDs")
		}
	})
}

// ============================================================
// Unit Tests — isFilterRejection
// ============================================================

func TestIsFilterRejection(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "HTTP 400 with $filter body is rejection",
			err:  fmt.Errorf("HTTP 400: Invalid $filter expression"),
			want: true,
		},
		{
			name: "HTTP 400 with orderby not supported",
			err:  fmt.Errorf("HTTP 400: orderby not supported"),
			want: true,
		},
		{
			name: "HTTP 501 Not implemented",
			err:  fmt.Errorf("HTTP 501: Not implemented"),
			want: true,
		},
		{
			name: "Authentication failed is not rejection",
			err:  fmt.Errorf("Authentication failed."),
			want: false,
		},
		{
			name: "HTTP 500 server error is not rejection",
			err:  fmt.Errorf("HTTP 500: Internal server error"),
			want: false,
		},
		{
			name: "HTTP 400 without filter keywords is not rejection",
			err:  fmt.Errorf("HTTP 400: Bad request"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isFilterRejection(tt.err)
			if got != tt.want {
				t.Errorf("isFilterRejection(%q) = %v, want %v", tt.err.Error(), got, tt.want)
			}
		})
	}
}

// ============================================================
// Integration Tests — output and basic flow
// ============================================================

func TestDefaultTailIfUnbounded(t *testing.T) {
	tests := []struct {
		name  string
		since string
		tail  int
		want  int
	}{
		{"no flags defaults to 100", "", 0, 100},
		{"explicit tail preserved", "", 50, 50},
		{"since set, tail stays 0", "2026-04-25T10:00:00Z", 0, 0},
		{"both set", "2026-04-25T10:00:00Z", 50, 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultTailIfUnbounded(tt.since, tt.tail); got != tt.want {
				t.Errorf("defaultTailIfUnbounded(%q, %d) = %d, want %d", tt.since, tt.tail, got, tt.want)
			}
		})
	}
}

func TestRunLogsMessages_DefaultsToTail100(t *testing.T) {
	resetCmdFlags(t)

	var capturedURL string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "first"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "second"},
			model.MessageLogEntry{ID: "3", TimeStamp: "2026-04-25T10:02:00Z", Level: "Info", Message: "third"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, err := decodedQuery(capturedURL)
	if err != nil {
		t.Fatalf("cannot decode query: %v", err)
	}
	if !strings.Contains(decoded, "$top=100") {
		t.Errorf("URL query %q should contain $top=100", decoded)
	}
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("URL query %q should contain $orderby=TimeStamp desc", decoded)
	}
	// Entries reversed for chronological display
	if !strings.Contains(out.Stdout, "first") {
		t.Errorf("stdout should contain 'first'")
	}
}

func TestRunLogsMessages_TableOutput(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", User: "alice", Logger: "System", Message: "msg1"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Error", User: "bob", Logger: "App", Message: "msg2"},
			model.MessageLogEntry{ID: "3", TimeStamp: "2026-04-25T10:02:00Z", Level: "Warning", User: "carol", Logger: "DB", Message: "msg3"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, header := range []string{"TIME", "LEVEL", "USER", "LOGGER", "MESSAGE"} {
		if !strings.Contains(out.Stdout, header) {
			t.Errorf("stdout missing header %q", header)
		}
	}
	for _, msg := range []string{"msg1", "msg2", "msg3"} {
		if !strings.Contains(out.Stdout, msg) {
			t.Errorf("stdout missing %q", msg)
		}
	}
}

func TestRunLogsMessages_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "hello"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Error", Message: "world"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result []model.MessageLogEntry
	if err := json.Unmarshal([]byte(out.Stdout), &result); err != nil {
		t.Fatalf("stdout is not valid JSON array: %v\noutput: %s", err, out.Stdout)
	}
	if len(result) != 2 {
		t.Errorf("got %d entries, want 2", len(result))
	}
}

func TestRunLogsMessages_RawOutput(t *testing.T) {
	resetCmdFlags(t)
	logsMsgRaw = true

	// Mock returns entries in descending order (newest-first) as TM1 does for --tail.
	// After reverseEntries, the earlier entry (alice, with embedded newline) comes first.
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Error", User: "bob", Message: "normal"},
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", User: "alice", Message: "line1\nline2"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	lines := strings.Split(strings.TrimRight(out.Stdout, "\n"), "\n")
	// Should have exactly 2 lines (embedded newline sanitized to space)
	if len(lines) != 2 {
		t.Errorf("expected 2 lines in raw output, got %d:\n%s", len(lines), out.Stdout)
	}
	// After reversal: chronological — alice (T1=10:00) first, bob (T2=10:01) second.
	// Embedded newline replaced with space.
	if !strings.Contains(lines[0], "line1 line2") {
		t.Errorf("line[0] should contain 'line1 line2' (sanitized newline), got %q", lines[0])
	}
	// Raw format check: <time> <level> [<user>] <msg>
	if !strings.Contains(lines[0], "[alice]") {
		t.Errorf("line[0] should contain '[alice]', got %q", lines[0])
	}
}

func TestRunLogsMessages_RawWithJSONRejected(t *testing.T) {
	resetCmdFlags(t)
	logsMsgRaw = true
	flagOutput = "json"

	// No mock server needed — should error before making requests.
	// We need a config to exist so loadConfig succeeds.
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make any HTTP request")
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "--raw cannot be combined with --output json") {
		t.Errorf("stderr should contain conflict message, got: %q", out.Stderr)
	}
}

func TestRunLogsMessages_TailOrdering(t *testing.T) {
	resetCmdFlags(t)
	logsMsgTail = 2

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		// Return descending: newest first
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "3", TimeStamp: "2026-04-25T12:00:00Z", Level: "Info", Message: "third"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T11:00:00Z", Level: "Info", Message: "second"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "$top=2") {
		t.Errorf("query %q should contain $top=2", decoded)
	}
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("query %q should contain $orderby=TimeStamp desc", decoded)
	}

	// Chronological order in output: "second" should appear before "third"
	idxSecond := strings.Index(out.Stdout, "second")
	idxThird := strings.Index(out.Stdout, "third")
	if idxSecond == -1 || idxThird == -1 {
		t.Fatalf("stdout should contain both 'second' and 'third', got:\n%s", out.Stdout)
	}
	if idxSecond > idxThird {
		t.Errorf("'second' should appear before 'third' in chronological output")
	}
}

func TestRunLogsMessages_LevelFilter_ServerSide(t *testing.T) {
	resetCmdFlags(t)
	logsMsgLevel = "error"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Error", Message: "oops"},
		))
	})

	captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "$filter=") {
		t.Errorf("query should contain $filter, got %q", decoded)
	}
	if !strings.Contains(decoded, "Level eq 'Error'") {
		t.Errorf("filter should contain Level eq 'Error', got %q", decoded)
	}
}

func TestRunLogsMessages_SinceDuration(t *testing.T) {
	resetCmdFlags(t)
	logsMsgSince = "1h"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON())
	})

	captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "TimeStamp ge ") {
		t.Errorf("filter should contain 'TimeStamp ge ', got %q", decoded)
	}
	// Should be an RFC3339 UTC timestamp roughly 1h before now
	if !strings.Contains(decoded, "T") || !strings.Contains(decoded, "Z") {
		t.Errorf("filter should contain an RFC3339 UTC timestamp, got %q", decoded)
	}
}

func TestRunLogsMessages_SinceAbsolute(t *testing.T) {
	resetCmdFlags(t)
	logsMsgSince = "2026-04-24T10:00"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON())
	})

	captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "TimeStamp ge 2026-04-24T10:00:00Z") {
		t.Errorf("filter should contain 'TimeStamp ge 2026-04-24T10:00:00Z', got %q", decoded)
	}
}

func TestRunLogsMessages_ContainsClientSide(t *testing.T) {
	resetCmdFlags(t)
	logsMsgContains = "bar"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "foo"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "BAR baz"},
			model.MessageLogEntry{ID: "3", TimeStamp: "2026-04-25T10:02:00Z", Level: "Info", Message: "barbaz"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if strings.Contains(decoded, "contains(") {
		t.Errorf("server-side filter should NOT include contains(), got %q", decoded)
	}
	if !strings.Contains(out.Stdout, "BAR") {
		t.Errorf("stdout should contain 'BAR', got:\n%s", out.Stdout)
	}
	if !strings.Contains(out.Stdout, "barbaz") {
		t.Errorf("stdout should contain 'barbaz', got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "foo") {
		t.Errorf("stdout should NOT contain 'foo' (filtered out), got:\n%s", out.Stdout)
	}
}

func TestRunLogsMessages_UserClientSide_FromUserField(t *testing.T) {
	resetCmdFlags(t)
	logsMsgUser = "alice"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", User: "alice", Message: "msg1"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", User: "bob", Message: "msg2"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if strings.Contains(decoded, "User eq") {
		t.Errorf("server-side filter should NOT include User eq, got %q", decoded)
	}
	if !strings.Contains(out.Stdout, "msg1") {
		t.Errorf("stdout should contain 'msg1' (alice), got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "msg2") {
		t.Errorf("stdout should NOT contain 'msg2' (bob), got:\n%s", out.Stdout)
	}
}

func TestRunLogsMessages_UserClientSide_FromMessage(t *testing.T) {
	resetCmdFlags(t)
	logsMsgUser = "alice"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", User: "", Message: "alice did X"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", User: "", Message: "bob did Y"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(out.Stdout, "alice did X") {
		t.Errorf("stdout should contain 'alice did X', got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "bob did Y") {
		t.Errorf("stdout should NOT contain 'bob did Y', got:\n%s", out.Stdout)
	}
}

// ============================================================
// Integration Tests — filter fallback
// ============================================================

func TestRunLogsMessages_FilterFallback(t *testing.T) {
	resetCmdFlags(t)
	logsMsgLevel = "error"

	var requestCount int32
	var secondQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		query := r.URL.RawQuery
		if strings.Contains(query, "%24filter") || strings.Contains(query, "$filter") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("$filter not supported"))
			return
		}
		secondQuery = query
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "info msg"},
			model.MessageLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", Level: "Error", Message: "error msg"},
		))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "[warn]") {
		t.Errorf("stderr should contain [warn] message, got: %q", out.Stderr)
	}
	if !strings.Contains(out.Stderr, "filtering locally") {
		t.Errorf("stderr should mention 'filtering locally', got: %q", out.Stderr)
	}
	if !strings.Contains(out.Stdout, "error msg") {
		t.Errorf("stdout should contain 'error msg', got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "info msg") {
		t.Errorf("stdout should NOT contain 'info msg' (client-filtered), got:\n%s", out.Stdout)
	}
	// Fallback request must preserve $top=100 and $orderby
	decodedSecond, _ := decodedQuery(secondQuery)
	if !strings.Contains(decodedSecond, "$top=100") {
		t.Errorf("fallback query %q should still contain $top=100", decodedSecond)
	}
}

func TestRunLogsMessages_NoFallbackOn401(t *testing.T) {
	resetCmdFlags(t)

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Unauthorized"))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	count := atomic.LoadInt32(&requestCount)
	if count != 1 {
		t.Errorf("expected exactly 1 request (no retry on 401), got %d", count)
	}
	if !strings.Contains(out.Stderr, "Authentication failed") {
		t.Errorf("stderr should contain auth error, got: %q", out.Stderr)
	}
}

func TestRunLogsMessages_NoFallbackOn404(t *testing.T) {
	resetCmdFlags(t)

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Not found"))
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	count := atomic.LoadInt32(&requestCount)
	if count != 1 {
		t.Errorf("expected exactly 1 request (no retry on 404), got %d", count)
	}
	if !strings.Contains(out.Stderr, "Not found") {
		t.Errorf("stderr should contain 'Not found', got: %q", out.Stderr)
	}
}

// ============================================================
// Integration Tests — validation errors
// ============================================================

func TestRunLogsMessages_InvalidLevel(t *testing.T) {
	resetCmdFlags(t)
	logsMsgLevel = "trace"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make any HTTP request for invalid level")
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "info, warn, error") {
		t.Errorf("stderr should contain level help, got: %q", out.Stderr)
	}
}

func TestRunLogsMessages_InvalidSince(t *testing.T) {
	resetCmdFlags(t)
	logsMsgSince = "yesterday"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make any HTTP request for invalid since")
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "not a duration") {
		t.Errorf("stderr should contain 'not a duration', got: %q", out.Stderr)
	}
}

// ============================================================
// Integration Tests — empty / edge cases
// ============================================================

func TestRunLogsMessages_EmptyResponse(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON())
	})

	out := captureAll(t, func() {
		err := runLogsMessages(logsMessagesCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should still print headers
	if !strings.Contains(out.Stdout, "TIME") {
		t.Errorf("stdout should contain 'TIME' header even for empty response, got:\n%s", out.Stdout)
	}
}

func TestRunLogsMessages_MissingUserField(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Entry has no User field
		w.Write(messageLogJSON(
			model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "test message"},
		))
	})

	t.Run("table renders empty user", func(t *testing.T) {
		out := captureAll(t, func() {
			err := runLogsMessages(logsMessagesCmd, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(out.Stdout, "test message") {
			t.Errorf("table should contain 'test message', got:\n%s", out.Stdout)
		}
	})

	t.Run("raw renders dash for missing user", func(t *testing.T) {
		resetCmdFlags(t)
		logsMsgRaw = true
		setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", Level: "Info", Message: "test message"},
			))
		})
		out := captureAll(t, func() {
			err := runLogsMessages(logsMessagesCmd, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(out.Stdout, "[-]") {
			t.Errorf("raw output should show '[-]' for missing user, got:\n%s", out.Stdout)
		}
	})
}

// ============================================================
// Integration Tests — follow-loop (no real signals)
// ============================================================

func TestFollowMessageLogs_PollsAndAdvancesWatermark(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32
	var capturedFilters []string

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		decoded, _ := decodedQuery(r.URL.RawQuery)
		capturedFilters = append(capturedFilters, decoded)

		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "a", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "entry-T1"},
			))
		case 2:
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "b", TimeStamp: "2026-04-25T10:02:00Z", Level: "Info", Message: "entry-T2"},
			))
		default:
			w.Write(messageLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followMessageLogs(ctx, cl, "2026-04-25T10:00:00Z", nil, "", "", "", 5*time.Millisecond, false, false)
		}()

		// Wait until we get at least 3 polls then cancel
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	if !strings.Contains(out.Stdout, "entry-T1") {
		t.Errorf("stdout should contain 'entry-T1', got:\n%s", out.Stdout)
	}
	if !strings.Contains(out.Stdout, "entry-T2") {
		t.Errorf("stdout should contain 'entry-T2', got:\n%s", out.Stdout)
	}
}

func TestFollowMessageLogs_DropsDuplicateIDsAtBoundary(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			// Poll 1: entry "a" at T1
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "a", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "entry-A"},
			))
		case 2:
			// Poll 2: "a" at T1 (duplicate boundary) + "b" at T2 (new)
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "a", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "entry-A"},
				model.MessageLogEntry{ID: "b", TimeStamp: "2026-04-25T10:02:00Z", Level: "Info", Message: "entry-B"},
			))
		default:
			w.Write(messageLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followMessageLogs(ctx, cl, "2026-04-25T10:00:00Z", nil, "", "", "", 5*time.Millisecond, false, false)
		}()

		// Wait until at least 3 polls then cancel
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	// "entry-A" should appear exactly once (duplicate dropped on poll 2)
	countA := strings.Count(out.Stdout, "entry-A")
	if countA != 1 {
		t.Errorf("'entry-A' should appear exactly once, got %d times in:\n%s", countA, out.Stdout)
	}
	// "entry-B" should appear exactly once
	countB := strings.Count(out.Stdout, "entry-B")
	if countB != 1 {
		t.Errorf("'entry-B' should appear exactly once, got %d times in:\n%s", countB, out.Stdout)
	}
}

func TestFollowMessageLogs_ContextCancellationStopsLoop(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(messageLogJSON())
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately before first interval

	done := make(chan error, 1)
	go func() {
		err := followMessageLogs(ctx, cl, "", nil, "", "", "", 5*time.Millisecond, false, false)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("followMessageLogs should return nil on context cancel, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("followMessageLogs did not stop within 100ms after context cancellation")
	}
}

func TestFollowMessageLogs_TransientErrorContinues(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		switch n {
		case 1:
			// Transient error
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
		case 2:
			// Normal entry on second poll
			w.Header().Set("Content-Type", "application/json")
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "recovery-entry"},
			))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Write(messageLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followMessageLogs(ctx, cl, "", nil, "", "", "", 5*time.Millisecond, false, false)
		}()

		// Wait until at least 3 polls then cancel
		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	if !strings.Contains(out.Stderr, "[warn] poll failed") {
		t.Errorf("stderr should contain '[warn] poll failed', got: %q", out.Stderr)
	}
	if !strings.Contains(out.Stdout, "recovery-entry") {
		t.Errorf("stdout should contain 'recovery-entry' from poll 2, got:\n%s", out.Stdout)
	}
}

func TestFollowMessageLogs_NDJSONOutput(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.Write(messageLogJSON(
				model.MessageLogEntry{ID: "1", TimeStamp: "2026-04-25T10:01:00Z", Level: "Info", Message: "ndjson-entry"},
			))
		} else {
			w.Write(messageLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followMessageLogs(ctx, cl, "", nil, "", "", "", 5*time.Millisecond, true, false)
		}()

		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	// Each follow chunk in JSON mode emits one JSON object per line (NDJSON)
	lines := strings.Split(strings.TrimRight(out.Stdout, "\n"), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		var entry model.MessageLogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Errorf("each stdout line should be valid JSON object, got %q: %v", line, err)
		}
	}
	if !strings.Contains(out.Stdout, "ndjson-entry") {
		t.Errorf("stdout should contain 'ndjson-entry', got:\n%s", out.Stdout)
	}
}

// ============================================================
// Helpers
// ============================================================

// decodedQuery URL-decodes the raw query string for readable assertions.
func decodedQuery(raw string) (string, error) {
	return url.QueryUnescape(raw)
}
