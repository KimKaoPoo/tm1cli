package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// ============================================================
// Unit tests — buildAuditFilter
// ============================================================

func TestBuildAuditFilter(t *testing.T) {
	tests := []struct {
		name       string
		sinceTS    string
		untilTS    string
		objectType string
		objectName string
		user       string
		want       string
	}{
		{"empty", "", "", "", "", "", ""},
		{"since only", "2026-04-25T10:00:00Z", "", "", "", "", "TimeStamp ge 2026-04-25T10:00:00Z"},
		{"until only", "", "2026-04-25T18:00:00Z", "", "", "", "TimeStamp le 2026-04-25T18:00:00Z"},
		{"both times", "2026-04-25T10:00:00Z", "2026-04-25T18:00:00Z", "", "", "",
			"TimeStamp ge 2026-04-25T10:00:00Z and TimeStamp le 2026-04-25T18:00:00Z"},
		{"object-type only", "", "", "Cube", "", "", "ObjectType eq 'Cube'"},
		{"object-name only", "", "", "", "Sales", "", "ObjectName eq 'Sales'"},
		{"user only", "", "", "", "", "admin", "User eq 'admin'"},
		{"object-type and object-name", "", "", "Cube", "Sales", "", "ObjectType eq 'Cube' and ObjectName eq 'Sales'"},
		{"object-name and user", "", "", "", "ImportSales", "admin", "ObjectName eq 'ImportSales' and User eq 'admin'"},
		{"all five", "2026-04-25T10:00:00Z", "2026-04-25T18:00:00Z", "Process", "ImportSales", "admin",
			"TimeStamp ge 2026-04-25T10:00:00Z and TimeStamp le 2026-04-25T18:00:00Z and ObjectType eq 'Process' and ObjectName eq 'ImportSales' and User eq 'admin'"},
		{"object-type with embedded quote", "", "", "O'Brien", "", "", "ObjectType eq 'O''Brien'"},
		{"object-name with embedded quote", "", "", "", "O'Brien", "", "ObjectName eq 'O''Brien'"},
		{"user with embedded quote", "", "", "", "", "O'Brien", "User eq 'O''Brien'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildAuditFilter(tt.sinceTS, tt.untilTS, tt.objectType, tt.objectName, tt.user)
			if got != tt.want {
				t.Errorf("buildAuditFilter() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — buildAuditQuery
// ============================================================

func TestBuildAuditQuery_NoParamsBare(t *testing.T) {
	got := buildAuditQuery("", 0, "")
	if got != "AuditLogEntries" {
		t.Errorf("got %q, want AuditLogEntries", got)
	}
}

func TestBuildAuditQuery_FilterTopOrderDesc(t *testing.T) {
	got := buildAuditQuery("ObjectType eq 'Cube'", 50, orderTimeStampDesc)
	decoded, err := decodedQuery(strings.TrimPrefix(got, "AuditLogEntries?"))
	if err != nil {
		t.Fatalf("cannot decode: %v", err)
	}
	for _, want := range []string{"$filter=ObjectType eq 'Cube'", "$top=50", "$orderby=TimeStamp desc"} {
		if !strings.Contains(decoded, want) {
			t.Errorf("query %q missing %q", decoded, want)
		}
	}
}

func TestBuildAuditQuery_AscFromBuilder(t *testing.T) {
	got := buildAuditQuery("", 100, orderTimeStampAsc)
	decoded, _ := decodedQuery(strings.TrimPrefix(got, "AuditLogEntries?"))
	if !strings.Contains(decoded, "$orderby=TimeStamp asc") {
		t.Errorf("query %q should request asc, got %q", got, decoded)
	}
}

func TestBuildAuditQuery_TopZeroOmitsTop(t *testing.T) {
	got := buildAuditQuery("ObjectType eq 'Cube'", 0, orderTimeStampDesc)
	if strings.Contains(got, "$top=") {
		t.Errorf("query %q should not contain $top", got)
	}
}

func TestBuildAuditQuery_EmptyOrderByOmitsOrderby(t *testing.T) {
	got := buildAuditQuery("ObjectType eq 'Cube'", 10, "")
	if strings.Contains(got, "$orderby") {
		t.Errorf("query %q should not contain $orderby", got)
	}
}

// ============================================================
// Unit tests — applyAuditClientFilters
// ============================================================

func TestApplyAuditClientFilters(t *testing.T) {
	entries := []model.AuditLogEntry{
		{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice", ObjectType: "Cube", ObjectName: "Sales"},
		{ID: "2", TimeStamp: "2026-04-25T11:00:00Z", User: "bob", ObjectType: "Process", ObjectName: "ImportSales"},
		{ID: "3", TimeStamp: "2026-04-25T12:00:00Z", User: "alice", ObjectType: "Cube", ObjectName: "Sales"},
	}

	t.Run("since drops older", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "2026-04-25T10:30:00Z", "", "", "", "")
		if len(got) != 2 {
			t.Fatalf("got %d, want 2", len(got))
		}
		if got[0].ID != "2" || got[1].ID != "3" {
			t.Errorf("got IDs %s,%s; want 2,3", got[0].ID, got[1].ID)
		}
	})

	t.Run("until drops newer", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "", "2026-04-25T11:30:00Z", "", "", "")
		if len(got) != 2 {
			t.Fatalf("got %d, want 2", len(got))
		}
		if got[0].ID != "1" || got[1].ID != "2" {
			t.Errorf("got IDs %s,%s; want 1,2", got[0].ID, got[1].ID)
		}
	})

	t.Run("object-type exact-match case-sensitive", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "", "", "Cube", "", "")
		if len(got) != 2 {
			t.Errorf("Cube should match 2, got %d", len(got))
		}
		gotMixed := applyAuditClientFilters(entries, "", "", "cube", "", "")
		if len(gotMixed) != 0 {
			t.Errorf("lowercase 'cube' should NOT match 'Cube' (case-sensitive), got %d", len(gotMixed))
		}
	})

	t.Run("object-name exact-match case-sensitive", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "", "", "", "Sales", "")
		if len(got) != 2 {
			t.Errorf("Sales should match 2, got %d", len(got))
		}
		gotMixed := applyAuditClientFilters(entries, "", "", "", "sales", "")
		if len(gotMixed) != 0 {
			t.Errorf("lowercase 'sales' should NOT match 'Sales', got %d", len(gotMixed))
		}
	})

	t.Run("user exact-match case-sensitive", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "", "", "", "", "alice")
		if len(got) != 2 {
			t.Errorf("alice should match 2, got %d", len(got))
		}
		gotMixed := applyAuditClientFilters(entries, "", "", "", "", "Alice")
		if len(gotMixed) != 0 {
			t.Errorf("'Alice' should NOT match 'alice', got %d", len(gotMixed))
		}
	})

	t.Run("combined", func(t *testing.T) {
		got := applyAuditClientFilters(entries, "2026-04-25T10:30:00Z", "2026-04-25T11:30:00Z", "Process", "ImportSales", "bob")
		if len(got) != 1 || got[0].ID != "2" {
			t.Errorf("expected only ID=2, got %+v", got)
		}
	})

	t.Run("unparseable timestamp dropped on time filter", func(t *testing.T) {
		bad := []model.AuditLogEntry{{ID: "99", TimeStamp: "not-a-time"}}
		got := applyAuditClientFilters(bad, "2026-04-25T10:00:00Z", "", "", "", "")
		if len(got) != 0 {
			t.Errorf("unparseable should be dropped when since is set, got %+v", got)
		}
	})
}

// ============================================================
// Unit tests — sortAuditByTimeStamp
// ============================================================

func TestSortAuditByTimeStamp(t *testing.T) {
	t.Run("ascending order", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "3", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "2", TimeStamp: "2026-04-25T11:00:00Z"},
		}
		sortAuditByTimeStamp(entries)
		for i, want := range []string{"1", "2", "3"} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %s, want %s", i, entries[i].ID, want)
			}
		}
	})

	t.Run("tie-break by ID", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "c", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "a", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "b", TimeStamp: "2026-04-25T10:00:00Z"},
		}
		sortAuditByTimeStamp(entries)
		for i, want := range []string{"a", "b", "c"} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %s, want %s", i, entries[i].ID, want)
			}
		}
	})
}

// ============================================================
// Unit tests — sortAuditByTimeStampDesc
// ============================================================

func TestSortAuditByTimeStampDesc(t *testing.T) {
	t.Run("descending order", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "3", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "2", TimeStamp: "2026-04-25T11:00:00Z"},
		}
		sortAuditByTimeStampDesc(entries)
		for i, want := range []string{"3", "2", "1"} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %s, want %s", i, entries[i].ID, want)
			}
		}
	})

	t.Run("tie-break by ID descending", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "a", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "c", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "b", TimeStamp: "2026-04-25T10:00:00Z"},
		}
		sortAuditByTimeStampDesc(entries)
		for i, want := range []string{"c", "b", "a"} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %s, want %s", i, entries[i].ID, want)
			}
		}
	})

	t.Run("mixed-precision ordering matches chronology", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "1", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "2", TimeStamp: "2026-04-25T10:00:00.5Z"},
			{ID: "3", TimeStamp: "2026-04-25T10:00:00.250Z"},
		}
		sortAuditByTimeStampDesc(entries)
		for i, want := range []string{"2", "3", "1"} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %s, want %s (chronological order)", i, entries[i].ID, want)
			}
		}
	})
}

// ============================================================
// Unit tests — reverseAuditEntries
// ============================================================

func TestReverseAuditEntries(t *testing.T) {
	entries := []model.AuditLogEntry{
		{ID: "1"}, {ID: "2"}, {ID: "3"},
	}
	reverseAuditEntries(entries)
	for i, want := range []string{"3", "2", "1"} {
		if entries[i].ID != want {
			t.Errorf("entries[%d].ID = %s, want %s", i, entries[i].ID, want)
		}
	}
}

// ============================================================
// Unit tests — boundaryAuditIDs
// ============================================================

func TestBoundaryAuditIDs(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		ts, ids := boundaryAuditIDs(nil)
		if ts != "" || ids != nil {
			t.Errorf("got (%q, %v), want (\"\", nil)", ts, ids)
		}
	})

	t.Run("single entry", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "abc", TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryAuditIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if _, ok := ids["abc"]; !ok {
			t.Errorf("ids should contain 'abc', got %v", ids)
		}
	})

	t.Run("two boundary entries with same TS", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "x", TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: "y", TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: "z", TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryAuditIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 boundary IDs, got %d (%v)", len(ids), ids)
		}
		for _, want := range []string{"y", "z"} {
			if _, ok := ids[want]; !ok {
				t.Errorf("ids missing %q: %v", want, ids)
			}
		}
	})

	t.Run("ID-less entries get synthetic dedupe key", func(t *testing.T) {
		entries := []model.AuditLogEntry{
			{ID: "", TimeStamp: "2026-04-25T12:00:00Z", User: "u", ObjectType: "Cube", ObjectName: "Sales", Description: "Created"},
			{ID: "abc", TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryAuditIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if len(ids) != 2 {
			t.Errorf("both boundary entries should be in dedupe set; got %d: %v", len(ids), ids)
		}
		if _, ok := ids["abc"]; !ok {
			t.Errorf("explicit ID should appear as-is, got %v", ids)
		}
	})
}

// ============================================================
// Unit tests — auditIDKey
// ============================================================

func TestAuditIDKey(t *testing.T) {
	t.Run("uses ID when non-empty", func(t *testing.T) {
		e := model.AuditLogEntry{ID: "some-uuid"}
		if got := auditIDKey(e); got != "some-uuid" {
			t.Errorf("auditIDKey(ID=some-uuid) = %q, want some-uuid", got)
		}
	})

	t.Run("synthesizes key when ID empty", func(t *testing.T) {
		e := model.AuditLogEntry{ID: "", TimeStamp: "2026-04-25T10:00:00Z", User: "u", ObjectType: "Cube", ObjectName: "Sales"}
		got := auditIDKey(e)
		if got == "" {
			t.Errorf("auditIDKey for ID='' must synthesize a non-empty key")
		}
		if !strings.HasPrefix(got, "syn") {
			t.Errorf("synthetic key should be prefixed to avoid colliding with real IDs, got %q", got)
		}
	})

	t.Run("distinct same-TS entries get distinct synth keys", func(t *testing.T) {
		ts := "2026-04-25T10:00:00Z"
		a := model.AuditLogEntry{ID: "", TimeStamp: ts, User: "u", ObjectType: "Cube", ObjectName: "SalesA"}
		b := model.AuditLogEntry{ID: "", TimeStamp: ts, User: "u", ObjectType: "Cube", ObjectName: "SalesB"}
		if auditIDKey(a) == auditIDKey(b) {
			t.Errorf("distinct same-timestamp entries must produce distinct synth keys")
		}
	})

	t.Run("identical entries get identical synth keys", func(t *testing.T) {
		ts := "2026-04-25T10:00:00Z"
		a := model.AuditLogEntry{ID: "", TimeStamp: ts, User: "u", ObjectType: "Cube", ObjectName: "Sales"}
		b := model.AuditLogEntry{ID: "", TimeStamp: ts, User: "u", ObjectType: "Cube", ObjectName: "Sales"}
		if auditIDKey(a) != auditIDKey(b) {
			t.Errorf("identical entries must produce identical synth keys")
		}
	})
}

// ============================================================
// Unit tests — isAuditLogDisabled
// ============================================================

func TestIsAuditLogDisabled(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"HTTP 400 disabled", fmt.Errorf("HTTP 400: Audit log is disabled."), true},
		{"HTTP 400 not enabled", fmt.Errorf("HTTP 400: Audit log feature is not enabled"), true},
		{"HTTP 501 not supported", fmt.Errorf("HTTP 501: auditing not supported"), true},
		{"HTTP 501 not available", fmt.Errorf("HTTP 501: audit log not available on this server"), true},
		{"HTTP 401 auth failed", fmt.Errorf("HTTP 401: Authentication failed."), false},
		{"HTTP 403 forbidden", fmt.Errorf("HTTP 403: Forbidden"), false},
		{"HTTP 404 not found", fmt.Errorf("HTTP 404: Not found"), false},
		{"HTTP 400 filter not supported (no audit keyword)", fmt.Errorf("HTTP 400: filter not supported"), false},
		{"connection timeout (no audit keyword)", fmt.Errorf("Connection error: timeout"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAuditLogDisabled(tt.err); got != tt.want {
				t.Errorf("isAuditLogDisabled(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — AuditLogEntry JSON round-trip
// ============================================================

func TestRunLogsAudit_AuditLogEntryRoundTrip(t *testing.T) {
	body := []byte(`{
		"value": [
			{"ID":"uuid-abc","TimeStamp":"2026-04-25T10:00:00Z","User":"alice","ObjectType":"Cube","ObjectName":"Sales","Description":"Created","AuditDetails":"some detail"},
			{"TimeStamp":"2026-04-25T10:01:00Z","User":"bob","ObjectType":"Process","ObjectName":"ImportSales","Description":"Updated"}
		]
	}`)

	var resp model.AuditLogResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(resp.Value) != 2 {
		t.Fatalf("got %d entries, want 2", len(resp.Value))
	}

	e0 := resp.Value[0]
	if e0.ID != "uuid-abc" {
		t.Errorf("ID = %q, want uuid-abc", e0.ID)
	}
	if e0.AuditDetails != "some detail" {
		t.Errorf("AuditDetails = %q, want 'some detail'", e0.AuditDetails)
	}

	e1 := resp.Value[1]
	if e1.ID != "" {
		t.Errorf("missing ID should unmarshal to empty string, got %q", e1.ID)
	}
	if e1.User != "bob" || e1.ObjectType != "Process" {
		t.Errorf("unexpected fields: User=%q ObjectType=%q", e1.User, e1.ObjectType)
	}
}

// ============================================================
// Integration tests — flag validation
// ============================================================

func TestRunLogsAudit_NegativeTailRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditTail = -1

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.Write(auditLogJSON())
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--tail must be non-negative") {
		t.Errorf("stderr should reject negative --tail, got: %q", out.Stderr)
	}
	if got := atomic.LoadInt32(&requestCount); got != 0 {
		t.Errorf("expected 0 HTTP requests, got %d", got)
	}
}

func TestRunLogsAudit_ZeroIntervalWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditFollow = true
	logsAuditInterval = 0

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(auditLogJSON())
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--interval must be greater than zero") {
		t.Errorf("stderr should reject zero --interval, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_NegativeIntervalWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditFollow = true
	logsAuditInterval = -5 * time.Second

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(auditLogJSON())
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--interval must be greater than zero") {
		t.Errorf("stderr should reject negative --interval, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_UntilWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditFollow = true
	logsAuditUntil = "1h"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--until cannot be combined with --follow") {
		t.Errorf("stderr should reject --until + --follow, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_SinceAfterUntilRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditSince = "2026-04-25T18:00:00Z"
	logsAuditUntil = "2026-04-25T10:00:00Z"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--since must be earlier than --until") {
		t.Errorf("stderr should reject since>until, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_InvalidSince(t *testing.T) {
	resetCmdFlags(t)
	logsAuditSince = "not-a-time"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--since") {
		t.Errorf("stderr should mention --since, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_InvalidUntil(t *testing.T) {
	resetCmdFlags(t)
	logsAuditUntil = "garbage"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--until") {
		t.Errorf("stderr should mention --until, got: %q", out.Stderr)
	}
	if strings.Contains(out.Stderr, "--since") {
		t.Errorf("stderr should NOT mention --since for --until error, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_RawWithJSONRejected(t *testing.T) {
	resetCmdFlags(t)
	logsAuditRaw = true
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make any HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--raw cannot be combined with --output json") {
		t.Errorf("stderr should contain conflict, got: %q", out.Stderr)
	}
}

// ============================================================
// Integration tests — output formats
// ============================================================

func TestRunLogsAudit_TableOutput(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON(
			model.AuditLogEntry{
				ID: "uuid-1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice",
				ObjectType: "Cube", ObjectName: "Sales", Description: "Created",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, header := range []string{"TIME", "USER", "OBJECT_TYPE", "OBJECT_NAME", "DESCRIPTION"} {
		if !strings.Contains(out.Stdout, header) {
			t.Errorf("stdout missing header %q", header)
		}
	}
	for _, want := range []string{"alice", "Cube", "Sales", "Created"} {
		if !strings.Contains(out.Stdout, want) {
			t.Errorf("stdout missing %q\noutput:\n%s", want, out.Stdout)
		}
	}
}

func TestRunLogsAudit_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON(
			model.AuditLogEntry{
				ID: "uuid-1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice",
				ObjectType: "Cube", ObjectName: "Sales", Description: "Created",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got []model.AuditLogEntry
	if err := json.Unmarshal([]byte(out.Stdout), &got); err != nil {
		t.Fatalf("stdout is not valid JSON array: %v\noutput: %s", err, out.Stdout)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].User != "alice" || got[0].ObjectType != "Cube" {
		t.Errorf("got %+v, want alice/Cube", got[0])
	}
}

func TestRunLogsAudit_RawOutput(t *testing.T) {
	resetCmdFlags(t)
	logsAuditRaw = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON(
			model.AuditLogEntry{
				ID: "uuid-1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice",
				ObjectType: "Cube", ObjectName: "Sales", Description: "Created",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	lines := strings.Split(strings.TrimRight(out.Stdout, "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 raw line, got %d:\n%s", len(lines), out.Stdout)
	}
	for _, want := range []string{"alice", "Cube", "Sales", "Created"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("raw line missing %q: %q", want, lines[0])
		}
	}
}

func TestRunLogsAudit_RawSanitizesAllFields(t *testing.T) {
	resetCmdFlags(t)
	logsAuditRaw = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON(
			model.AuditLogEntry{
				ID:          "uuid-1",
				TimeStamp:   "2026-04-25T10:00:00Z",
				User:        "ali\nce",
				ObjectType:  "Cu\rbe",
				ObjectName:  "Sa\tles",
				Description: "Cre\nated",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	lines := strings.Split(strings.TrimRight(out.Stdout, "\n"), "\n")
	if len(lines) != 1 {
		t.Errorf("expected 1 line after sanitization, got %d:\n%s", len(lines), out.Stdout)
	}
}

// ============================================================
// Integration tests — defaults / ordering
// ============================================================

func TestRunLogsAudit_DefaultsToTail100(t *testing.T) {
	resetCmdFlags(t)

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, err := decodedQuery(capturedQuery)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(decoded, "$top=100") {
		t.Errorf("query %q should contain $top=100", decoded)
	}
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("query %q should contain $orderby=TimeStamp desc", decoded)
	}
}

func TestRunLogsAudit_ServerSideFilterReachesServer(t *testing.T) {
	resetCmdFlags(t)
	logsAuditObjectType = "Cube"
	logsAuditObjectName = "ImportSales"
	logsAuditUser = "admin"
	logsAuditSince = "1h"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	for _, want := range []string{
		"TimeStamp ge ",
		"ObjectType eq 'Cube'",
		"ObjectName eq 'ImportSales'",
		"User eq 'admin'",
	} {
		if !strings.Contains(decoded, want) {
			t.Errorf("query %q missing %q", decoded, want)
		}
	}
}

func TestRunLogsAudit_TailOrdering(t *testing.T) {
	resetCmdFlags(t)
	logsAuditTail = 2

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Server returns DESC order — newest first
		w.Write(auditLogJSON(
			model.AuditLogEntry{ID: "2", TimeStamp: "2026-04-25T11:00:00Z", User: "bob", ObjectType: "Cube", ObjectName: "Sales", Description: "Updated"},
			model.AuditLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice", ObjectType: "Cube", ObjectName: "Sales", Description: "Created"},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	idxAlice := strings.Index(out.Stdout, "alice")
	idxBob := strings.Index(out.Stdout, "bob")
	if idxAlice < 0 || idxBob < 0 {
		t.Fatalf("missing rows, output:\n%s", out.Stdout)
	}
	if idxAlice > idxBob {
		t.Errorf("expected ASC after reverse: alice (older) before bob (newer); got bob first")
	}
}

// ============================================================
// Integration tests — fallback behavior
// ============================================================

func TestRunLogsAudit_FilterFallbackOnHTTP400(t *testing.T) {
	resetCmdFlags(t)
	logsAuditObjectType = "Cube"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		// Second call: no $filter — return mixed types; client should drop non-Cube.
		w.Write(auditLogJSON(
			model.AuditLogEntry{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice", ObjectType: "Cube", ObjectName: "Sales", Description: "Created"},
			model.AuditLogEntry{ID: "2", TimeStamp: "2026-04-25T10:01:00Z", User: "bob", ObjectType: "Process", ObjectName: "Import", Description: "Updated"},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "[warn] Server-side filter not supported") {
		t.Errorf("stderr should print fallback warning, got: %q", out.Stderr)
	}
	if !strings.Contains(out.Stdout, "Cube") {
		t.Errorf("stdout should contain Cube row, got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "Process") {
		t.Errorf("stdout should NOT contain Process after client-filter, got:\n%s", out.Stdout)
	}
}

func TestRunLogsAudit_FallbackUntilOnlyRetriesAsc(t *testing.T) {
	resetCmdFlags(t)
	logsAuditUntil = "2030-01-01T00:00:00Z"

	var capturedRetryQuery string
	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		capturedRetryQuery = r.URL.RawQuery
		w.Write(auditLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if !strings.Contains(decoded, "$orderby=TimeStamp asc") {
		t.Errorf("retry for --until-only must explicitly request ASC; got %q", decoded)
	}
	if strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("retry for --until-only should not use DESC; got %q", decoded)
	}
	if !strings.Contains(out.Stderr, "--until cannot be enforced server-side") {
		t.Errorf("stderr should warn that --until is unenforceable under fallback, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_FallbackTrimToTail(t *testing.T) {
	resetCmdFlags(t)
	logsAuditTail = 3
	logsAuditObjectType = "Cube"

	const wantBuffered = 3 * fallbackTailMultiplier // 30

	var capturedRetryQuery string
	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		capturedRetryQuery = r.URL.RawQuery
		// Return a mix: 7 Cube rows interleaved with Process rows. With buffer we
		// should find them and trim to tail=3.
		entries := make([]model.AuditLogEntry, 0, 40)
		for i := 0; i < 40; i++ {
			objType := "Process"
			if i < 7 {
				objType = "Cube"
			}
			entries = append(entries, model.AuditLogEntry{
				ID:          fmt.Sprintf("%d", i+1),
				TimeStamp:   time.Date(2026, 4, 25, 10, 0, i, 0, time.UTC).Format(time.RFC3339),
				User:        "u",
				ObjectType:  objType,
				ObjectName:  "Sales",
				Description: "Changed",
			})
		}
		w.Write(auditLogJSON(entries...))
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if !strings.Contains(decoded, fmt.Sprintf("$top=%d", wantBuffered)) {
		t.Errorf("retry query %q should buffer $top to %d (=tail*multiplier)", decoded, wantBuffered)
	}
	// Only Cube rows should remain; at most --tail=3 after truncation.
	if strings.Contains(out.Stdout, "Process") {
		t.Errorf("stdout should not contain Process after client filter:\n%s", out.Stdout)
	}
	cubeCount := strings.Count(out.Stdout, "Cube")
	if cubeCount != 3 {
		t.Errorf("expected exactly 3 Cube rows after truncation, got %d:\n%s", cubeCount, out.Stdout)
	}
}

// ============================================================
// Integration tests — audit log disabled
// ============================================================

func TestRunLogsAudit_AuditLogDisabledFriendlyError(t *testing.T) {
	resetCmdFlags(t)

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"Audit log is disabled."}}`)
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	// No retry on disabled — exactly one request.
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected 1 HTTP request (no retry), got %d", got)
	}
	if !strings.Contains(out.Stderr, "AuditLog=T") {
		t.Errorf("stderr should mention AuditLog=T, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_AuditLogDisabledOnFallbackRetry(t *testing.T) {
	resetCmdFlags(t)
	logsAuditObjectType = "Cube"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if n == 1 {
			// First 400: filter not supported → triggers fallback retry.
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
		} else {
			// Second 400: disabled error on the unfiltered retry.
			fmt.Fprint(w, `{"error":{"message":"Audit log is disabled."}}`)
		}
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Errorf("expected 2 HTTP requests (first filter rejection, second disabled), got %d", got)
	}
	if !strings.Contains(out.Stderr, "AuditLog=T") {
		t.Errorf("stderr should contain friendly AuditLog=T message, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_AuthErrorNotTreatedAsDisabled(t *testing.T) {
	resetCmdFlags(t)
	logsAuditObjectType = "Cube"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"Authentication failed."}`)
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if strings.Contains(out.Stderr, "AuditLog=T") {
		t.Errorf("auth error should NOT be treated as disabled, but got AuditLog=T in: %q", out.Stderr)
	}
	if !strings.Contains(out.Stderr, "Authentication failed") {
		t.Errorf("stderr should mention auth failure, got: %q", out.Stderr)
	}
}

func TestRunLogsAudit_NotFoundNotTreatedAsDisabled(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error":"audit log endpoint not found"}`)
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if strings.Contains(out.Stderr, "AuditLog=T") {
		t.Errorf("404 should NOT be treated as disabled, but got AuditLog=T in: %q", out.Stderr)
	}
}

func TestRunLogsAudit_AuditLogDisabledJSONMode(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"Audit log is disabled."}}`)
	})

	out := captureAll(t, func() {
		err := runLogsAudit(logsAuditCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	// In JSON mode errors are emitted as JSON envelopes to stderr.
	if !strings.Contains(out.Stderr, "Audit log is disabled") {
		t.Errorf("JSON-mode error should contain disabled message, got: %q", out.Stderr)
	}
}

// ============================================================
// Integration tests — edge cases
// ============================================================

func TestRunLogsAudit_EmptyResponseNoCrash(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(auditLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	// Empty response should print headers (table still renders) but no data rows.
	if !strings.Contains(out.Stdout, "TIME") {
		t.Errorf("table headers should still print on empty response, got:\n%s", out.Stdout)
	}
}

func TestRunLogsAudit_PaginationWarning(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"value":[],"@odata.nextLink":"AuditLogEntries?$skiptoken=42"}`)
	})

	out := captureAll(t, func() {
		if err := runLogsAudit(logsAuditCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "paginated") {
		t.Errorf("stderr should warn about paginated response, got: %q", out.Stderr)
	}
}

// ============================================================
// Integration tests — follow mode
// ============================================================

func TestRunLogsAudit_FollowEmitsNDJSON(t *testing.T) {
	resetCmdFlags(t)

	entries := []model.AuditLogEntry{
		{ID: "1", TimeStamp: "2026-04-25T10:00:00Z", User: "alice", ObjectType: "Cube", ObjectName: "Sales", Description: "Created"},
		{ID: "2", TimeStamp: "2026-04-25T10:00:01Z", User: "bob", ObjectType: "Process", ObjectName: "Import", Description: "Updated"},
	}

	out := captureStdout(t, func() {
		printAuditEntries(entries, true, false, true)
	})

	stdout := strings.TrimSpace(out)
	if stdout == "" {
		t.Fatalf("expected NDJSON, got empty stdout")
	}
	if strings.HasPrefix(stdout, "[") {
		t.Errorf("follow+json should be NDJSON not array, got: %q", stdout)
	}
	lines := strings.Split(stdout, "\n")
	if len(lines) != len(entries) {
		t.Fatalf("expected %d NDJSON lines, got %d", len(entries), len(lines))
	}
	for i, line := range lines {
		var e model.AuditLogEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Errorf("line %d not a JSON object: %v: %q", i, err, line)
		}
	}
}

func TestRunLogsAudit_FollowSameBoundaryDedupe(t *testing.T) {
	resetCmdFlags(t)

	const ts = "2026-04-25T10:00:00Z"
	entryA := model.AuditLogEntry{ID: "1", TimeStamp: ts, User: "alice", ObjectType: "Cube", ObjectName: "Sales", Description: "Created"}
	entryB := model.AuditLogEntry{ID: "2", TimeStamp: ts, User: "bob", ObjectType: "Cube", ObjectName: "Inventory", Description: "Deleted"}

	var pollCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			// First poll: A and B both arrive.
			w.Write(auditLogJSON(entryA, entryB))
		default:
			// Subsequent polls keep returning both at the same boundary TS.
			w.Write(auditLogJSON(entryA, entryB))
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			// Start with watermarkIDs={1} (A already seen) and watermarkTS=ts.
			followAuditLogs(ctx, cl, ts, map[string]struct{}{"1": {}}, "", "", "", 5*time.Millisecond, false, true)
		}()
		for i := 0; i < 200; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	// B should appear exactly once (first poll).
	if got := strings.Count(out.Stdout, "Inventory"); got != 1 {
		t.Errorf("entry B (Inventory) should appear exactly once, got %d:\n%s", got, out.Stdout)
	}
	// A should never re-appear (it was already in the prior dedup set).
	if got := strings.Count(out.Stdout, "Sales"); got != 0 {
		t.Errorf("entry A (Sales) should not be re-emitted, got %d times:\n%s", got, out.Stdout)
	}
}
