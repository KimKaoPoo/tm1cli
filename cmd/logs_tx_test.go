package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// ============================================================
// Unit tests — odataEscape
// ============================================================

func TestOdataEscape(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Sales", "Sales"},
		{"O'Brien", "O''Brien"},
		{"a''b", "a''''b"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("input=%q", tt.input), func(t *testing.T) {
			if got := odataEscape(tt.input); got != tt.want {
				t.Errorf("odataEscape(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — parseTimeFlag
// ============================================================

func TestParseTimeFlag(t *testing.T) {
	now := time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)

	t.Run("valid input passes through", func(t *testing.T) {
		got, err := parseTimeFlag("--until", "2026-04-24T10:00:00Z", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "2026-04-24T10:00:00Z" {
			t.Errorf("got %q, want 2026-04-24T10:00:00Z", got)
		}
	})

	t.Run("--since error names --since", func(t *testing.T) {
		_, err := parseTimeFlag("--since", "garbage", now)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "--since") {
			t.Errorf("error %q should mention --since", err.Error())
		}
		if strings.Contains(err.Error(), "--until") {
			t.Errorf("error %q should NOT mention --until", err.Error())
		}
	})

	t.Run("--until error names --until", func(t *testing.T) {
		_, err := parseTimeFlag("--until", "garbage", now)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "--until") {
			t.Errorf("error %q should mention --until", err.Error())
		}
		if strings.Contains(err.Error(), "--since") {
			t.Errorf("error %q should NOT mention --since", err.Error())
		}
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		got, err := parseTimeFlag("--until", "", now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Errorf("got %q, want empty", got)
		}
	})
}

// ============================================================
// Unit tests — formatTxValue
// ============================================================

func TestFormatTxValue(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{"null", json.RawMessage(`null`), ""},
		{"empty", json.RawMessage(``), ""},
		{"whitespace", json.RawMessage(`   `), ""},
		{"string", json.RawMessage(`"hello"`), "hello"},
		{"string with spaces", json.RawMessage(`"hello world"`), "hello world"},
		{"number int", json.RawMessage(`42`), "42"},
		{"number float", json.RawMessage(`3.14`), "3.14"},
		{"bool true", json.RawMessage(`true`), "true"},
		{"bool false", json.RawMessage(`false`), "false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatTxValue(tt.raw); got != tt.want {
				t.Errorf("formatTxValue(%s) = %q, want %q", string(tt.raw), got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — txIDKey
// ============================================================

func TestTxIDKey(t *testing.T) {
	t.Run("uses ID when present", func(t *testing.T) {
		e := model.TransactionLogEntry{ID: 12345}
		if got := txIDKey(e); got != "12345" {
			t.Errorf("txIDKey(ID=12345) = %q, want %q", got, "12345")
		}
	})
	t.Run("max int64", func(t *testing.T) {
		e := model.TransactionLogEntry{ID: 9223372036854775807}
		if got := txIDKey(e); got != "9223372036854775807" {
			t.Errorf("txIDKey(maxInt64) = %q", got)
		}
	})
	t.Run("synthesizes key when ID omitted", func(t *testing.T) {
		e := model.TransactionLogEntry{
			ID: 0, TimeStamp: "2026-04-25T10:00:00Z", User: "u", Cube: "C",
			Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`),
		}
		got := txIDKey(e)
		if got == "" {
			t.Errorf("txIDKey for ID=0 must synthesize a non-empty key")
		}
		if !strings.HasPrefix(got, "syn") {
			t.Errorf("synthetic key should be prefixed to avoid colliding with numeric IDs, got %q", got)
		}
	})
	t.Run("synthesized keys differ for distinct same-timestamp entries", func(t *testing.T) {
		ts := "2026-04-25T10:00:00Z"
		a := model.TransactionLogEntry{ID: 0, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)}
		b := model.TransactionLogEntry{ID: 0, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"y"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`2`)}
		if txIDKey(a) == txIDKey(b) {
			t.Errorf("distinct same-timestamp entries must produce distinct synth keys")
		}
	})
	t.Run("synthesized keys match for identical entries", func(t *testing.T) {
		ts := "2026-04-25T10:00:00Z"
		a := model.TransactionLogEntry{ID: 0, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)}
		b := model.TransactionLogEntry{ID: 0, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)}
		if txIDKey(a) != txIDKey(b) {
			t.Errorf("identical entries must produce identical synth keys")
		}
	})
}

// ============================================================
// Unit tests — tupleString
// ============================================================

func TestTupleString(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  string
	}{
		{"empty", []string{}, ""},
		{"nil", nil, ""},
		{"single", []string{"a"}, "a"},
		{"three", []string{"Q1", "2025", "Sales"}, "Q1:2025:Sales"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tupleString(tt.input); got != tt.want {
				t.Errorf("tupleString(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — buildTxFilter
// ============================================================

func TestBuildTxFilter(t *testing.T) {
	tests := []struct {
		name    string
		sinceTS string
		untilTS string
		cube    string
		user    string
		want    string
	}{
		{"empty", "", "", "", "", ""},
		{"since only", "2026-04-25T10:00:00Z", "", "", "", "TimeStamp ge 2026-04-25T10:00:00Z"},
		{"until only", "", "2026-04-25T18:00:00Z", "", "", "TimeStamp le 2026-04-25T18:00:00Z"},
		{"both times", "2026-04-25T10:00:00Z", "2026-04-25T18:00:00Z", "", "",
			"TimeStamp ge 2026-04-25T10:00:00Z and TimeStamp le 2026-04-25T18:00:00Z"},
		{"cube only", "", "", "Sales", "", "Cube eq 'Sales'"},
		{"user only", "", "", "", "admin", "User eq 'admin'"},
		{"cube and user", "", "", "Sales", "admin", "Cube eq 'Sales' and User eq 'admin'"},
		{"all four", "2026-04-25T10:00:00Z", "2026-04-25T18:00:00Z", "Sales", "admin",
			"TimeStamp ge 2026-04-25T10:00:00Z and TimeStamp le 2026-04-25T18:00:00Z and Cube eq 'Sales' and User eq 'admin'"},
		{"cube with embedded quote escaped", "", "", "O'Brien", "", "Cube eq 'O''Brien'"},
		{"user with embedded quote escaped", "", "", "", "o'reilly", "User eq 'o''reilly'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTxFilter(tt.sinceTS, tt.untilTS, tt.cube, tt.user)
			if got != tt.want {
				t.Errorf("buildTxFilter() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ============================================================
// Unit tests — buildTxQuery
// ============================================================

func TestBuildTxQuery(t *testing.T) {
	t.Run("no params returns bare endpoint", func(t *testing.T) {
		got := buildTxQuery("", 0, false)
		if got != "TransactionLogEntries" {
			t.Errorf("got %q, want TransactionLogEntries", got)
		}
	})

	t.Run("filter + top + orderby desc", func(t *testing.T) {
		got := buildTxQuery("Cube eq 'Sales'", 50, true)
		decoded, err := decodedQuery(strings.TrimPrefix(got, "TransactionLogEntries?"))
		if err != nil {
			t.Fatalf("cannot decode: %v", err)
		}
		for _, want := range []string{"$filter=Cube eq 'Sales'", "$top=50", "$orderby=TimeStamp desc"} {
			if !strings.Contains(decoded, want) {
				t.Errorf("query %q missing %q", decoded, want)
			}
		}
	})

	t.Run("top=0 omits $top", func(t *testing.T) {
		got := buildTxQuery("Cube eq 'X'", 0, true)
		if strings.Contains(got, "$top=") {
			t.Errorf("query %q should not contain $top", got)
		}
	})

	t.Run("orderDesc=false omits $orderby", func(t *testing.T) {
		got := buildTxQuery("Cube eq 'X'", 10, false)
		if strings.Contains(got, "$orderby") {
			t.Errorf("query %q should not contain $orderby", got)
		}
	})
}

// ============================================================
// Unit tests — applyTxClientFilters
// ============================================================

func TestApplyTxClientFilters(t *testing.T) {
	entries := []model.TransactionLogEntry{
		{ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "alice", Cube: "Sales"},
		{ID: 2, TimeStamp: "2026-04-25T11:00:00Z", User: "bob", Cube: "Inventory"},
		{ID: 3, TimeStamp: "2026-04-25T12:00:00Z", User: "alice", Cube: "Sales"},
	}

	t.Run("since drops older", func(t *testing.T) {
		got := applyTxClientFilters(entries, "2026-04-25T10:30:00Z", "", "", "")
		if len(got) != 2 {
			t.Fatalf("got %d, want 2", len(got))
		}
		if got[0].ID != 2 || got[1].ID != 3 {
			t.Errorf("got IDs %d,%d; want 2,3", got[0].ID, got[1].ID)
		}
	})

	t.Run("until drops newer", func(t *testing.T) {
		got := applyTxClientFilters(entries, "", "2026-04-25T11:30:00Z", "", "")
		if len(got) != 2 {
			t.Fatalf("got %d, want 2", len(got))
		}
		if got[0].ID != 1 || got[1].ID != 2 {
			t.Errorf("got IDs %d,%d; want 1,2", got[0].ID, got[1].ID)
		}
	})

	t.Run("cube exact-match case-sensitive", func(t *testing.T) {
		got := applyTxClientFilters(entries, "", "", "Sales", "")
		if len(got) != 2 {
			t.Errorf("Sales should match 2, got %d", len(got))
		}

		gotMixed := applyTxClientFilters(entries, "", "", "sales", "") // lowercase
		if len(gotMixed) != 0 {
			t.Errorf("lowercase 'sales' should NOT match 'Sales' (case-sensitive), got %d", len(gotMixed))
		}
	})

	t.Run("user exact-match case-sensitive", func(t *testing.T) {
		got := applyTxClientFilters(entries, "", "", "", "alice")
		if len(got) != 2 {
			t.Errorf("alice should match 2, got %d", len(got))
		}

		gotMixed := applyTxClientFilters(entries, "", "", "", "Alice")
		if len(gotMixed) != 0 {
			t.Errorf("'Alice' should NOT match 'alice' (case-sensitive), got %d", len(gotMixed))
		}
	})

	t.Run("combined", func(t *testing.T) {
		got := applyTxClientFilters(entries, "2026-04-25T10:30:00Z", "2026-04-25T11:30:00Z", "Inventory", "bob")
		if len(got) != 1 || got[0].ID != 2 {
			t.Errorf("expected only ID=2, got %+v", got)
		}
	})

	t.Run("unparseable timestamp dropped on time filter", func(t *testing.T) {
		bad := []model.TransactionLogEntry{{ID: 99, TimeStamp: "not-a-time"}}
		got := applyTxClientFilters(bad, "2026-04-25T10:00:00Z", "", "", "")
		if len(got) != 0 {
			t.Errorf("unparseable should be dropped when since is set, got %+v", got)
		}
	})
}

// ============================================================
// Unit tests — sortTxByTimeStamp
// ============================================================

func TestSortTxByTimeStamp(t *testing.T) {
	t.Run("ascending order", func(t *testing.T) {
		entries := []model.TransactionLogEntry{
			{ID: 3, TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: 1, TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: 2, TimeStamp: "2026-04-25T11:00:00Z"},
		}
		sortTxByTimeStamp(entries)
		for i, want := range []int64{1, 2, 3} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %d, want %d", i, entries[i].ID, want)
			}
		}
	})

	t.Run("tie-break by ID", func(t *testing.T) {
		entries := []model.TransactionLogEntry{
			{ID: 5, TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: 1, TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: 3, TimeStamp: "2026-04-25T10:00:00Z"},
		}
		sortTxByTimeStamp(entries)
		for i, want := range []int64{1, 3, 5} {
			if entries[i].ID != want {
				t.Errorf("entries[%d].ID = %d, want %d", i, entries[i].ID, want)
			}
		}
	})
}

// ============================================================
// Unit tests — reverseTxEntries
// ============================================================

func TestReverseTxEntries(t *testing.T) {
	entries := []model.TransactionLogEntry{
		{ID: 1}, {ID: 2}, {ID: 3},
	}
	reverseTxEntries(entries)
	for i, want := range []int64{3, 2, 1} {
		if entries[i].ID != want {
			t.Errorf("entries[%d].ID = %d, want %d", i, entries[i].ID, want)
		}
	}
}

// ============================================================
// Unit tests — boundaryTxIDs
// ============================================================

func TestBoundaryTxIDs(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		ts, ids := boundaryTxIDs(nil)
		if ts != "" || ids != nil {
			t.Errorf("got (%q, %v), want (\"\", nil)", ts, ids)
		}
	})

	t.Run("single entry", func(t *testing.T) {
		entries := []model.TransactionLogEntry{
			{ID: 7, TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryTxIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if _, ok := ids["7"]; !ok {
			t.Errorf("ids should contain '7', got %v", ids)
		}
	})

	t.Run("two boundary entries with same TS", func(t *testing.T) {
		entries := []model.TransactionLogEntry{
			{ID: 1, TimeStamp: "2026-04-25T10:00:00Z"},
			{ID: 2, TimeStamp: "2026-04-25T12:00:00Z"},
			{ID: 3, TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryTxIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 boundary IDs, got %d (%v)", len(ids), ids)
		}
		for _, want := range []string{"2", "3"} {
			if _, ok := ids[want]; !ok {
				t.Errorf("ids missing %q: %v", want, ids)
			}
		}
	})

	t.Run("ID-less entries get synthetic dedupe key", func(t *testing.T) {
		// Older TM1 omits ID. Synthetic content keys preserve dedupe coverage
		// without skipping the entry — important for tx logs where same-
		// timestamp batch writes are common.
		entries := []model.TransactionLogEntry{
			{ID: 0, TimeStamp: "2026-04-25T12:00:00Z", User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
			{ID: 5, TimeStamp: "2026-04-25T12:00:00Z"},
		}
		ts, ids := boundaryTxIDs(entries)
		if ts != "2026-04-25T12:00:00Z" {
			t.Errorf("ts = %q", ts)
		}
		if len(ids) != 2 {
			t.Errorf("both boundary entries should be in dedupe set (one numeric, one synthetic); got %d: %v", len(ids), ids)
		}
		if _, ok := ids["5"]; !ok {
			t.Errorf("numeric ID should appear as-is, got %v", ids)
		}
	})
}

// ============================================================
// Unit tests — TransactionLogEntry JSON unmarshal (PrimitiveType fidelity)
// ============================================================

func TestTransactionLogEntry_UnmarshalPrimitiveTypes(t *testing.T) {
	body := []byte(`{
		"value": [
			{"ID": 1, "TimeStamp":"2026-04-25T10:00:00Z","User":"alice","Cube":"Sales","Tuple":["Q1","2025"],"OldValue":null,"NewValue":42},
			{"ID": 2, "TimeStamp":"2026-04-25T10:01:00Z","User":"bob","Cube":"Sales","Tuple":["Q1","2025"],"OldValue":42,"NewValue":"forty-two"},
			{"ID": 3, "TimeStamp":"2026-04-25T10:02:00Z","User":"carol","Cube":"Sales","Tuple":["Q1","2025"],"OldValue":true,"NewValue":false}
		]
	}`)

	var resp model.TransactionLogResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(resp.Value) != 3 {
		t.Fatalf("got %d entries, want 3", len(resp.Value))
	}
	checks := []struct {
		idx      int
		old, new string
	}{
		{0, "", "42"},
		{1, "42", "forty-two"},
		{2, "true", "false"},
	}
	for _, c := range checks {
		if got := formatTxValue(resp.Value[c.idx].OldValue); got != c.old {
			t.Errorf("entry[%d].OldValue formatted = %q, want %q", c.idx, got, c.old)
		}
		if got := formatTxValue(resp.Value[c.idx].NewValue); got != c.new {
			t.Errorf("entry[%d].NewValue formatted = %q, want %q", c.idx, got, c.new)
		}
	}
	if resp.Value[0].ID != 1 || resp.Value[2].ID != 3 {
		t.Errorf("numeric ID round-trip failed: got %d, %d", resp.Value[0].ID, resp.Value[2].ID)
	}
}

// ============================================================
// Integration tests — flag validation
// ============================================================

func TestRunLogsTx_NegativeTailRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxTail = -1

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
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

func TestRunLogsTx_ZeroIntervalWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxFollow = true
	logsTxInterval = 0

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(transactionLogJSON())
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--interval must be greater than zero") {
		t.Errorf("stderr should reject zero --interval, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_NegativeIntervalWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxFollow = true
	logsTxInterval = -5 * time.Second

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(transactionLogJSON())
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--interval must be greater than zero") {
		t.Errorf("stderr should reject negative --interval, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_UntilWithFollowRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxFollow = true
	logsTxUntil = "1h"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--until cannot be combined with --follow") {
		t.Errorf("stderr should reject --until + --follow, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_SinceAfterUntilRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T18:00:00Z"
	logsTxUntil = "2026-04-25T10:00:00Z"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--since must be earlier than --until") {
		t.Errorf("stderr should reject since>until, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_InvalidSince(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "not-a-time"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--since") {
		t.Errorf("stderr should mention --since, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_InvalidUntil(t *testing.T) {
	resetCmdFlags(t)
	logsTxUntil = "garbage"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
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

// ============================================================
// Integration tests — output formats
// ============================================================

func TestRunLogsTx_TableOutput(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{
				ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "alice", Cube: "Sales",
				Tuple:    []string{"Q1", "2025", "Revenue"},
				OldValue: json.RawMessage(`100`), NewValue: json.RawMessage(`200`),
				StatusMessage: "ok",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, header := range []string{"TIME", "USER", "CUBE", "TUPLE", "OLD", "NEW", "STATUS"} {
		if !strings.Contains(out.Stdout, header) {
			t.Errorf("stdout missing header %q", header)
		}
	}
	for _, want := range []string{"alice", "Sales", "Q1:2025:Revenue", "100", "200", "ok"} {
		if !strings.Contains(out.Stdout, want) {
			t.Errorf("stdout missing %q\noutput:\n%s", want, out.Stdout)
		}
	}
}

func TestRunLogsTx_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{
				ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "alice", Cube: "Sales",
				Tuple:    []string{"Q1"},
				OldValue: json.RawMessage(`100`), NewValue: json.RawMessage(`200`),
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got []model.TransactionLogEntry
	if err := json.Unmarshal([]byte(out.Stdout), &got); err != nil {
		t.Fatalf("stdout is not valid JSON array: %v\noutput: %s", err, out.Stdout)
	}
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}
	if got[0].User != "alice" || got[0].Cube != "Sales" {
		t.Errorf("got %+v, want alice/Sales", got[0])
	}
}

func TestRunLogsTx_RawOutput(t *testing.T) {
	resetCmdFlags(t)
	logsTxRaw = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{
				ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "alice", Cube: "Sales",
				Tuple:    []string{"Q1", "2025"},
				OldValue: json.RawMessage(`100`), NewValue: json.RawMessage(`200`),
				StatusMessage: "ok",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	lines := strings.Split(strings.TrimRight(out.Stdout, "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 raw line, got %d:\n%s", len(lines), out.Stdout)
	}
	for _, want := range []string{"alice", "Sales:Q1:2025", "100 -> 200", "ok"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("raw line missing %q: %q", want, lines[0])
		}
	}
}

func TestRunLogsTx_RawWithJSONRejected(t *testing.T) {
	resetCmdFlags(t)
	logsTxRaw = true
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not make any HTTP request")
	})
	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if !strings.Contains(out.Stderr, "--raw cannot be combined with --output json") {
		t.Errorf("stderr should contain conflict, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_RawSanitizesAllFields(t *testing.T) {
	resetCmdFlags(t)
	logsTxRaw = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{
				ID:            1,
				TimeStamp:     "2026-04-25T10:00:00Z",
				User:          "ali\nce",
				Cube:          "Sa\rles",
				Tuple:         []string{"Q1\t", "2025"},
				OldValue:      json.RawMessage(`"old\nval"`),
				NewValue:      json.RawMessage(`"new\rval"`),
				StatusMessage: "status\nlines",
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
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

func TestRunLogsTx_DefaultsToTail100(t *testing.T) {
	resetCmdFlags(t)

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
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

func TestRunLogsTx_TailOrdering(t *testing.T) {
	resetCmdFlags(t)
	logsTxTail = 2

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Server returns DESC order — newest first
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{ID: 2, TimeStamp: "2026-04-25T11:00:00Z", User: "bob", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`1`), NewValue: json.RawMessage(`2`)},
			model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "alice", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
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
// Integration tests — server-side filters
// ============================================================

func TestRunLogsTx_CubeFilter_ServerSide(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "Sales"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "Cube eq 'Sales'") {
		t.Errorf("query %q should contain Cube eq 'Sales'", decoded)
	}
}

func TestRunLogsTx_UserFilter_ServerSide(t *testing.T) {
	resetCmdFlags(t)
	logsTxUser = "admin"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "User eq 'admin'") {
		t.Errorf("query %q should contain User eq 'admin'", decoded)
	}
}

func TestRunLogsTx_SinceDuration(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "10m"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "TimeStamp ge ") {
		t.Errorf("query %q should contain TimeStamp ge", decoded)
	}
}

func TestRunLogsTx_SinceAbsolute(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-24T10:00:00Z"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "TimeStamp ge 2026-04-24T10:00:00Z") {
		t.Errorf("query %q should contain TimeStamp ge 2026-04-24T10:00:00Z", decoded)
	}
}

func TestRunLogsTx_UntilApplied(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T10:00:00Z"
	logsTxUntil = "2026-04-25T11:30:00Z"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		// Server returns three entries — last one is past --until and must be
		// dropped client-side as defense in depth.
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:30:00Z", User: "u", Cube: "c", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
			model.TransactionLogEntry{ID: 2, TimeStamp: "2026-04-25T11:00:00Z", User: "u", Cube: "c", Tuple: []string{"x"}, OldValue: json.RawMessage(`1`), NewValue: json.RawMessage(`2`)},
			model.TransactionLogEntry{ID: 3, TimeStamp: "2026-04-25T12:00:00Z", User: "u", Cube: "c", Tuple: []string{"x"}, OldValue: json.RawMessage(`2`), NewValue: json.RawMessage(`3`)},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "TimeStamp le 2026-04-25T11:30:00Z") {
		t.Errorf("query %q should include 'TimeStamp le 2026-04-25T11:30:00Z'", decoded)
	}
	// Client-side: third entry (12:00) past until → must be dropped.
	if strings.Contains(out.Stdout, "2026-04-25T12:00:00Z") {
		t.Errorf("stdout should NOT contain entry past --until, got:\n%s", out.Stdout)
	}
}

func TestRunLogsTx_CombinedFilters(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T10:00:00Z"
	logsTxUntil = "2026-04-25T18:00:00Z"
	logsTxCube = "Sales"
	logsTxUser = "admin"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	for _, want := range []string{
		"TimeStamp ge 2026-04-25T10:00:00Z",
		"TimeStamp le 2026-04-25T18:00:00Z",
		"Cube eq 'Sales'",
		"User eq 'admin'",
		" and ",
	} {
		if !strings.Contains(decoded, want) {
			t.Errorf("query %q missing %q", decoded, want)
		}
	}
}

func TestRunLogsTx_CubeWithQuoteEscaped(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "O'Brien"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if !strings.Contains(decoded, "Cube eq 'O''Brien'") {
		t.Errorf("query %q should escape ' as '': want Cube eq 'O''Brien'", decoded)
	}
}

// ============================================================
// Integration tests — fallback behavior
// ============================================================

func TestRunLogsTx_FilterFallback(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "Sales"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		// Second call: no $filter — return mixed cubes; client should drop non-Sales.
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "u", Cube: "Sales", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
			model.TransactionLogEntry{ID: 2, TimeStamp: "2026-04-25T10:01:00Z", User: "u", Cube: "Inventory", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "[warn] Server-side filter not supported") {
		t.Errorf("stderr should print fallback warning, got: %q", out.Stderr)
	}
	if !strings.Contains(out.Stdout, "Sales") {
		t.Errorf("stdout should contain Sales row, got:\n%s", out.Stdout)
	}
	if strings.Contains(out.Stdout, "Inventory") {
		t.Errorf("stdout should NOT contain Inventory after client-filter, got:\n%s", out.Stdout)
	}
}

func TestRunLogsTx_FilterFallbackCapsTop(t *testing.T) {
	resetCmdFlags(t)
	// --since defeats defaultTailIfUnbounded so top=0 reaches the fetch;
	// only then does fallback exercise its safety cap.
	logsTxSince = "2026-04-25T10:00:00Z"
	logsTxCube = "Sales"

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
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if !strings.Contains(decoded, fmt.Sprintf("$top=%d", fallbackSafetyCap)) {
		t.Errorf("retry query %q should cap $top at %d", decoded, fallbackSafetyCap)
	}
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("retry query %q should force desc order", decoded)
	}
}

// TestRunLogsTx_FilterFallbackBuffersTailForCubeFilter verifies that when
// --tail is set alongside a content filter and the server rejects $filter,
// the retry over-fetches via fallbackTailMultiplier so client-side filtering
// can find tail-many matches even when most fetched rows are non-matching.
// Without the buffer, --tail 5 --cube Sales on a server without $filter
// support would return at most 5 rows from the latest 5 globally — possibly
// zero matches.
func TestRunLogsTx_FilterFallbackBuffersTailForCubeFilter(t *testing.T) {
	resetCmdFlags(t)
	logsTxTail = 5
	logsTxCube = "Sales"

	const wantBuffered = 5 * fallbackTailMultiplier // 50

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
		// Simulate a window where Sales matches are sparse: 60 rows, only 7
		// match cube=Sales. Without the buffer the retry would fetch 5 rows
		// none of which were Sales; with the buffer (50) we get 7 matches and
		// truncate to 5.
		entries := make([]model.TransactionLogEntry, 0, 60)
		for i := 0; i < 60; i++ {
			cube := "Inventory"
			// Place 7 Sales rows interleaved among the newest entries.
			if i < 7 {
				cube = "Sales"
			}
			entries = append(entries, model.TransactionLogEntry{
				ID:        int64(i + 1),
				TimeStamp: time.Date(2026, 4, 25, 10, 0, i, 0, time.UTC).Format(time.RFC3339),
				User:      "u",
				Cube:      cube,
				Tuple:     []string{"x"},
				OldValue:  json.RawMessage(`0`),
				NewValue:  json.RawMessage(`1`),
			})
		}
		w.Write(transactionLogJSON(entries...))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if !strings.Contains(decoded, fmt.Sprintf("$top=%d", wantBuffered)) {
		t.Errorf("retry query %q should buffer $top to %d (=tail*multiplier)", decoded, wantBuffered)
	}
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("retry query %q should force desc order", decoded)
	}
	// Only Sales rows should remain after client filter, and at most --tail=5
	// after truncation. Inventory must NOT appear.
	if strings.Contains(out.Stdout, "Inventory") {
		t.Errorf("stdout should not contain Inventory after client filter:\n%s", out.Stdout)
	}
	salesCount := strings.Count(out.Stdout, "Sales")
	// Headers contain "Sales" only via row data; with 7 matches truncated to
	// 5, we expect exactly 5 "Sales" cells.
	if salesCount != 5 {
		t.Errorf("expected exactly 5 Sales rows after truncation, got %d:\n%s", salesCount, out.Stdout)
	}
}

// TestInitialFollowWatermark_DedupesBoundaryWhenIDsOmitted is a regression
// guard for older TM1 versions where TransactionLogEntries omits the ID
// field. The watermark stays at the boundary timestamp (so same-timestamp
// new arrivals on the next poll aren't skipped — common in tx logs where
// batch writes share a timestamp), and the dedup set holds a synthetic
// content key so the boundary entry isn't re-emitted.
func TestInitialFollowWatermark_DedupesBoundaryWhenIDsOmitted(t *testing.T) {
	boundaryTS := "2026-04-25T10:01:00Z"
	entries := []model.TransactionLogEntry{
		{ID: 0, TimeStamp: boundaryTS, User: "u", Cube: "C", Tuple: []string{"x"},
			OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
	}
	maxTS, ids := initialFollowWatermark(entries, "", time.Now())
	if maxTS != boundaryTS {
		t.Errorf("watermark should stay at boundary %s (synthetic key handles dedupe), got %s", boundaryTS, maxTS)
	}
	if len(ids) != 1 {
		t.Errorf("expected synthetic dedupe key for ID-less boundary entry, got %v", ids)
	}
	wantKey := txIDKey(entries[0])
	if _, ok := ids[wantKey]; !ok {
		t.Errorf("dedup set should contain synthetic key %q, got %v", wantKey, ids)
	}
}

// TestInitialFollowWatermark_KeepsBoundaryWhenIDsPresent verifies the
// happy-path: when TM1 supplies IDs, the watermark stays at the boundary
// (so an entry that arrived at the same exact timestamp on the next poll
// is fetched, then dropped via the dedupe set).
func TestInitialFollowWatermark_KeepsBoundaryWhenIDsPresent(t *testing.T) {
	boundaryTS := "2026-04-25T10:01:00Z"
	entries := []model.TransactionLogEntry{
		{ID: 42, TimeStamp: boundaryTS, User: "u", Cube: "C", Tuple: []string{"x"},
			OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
	}
	maxTS, ids := initialFollowWatermark(entries, "", time.Now())
	if maxTS != boundaryTS {
		t.Errorf("watermark should stay at boundary %s, got %s", boundaryTS, maxTS)
	}
	if _, ok := ids["42"]; !ok {
		t.Errorf("dedupe set should contain ID 42, got %v", ids)
	}
}

// TestRunLogsTx_FallbackWarningDisclosesCap verifies the fallback warning
// states the row cap so users know the local filter is bounded. Without
// disclosure, a --since query that hits the cap silently drops matching
// entries while the warning falsely implies a complete local filter.
func TestRunLogsTx_FallbackWarningDisclosesCap(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T10:00:00Z"
	logsTxCube = "Sales"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	wantCap := fmt.Sprintf("most recent %d entries", fallbackSafetyCap)
	if !strings.Contains(out.Stderr, wantCap) {
		t.Errorf("fallback warning should disclose row cap %q, got: %q", wantCap, out.Stderr)
	}
}

// TestRunLogsTx_FallbackAtCapWarnsTruncation verifies the second warning
// fires when the retry returns exactly retryTop rows — a strong signal the
// cap was hit and matches likely exist beyond the window.
func TestRunLogsTx_FallbackAtCapWarnsTruncation(t *testing.T) {
	resetCmdFlags(t)
	logsTxTail = 5
	logsTxCube = "Sales"

	const retryTop = 5 * fallbackTailMultiplier // 50, matches fallbackRetryTop(5)

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		entries := make([]model.TransactionLogEntry, 0, retryTop)
		for i := 0; i < retryTop; i++ {
			entries = append(entries, model.TransactionLogEntry{
				ID:        int64(i + 1),
				TimeStamp: time.Date(2026, 4, 25, 10, 0, i, 0, time.UTC).Format(time.RFC3339),
				User:      "u",
				Cube:      "Sales",
				Tuple:     []string{"x"},
				OldValue:  json.RawMessage(`0`),
				NewValue:  json.RawMessage(`1`),
			})
		}
		w.Write(transactionLogJSON(entries...))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	wantCapMsg := fmt.Sprintf("Hit fallback cap of %d", retryTop)
	if !strings.Contains(out.Stderr, wantCapMsg) {
		t.Errorf("at-cap retry should emit truncation warning %q, got: %q", wantCapMsg, out.Stderr)
	}
}

// TestRunLogsTx_FallbackBelowCapNoTruncationWarning verifies the second
// warning does NOT fire when the retry returns fewer rows than retryTop —
// in that case there are no entries beyond the window, so warning the user
// about truncation would be misleading.
func TestRunLogsTx_FallbackBelowCapNoTruncationWarning(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "Sales"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "u", Cube: "Sales", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if strings.Contains(out.Stderr, "Hit fallback cap") {
		t.Errorf("below-cap retry should not emit truncation warning, got: %q", out.Stderr)
	}
}

// TestFallbackRetryTop covers the helper directly.
func TestFallbackRetryTop(t *testing.T) {
	tests := []struct {
		name string
		top  int
		want int
	}{
		{"top=0 returns safety cap", 0, fallbackSafetyCap},
		{"top=1 buffered to 10", 1, 10},
		{"top=50 buffered to 500", 50, 500},
		{"top at half-cap stays buffered", 99, 990},
		{"top equal to cap clamps to cap", fallbackSafetyCap / fallbackTailMultiplier, fallbackSafetyCap},
		{"top above cap clamps to cap", 5000, fallbackSafetyCap},
		// Regression guard: very large --tail must not overflow the multiply.
		{"top near math.MaxInt32 clamps without overflow", math.MaxInt32, fallbackSafetyCap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fallbackRetryTop(tt.top); got != tt.want {
				t.Errorf("fallbackRetryTop(%d) = %d, want %d", tt.top, got, tt.want)
			}
		})
	}
}

// TestRunLogsTx_UntilOnlyDoesNotCap is a regression guard. Previously,
// `tm1cli logs tx --until X` (no --since, no --tail) silently capped
// results to the latest 100 entries before X, because the boundedness
// check only considered --since. An analyst auditing "all changes before
// incident X" would miss older entries with no warning.
func TestRunLogsTx_UntilOnlyDoesNotCap(t *testing.T) {
	resetCmdFlags(t)
	logsTxUntil = "2030-01-01T00:00:00Z"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedQuery)
	if strings.Contains(decoded, "$top=") {
		t.Errorf("--until alone should NOT impose a $top cap; query was %q", decoded)
	}
	if !strings.Contains(decoded, "TimeStamp le 2030-01-01T00:00:00Z") {
		t.Errorf("query %q should still bound by TimeStamp le", decoded)
	}
}

func TestRunLogsTx_NoFallbackOn401(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "Sales"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"unauthorized"}`)
	})

	out := captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected 1 request (no retry), got %d", got)
	}
	if !strings.Contains(out.Stderr, "Authentication failed") {
		t.Errorf("stderr should mention auth failure, got: %q", out.Stderr)
	}
}

func TestRunLogsTx_NoFallbackOn404(t *testing.T) {
	resetCmdFlags(t)
	logsTxCube = "Sales"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusNotFound)
	})

	captureAll(t, func() {
		err := runLogsTx(logsTxCmd, nil)
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected 1 request (no retry), got %d", got)
	}
}

// ============================================================
// Integration tests — edge cases
// ============================================================

func TestRunLogsTx_EmptyResponse(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	// Empty response should print headers (still a table) but no rows.
	if !strings.Contains(out.Stdout, "TIME") {
		t.Errorf("table headers should still print on empty response, got:\n%s", out.Stdout)
	}
}

func TestRunLogsTx_StatusMessageOmitted(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Note: no StatusMessage field
		fmt.Fprint(w, `{"value":[{"ID":1,"TimeStamp":"2026-04-25T10:00:00Z","User":"alice","Cube":"Sales","Tuple":["x"],"OldValue":null,"NewValue":42}]}`)
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	if !strings.Contains(out.Stdout, "alice") {
		t.Errorf("entry should still render without StatusMessage, got:\n%s", out.Stdout)
	}
}

func TestRunLogsTx_TupleRendering(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(transactionLogJSON(
			model.TransactionLogEntry{
				ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "u", Cube: "C",
				Tuple:    []string{"Q1", "2025", "Sales"},
				OldValue: json.RawMessage(`null`), NewValue: json.RawMessage(`42`),
			},
		))
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	if !strings.Contains(out.Stdout, "Q1:2025:Sales") {
		t.Errorf("tuple should render colon-joined, got:\n%s", out.Stdout)
	}
}

func TestRunLogsTx_OldNewValuesPresent(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"value":[{"ID":1,"TimeStamp":"2026-04-25T10:00:00Z","User":"u","Cube":"C","Tuple":["x"],"OldValue":99,"NewValue":"hello"}]}`)
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})
	if !strings.Contains(out.Stdout, "99") {
		t.Errorf("stdout should contain numeric old value 99, got:\n%s", out.Stdout)
	}
	if !strings.Contains(out.Stdout, "hello") {
		t.Errorf("stdout should contain string new value 'hello', got:\n%s", out.Stdout)
	}
}

// ============================================================
// Integration tests — follow mode
// ============================================================

// TestPrintTxEntries_NDJSONInFollowChunk verifies the follow+json render path
// emits one JSON object per line (NDJSON) instead of a JSON array, so that
// downstream stream parsers can ingest a uniform format across the initial
// batch and subsequent poll chunks.
func TestPrintTxEntries_NDJSONInFollowChunk(t *testing.T) {
	entries := []model.TransactionLogEntry{
		{ID: 1, TimeStamp: "2026-04-25T10:00:00Z", User: "u", Cube: "C",
			Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
		{ID: 2, TimeStamp: "2026-04-25T10:00:01Z", User: "u", Cube: "C",
			Tuple: []string{"x"}, OldValue: json.RawMessage(`1`), NewValue: json.RawMessage(`2`)},
	}

	out := captureStdout(t, func() {
		printTxEntries(entries, true, false, true)
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
		var e model.TransactionLogEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Errorf("line %d not a JSON object: %v: %q", i, err, line)
		}
	}
}

func TestFollowTxLogs_PollsAndAdvancesWatermark(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			w.Write(transactionLogJSON(
				model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:01:00Z", User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)},
			))
		case 2:
			w.Write(transactionLogJSON(
				model.TransactionLogEntry{ID: 2, TimeStamp: "2026-04-25T10:02:00Z", User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`1`), NewValue: json.RawMessage(`2`)},
			))
		default:
			w.Write(transactionLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followTxLogs(ctx, cl, "2026-04-25T10:00:00Z", nil, "", "", 5*time.Millisecond, false, false)
		}()

		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	if !strings.Contains(out.Stdout, "2026-04-25T10:01:00Z") {
		t.Errorf("stdout should contain T1 entry, got:\n%s", out.Stdout)
	}
	if !strings.Contains(out.Stdout, "2026-04-25T10:02:00Z") {
		t.Errorf("stdout should contain T2 entry, got:\n%s", out.Stdout)
	}
}

func TestFollowTxLogs_DropsDuplicateIDsAtBoundary(t *testing.T) {
	resetCmdFlags(t)

	var pollCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		entryA := model.TransactionLogEntry{ID: 1, TimeStamp: "2026-04-25T10:01:00Z", User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)}
		entryB := model.TransactionLogEntry{ID: 2, TimeStamp: "2026-04-25T10:02:00Z", User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`1`), NewValue: json.RawMessage(`2`)}
		switch n {
		case 1:
			w.Write(transactionLogJSON(entryA))
		case 2:
			// Boundary entry A reappears at the boundary; B is new
			w.Write(transactionLogJSON(entryA, entryB))
		default:
			w.Write(transactionLogJSON())
		}
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	out := captureAll(t, func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			followTxLogs(ctx, cl, "2026-04-25T10:00:00Z", nil, "", "", 5*time.Millisecond, false, false)
		}()

		for i := 0; i < 100; i++ {
			if atomic.LoadInt32(&pollCount) >= 3 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		<-done
	})

	// Entry A's timestamp should appear exactly once
	countA := strings.Count(out.Stdout, "2026-04-25T10:01:00Z")
	if countA != 1 {
		t.Errorf("entry A should appear exactly once, got %d times in:\n%s", countA, out.Stdout)
	}
	countB := strings.Count(out.Stdout, "2026-04-25T10:02:00Z")
	if countB != 1 {
		t.Errorf("entry B should appear exactly once, got %d times in:\n%s", countB, out.Stdout)
	}
}

// TestFollowTxLogs_MergesBoundaryIDsAcrossSameTimestampPolls is a regression
// guard for the prior bug where each poll *replaced* the dedup set with the
// IDs of post-dedup boundary entries. When new entries arrived at the same
// timestamp as the prior watermark, the old boundary IDs were dropped and
// the server's next response re-emitted them.
//
// Sequence:
//   poll1: server returns [A@T]. dedup set becomes {A}.
//   poll2: server returns [A@T, B@T] (B is new at the same timestamp).
//          Dedup drops A, leaves [B]. Without the merge, dedup set becomes {B}.
//   poll3: server returns [A@T, B@T] again (A is at the inclusive lower bound).
//          Dedup against {B} only drops B — A is re-emitted. With the merge,
//          dedup set is {A,B} and both are dropped.
func TestFollowTxLogs_MergesBoundaryIDsAcrossSameTimestampPolls(t *testing.T) {
	resetCmdFlags(t)

	const ts = "2026-04-25T10:00:00Z"
	entryA := model.TransactionLogEntry{ID: 1, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"x"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`1`)}
	entryB := model.TransactionLogEntry{ID: 2, TimeStamp: ts, User: "u", Cube: "C", Tuple: []string{"y"}, OldValue: json.RawMessage(`0`), NewValue: json.RawMessage(`2`)}

	var pollCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&pollCount, 1))
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			// Initial follow has already emitted [A]; pretend B just arrived.
			w.Write(transactionLogJSON(entryA, entryB))
		default:
			// Subsequent polls keep returning the boundary entries — server's
			// inclusive `TimeStamp ge ts` always includes them.
			w.Write(transactionLogJSON(entryA, entryB))
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
			// Raw mode keeps each entry on a single line with deterministic
			// "old -> new" formatting we can count on.
			followTxLogs(ctx, cl, ts, map[string]struct{}{"1": {}}, "", "", 5*time.Millisecond, false, true)
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

	// B should appear exactly once (emitted on poll1).
	if got := strings.Count(out.Stdout, "0 -> 2"); got != 1 {
		t.Errorf("entry B should appear exactly once, got %d:\n%s", got, out.Stdout)
	}
	// A should NEVER appear (it was already in the prior dedup set, and the
	// merge keeps it across same-timestamp polls).
	if got := strings.Count(out.Stdout, "0 -> 1"); got != 0 {
		t.Errorf("entry A should not be re-emitted across polls, got %d times:\n%s", got, out.Stdout)
	}
}

// TestFetchTxEntries_WarnsOnPaginatedResponse is a regression guard ensuring
// the @odata.nextLink continuation marker isn't silently dropped. Without
// the warning, large --since/--until windows could return only the first
// server page with no signal that the result set is incomplete.
func TestFetchTxEntries_WarnsOnPaginatedResponse(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"value":[],"@odata.nextLink":"TransactionLogEntries?$skiptoken=42"}`)
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	out := captureAll(t, func() {
		_, _, err := fetchTxEntries(cl, "", 0, false, false, false)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if !strings.Contains(out.Stderr, "paginated") {
		t.Errorf("stderr should warn about paginated response, got: %q", out.Stderr)
	}
}

// TestRunLogsTx_FallbackUsesAscOrderForUntilOnlyForensicQuery is a
// regression guard for the silent-empty-result bug: with --until alone
// (forensic / audit query), DESC retry would return the latest 1000 rows
// — all newer than untilTS — and applyTxClientFilters would drop them all.
// Output: empty, no error, missed historical entries. The fallback must
// retry ASC so the cap captures the OLDEST rows (most likely before
// untilTS), and emit a warning explaining --until cannot be enforced
// server-side under fallback.
func TestRunLogsTx_FallbackUsesAscOrderForUntilOnlyForensicQuery(t *testing.T) {
	resetCmdFlags(t)
	logsTxUntil = "2030-01-01T00:00:00Z"

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
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("retry for --until-only forensic query should NOT use DESC ordering (DESC returns the latest rows, all newer than untilTS); got %q", decoded)
	}
	if !strings.Contains(out.Stderr, "--until cannot be enforced server-side") {
		t.Errorf("stderr should warn that --until is unenforceable under fallback, got: %q", out.Stderr)
	}
}

// TestRunLogsTx_FallbackUsesDescOrderForSinceQuery verifies the retry stays
// DESC when --since is set (with or without --until) — the user wants the
// latest entries within the window, not the oldest entries since epoch.
func TestRunLogsTx_FallbackUsesDescOrderForSinceQuery(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T10:00:00Z"
	logsTxUntil = "2030-01-01T00:00:00Z"

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
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	decoded, _ := decodedQuery(capturedRetryQuery)
	if !strings.Contains(decoded, "$orderby=TimeStamp desc") {
		t.Errorf("retry with --since should keep DESC ordering, got %q", decoded)
	}
	// --until is set, so the warning must still fire.
	if !strings.Contains(out.Stderr, "--until cannot be enforced server-side") {
		t.Errorf("stderr should still warn about --until limitation, got: %q", out.Stderr)
	}
}

// TestRunLogsTx_FallbackNoUntilWarningWhenUntilUnset is a sanity check.
func TestRunLogsTx_FallbackNoUntilWarningWhenUntilUnset(t *testing.T) {
	resetCmdFlags(t)
	logsTxSince = "2026-04-25T10:00:00Z"

	var requestCount int32
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":"$filter not supported"}`)
			return
		}
		w.Write(transactionLogJSON())
	})

	out := captureAll(t, func() {
		if err := runLogsTx(logsTxCmd, nil); err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if strings.Contains(out.Stderr, "--until cannot be enforced") {
		t.Errorf("--until warning should not fire when --until is unset, got: %q", out.Stderr)
	}
}

// TestFetchTxEntries_NoWarningWhenNotPaginated is a sanity check that the
// pagination warning only fires when @odata.nextLink is present.
func TestFetchTxEntries_NoWarningWhenNotPaginated(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"value":[]}`)
	})

	cfg, _ := loadConfig()
	cl, _ := createClient(cfg)

	out := captureAll(t, func() {
		_, _, err := fetchTxEntries(cl, "", 0, false, false, false)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	})

	if strings.Contains(out.Stderr, "paginated") {
		t.Errorf("stderr should not warn about pagination on full response, got: %q", out.Stderr)
	}
}
