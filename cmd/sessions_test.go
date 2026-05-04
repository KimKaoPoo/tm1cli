package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// --- Unit tests ---

func TestFilterSessions(t *testing.T) {
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	sessions := []model.Session{
		{ID: 1, User: model.SessionUser{Name: "Admin"}, LastActivity: now.Add(-2 * time.Hour).Format(time.RFC3339)},
		{ID: 2, User: model.SessionUser{Name: "UserA"}, LastActivity: now.Add(-5 * time.Minute).Format(time.RFC3339)},
		{ID: 3, User: model.SessionUser{Name: "worker"}, LastActivity: ""},
	}

	tests := []struct {
		name        string
		user        string
		inactiveFor time.Duration
		wantIDs     []int64
	}{
		{
			name:    "no filters returns all",
			wantIDs: []int64{1, 2, 3},
		},
		{
			name:    "filter by user case-insensitively",
			user:    "admin",
			wantIDs: []int64{1},
		},
		{
			name:        "filter by inactive-for keeps stale sessions",
			inactiveFor: 1 * time.Hour,
			wantIDs:     []int64{1, 3}, // session 3 has empty LastActivity → kept (unknown age)
		},
		{
			name:        "filter by inactive-for combined with user",
			user:        "admin",
			inactiveFor: 1 * time.Hour,
			wantIDs:     []int64{1},
		},
		{
			name:    "all filtered out returns nil",
			user:    "nobody",
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSessions(sessions, tt.user, tt.inactiveFor, now)
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("got %d sessions, want %d", len(got), len(tt.wantIDs))
			}
			for i, id := range tt.wantIDs {
				if got[i].ID != id {
					t.Errorf("got[%d].ID = %d, want %d", i, got[i].ID, id)
				}
			}
		})
	}
}

func TestFormatLastActivity(t *testing.T) {
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "30s ago", input: now.Add(-30 * time.Second).Format(time.RFC3339), want: "30s ago"},
		{name: "5m ago", input: now.Add(-5 * time.Minute).Format(time.RFC3339), want: "5m ago"},
		{name: "2h ago", input: now.Add(-2 * time.Hour).Format(time.RFC3339), want: "2h ago"},
		{name: "3d ago", input: now.Add(-3 * 24 * time.Hour).Format(time.RFC3339), want: "3d ago"},
		{name: "absolute past 7d", input: now.Add(-30 * 24 * time.Hour).Format(time.RFC3339), want: "2026-04-04 12:00"},
		{name: "rfc3339nano accepted", input: now.Add(-1 * time.Hour).Format(time.RFC3339Nano), want: "1h ago"},
		{name: "future timestamp clamps to 0s", input: now.Add(1 * time.Hour).Format(time.RFC3339), want: "0s ago"},
		{name: "unparseable echoed raw", input: "not-a-time", want: "not-a-time"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatLastActivity(tt.input, now)
			if got != tt.want {
				t.Errorf("formatLastActivity(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsExpandRejection(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"http 400 mentioning expand", fmt.Errorf("HTTP 400: $expand=Threads not supported"), true},
		{"http 501 not implemented", fmt.Errorf("HTTP 501: not implemented"), true},
		{"http 400 unrelated bad-request", fmt.Errorf("HTTP 400: invalid query"), false},
		{"http 403 forbidden", fmt.Errorf("HTTP 403: forbidden"), false},
		{"http 500 server error", fmt.Errorf("HTTP 500: oops"), false},
		{"auth failure", fmt.Errorf("Authentication failed. Check credentials."), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpandRejection(tt.err); got != tt.want {
				t.Errorf("isExpandRejection = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- Integration: sessions list ---

// makeSession returns a Session with sensible defaults relative to now.
func makeSession(id int64, user string, ageMinutes int, threads int) model.Session {
	now := time.Now()
	threadList := make([]model.SessionThread, threads)
	for i := 0; i < threads; i++ {
		threadList[i] = model.SessionThread{ID: int64(i + 1)}
	}
	return model.Session{
		ID:           id,
		User:         model.SessionUser{Name: user},
		Context:      "REST",
		Active:       false,
		LastActivity: now.Add(-time.Duration(ageMinutes) * time.Minute).Format(time.RFC3339),
		Threads:      threadList,
	}
}

func TestSessionsList_Empty(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON())
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if cap.Stderr != "" && !strings.Contains(cap.Stderr, "Showing") {
		// Only acceptable stderr output is summary lines, none expected here.
		if !strings.Contains(cap.Stderr, "[") {
			// allow [verbose] etc. but no error markers
		}
	}
	if !strings.Contains(cap.Stdout, "ID") || !strings.Contains(cap.Stdout, "USER") {
		t.Errorf("expected table headers in stdout, got: %s", cap.Stdout)
	}
}

func TestSessionsList_Populated(t *testing.T) {
	resetCmdFlags(t)
	sessions := []model.Session{
		makeSession(1, "Admin", 10, 2),
		makeSession(2, "UserA", 60, 0),
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	for _, want := range []string{"Admin", "UserA"} {
		if !strings.Contains(cap.Stdout, want) {
			t.Errorf("stdout missing %q\nstdout: %s", want, cap.Stdout)
		}
	}
}

func TestSessionsList_FilterByUser(t *testing.T) {
	resetCmdFlags(t)
	sessions := []model.Session{
		makeSession(1, "Admin", 10, 0),
		makeSession(2, "UserA", 10, 0),
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--user", "admin"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stdout, "Admin") {
		t.Errorf("expected Admin in stdout, got: %s", cap.Stdout)
	}
	if strings.Contains(cap.Stdout, "UserA") {
		t.Errorf("UserA should be filtered out, got: %s", cap.Stdout)
	}
}

func TestSessionsList_FilterByInactiveFor(t *testing.T) {
	resetCmdFlags(t)
	// Recent session (5 min ago) should be filtered out by --inactive-for 1h.
	// Stale session (2h ago) should remain.
	recent := makeSession(1, "Recent", 5, 0)
	stale := makeSession(2, "Stale", 120, 0)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(recent, stale))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--inactive-for", "1h"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stdout, "Stale") {
		t.Errorf("expected Stale in stdout, got: %s", cap.Stdout)
	}
	if strings.Contains(cap.Stdout, "Recent") {
		t.Errorf("Recent should be filtered out, got: %s", cap.Stdout)
	}
}

func TestSessionsList_InvalidInactiveFor(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON())
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--inactive-for", "bad"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "inactive-for") {
		t.Errorf("expected error mentioning inactive-for, got: %s", cap.Stderr)
	}
}

func TestSessionsListJSON(t *testing.T) {
	resetCmdFlags(t)
	session := makeSession(42, "Admin", 0, 1)
	session.Active = true
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(session))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--output", "json"})
		rootCmd.Execute()
	})

	var result []model.Session
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\noutput: %s", err, out)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 session, got %d", len(result))
	}
	if result[0].ID != 42 || result[0].User.Name != "Admin" || !result[0].Active {
		t.Errorf("unexpected session: %+v", result[0])
	}
}

func TestSessionsList_All(t *testing.T) {
	resetCmdFlags(t)
	sessions := make([]model.Session, 60)
	for i := range sessions {
		sessions[i] = makeSession(int64(i+1), "user", 1, 0)
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--all"})
		rootCmd.Execute()
	})

	if strings.Contains(cap.Stderr, "Showing 50 of 60") {
		t.Error("--all should not truncate to 50")
	}
	if strings.Count(cap.Stdout, "\n") < 61 {
		t.Errorf("expected at least 60 data rows, got output:\n%s", cap.Stdout)
	}
}

func TestSessionsList_Truncation(t *testing.T) {
	resetCmdFlags(t)
	sessions := make([]model.Session, 55)
	for i := range sessions {
		sessions[i] = makeSession(int64(i+1), "user", 1, 0)
	}
	var gotURL string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(gotURL, "$top=") {
		t.Errorf("expected $top in request URL when no filters active, got: %s", gotURL)
	}
	if !strings.Contains(cap.Stderr, "Showing 50 of 55") {
		t.Errorf("expected truncation summary on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsList_FilterSkipsTop(t *testing.T) {
	resetCmdFlags(t)
	sessions := []model.Session{makeSession(1, "Admin", 1, 0)}
	var gotURL string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--user", "admin"})
		rootCmd.Execute()
	})

	if strings.Contains(gotURL, "$top=") {
		t.Errorf("$top must not be sent when --user filter is active, got: %s", gotURL)
	}
}

func TestSessionsList_VerboseShowsEndpoint(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON())
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list", "--verbose"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Sessions") {
		t.Errorf("expected verbose stderr to mention Sessions endpoint, got: %s", cap.Stderr)
	}
}

func TestSessionsList_ThreadsExpandFallback(t *testing.T) {
	resetCmdFlags(t)
	sessions := []model.Session{makeSession(1, "Admin", 1, 0)}
	calls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		// First call (with Threads expand) → 400. Second call → success.
		if calls == 1 && strings.Contains(r.URL.RawQuery, "Threads") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":{"message":"Threads expand not supported"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Threads column unavailable") {
		t.Errorf("expected threads-fallback warning on stderr, got: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stdout, "Admin") {
		t.Errorf("expected Admin row in stdout after fallback, got: %s", cap.Stdout)
	}
	// THREADS column should render "-" (not "0") on fallback.
	lines := strings.Split(cap.Stdout, "\n")
	var dataLine string
	for _, line := range lines {
		if strings.Contains(line, "Admin") {
			dataLine = line
			break
		}
	}
	if !strings.Contains(dataLine, "-") {
		t.Errorf("expected '-' in THREADS column on fallback, got: %q", dataLine)
	}
	if calls != 2 {
		t.Errorf("expected exactly 2 GET calls (initial + retry), got %d", calls)
	}
}

// --- Integration: sessions close ---

// closeHandlerOpts controls the per-test behavior of newCloseHandler.
type closeHandlerOpts struct {
	activeSessionID     int64 // ID returned by GET /ActiveSession; 0 means "no active session"
	activeSessionStatus int   // override status code for /ActiveSession (0 = use 200)
	closeStatus         int   // status returned by POST /Sessions(...)/tm1.Close (default 200)
	postsCaptured       *int  // counter incremented on each POST
	postPathsCaptured   *[]string
}

func newCloseHandler(opts closeHandlerOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/ActiveSession"):
			if opts.activeSessionStatus != 0 && opts.activeSessionStatus != 200 {
				w.WriteHeader(opts.activeSessionStatus)
				w.Write([]byte(`{"error":"failed"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(activeSessionJSON(opts.activeSessionID))
		case r.Method == "POST" && strings.Contains(r.URL.Path, "tm1.Close"):
			if opts.postsCaptured != nil {
				*opts.postsCaptured++
			}
			if opts.postPathsCaptured != nil {
				*opts.postPathsCaptured = append(*opts.postPathsCaptured, r.URL.Path)
			}
			status := opts.closeStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			if status == http.StatusNoContent {
				return
			}
			w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"unexpected route"}`))
		}
	}
}

func TestSessionsClose_Success(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	paths := []string{}
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID:   999, // not us → no self-close warning
		postsCaptured:     &posts,
		postPathsCaptured: &paths,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "123"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected 1 POST, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "Closed session '123'") {
		t.Errorf("expected success message in stdout, got: %s", cap.Stdout)
	}
	if len(paths) != 1 || !strings.Contains(paths[0], "Sessions('123')/tm1.Close") {
		t.Errorf("expected POST to Sessions('123')/tm1.Close, got: %v", paths)
	}
}

func TestSessionsClose_204NoContent(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 999,
		closeStatus:     http.StatusNoContent,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "123"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stdout, "Closed session '123'") {
		t.Errorf("expected success on 204, got: %s", cap.Stdout)
	}
}

func TestSessionsClose_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42, // not 999 → not self-close
		closeStatus:     http.StatusNotFound,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "999"})
		rootCmd.Execute()
	})
	if !strings.Contains(cap.Stderr, "Session '999' not found") {
		t.Errorf("expected 'not found' error on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_SelfCloseAborts(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42,
		postsCaptured:   &posts,
	}))
	injectStdin(t, "n\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected no POST when user declines self-close, got %d", posts)
	}
	if !strings.Contains(cap.Stderr, "your active session") {
		t.Errorf("expected self-close warning on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_SelfCloseWithYes(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42,
		postsCaptured:   &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42", "--yes"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected POST when --yes set on self-close, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "Closed session '42'") {
		t.Errorf("expected close confirmation, got: %s", cap.Stdout)
	}
}

func TestSessionsClose_DryRun(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 999,
		postsCaptured:   &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "123", "--dry-run"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected 0 POSTs on --dry-run, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "[dry-run] Would close session '123'") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestSessionsClose_InvalidID(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		postsCaptured: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "abc"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected no HTTP traffic on invalid ID, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stderr, "Invalid session ID") {
		t.Errorf("expected validation error on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_JSON_Success(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 999,
	}))

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "123", "--yes", "--output", "json"})
		rootCmd.Execute()
	})

	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, out)
	}
	if result["status"] != "closed" || result["id"] != "123" {
		t.Errorf("unexpected JSON result: %+v", result)
	}
}

func TestSessionsClose_JSONNotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42,
		closeStatus:     http.StatusNotFound,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "999", "--output", "json"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, `"error"`) || !strings.Contains(cap.Stderr, "Session '999' not found") {
		t.Errorf("expected JSON-shaped error mentioning 'Session 999 not found', got: %s", cap.Stderr)
	}
}

func TestSessionsClose_JSONInvalidID(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "abc", "--output", "json"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, `"error"`) || !strings.Contains(cap.Stderr, "Invalid session ID") {
		t.Errorf("expected JSON-shaped error mentioning 'Invalid session ID', got: %s", cap.Stderr)
	}
}

func TestSessionsClose_ActiveSessionLookupFails(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionStatus: http.StatusInternalServerError,
		postsCaptured:       &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42", "--verbose"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected POST to proceed despite ActiveSession lookup failure, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stderr, "could not verify whether this is your active session") {
		t.Errorf("expected user-visible warning on stderr, got: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stderr, "[verbose] ActiveSession lookup error") {
		t.Errorf("expected verbose diagnostic on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_YesSkipsActiveSessionLookup(t *testing.T) {
	resetCmdFlags(t)
	getCalls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			getCalls++
		}
		if r.Method == "POST" && strings.Contains(r.URL.Path, "tm1.Close") {
			w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42", "--yes"})
		rootCmd.Execute()
	})

	if getCalls != 0 {
		t.Errorf("expected no GET when --yes is set, got %d", getCalls)
	}
	if !strings.Contains(cap.Stdout, "Closed session '42'") {
		t.Errorf("expected close success, got: %s", cap.Stdout)
	}
}
