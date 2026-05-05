package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// --- Unit tests ---

func TestFilterSessions(t *testing.T) {
	sessions := []model.Session{
		{ID: 1, User: model.SessionUser{Name: "Admin"}},
		{ID: 2, User: model.SessionUser{Name: "UserA"}},
		{ID: 3, User: model.SessionUser{Name: "worker"}},
	}

	tests := []struct {
		name    string
		user    string
		wantIDs []int64
	}{
		{name: "no filter returns all", wantIDs: []int64{1, 2, 3}},
		{name: "filter by user case-insensitively", user: "admin", wantIDs: []int64{1}},
		{name: "partial match", user: "user", wantIDs: []int64{2}},
		{name: "all filtered out returns nil", user: "nobody", wantIDs: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSessions(sessions, tt.user)
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

func makeSession(id int64, user string, threads int) model.Session {
	threadList := make([]model.SessionThread, threads)
	for i := 0; i < threads; i++ {
		threadList[i] = model.SessionThread{ID: int64(i + 1)}
	}
	return model.Session{
		ID:      id,
		User:    model.SessionUser{Name: user},
		Context: "REST",
		Active:  false,
		Threads: threadList,
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
		makeSession(1, "Admin", 2),
		makeSession(2, "UserA", 0),
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
		makeSession(1, "Admin", 0),
		makeSession(2, "UserA", 0),
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

func TestSessionsListJSON(t *testing.T) {
	resetCmdFlags(t)
	session := makeSession(42, "Admin", 1)
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
		sessions[i] = makeSession(int64(i+1), "user", 0)
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

func TestSessionsList_TruncationUsesODataCount(t *testing.T) {
	resetCmdFlags(t)
	// Return 150 rows but tell the client the server actually has 283 via
	// @odata.count. Summary must reflect the authoritative server total.
	sessions := make([]model.Session, 150)
	for i := range sessions {
		sessions[i] = makeSession(int64(i+1), "user", 0)
	}
	body := struct {
		Count int64           `json:"@odata.count"`
		Value []model.Session `json:"value"`
	}{Count: 283, Value: sessions}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(body)
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Showing 50 of 283") {
		t.Errorf("expected truncation summary to use @odata.count (283), got: %s", cap.Stderr)
	}
	if strings.Contains(cap.Stderr, "of 150") || strings.Contains(cap.Stderr, "150+") {
		t.Errorf("summary should not show 150 when server reported 283, got: %s", cap.Stderr)
	}
}

func TestSessionsList_TruncationApproximateOnCapHit(t *testing.T) {
	resetCmdFlags(t)
	// Server doesn't honor $count=true (no @odata.count in body) AND
	// returns exactly limit+probe rows → summary must signal cap-hit
	// with a "+" instead of asserting precision we don't have.
	sessions := make([]model.Session, 150)
	for i := range sessions {
		sessions[i] = makeSession(int64(i+1), "user", 0)
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sessionsJSON(sessions...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Showing 50 of 150+") {
		t.Errorf("expected approximate-total summary 'Showing 50 of 150+', got: %s", cap.Stderr)
	}
}

func TestSessionsList_Truncation(t *testing.T) {
	resetCmdFlags(t)
	sessions := make([]model.Session, 55)
	for i := range sessions {
		sessions[i] = makeSession(int64(i+1), "user", 0)
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
	sessions := []model.Session{makeSession(1, "Admin", 0)}
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

	if !strings.Contains(cap.Stderr, "[verbose]") || !strings.Contains(cap.Stderr, "GET ") || !strings.Contains(cap.Stderr, "/Sessions") {
		t.Errorf("expected verbose stderr to log the GET /Sessions request, got: %s", cap.Stderr)
	}
}

func TestSessionsList_ThreadsExpandFallbackRetryFails(t *testing.T) {
	resetCmdFlags(t)
	calls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"$expand=Threads not supported"}`))
			return
		}
		// Retry fails with a different, actionable error (e.g. auth lost mid-call).
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"upstream went away"}`))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "HTTP 500") {
		t.Errorf("expected retry-error (HTTP 500) on stderr, got: %s", cap.Stderr)
	}
	if strings.Contains(cap.Stderr, "Threads column unavailable") {
		t.Errorf("fallback warning should not fire when retry itself fails, got: %s", cap.Stderr)
	}
	if calls != 2 {
		t.Errorf("expected 2 GET attempts (initial + retry), got %d", calls)
	}
}

func TestSessionsList_ThreadsExpandFallback(t *testing.T) {
	resetCmdFlags(t)
	sessions := []model.Session{makeSession(1, "Admin", 0)}
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
	if len(paths) != 1 || !strings.Contains(paths[0], "Sessions(123)/tm1.Close") {
		t.Errorf("expected POST to Sessions(123)/tm1.Close, got: %v", paths)
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
	httpCalls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		httpCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "123", "--dry-run"})
		rootCmd.Execute()
	})

	// Dry-run is a pure preview: zero HTTP traffic, including the
	// /ActiveSession lookup that drives self-close detection.
	if httpCalls != 0 {
		t.Errorf("expected 0 HTTP calls on --dry-run, got %d", httpCalls)
	}
	if !strings.Contains(cap.Stdout, "[dry-run] Would close session '123'") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestSessionsClose_SelfCloseLeadingZero(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42,
		postsCaptured:   &posts,
	}))
	injectStdin(t, "n\n")

	cap := captureAll(t, func() {
		// Leading zeros must still match the active session ID 42 numerically.
		rootCmd.SetArgs([]string{"sessions", "close", "0042"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected leading-zero ID to be detected as self-close (no POST), got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stderr, "your active session") {
		t.Errorf("expected self-close warning, got: %s", cap.Stderr)
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

func TestSessionsClose_ActiveSessionLookupFails_AbortDeclined(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionStatus: http.StatusInternalServerError,
		postsCaptured:       &posts,
	}))
	injectStdin(t, "n\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42", "--verbose"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected fail-closed abort (no POST) when user declines, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stderr, "could not verify whether this is your active session") {
		t.Errorf("expected user-visible warning on stderr, got: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stderr, "[verbose] ActiveSession lookup error") {
		t.Errorf("expected verbose diagnostic on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_ActiveSessionLookupFails_ConfirmProceed(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionStatus: http.StatusInternalServerError,
		postsCaptured:       &posts,
	}))
	injectStdin(t, "y\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected POST after explicit confirm, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stderr, "could not verify whether this is your active session") {
		t.Errorf("expected user-visible warning on stderr, got: %s", cap.Stderr)
	}
}

func TestSessionsClose_JSON_PromptDoesNotCorruptStdout(t *testing.T) {
	resetCmdFlags(t)
	// Self-close path WITHOUT --yes triggers an interactive prompt.
	// In --output json mode, the prompt must not leak onto stdout.
	posts := 0
	setupMockTM1(t, newCloseHandler(closeHandlerOpts{
		activeSessionID: 42,
		postsCaptured:   &posts,
	}))
	injectStdin(t, "y\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"sessions", "close", "42", "--output", "json"})
		rootCmd.Execute()
	})

	if strings.Contains(cap.Stdout, "(y/N)") || strings.Contains(cap.Stdout, "Continue") {
		t.Errorf("prompt text leaked onto stdout (corrupts JSON consumers): %q", cap.Stdout)
	}
	// Stdout should be ONLY the success JSON document.
	var result map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(cap.Stdout)), &result); err != nil {
		t.Fatalf("stdout is not clean JSON: %v\nstdout: %q", err, cap.Stdout)
	}
	if result["status"] != "closed" || result["id"] != "42" {
		t.Errorf("unexpected JSON: %+v", result)
	}
	if posts != 1 {
		t.Errorf("expected POST after explicit confirm, got %d", posts)
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
