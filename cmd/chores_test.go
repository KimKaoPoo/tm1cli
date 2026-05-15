package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
	"tm1cli/internal/model"
)

// --- Unit: formatChoreFrequency / parseISO8601Duration ---

func TestFormatChoreFrequency(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"P1D", "Every 1 day"},
		{"P1DT0H0M0S", "Every 1 day"},
		{"PT1H", "Every 1 hour"},
		{"PT30M", "Every 30 minutes"},
		{"PT1M30S", "Every 1 minute 30 seconds"},
		{"P0DT0H0M0S", "Every 0 seconds"},
		{"P0D", "Every 0 seconds"},
		{"P7DT12H0M0S", "Every 7 days 12 hours"},
		{"P2DT3H4M5S", "Every 2 days 3 hours 4 minutes 5 seconds"},
		{"PT45S", "Every 45 seconds"},
		{"garbage", "garbage"},
		{"", ""},
		{"P1W", "P1W"},
		{"P1Y", "P1Y"},
		{"P1DT", "P1DT"},
		{"P1H", "P1H"},
		{"PT", "PT"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := formatChoreFrequency(tt.in)
			if got != tt.want {
				t.Errorf("formatChoreFrequency(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseISO8601Duration(t *testing.T) {
	tests := []struct {
		in                       string
		days, hours, mins, secs  int
		ok                       bool
	}{
		{"P1D", 1, 0, 0, 0, true},
		{"P0D", 0, 0, 0, 0, true},
		{"PT1H30M", 0, 1, 30, 0, true},
		{"P7DT12H", 7, 12, 0, 0, true},
		{"PT0H0M0S", 0, 0, 0, 0, true},
		{"P1W", 0, 0, 0, 0, false},
		{"P1Y", 0, 0, 0, 0, false},
		{"P1DT", 0, 0, 0, 0, false},
		{"P1H", 0, 0, 0, 0, false},
		{"", 0, 0, 0, 0, false},
		{"foo", 0, 0, 0, 0, false},
		{"P", 0, 0, 0, 0, false},
		// Out-of-order time fields should fail (we accept H,M,S strictly).
		{"PT1S1H", 0, 0, 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			d, h, m, s, ok := parseISO8601Duration(tt.in)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if d != tt.days || h != tt.hours || m != tt.mins || s != tt.secs {
				t.Errorf("got d=%d h=%d m=%d s=%d, want d=%d h=%d m=%d s=%d",
					d, h, m, s, tt.days, tt.hours, tt.mins, tt.secs)
			}
		})
	}
}

// --- Unit: odataKey ---

func TestOdataKey(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Daily", "Daily"},
		{"Daily Load", "Daily%20Load"},
		{"Daily's Load", "Daily%27%27s%20Load"},
		{"a/b", "a%2Fb"},
		{"It's a name's", "It%27%27s%20a%20name%27%27s"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := odataKey(tt.in)
			if got != tt.want {
				t.Errorf("odataKey(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// --- Unit: filters ---

func TestFilterChoresByActive(t *testing.T) {
	chores := []model.Chore{
		{Name: "A", Active: true},
		{Name: "B", Active: false},
		{Name: "C", Active: true},
	}
	tests := []struct {
		name             string
		active, inactive bool
		wantNames        []string
	}{
		{"no flags returns all", false, false, []string{"A", "B", "C"}},
		{"active returns only active", true, false, []string{"A", "C"}},
		{"inactive returns only inactive", false, true, []string{"B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterChoresByActive(chores, tt.active, tt.inactive)
			if len(got) != len(tt.wantNames) {
				t.Fatalf("got %d, want %d", len(got), len(tt.wantNames))
			}
			for i, name := range tt.wantNames {
				if got[i].Name != name {
					t.Errorf("got[%d].Name = %q, want %q", i, got[i].Name, name)
				}
			}
		})
	}
}

func TestFilterChoresByName(t *testing.T) {
	chores := []model.Chore{
		{Name: "DailyLoad"},
		{Name: "WeeklyReport"},
		{Name: "dailyBackup"},
	}
	got := filterChoresByName(chores, "daily")
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
	if got[0].Name != "DailyLoad" || got[1].Name != "dailyBackup" {
		t.Errorf("unexpected matches: %+v", got)
	}
}

// --- Unit: renderChoreParams ---

func TestRenderChoreParams(t *testing.T) {
	tests := []struct {
		name string
		in   []model.ChoreTaskParam
		want string
	}{
		{"empty", nil, "(none)"},
		{"single string", []model.ChoreTaskParam{{Name: "pYear", Value: "2024"}}, "pYear=2024"},
		{"multiple", []model.ChoreTaskParam{
			{Name: "pYear", Value: float64(2024)},
			{Name: "pSource", Value: "file.csv"},
		}, "pYear=2024, pSource=file.csv"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := renderChoreParams(tt.in)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Integration: chores list ---

func sampleChores() []model.Chore {
	return []model.Chore{
		{
			Name: "DailyLoad", Active: true, StartTime: "2025-01-01T08:00:00",
			DSTSensitive: false, Frequency: "P1DT0H0M0S",
			Tasks: []model.ChoreTask{{Step: 0}, {Step: 1}},
		},
		{
			Name: "WeeklyReport", Active: false, StartTime: "2025-01-01T09:00:00",
			DSTSensitive: true, Frequency: "P7DT0H0M0S",
			Tasks: []model.ChoreTask{{Step: 0}},
		},
	}
}

func TestChoresList_Table(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list"})
		rootCmd.Execute()
	})

	for _, want := range []string{"DailyLoad", "WeeklyReport", "Every 1 day", "Every 7 days", "true", "false"} {
		if !strings.Contains(cap.Stdout, want) {
			t.Errorf("stdout missing %q\nstdout: %s", want, cap.Stdout)
		}
	}
	// Tasks count rendered.
	if !strings.Contains(cap.Stdout, "2") || !strings.Contains(cap.Stdout, "1") {
		t.Errorf("expected tasks counts 1 and 2 in stdout: %s", cap.Stdout)
	}
}

func TestChoresList_JSON(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--output", "json"})
		rootCmd.Execute()
	})

	var got []model.Chore
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, out)
	}
	if len(got) != 2 {
		t.Fatalf("got %d chores, want 2", len(got))
	}
	// JSON preserves raw ISO 8601 frequency.
	if got[0].Frequency != "P1DT0H0M0S" {
		t.Errorf("Frequency = %q, want raw ISO 8601 P1DT0H0M0S", got[0].Frequency)
	}
}

func TestChoresList_Empty(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON())
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--output", "json"})
		rootCmd.Execute()
	})

	var got []model.Chore
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty list, got %+v", got)
	}
}

func TestChoresList_ZeroTasks(t *testing.T) {
	resetCmdFlags(t)
	chore := model.Chore{Name: "EmptyChore", Active: true, Frequency: "P1D"}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(chore))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "EmptyChore") {
		t.Errorf("missing chore name: %s", out)
	}
	// Tasks column reads 0.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected header+row, got: %s", out)
	}
	if !strings.HasSuffix(strings.TrimSpace(lines[1]), "0") {
		t.Errorf("expected row to end with task count 0, got: %q", lines[1])
	}
}

func TestChoresList_FilterServerSide(t *testing.T) {
	resetCmdFlags(t)
	var gotURL string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()[:1]...))
	})

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--filter", "load"})
		rootCmd.Execute()
	})

	if !strings.Contains(gotURL, "%24filter=") {
		t.Errorf("expected $filter in URL: %s", gotURL)
	}
	if !strings.Contains(gotURL, "contains") {
		t.Errorf("expected contains() in URL: %s", gotURL)
	}
}

func TestChoresList_FilterFallback(t *testing.T) {
	resetCmdFlags(t)
	calls := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.URL.RawQuery, "%24filter=") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"filter not supported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--filter", "Daily"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Server-side filter not supported") {
		t.Errorf("expected fallback warning, got stderr: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stdout, "DailyLoad") {
		t.Errorf("expected DailyLoad in stdout: %s", cap.Stdout)
	}
	if strings.Contains(cap.Stdout, "WeeklyReport") {
		t.Errorf("WeeklyReport should be filtered out client-side: %s", cap.Stdout)
	}
	if calls != 2 {
		t.Errorf("expected 2 server calls (filtered + unfiltered), got %d", calls)
	}
}

func TestChoresList_Active(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--active"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "DailyLoad") {
		t.Errorf("expected active chore in output: %s", out)
	}
	if strings.Contains(out, "WeeklyReport") {
		t.Errorf("inactive chore should be filtered out: %s", out)
	}
}

func TestChoresList_Inactive(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--inactive"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "WeeklyReport") {
		t.Errorf("expected inactive chore in output: %s", out)
	}
	if strings.Contains(out, "DailyLoad") {
		t.Errorf("active chore should be filtered out: %s", out)
	}
}

func TestChoresList_ActiveAndFilter(t *testing.T) {
	resetCmdFlags(t)
	chores := []model.Chore{
		{Name: "DailyLoad", Active: true, Frequency: "P1D"},
		{Name: "DailyReport", Active: false, Frequency: "P1D"},
		{Name: "WeeklyArchive", Active: true, Frequency: "P7D"},
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		// Mimic TM1's server-side $filter: when present, return only matching names.
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.RawQuery, "%24filter=") {
			var subset []model.Chore
			for _, c := range chores {
				if strings.Contains(strings.ToLower(c.Name), "daily") {
					subset = append(subset, c)
				}
			}
			w.Write(choresJSON(subset...))
			return
		}
		w.Write(choresJSON(chores...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--active", "--filter", "daily"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "DailyLoad") {
		t.Errorf("expected DailyLoad: %s", out)
	}
	if strings.Contains(out, "DailyReport") {
		t.Errorf("DailyReport (inactive) should be filtered: %s", out)
	}
	if strings.Contains(out, "WeeklyArchive") {
		t.Errorf("WeeklyArchive (no name match) should be filtered: %s", out)
	}
}

func TestChoresList_ActiveInactiveMutexError(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON())
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--active", "--inactive"})
		rootCmd.Execute()
	})

	if cap.Stderr == "" {
		t.Error("expected error on stderr when both --active and --inactive set")
	}
	if !strings.Contains(cap.Stderr, "mutually exclusive") {
		t.Errorf("expected mutex error message, got: %s", cap.Stderr)
	}
}

func TestChoresList_MalformedJSON(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not valid json`))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Cannot parse server response") {
		t.Errorf("expected parse-error message on stderr, got: %s", cap.Stderr)
	}
}

// --- Integration: chores show ---

func sampleChoreDetail() model.Chore {
	return model.Chore{
		Name: "DailyLoad", Active: true, StartTime: "2025-01-01T08:00:00",
		DSTSensitive: false, Frequency: "P1DT0H0M0S",
		Tasks: []model.ChoreTask{
			{
				Step:    0,
				Process: model.ChoreTaskProcess{Name: "LoadSales"},
				Parameters: []model.ChoreTaskParam{
					{Name: "pYear", Value: "2024"},
				},
			},
			{
				Step:       1,
				Process:    model.ChoreTaskProcess{Name: "RunReport"},
				Parameters: []model.ChoreTaskParam{},
			},
		},
	}
}

func TestChoresShow_Table(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choreDetailJSON(sampleChoreDetail()))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "DailyLoad"})
		rootCmd.Execute()
	})

	for _, want := range []string{
		"Name:           DailyLoad",
		"Active:         true",
		"StartTime:      2025-01-01T08:00:00",
		"DSTSensitive:   false",
		"Frequency:      Every 1 day",
		"STEP", "PROCESS", "PARAMETERS",
		"LoadSales", "pYear=2024", "RunReport", "(none)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestChoresShow_JSON(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choreDetailJSON(sampleChoreDetail()))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "DailyLoad", "--output", "json"})
		rootCmd.Execute()
	})

	var got model.Chore
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, out)
	}
	if got.Name != "DailyLoad" {
		t.Errorf("Name = %q, want DailyLoad", got.Name)
	}
	if got.Frequency != "P1DT0H0M0S" {
		t.Errorf("Frequency = %q, want raw P1DT0H0M0S", got.Frequency)
	}
	if len(got.Tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(got.Tasks))
	}
}

func TestChoresShow_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":{"code":"NotFound"}}`))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "Missing"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Missing") || !strings.Contains(cap.Stderr, "not found") {
		t.Errorf("expected not-found error mentioning chore name, got: %s", cap.Stderr)
	}
}

// TestChoresShow_NotFoundExitCode verifies the not-found path returns the
// errExit(3) sentinel so the binary exits with code 3, as documented in the
// command help. We invoke runChoresShow directly so we can inspect the
// returned error rather than relying on rootCmd.Execute()'s os.Exit.
func TestChoresShow_NotFoundExitCode(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":{"code":"NotFound"}}`))
	})

	var err error
	captureAll(t, func() {
		err = runChoresShow(choresShowCmd, []string{"Missing"})
	})

	if err == nil {
		t.Fatal("expected non-nil error from runChoresShow on 404")
	}
	if got := exitCodeForError(err); got != 3 {
		t.Errorf("exit code = %d, want 3", got)
	}
}

func TestChoresShow_NotFoundJSON(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{}`))
	})

	stderr := captureStderr(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "Missing", "--output", "json"})
		rootCmd.Execute()
	})

	// SilenceUsage on choresShowCmd ensures cobra does not append a Usage
	// block after our JSON error, so the entire stderr is a single JSON object.
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(stderr), &obj); err != nil {
		t.Fatalf("expected pure JSON error object on stderr, got: %s", stderr)
	}
	if _, ok := obj["error"]; !ok {
		t.Errorf("expected 'error' key in JSON object, got: %v", obj)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr must not contain cobra Usage block in --output json mode, got: %s", stderr)
	}
}

func TestChoresShow_SpecialChars(t *testing.T) {
	resetCmdFlags(t)
	var gotPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		w.Write(choreDetailJSON(model.Chore{Name: "Daily's Load", Frequency: "P1D"}))
	})

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "Daily's Load"})
		rootCmd.Execute()
	})

	// Apostrophe must be doubled (OData literal escape) then percent-encoded.
	if !strings.Contains(gotPath, "Chores('Daily%27%27s%20Load')") {
		t.Errorf("expected escaped path Chores('Daily%%27%%27s%%20Load'), got: %s", gotPath)
	}
}

func TestChoresShow_TasksSortedByStep(t *testing.T) {
	resetCmdFlags(t)
	chore := model.Chore{
		Name: "OutOfOrder", Frequency: "P1D",
		Tasks: []model.ChoreTask{
			{Step: 3, Process: model.ChoreTaskProcess{Name: "ThirdProc"}},
			{Step: 1, Process: model.ChoreTaskProcess{Name: "FirstProc"}},
			{Step: 2, Process: model.ChoreTaskProcess{Name: "SecondProc"}},
		},
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choreDetailJSON(chore))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "OutOfOrder"})
		rootCmd.Execute()
	})

	idxFirst := strings.Index(out, "FirstProc")
	idxSecond := strings.Index(out, "SecondProc")
	idxThird := strings.Index(out, "ThirdProc")
	if idxFirst < 0 || idxSecond < 0 || idxThird < 0 {
		t.Fatalf("missing process names in output: %s", out)
	}
	if !(idxFirst < idxSecond && idxSecond < idxThird) {
		t.Errorf("tasks not sorted by step (got order First=%d Second=%d Third=%d):\n%s",
			idxFirst, idxSecond, idxThird, out)
	}

	// JSON output should also be sorted.
	resetCmdFlags(t)
	jsonOut := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "OutOfOrder", "--output", "json"})
		rootCmd.Execute()
	})
	var got model.Chore
	if err := json.Unmarshal([]byte(jsonOut), &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, jsonOut)
	}
	if len(got.Tasks) != 3 {
		t.Fatalf("got %d tasks, want 3", len(got.Tasks))
	}
	if got.Tasks[0].Step != 1 || got.Tasks[1].Step != 2 || got.Tasks[2].Step != 3 {
		t.Errorf("JSON tasks not sorted: %+v", got.Tasks)
	}
}

// --- Unit: filterSystemChores ---

func TestFilterSystemChores(t *testing.T) {
	chores := []model.Chore{
		{Name: "DailyLoad"},
		{Name: "}StatsRefresh"},
		{Name: "WeeklyReport"},
	}
	got := filterSystemChores(chores, false)
	if len(got) != 2 || got[0].Name != "DailyLoad" || got[1].Name != "WeeklyReport" {
		t.Errorf("system chore not filtered: %+v", got)
	}
	gotAll := filterSystemChores(chores, true)
	if len(gotAll) != 3 {
		t.Errorf("expected all chores when showSystem=true, got %d", len(gotAll))
	}
}

// --- Integration: --show-system, --limit, --all ---

func TestChoresList_HidesSystemByDefault(t *testing.T) {
	resetCmdFlags(t)
	chores := []model.Chore{
		{Name: "DailyLoad", Active: true, Frequency: "P1D"},
		{Name: "}StatsRefresh", Active: true, Frequency: "P1D"},
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(chores...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "DailyLoad") {
		t.Errorf("expected user chore in output: %s", out)
	}
	if strings.Contains(out, "}StatsRefresh") {
		t.Errorf("system chore should be hidden by default: %s", out)
	}
}

func TestChoresList_ShowSystemFlag(t *testing.T) {
	resetCmdFlags(t)
	chores := []model.Chore{
		{Name: "DailyLoad", Active: true, Frequency: "P1D"},
		{Name: "}StatsRefresh", Active: true, Frequency: "P1D"},
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(chores...))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--show-system"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "}StatsRefresh") {
		t.Errorf("--show-system should reveal system chores: %s", out)
	}
}

func TestChoresList_DefaultLimitTruncates(t *testing.T) {
	resetCmdFlags(t)
	chores := make([]model.Chore, 55)
	for i := range chores {
		chores[i] = model.Chore{Name: fmt.Sprintf("Chore%02d", i), Active: true, Frequency: "P1D"}
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(chores...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list"})
		rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Showing 50 of 55") {
		t.Errorf("expected truncation summary, got stderr: %s", cap.Stderr)
	}
}

func TestChoresList_AllDisablesLimit(t *testing.T) {
	resetCmdFlags(t)
	chores := make([]model.Chore, 55)
	for i := range chores {
		chores[i] = model.Chore{Name: fmt.Sprintf("Chore%02d", i), Active: true, Frequency: "P1D"}
	}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(chores...))
	})

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--all"})
		rootCmd.Execute()
	})

	if strings.Contains(cap.Stderr, "Showing 50 of 55") {
		t.Errorf("--all should not truncate, got stderr: %s", cap.Stderr)
	}
	// 55 data rows + 1 header line, plus tabwriter padding lines.
	if strings.Count(cap.Stdout, "\n") < 56 {
		t.Errorf("expected at least 55 data rows in output, got %d lines", strings.Count(cap.Stdout, "\n"))
	}
}

// TestChoresList_TopOmittedWithFilter verifies that --filter suppresses
// server-side $top so the client has the complete set to substring-match
// against (the server-side $filter may not be honored on every TM1 version).
func TestChoresList_TopOmittedWithFilter(t *testing.T) {
	resetCmdFlags(t)
	var gotURLs []string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotURLs = append(gotURLs, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--filter", "daily"})
		rootCmd.Execute()
	})

	for _, q := range gotURLs {
		if strings.Contains(q, "$top=") {
			t.Errorf("$top must not be sent when --filter is active: %s", q)
		}
	}
}

// TestChoresList_TopSentByDefault verifies the default invocation honors
// the 50-row limit by sending $top=N+500 server-side, matching the
// cubes/dims convention so we don't fetch the full collection unnecessarily.
// --show-system, --active, and --inactive must NOT suppress $top because
// the +500 cushion absorbs their client-side trimming.
func TestChoresList_TopSentByDefault(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"plain default", []string{"chores", "list"}},
		{"with --show-system", []string{"chores", "list", "--show-system"}},
		{"with --active", []string{"chores", "list", "--active"}},
		{"with --inactive", []string{"chores", "list", "--inactive"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCmdFlags(t)
			var gotURL string
			setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
				gotURL = r.URL.RawQuery
				w.Header().Set("Content-Type", "application/json")
				w.Write(choresJSON(sampleChores()...))
			})

			captureAll(t, func() {
				rootCmd.SetArgs(tc.args)
				rootCmd.Execute()
			})

			if !strings.Contains(gotURL, "$top=") {
				t.Errorf("expected $top in URL for %v, got: %s", tc.args, gotURL)
			}
		})
	}
}

func TestChoresShow_NoTasks(t *testing.T) {
	resetCmdFlags(t)
	chore := model.Chore{Name: "Bare", Frequency: "P1D", Tasks: []model.ChoreTask{}}
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(choreDetailJSON(chore))
	})

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "show", "Bare"})
		rootCmd.Execute()
	})

	if !strings.Contains(out, "Name:           Bare") {
		t.Errorf("expected header block, got: %s", out)
	}
	if !strings.Contains(out, "STEP") {
		t.Errorf("expected STEP header even when empty, got: %s", out)
	}
}

// ============================================================
// chores activate / deactivate
// ============================================================

type choreToggleHandlerOpts struct {
	chore      *model.Chore
	getBody    []byte // overrides marshalled chore body when non-nil
	getStatus  int
	postStatus int
	posts      *int
	gets       *int
	postPaths  *[]string
	getPaths   *[]string
}

func newChoreToggleHandler(opts choreToggleHandlerOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Chores("):
			if opts.gets != nil {
				*opts.gets++
			}
			if opts.getPaths != nil {
				*opts.getPaths = append(*opts.getPaths, r.URL.Path)
			}
			status := opts.getStatus
			if status == 0 {
				if opts.chore == nil && opts.getBody == nil {
					status = http.StatusNotFound
				} else {
					status = http.StatusOK
				}
			}
			if status >= 400 {
				w.WriteHeader(status)
				w.Write([]byte(`{"error":"failed"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if opts.getBody != nil {
				w.Write(opts.getBody)
				return
			}
			body, _ := json.Marshal(opts.chore)
			w.Write(body)
		case r.Method == "POST" &&
			(strings.Contains(r.URL.Path, "tm1.Activate") || strings.Contains(r.URL.Path, "tm1.Deactivate")):
			if opts.posts != nil {
				*opts.posts++
			}
			if opts.postPaths != nil {
				*opts.postPaths = append(*opts.postPaths, r.URL.Path)
			}
			status := opts.postStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			if status == http.StatusNoContent {
				return
			}
			if status >= 400 {
				w.Write([]byte(`{"error":"failed"}`))
				return
			}
			w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"unexpected route"}`))
		}
	}
}

func TestChoresActivate_Success(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	postPaths := []string{}
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:     &model.Chore{Name: "Daily", Active: false},
		posts:     &posts,
		postPaths: &postPaths,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected 1 POST, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "Chore 'Daily' activated.") {
		t.Errorf("expected success message in stdout, got: %s", cap.Stdout)
	}
	if len(postPaths) != 1 || !strings.Contains(postPaths[0], "Chores('Daily')/tm1.Activate") {
		t.Errorf("expected POST to Chores('Daily')/tm1.Activate, got: %v", postPaths)
	}
}

func TestChoresDeactivate_Success(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	postPaths := []string{}
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:     &model.Chore{Name: "Daily", Active: true},
		posts:     &posts,
		postPaths: &postPaths,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "deactivate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected 1 POST, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "Chore 'Daily' deactivated.") {
		t.Errorf("expected success message in stdout, got: %s", cap.Stdout)
	}
	if len(postPaths) != 1 || !strings.Contains(postPaths[0], "Chores('Daily')/tm1.Deactivate") {
		t.Errorf("expected POST to Chores('Daily')/tm1.Deactivate, got: %v", postPaths)
	}
}

func TestChoresActivate_AlreadyActive_Idempotent(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: true},
		posts: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("idempotent activate must not POST, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stdout, "already active") {
		t.Errorf("expected 'already active' info in stdout, got: %s", cap.Stdout)
	}
}

func TestChoresDeactivate_AlreadyInactive_Idempotent(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "deactivate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("idempotent deactivate must not POST, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stdout, "already inactive") {
		t.Errorf("expected 'already inactive' info in stdout, got: %s", cap.Stdout)
	}
}

func TestChoresActivate_NotFound(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		getStatus: http.StatusNotFound,
		posts:     &posts,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Missing", "--yes"})
		execErr = rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected no POST on not-found, got %d", posts)
	}
	if !strings.Contains(cap.Stderr, "Chore 'Missing' not found.") {
		t.Errorf("expected 'not found' on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if !errors.As(execErr, &coded) {
		t.Fatalf("expected *silentErrCoded, got: %T %v", execErr, execErr)
	}
	if coded.code != 3 {
		t.Errorf("expected exit code 3, got %d", coded.code)
	}
}

func TestChoresDeactivate_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		getStatus: http.StatusNotFound,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "deactivate", "Missing", "--yes"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Chore 'Missing' not found.") {
		t.Errorf("expected 'not found' on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if !errors.As(execErr, &coded) || coded.code != 3 {
		t.Errorf("expected exit code 3, got: %v", execErr)
	}
}

func TestChoresActivate_PermissionDenied(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:      &model.Chore{Name: "Daily", Active: false},
		postStatus: http.StatusForbidden,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Permission denied") {
		t.Errorf("expected permission-denied message on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if errors.As(execErr, &coded) {
		t.Errorf("403 should map to errSilent (exit 1), got coded exit %d", coded.code)
	}
}

func TestChoresDeactivate_PermissionDenied(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:      &model.Chore{Name: "Daily", Active: true},
		postStatus: http.StatusForbidden,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "deactivate", "Daily", "--yes"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Permission denied") {
		t.Errorf("expected permission-denied message on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if errors.As(execErr, &coded) {
		t.Errorf("403 should map to errSilent (exit 1), got coded exit %d", coded.code)
	}
}

func TestChoresActivate_GET_PermissionDenied(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		getStatus: http.StatusForbidden,
		posts:     &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected no POST when GET returns 403, got %d", posts)
	}
	if !strings.Contains(cap.Stderr, "Permission denied") {
		t.Errorf("expected permission-denied message on stderr, got: %s", cap.Stderr)
	}
}

func TestChoresActivate_PostNotFoundRace(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:      &model.Chore{Name: "Daily", Active: false},
		postStatus: http.StatusNotFound,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "Chore 'Daily' not found.") {
		t.Errorf("expected 'not found' on stderr after race, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if !errors.As(execErr, &coded) || coded.code != 3 {
		t.Errorf("expected exit code 3, got: %v", execErr)
	}
}

func TestChoresActivate_GenericServerError(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:      &model.Chore{Name: "Daily", Active: false},
		postStatus: http.StatusInternalServerError,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, "HTTP 500") {
		t.Errorf("expected HTTP 500 message on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if errors.As(execErr, &coded) {
		t.Errorf("500 should map to errSilent (exit 1), got coded exit %d", coded.code)
	}
}

func TestChoresActivate_MissingActiveField(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		getBody: []byte(`{"Name":"Daily"}`),
		posts:   &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("must not POST when Active field missing, got %d", posts)
	}
	if !strings.Contains(cap.Stderr, "missing 'Active' field") {
		t.Errorf("expected missing-Active error, got: %s", cap.Stderr)
	}
}

func TestChoresActivate_DryRun(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	gets := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
		gets:  &gets,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--dry-run"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("dry-run must not POST, got %d POSTs", posts)
	}
	if gets != 1 {
		t.Errorf("expected 1 GET for state lookup, got %d", gets)
	}
	if !strings.Contains(cap.Stdout, "[dry-run] Would activate chore 'Daily'.") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestChoresDeactivate_DryRun(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: true},
		posts: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "deactivate", "Daily", "--dry-run"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("dry-run must not POST, got %d POSTs", posts)
	}
	if !strings.Contains(cap.Stdout, "[dry-run] Would deactivate chore 'Daily'.") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestChoresActivate_DryRunBeatsYes(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--dry-run", "--yes"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("--dry-run must take precedence over --yes (no POST), got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "[dry-run]") {
		t.Errorf("expected dry-run preview, got: %s", cap.Stdout)
	}
}

func TestChoresActivate_DryRun_JSON(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
	}))

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--dry-run", "--output", "json"})
		rootCmd.Execute()
	})

	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\noutput: %s", err, out)
	}
	if result["status"] != "dry-run" || result["chore"] != "Daily" || result["action"] != "activate" {
		t.Errorf("unexpected JSON result: %+v", result)
	}
}

func TestChoresActivate_PromptDecline(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))
	injectStdin(t, "n\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily"})
		rootCmd.Execute()
	})

	if posts != 0 {
		t.Errorf("expected no POST when user declines, got %d", posts)
	}
	if !strings.Contains(cap.Stderr, "About to activate chore 'Daily'.") {
		t.Errorf("expected confirmation banner on stderr, got: %s", cap.Stderr)
	}
}

func TestChoresActivate_PromptAccept(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))
	injectStdin(t, "y\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected POST after accept, got %d", posts)
	}
	if !strings.Contains(cap.Stdout, "Chore 'Daily' activated.") {
		t.Errorf("expected success message, got: %s", cap.Stdout)
	}
}

func TestChoresActivate_YesBypassesPrompt(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes"})
		rootCmd.Execute()
	})

	if posts != 1 {
		t.Errorf("expected 1 POST, got %d", posts)
	}
	if strings.Contains(cap.Stderr, "(y/N)") || strings.Contains(cap.Stderr, "About to activate") {
		t.Errorf("--yes should suppress prompt, but prompt text appeared: %s", cap.Stderr)
	}
}

func TestChoresActivate_JSON_Success(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
	}))

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes", "--output", "json"})
		rootCmd.Execute()
	})

	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\noutput: %s", err, out)
	}
	if result["status"] != "activated" || result["chore"] != "Daily" {
		t.Errorf("unexpected JSON result: %+v", result)
	}
}

func TestChoresActivate_JSON_NoopIdempotent(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: true},
	}))

	out := captureStdout(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--yes", "--output", "json"})
		rootCmd.Execute()
	})

	var result map[string]string
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\noutput: %s", err, out)
	}
	if result["status"] != "noop" || result["chore"] != "Daily" || result["active"] != "true" {
		t.Errorf("unexpected JSON result: %+v", result)
	}
}

func TestChoresActivate_JSON_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		getStatus: http.StatusNotFound,
	}))

	var execErr error
	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Missing", "--yes", "--output", "json"})
		execErr = rootCmd.Execute()
	})

	if !strings.Contains(cap.Stderr, `"error"`) || !strings.Contains(cap.Stderr, "Chore 'Missing' not found.") {
		t.Errorf("expected JSON-shaped error on stderr, got: %s", cap.Stderr)
	}
	var coded *silentErrCoded
	if !errors.As(execErr, &coded) || coded.code != 3 {
		t.Errorf("expected exit code 3, got: %v", execErr)
	}
}

func TestChoresActivate_JSON_PromptDoesNotCorruptStdout(t *testing.T) {
	resetCmdFlags(t)
	posts := 0
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore: &model.Chore{Name: "Daily", Active: false},
		posts: &posts,
	}))
	injectStdin(t, "y\n")

	cap := captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily", "--output", "json"})
		rootCmd.Execute()
	})

	if strings.Contains(cap.Stdout, "(y/N)") || strings.Contains(cap.Stdout, "Continue") || strings.Contains(cap.Stdout, "About to") {
		t.Errorf("prompt text leaked onto stdout (corrupts JSON consumers): %q", cap.Stdout)
	}
	var result map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(cap.Stdout)), &result); err != nil {
		t.Fatalf("stdout is not clean JSON: %v\nstdout: %q", err, cap.Stdout)
	}
	if result["status"] != "activated" || result["chore"] != "Daily" {
		t.Errorf("unexpected JSON: %+v", result)
	}
	if posts != 1 {
		t.Errorf("expected POST after accept, got %d", posts)
	}
}

func TestChoresActivate_NameWithQuote_URLEscaped(t *testing.T) {
	resetCmdFlags(t)
	postPaths := []string{}
	getPaths := []string{}
	setupMockTM1(t, newChoreToggleHandler(choreToggleHandlerOpts{
		chore:     &model.Chore{Name: "Daily's", Active: false},
		postPaths: &postPaths,
		getPaths:  &getPaths,
	}))

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "activate", "Daily's", "--yes"})
		rootCmd.Execute()
	})

	if len(postPaths) != 1 {
		t.Fatalf("expected 1 POST, got %d", len(postPaths))
	}
	// Single quote is OData-doubled by odataEscape, then URL-escaped on the wire.
	// httptest's r.URL.Path is the decoded form, so we observe the doubled-quote
	// literally as Daily''s. The URL-escape correctness is exercised separately
	// by TestOdataKey on odataKey.
	if !strings.Contains(postPaths[0], "Chores('Daily''s')/tm1.Activate") {
		t.Errorf("expected POST path with OData-doubled quote, got: %s", postPaths[0])
	}
	if len(getPaths) != 1 || !strings.Contains(getPaths[0], "Chores('Daily''s')") {
		t.Errorf("expected GET path with OData-doubled quote, got: %v", getPaths)
	}
}

// ============================================================
// chores run
// ============================================================

func TestRunChoresRun_Success(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !strings.Contains(cap.Stdout, "executed successfully") {
		t.Errorf("stdout missing 'executed successfully': %s", cap.Stdout)
	}
	if !strings.Contains(cap.Stdout, "Completed") {
		t.Errorf("stdout missing 'Completed': %s", cap.Stdout)
	}
}

func TestRunChoresRun_Async(t *testing.T) {
	resetCmdFlags(t)
	choreRunAsync = true
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "Threads('314')")
		w.WriteHeader(http.StatusAccepted)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !strings.Contains(cap.Stdout, "started asynchronously") {
		t.Errorf("stdout missing 'started asynchronously': %s", cap.Stdout)
	}
	if !strings.Contains(cap.Stdout, "314") {
		t.Errorf("stdout missing thread id '314': %s", cap.Stdout)
	}
}

func TestRunChoresRun_Async_JSON(t *testing.T) {
	resetCmdFlags(t)
	choreRunAsync = true
	flagOutput = "json"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "Threads('314')")
		w.WriteHeader(http.StatusAccepted)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "started" {
		t.Errorf("Status = %q, want %q", result.Status, "started")
	}
	if result.ThreadID != "314" {
		t.Errorf("ThreadID = %q, want %q", result.ThreadID, "314")
	}
}

func TestRunChoresRun_NotFound(t *testing.T) {
	resetCmdFlags(t)
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"not found"}`))
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Missing"})
	})

	if exitCodeForError(err) != 3 {
		t.Errorf("exit code = %d, want 3 (err: %v)", exitCodeForError(err), err)
	}
	if !strings.Contains(cap.Stderr, "Chore 'Missing' not found.") {
		t.Errorf("stderr missing 'not found' message: %s", cap.Stderr)
	}
}

func TestRunChoresRun_Timeout(t *testing.T) {
	resetCmdFlags(t)
	choreRunTimeout = 50 * time.Millisecond
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"SlowChore"})
	})

	if exitCodeForError(err) != 4 {
		t.Errorf("exit code = %d, want 4 (err: %v)", exitCodeForError(err), err)
	}
	if !strings.Contains(cap.Stderr, "did not complete within") {
		t.Errorf("stderr missing timeout message: %s", cap.Stderr)
	}
}

func TestRunChoresRun_TaskFailure(t *testing.T) {
	resetCmdFlags(t)
	failMsg := "Chore 'Nightly' failed at step 2: Process 'Bad' returned an error"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(failMsg))
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 1 {
		t.Errorf("exit code = %d, want 1", exitCodeForError(err))
	}
	if !strings.Contains(cap.Stderr, "failed at step 2") {
		t.Errorf("stderr missing failure message: %s", cap.Stderr)
	}
}

func TestRunChoresRun_TaskFailure_JSON(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"
	failMsg := "Chore 'Nightly' failed at step 2: Process 'Bad' returned an error"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(failMsg))
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 1 {
		t.Errorf("exit code = %d, want 1", exitCodeForError(err))
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "error" {
		t.Errorf("Status = %q, want %q", result.Status, "error")
	}
	if !strings.Contains(result.Message, "failed at step 2") {
		t.Errorf("Message does not contain server text: %q", result.Message)
	}
}

func TestRunChoresRun_NotFound_JSON(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`Not found`))
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 3 {
		t.Errorf("exit code = %d, want 3", exitCodeForError(err))
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "not_found" {
		t.Errorf("Status = %q, want %q", result.Status, "not_found")
	}
	if result.Chore != "Nightly" {
		t.Errorf("Chore = %q, want %q", result.Chore, "Nightly")
	}
	if !strings.Contains(result.Message, "not found") {
		t.Errorf("Message = %q, want it to mention 'not found'", result.Message)
	}
	if cap.Stderr != "" {
		t.Errorf("stderr should be empty in JSON mode, got: %q", cap.Stderr)
	}
}

func TestRunChoresRun_Timeout_JSON(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"
	choreRunTimeout = 50 * time.Millisecond
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 4 {
		t.Errorf("exit code = %d, want 4", exitCodeForError(err))
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "timeout" {
		t.Errorf("Status = %q, want %q", result.Status, "timeout")
	}
	if result.Timeout != choreRunTimeout.String() {
		t.Errorf("Timeout = %q, want %q", result.Timeout, choreRunTimeout.String())
	}
	if !strings.Contains(result.Message, "did not complete within") {
		t.Errorf("Message = %q, want it to mention 'did not complete within'", result.Message)
	}
	if cap.Stderr != "" {
		t.Errorf("stderr should be empty in JSON mode, got: %q", cap.Stderr)
	}
}

func TestRunChoresRun_JSONSuccess(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "completed" {
		t.Errorf("Status = %q, want %q", result.Status, "completed")
	}
	if result.Chore != "Nightly" {
		t.Errorf("Chore = %q, want %q", result.Chore, "Nightly")
	}
	// DurationMs must be present (no omitempty); verify the raw JSON contains the key.
	if !strings.Contains(cap.Stdout, `"duration_ms"`) {
		t.Errorf("JSON output missing 'duration_ms' field: %s", cap.Stdout)
	}
	if result.DurationMs < 0 {
		t.Errorf("DurationMs = %d, want >= 0", result.DurationMs)
	}
}

func TestRunChoresRun_VerbosePreflightListsTasks(t *testing.T) {
	resetCmdFlags(t)
	flagVerbose = true
	tasksBody := `{"Name":"Nightly","Tasks":[{"Step":1,"Process":{"Name":"LoadData"}},{"Step":2,"Process":{"Name":"RunReport"}}]}`
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(tasksBody))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if !strings.Contains(cap.Stderr, "Chore 'Nightly' has 2 task(s):") {
		t.Errorf("stderr missing task list header: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stderr, "[1] LoadData") {
		t.Errorf("stderr missing '[1] LoadData': %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stderr, "[2] RunReport") {
		t.Errorf("stderr missing '[2] RunReport': %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stdout, "executed successfully") {
		t.Errorf("stdout missing 'executed successfully': %s", cap.Stdout)
	}
}

func TestRunChoresRun_VerbosePreflightFailureIsNonFatal(t *testing.T) {
	resetCmdFlags(t)
	flagVerbose = true
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"server error"}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Fatalf("expected nil error (preflight failure is non-fatal), got: %v", err)
	}
	if !strings.Contains(cap.Stderr, "[warn] cannot fetch chore tasks") {
		t.Errorf("stderr missing warn message: %s", cap.Stderr)
	}
	if !strings.Contains(cap.Stdout, "executed successfully") {
		t.Errorf("stdout missing 'executed successfully': %s", cap.Stdout)
	}
}

func TestRunChoresRun_URLEncoding(t *testing.T) {
	cases := []struct {
		name         string
		wantPathPart string
	}{
		{"Night Load", "Chores('Night%20Load')"},
		{"A/B", "Chores('A%2FB')"},
		{"A'B", "Chores('A%27%27B')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCmdFlags(t)
			var gotPath string
			setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.EscapedPath()
				w.WriteHeader(http.StatusNoContent)
			})

			captureAll(t, func() {
				_ = runChoresRun(choresRunCmd, []string{tc.name})
			})

			if !strings.Contains(gotPath, tc.wantPathPart) {
				t.Errorf("EscapedPath = %q, want it to contain %q", gotPath, tc.wantPathPart)
			}
		})
	}
}

func TestRunChoresRun_AsyncIgnoresTimeoutFlag(t *testing.T) {
	resetCmdFlags(t)
	choreRunAsync = true
	choreRunTimeout = 1 * time.Millisecond
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Location", "Threads('99')")
		w.WriteHeader(http.StatusAccepted)
	})

	var err error
	captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if err != nil {
		t.Errorf("expected nil error (async skips SetTimeout), got: %v", err)
	}
}

func TestRunChoresRun_TimeoutZero(t *testing.T) {
	resetCmdFlags(t)
	choreRunTimeout = 0

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 1 {
		t.Errorf("exit code = %d, want 1 (err: %v)", exitCodeForError(err), err)
	}
	if !strings.Contains(cap.Stderr, "--timeout must be greater than zero.") {
		t.Errorf("stderr missing timeout-zero error: %s", cap.Stderr)
	}
}

func TestRunChoresRun_TimeoutZero_JSON(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"
	choreRunTimeout = 0

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 1 {
		t.Errorf("exit code = %d, want 1", exitCodeForError(err))
	}
	var result model.ChoreRunResult
	if jsonErr := json.Unmarshal([]byte(cap.Stdout), &result); jsonErr != nil {
		t.Fatalf("invalid JSON output: %v\nstdout: %s", jsonErr, cap.Stdout)
	}
	if result.Status != "error" {
		t.Errorf("Status = %q, want %q", result.Status, "error")
	}
	if result.Chore != "Nightly" {
		t.Errorf("Chore = %q, want %q", result.Chore, "Nightly")
	}
	if !strings.Contains(result.Message, "--timeout must be greater than zero.") {
		t.Errorf("Message = %q, want timeout-zero text", result.Message)
	}
	if cap.Stderr != "" {
		t.Errorf("stderr should be empty in JSON mode, got: %q", cap.Stderr)
	}
}

func TestRunChoresRun_AsyncServerIgnoredPrefer(t *testing.T) {
	resetCmdFlags(t)
	choreRunAsync = true
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	var err error
	cap := captureAll(t, func() {
		err = runChoresRun(choresRunCmd, []string{"Nightly"})
	})

	if exitCodeForError(err) != 1 {
		t.Errorf("exit code = %d, want 1 (err: %v)", exitCodeForError(err), err)
	}
	if !strings.Contains(cap.Stderr, "async response missing Location") {
		t.Errorf("stderr missing async-no-location message: %s", cap.Stderr)
	}
}

func TestRunChoresRun_ExitCodes(t *testing.T) {
	t.Run("404 -> exit 3", func(t *testing.T) {
		resetCmdFlags(t)
		setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`not found`))
		})
		var err error
		captureAll(t, func() {
			err = runChoresRun(choresRunCmd, []string{"Nightly"})
		})
		if got := exitCodeForError(err); got != 3 {
			t.Errorf("exit code = %d, want 3 (err: %v)", got, err)
		}
	})

	t.Run("sleep > timeout -> exit 4", func(t *testing.T) {
		resetCmdFlags(t)
		choreRunTimeout = 50 * time.Millisecond
		setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusNoContent)
		})
		var err error
		captureAll(t, func() {
			err = runChoresRun(choresRunCmd, []string{"Nightly"})
		})
		if got := exitCodeForError(err); got != 4 {
			t.Errorf("exit code = %d, want 4 (err: %v)", got, err)
		}
	})

	t.Run("500 -> exit 1", func(t *testing.T) {
		resetCmdFlags(t)
		setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`server error`))
		})
		var err error
		captureAll(t, func() {
			err = runChoresRun(choresRunCmd, []string{"Nightly"})
		})
		if got := exitCodeForError(err); got != 1 {
			t.Errorf("exit code = %d, want 1 (err: %v)", got, err)
		}
	})

	t.Run("async missing Location -> exit 1", func(t *testing.T) {
		resetCmdFlags(t)
		choreRunAsync = true
		setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
		var err error
		captureAll(t, func() {
			err = runChoresRun(choresRunCmd, []string{"Nightly"})
		})
		if got := exitCodeForError(err); got != 1 {
			t.Errorf("exit code = %d, want 1 (err: %v)", got, err)
		}
	})
}
