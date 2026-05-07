package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
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
			DSTSensitivity: false, Frequency: "P1DT0H0M0S",
			Tasks: []model.ChoreTask{{Step: 0}, {Step: 1}},
		},
		{
			Name: "WeeklyReport", Active: false, StartTime: "2025-01-01T09:00:00",
			DSTSensitivity: true, Frequency: "P7DT0H0M0S",
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
		DSTSensitivity: false, Frequency: "P1DT0H0M0S",
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
		"DSTSensitivity: false",
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

func TestChoresList_TopOmittedWhenClientFiltering(t *testing.T) {
	resetCmdFlags(t)
	var gotURL string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(choresJSON(sampleChores()...))
	})

	captureAll(t, func() {
		rootCmd.SetArgs([]string{"chores", "list", "--active"})
		rootCmd.Execute()
	})

	if strings.Contains(gotURL, "%24top=") {
		t.Errorf("$top must not be sent when client-side filters are active: %s", gotURL)
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
