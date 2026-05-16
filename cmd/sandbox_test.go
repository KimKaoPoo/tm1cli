package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// ============================================================
// filterSandboxesByName
// ============================================================

func TestFilterSandboxesByName(t *testing.T) {
	sandboxes := []model.Sandbox{
		{Name: "Plan_FY24"},
		{Name: "Budget_FY24"},
		{Name: "FORECAST"},
		{Name: "actual"},
	}

	tests := []struct {
		name      string
		filter    string
		wantNames []string
	}{
		{"basic match", "fy24", []string{"Plan_FY24", "Budget_FY24"}},
		{"case-insensitive uppercase filter", "PLAN", []string{"Plan_FY24"}},
		{"empty filter returns all", "", []string{"Plan_FY24", "Budget_FY24", "FORECAST", "actual"}},
		{"no match returns nil", "xyz", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSandboxesByName(sandboxes, tt.filter)
			gotNames := sandboxNames(got)
			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterSandboxesByName(%q) = %v, want %v", tt.filter, gotNames, tt.wantNames)
			}
		})
	}
}

// ============================================================
// applySandboxBoolFilters
// ============================================================

func TestApplySandboxBoolFilters(t *testing.T) {
	sandboxes := []model.Sandbox{
		{Name: "LoadedOnly", IsLoaded: true, IsActive: false},
		{Name: "ActiveOnly", IsLoaded: false, IsActive: true},
		{Name: "Both", IsLoaded: true, IsActive: true},
		{Name: "Neither", IsLoaded: false, IsActive: false},
	}

	tests := []struct {
		name      string
		loaded    bool
		active    bool
		wantNames []string
	}{
		{"no filters returns input", false, false, []string{"LoadedOnly", "ActiveOnly", "Both", "Neither"}},
		{"loaded only", true, false, []string{"LoadedOnly", "Both"}},
		{"active only", false, true, []string{"ActiveOnly", "Both"}},
		{"loaded and active (AND)", true, true, []string{"Both"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applySandboxBoolFilters(sandboxes, tt.loaded, tt.active)
			gotNames := sandboxNames(got)
			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("applySandboxBoolFilters(loaded=%v, active=%v) = %v, want %v",
					tt.loaded, tt.active, gotNames, tt.wantNames)
			}
		})
	}
}

// ============================================================
// isDuplicateSandboxError
// ============================================================

func TestIsDuplicateSandboxError(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		err  error
		want bool
	}{
		{"nil err returns false", []byte("already exists"), nil, false},
		{"HTTP 400 with already exists body", []byte(`{"error":{"message":"Sandbox 'X' already exists."}}`), fmt.Errorf("HTTP 400: short"), true},
		{"HTTP 400 with already exists in err string", nil, fmt.Errorf("HTTP 400: already exists"), true},
		{"HTTP 409 with duplicate body", []byte("Sandbox is a duplicate"), fmt.Errorf("HTTP 409: conflict"), true},
		{"HTTP 400 with unrelated body", []byte("invalid query"), fmt.Errorf("HTTP 400: invalid query"), false},
		{"HTTP 500 with already exists body (ignored)", []byte("already exists"), fmt.Errorf("HTTP 500: server error"), false},
		{"HTTP 404 not duplicate", []byte("not found"), fmt.Errorf("HTTP 404: not found"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDuplicateSandboxError(tt.body, tt.err)
			if got != tt.want {
				t.Errorf("isDuplicateSandboxError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ============================================================
// displaySandboxes
// ============================================================

func TestDisplaySandboxes_TableHeaders(t *testing.T) {
	origCount := sandboxListCount
	defer func() { sandboxListCount = origCount }()
	sandboxListCount = false

	sandboxes := []model.Sandbox{
		{Name: "FY24Plan", IncludeInSandboxDimension: true, IsLoaded: true, IsActive: false, IsQueued: false},
	}

	out := captureStdout(t, func() {
		displaySandboxes(sandboxes, len(sandboxes), 0, false)
	})

	for _, h := range []string{"NAME", "IN SANDBOX DIM", "LOADED", "ACTIVE", "QUEUED"} {
		if !strings.Contains(out, h) {
			t.Errorf("table output missing header %q, got:\n%s", h, out)
		}
	}
	if !strings.Contains(out, "FY24Plan") {
		t.Errorf("table output missing sandbox name, got:\n%s", out)
	}
	if !strings.Contains(out, "true") {
		t.Errorf("table output missing 'true' for boolean column, got:\n%s", out)
	}
	if !strings.Contains(out, "false") {
		t.Errorf("table output missing 'false' for boolean column, got:\n%s", out)
	}
}

func TestDisplaySandboxes_JSONOutput(t *testing.T) {
	origCount := sandboxListCount
	defer func() { sandboxListCount = origCount }()
	sandboxListCount = false

	sandboxes := []model.Sandbox{
		{Name: "A", IsLoaded: true, IsActive: true},
		{Name: "B"},
	}

	out := captureStdout(t, func() {
		displaySandboxes(sandboxes, len(sandboxes), 0, true)
	})

	var parsed []model.Sandbox
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON output: %v\noutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Fatalf("JSON output length = %d, want 2", len(parsed))
	}
	if parsed[0].Name != "A" || !parsed[0].IsLoaded || !parsed[0].IsActive {
		t.Errorf("JSON[0] = %+v, want Name=A Loaded=true Active=true", parsed[0])
	}
}

func TestDisplaySandboxes_Count(t *testing.T) {
	origCount := sandboxListCount
	defer func() { sandboxListCount = origCount }()
	sandboxListCount = true

	sandboxes := []model.Sandbox{{Name: "A"}, {Name: "B"}, {Name: "C"}, {Name: "D"}, {Name: "E"}}

	out := captureStdout(t, func() {
		displaySandboxes(sandboxes, 5, 0, false)
	})

	if out != "5 sandboxes\n" {
		t.Errorf("count output = %q, want %q", out, "5 sandboxes\n")
	}
}

func TestDisplaySandboxes_CountJSON(t *testing.T) {
	origCount := sandboxListCount
	defer func() { sandboxListCount = origCount }()
	sandboxListCount = true

	out := captureStdout(t, func() {
		displaySandboxes([]model.Sandbox{{Name: "A"}}, 7, 0, true)
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\noutput:\n%s", err, out)
	}
	if parsed["count"] != 7 {
		t.Errorf("count = %d, want 7", parsed["count"])
	}
}

func TestDisplaySandboxes_LimitTruncation(t *testing.T) {
	origCount := sandboxListCount
	defer func() { sandboxListCount = origCount }()
	sandboxListCount = false

	sandboxes := []model.Sandbox{
		{Name: "A1"}, {Name: "A2"}, {Name: "A3"}, {Name: "A4"}, {Name: "A5"},
	}

	captured := captureAll(t, func() {
		displaySandboxes(sandboxes, 5, 2, false)
	})

	if !strings.Contains(captured.Stdout, "A1") || !strings.Contains(captured.Stdout, "A2") {
		t.Errorf("output should contain first 2 names, got:\n%s", captured.Stdout)
	}
	for _, name := range []string{"A3", "A4", "A5"} {
		if strings.Contains(captured.Stdout, name) {
			t.Errorf("output should not contain truncated name %q, got:\n%s", name, captured.Stdout)
		}
	}
	if !strings.Contains(captured.Stderr, "Showing 2 of 5") {
		t.Errorf("stderr should contain truncation summary, got:\n%s", captured.Stderr)
	}
}

// ============================================================
// Integration — runSandboxList
// ============================================================

func TestRunSandboxList_TableOutput(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "FY24Plan", IncludeInSandboxDimension: true, IsLoaded: true},
			model.Sandbox{Name: "FY24Budget"},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, want := range []string{"NAME", "FY24Plan", "FY24Budget"} {
		if !strings.Contains(captured.Stdout, want) {
			t.Errorf("stdout missing %q, got:\n%s", want, captured.Stdout)
		}
	}
}

func TestRunSandboxList_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(model.Sandbox{
			Name: "FY24Plan", IncludeInSandboxDimension: true, IsLoaded: true, IsActive: true, IsQueued: false,
		}))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var parsed []model.Sandbox
	if err := json.Unmarshal([]byte(captured.Stdout), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if len(parsed) != 1 {
		t.Fatalf("expected 1 sandbox, got %d", len(parsed))
	}
	got := parsed[0]
	if got.Name != "FY24Plan" || !got.IncludeInSandboxDimension || !got.IsLoaded || !got.IsActive || got.IsQueued {
		t.Errorf("unexpected fields: %+v", got)
	}
}

func TestRunSandboxList_LoadedFilter(t *testing.T) {
	resetCmdFlags(t)
	sandboxListLoaded = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "Loaded1", IsLoaded: true},
			model.Sandbox{Name: "NotLoaded", IsLoaded: false},
			model.Sandbox{Name: "Loaded2", IsLoaded: true, IsActive: true},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "Loaded1") || !strings.Contains(captured.Stdout, "Loaded2") {
		t.Errorf("output should contain Loaded sandboxes, got:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stdout, "NotLoaded") {
		t.Errorf("output should not contain NotLoaded, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_ActiveFilter(t *testing.T) {
	resetCmdFlags(t)
	sandboxListActive = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "Active1", IsActive: true},
			model.Sandbox{Name: "NotActive"},
			model.Sandbox{Name: "Active2", IsActive: true, IsLoaded: true},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "Active1") || !strings.Contains(captured.Stdout, "Active2") {
		t.Errorf("output should contain Active sandboxes, got:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stdout, "NotActive") {
		t.Errorf("output should not contain NotActive, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_NameFilterFallback(t *testing.T) {
	resetCmdFlags(t)
	sandboxListFilter = "plan"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "$filter") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"filter not supported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "FY24Plan"},
			model.Sandbox{Name: "Budget"},
			model.Sandbox{Name: "PlanB"},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("stderr should contain [warn] for fallback, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stderr, "filtering locally") {
		t.Errorf("stderr should mention local filtering, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stdout, "FY24Plan") || !strings.Contains(captured.Stdout, "PlanB") {
		t.Errorf("output should contain matching names, got:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stdout, "Budget") {
		t.Errorf("output should not contain non-matching names, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_ServerSideFilterSuccess(t *testing.T) {
	resetCmdFlags(t)
	sandboxListFilter = "plan"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "FY24Plan"},
			model.Sandbox{Name: "PlanB"},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("stderr should not contain [warn] when server filter succeeds, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stdout, "FY24Plan") {
		t.Errorf("output missing FY24Plan, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_FilterPlusBooleanFilters(t *testing.T) {
	resetCmdFlags(t)
	sandboxListFilter = "plan"
	sandboxListLoaded = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Server-side filter returns name-matched results; client must AND with --loaded.
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "FY24Plan", IsLoaded: true},
			model.Sandbox{Name: "PlanB", IsLoaded: false},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "FY24Plan") {
		t.Errorf("output should contain FY24Plan (matches both filters), got:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stdout, "PlanB") {
		t.Errorf("output should not contain PlanB (fails --loaded filter), got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_EmptyResult(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"value":[]}`))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output should contain header even for empty result, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_HTTPError(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"server explosion"}`))
	})

	captured := captureAll(t, func() {
		err := runSandboxList(sandboxListCmd, nil)
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Error:") {
		t.Errorf("stderr should contain 'Error:', got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxList_MalformedJSON(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not json`))
	})

	captured := captureAll(t, func() {
		err := runSandboxList(sandboxListCmd, nil)
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Cannot parse server response.") {
		t.Errorf("stderr should mention parse error, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxList_CountWithJSON(t *testing.T) {
	resetCmdFlags(t)
	sandboxListCount = true
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "A"},
			model.Sandbox{Name: "B"},
			model.Sandbox{Name: "C"},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(captured.Stdout), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\noutput: %s", err, captured.Stdout)
	}
	if parsed["count"] != 3 {
		t.Errorf("count = %d, want 3", parsed["count"])
	}
}

func TestRunSandboxList_AllOverridesLimit(t *testing.T) {
	resetCmdFlags(t)
	sandboxListAll = true
	sandboxListLimit = 5 // should be ignored when --all is set

	var capturedQuery string
	boxes := make([]model.Sandbox, 60)
	for i := range boxes {
		boxes[i] = model.Sandbox{Name: fmt.Sprintf("Box%02d", i)}
	}

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(boxes...))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "$top=") {
		t.Errorf("request should not include $top= when --all is set, got query: %s", capturedQuery)
	}
	if !strings.Contains(captured.Stdout, "Box00") || !strings.Contains(captured.Stdout, "Box59") {
		t.Errorf("output should contain all 60 sandboxes, got first/last check failed:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stderr, "Showing ") {
		t.Errorf("stderr should not show truncation summary with --all, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxList_TruncationSummary(t *testing.T) {
	resetCmdFlags(t)

	boxes := make([]model.Sandbox, 60)
	for i := range boxes {
		boxes[i] = model.Sandbox{Name: fmt.Sprintf("Box%02d", i)}
	}

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(boxes...))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Showing 50 of 60") {
		t.Errorf("stderr should contain 'Showing 50 of 60' truncation summary, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stdout, "Box00") || !strings.Contains(captured.Stdout, "Box49") {
		t.Errorf("stdout should contain first 50 names, got:\n%s", captured.Stdout)
	}
	if strings.Contains(captured.Stdout, "Box50") || strings.Contains(captured.Stdout, "Box59") {
		t.Errorf("stdout should not contain rows beyond limit, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxList_JSONEmptyArrayWhenFilterStripsAll(t *testing.T) {
	resetCmdFlags(t)
	sandboxListLoaded = true
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(
			model.Sandbox{Name: "NotLoaded1"},
			model.Sandbox{Name: "NotLoaded2"},
		))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	trimmed := strings.TrimSpace(captured.Stdout)
	if trimmed == "null" {
		t.Fatalf("JSON output should be [] when filter strips everything, got null (downstream jq/array consumers break)")
	}
	var parsed []model.Sandbox
	if err := json.Unmarshal([]byte(captured.Stdout), &parsed); err != nil {
		t.Fatalf("output is not valid JSON array: %v\noutput: %s", err, captured.Stdout)
	}
	if len(parsed) != 0 {
		t.Errorf("expected empty array, got %d entries", len(parsed))
	}
}

func TestRunSandboxList_CountReflectsActualTotal(t *testing.T) {
	resetCmdFlags(t)
	sandboxListCount = true

	boxes := make([]model.Sandbox, 60)
	for i := range boxes {
		boxes[i] = model.Sandbox{Name: fmt.Sprintf("Box%02d", i)}
	}

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(sandboxesJSON(boxes...))
	})

	captured := captureAll(t, func() {
		if err := runSandboxList(sandboxListCmd, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "60 sandboxes") {
		t.Errorf("--count should report 60 (actual total), not limit; got:\n%s", captured.Stdout)
	}
}

// ============================================================
// Integration — runSandboxCreate
// ============================================================

func TestRunSandboxCreate_Success(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"Name":"FY24Plan"}`))
	})

	captured := captureAll(t, func() {
		if err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "Created sandbox 'FY24Plan'.") {
		t.Errorf("stdout should confirm creation, got:\n%s", captured.Stdout)
	}
}

func TestRunSandboxCreate_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"Name":"FY24Plan"}`))
	})

	captured := captureAll(t, func() {
		if err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var parsed map[string]string
	if err := json.Unmarshal([]byte(captured.Stdout), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if parsed["status"] != "created" || parsed["name"] != "FY24Plan" {
		t.Errorf("unexpected JSON: %+v", parsed)
	}
}

func TestRunSandboxCreate_PostBodyShape(t *testing.T) {
	resetCmdFlags(t)

	var (
		gotMethod string
		gotPath   string
		gotBody   []byte
	)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"Name":"FY24Plan"}`))
	})

	captureAll(t, func() {
		if err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/v1/Sandboxes" {
		t.Errorf("path = %q, want /api/v1/Sandboxes", gotPath)
	}
	var parsed map[string]string
	if err := json.Unmarshal(gotBody, &parsed); err != nil {
		t.Fatalf("body is not valid JSON: %v\nbody: %s", err, gotBody)
	}
	if parsed["Name"] != "FY24Plan" {
		t.Errorf("body Name = %q, want FY24Plan; full body: %s", parsed["Name"], gotBody)
	}
}

func TestRunSandboxCreate_DuplicateName_HTTP400(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"Sandbox 'FY24Plan' already exists."}}`))
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Sandbox 'FY24Plan' already exists.") {
		t.Errorf("stderr should report duplicate, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxCreate_DuplicateName_HTTP409(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`Sandbox 'FY24Plan' already exists.`))
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "already exists.") {
		t.Errorf("stderr should report duplicate, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxCreate_DuplicateJSONError(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"Sandbox 'FY24Plan' already exists."}}`))
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	// PrintError emits pretty-printed JSON; decode it instead of exact match.
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(captured.Stderr), &parsed); err != nil {
		t.Fatalf("stderr is not valid JSON: %v\nstderr: %s", err, captured.Stderr)
	}
	msg, _ := parsed["error"].(string)
	if !strings.Contains(msg, "already exists") {
		t.Errorf("JSON error message = %q, want it to contain 'already exists'", msg)
	}
}

func TestRunSandboxCreate_EmptyName(t *testing.T) {
	resetCmdFlags(t)

	hit := false
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusCreated)
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{""})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if hit {
		t.Errorf("server should not be contacted for empty name")
	}
	if !strings.Contains(captured.Stderr, "Sandbox name cannot be empty.") {
		t.Errorf("stderr should reject empty name, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxCreate_WhitespaceOnlyName(t *testing.T) {
	resetCmdFlags(t)

	hit := false
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusCreated)
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{"   "})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if hit {
		t.Errorf("server should not be contacted for whitespace-only name")
	}
	if !strings.Contains(captured.Stderr, "Sandbox name cannot be empty.") {
		t.Errorf("stderr should reject whitespace-only name, got:\n%s", captured.Stderr)
	}
}

func TestRunSandboxCreate_GenericServerError(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"explosion"}`))
	})

	captured := captureAll(t, func() {
		err := runSandboxCreate(sandboxCreateCmd, []string{"FY24Plan"})
		if !errors.Is(err, errSilent) {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "HTTP 500") {
		t.Errorf("stderr should pass through HTTP 500, got:\n%s", captured.Stderr)
	}
}

// ============================================================
// Helpers
// ============================================================

func sandboxNames(boxes []model.Sandbox) []string {
	if boxes == nil {
		return nil
	}
	names := make([]string, len(boxes))
	for i, b := range boxes {
		names[i] = b.Name
	}
	return names
}
