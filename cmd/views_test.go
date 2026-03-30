package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// ============================================================
// filterViewsByName
// ============================================================

func TestFilterViewsByName(t *testing.T) {
	views := []model.View{
		{Name: "Actual"},
		{Name: "Budget"},
		{Name: "ActualVsBudget"},
		{Name: "ACTUAL_PLAN"},
		{Name: "Forecast"},
	}

	tests := []struct {
		name      string
		input     []model.View
		filter    string
		wantNames []string
	}{
		{
			name:      "case-insensitive partial match",
			input:     views,
			filter:    "actual",
			wantNames: []string{"Actual", "ActualVsBudget", "ACTUAL_PLAN"},
		},
		{
			name:      "uppercase filter matches lowercase names",
			input:     views,
			filter:    "BUDGET",
			wantNames: []string{"Budget", "ActualVsBudget"},
		},
		{
			name:      "mixed case filter",
			input:     views,
			filter:    "AcTuAl",
			wantNames: []string{"Actual", "ActualVsBudget", "ACTUAL_PLAN"},
		},
		{
			name:      "no match returns empty",
			input:     views,
			filter:    "nonexistent",
			wantNames: nil,
		},
		{
			name:      "empty filter matches all",
			input:     views,
			filter:    "",
			wantNames: []string{"Actual", "Budget", "ActualVsBudget", "ACTUAL_PLAN", "Forecast"},
		},
		{
			name:      "nil input returns nil",
			input:     nil,
			filter:    "actual",
			wantNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterViewsByName(tt.input, tt.filter)

			gotNames := viewNames(result)
			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterViewsByName(%q) = %v, want %v", tt.filter, gotNames, tt.wantNames)
			}
		})
	}
}

func TestFilterViewsByName_EmptyInput(t *testing.T) {
	result := filterViewsByName(nil, "anything")
	if len(result) != 0 {
		t.Errorf("filterViewsByName(nil) = %v, want empty", result)
	}

	result = filterViewsByName([]model.View{}, "anything")
	if len(result) != 0 {
		t.Errorf("filterViewsByName([]) = %v, want empty", result)
	}
}

// ============================================================
// displayViews
// ============================================================

func TestDisplayViews_TableOutput(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	views := []model.View{
		{Name: "Actual"},
		{Name: "Budget"},
		{Name: "Forecast"},
	}

	out := captureStdout(t, func() {
		displayViews(views, len(views), 0, false)
	})

	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header, got:\n%s", out)
	}
	for _, v := range views {
		if !strings.Contains(out, v.Name) {
			t.Errorf("table output missing view %q, got:\n%s", v.Name, out)
		}
	}
}

func TestDisplayViews_JSONOutput(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	views := []model.View{
		{Name: "Actual"},
		{Name: "Budget"},
	}

	out := captureStdout(t, func() {
		displayViews(views, len(views), 0, true)
	})

	var parsed []model.View
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON output: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d views, want 2", len(parsed))
	}
	if parsed[0].Name != "Actual" {
		t.Errorf("JSON first view = %q, want Actual", parsed[0].Name)
	}
	if parsed[1].Name != "Budget" {
		t.Errorf("JSON second view = %q, want Budget", parsed[1].Name)
	}
}

func TestDisplayViews_CountTable(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = true

	views := []model.View{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displayViews(views, 3, 0, false)
	})

	expected := "3 views\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayViews_CountJSON(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = true

	views := []model.View{
		{Name: "A"},
		{Name: "B"},
	}

	out := captureStdout(t, func() {
		displayViews(views, 7, 0, true)
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\nOutput:\n%s", err, out)
	}
	if parsed["count"] != 7 {
		t.Errorf("count = %d, want 7", parsed["count"])
	}
}

func TestDisplayViews_CountUsesTotalNotSliceLength(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = true

	views := []model.View{
		{Name: "A"},
	}

	out := captureStdout(t, func() {
		// total=5, even though only 1 view in slice
		displayViews(views, 5, 0, false)
	})

	expected := "5 views\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayViews_LimitTruncation(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	views := []model.View{
		{Name: "Alpha"},
		{Name: "Bravo"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	captured := captureAll(t, func() {
		displayViews(views, 5, 3, false)
	})

	if !strings.Contains(captured.Stdout, "Alpha") {
		t.Error("output missing view Alpha")
	}
	if !strings.Contains(captured.Stdout, "Charlie") {
		t.Error("output missing view Charlie")
	}
	if strings.Contains(captured.Stdout, "Delta") {
		t.Error("output should NOT contain view Delta (truncated)")
	}
	if strings.Contains(captured.Stdout, "Echo") {
		t.Error("output should NOT contain view Echo (truncated)")
	}

	if !strings.Contains(captured.Stderr, "Showing 3 of 5") {
		t.Errorf("stderr should contain summary, got: %q", captured.Stderr)
	}
}

func TestDisplayViews_LimitZeroShowsAll(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	views := []model.View{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displayViews(views, len(views), 0, false)
	})

	for _, v := range views {
		if !strings.Contains(out, v.Name) {
			t.Errorf("output missing view %q with limit=0", v.Name)
		}
	}
}

func TestDisplayViews_JSONLimitTruncation(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	views := []model.View{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
		{Name: "D"},
	}

	out := captureStdout(t, func() {
		displayViews(views, 4, 2, true)
	})

	var parsed []model.View
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d views, want 2 (limit=2)", len(parsed))
	}
}

func TestDisplayViews_EmptyViews(t *testing.T) {
	origCount := viewsCount
	defer func() { viewsCount = origCount }()
	viewsCount = false

	out := captureStdout(t, func() {
		displayViews([]model.View{}, 0, 0, false)
	})

	// Should still print header
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header for empty list, got:\n%s", out)
	}
}

// ============================================================
// Integration tests — runViews with httptest
// ============================================================

func TestRunViews_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(viewsJSON("Actual", "Budget", "Forecast"))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"SalesCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output missing header 'NAME'")
	}
	for _, name := range []string{"Actual", "Budget", "Forecast"} {
		if !strings.Contains(captured.Stdout, name) {
			t.Errorf("output missing view %q", name)
		}
	}
}

func TestRunViews_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(viewsJSON("Actual", "Budget"))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"SalesCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result []model.View
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 views, got %d", len(result))
	}
}

func TestRunViews_CountFlag(t *testing.T) {
	resetCmdFlags(t)
	viewsCount = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(viewsJSON("Actual", "Budget", "Forecast"))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"SalesCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "3 views") {
		t.Errorf("expected '3 views', got:\n%s", captured.Stdout)
	}
}

func TestRunViews_FilterFallbackWarning(t *testing.T) {
	resetCmdFlags(t)
	viewsFilter = "actual"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.RawQuery
		if strings.Contains(query, "$filter") {
			// Simulate server-side filter not supported
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"filter not supported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(viewsJSON("Actual", "ActualVsForecast", "Plan"))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"SalesCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("stderr should contain '[warn]' for filter fallback, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stderr, "filtering locally") {
		t.Errorf("stderr should contain 'filtering locally', got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stdout, "Actual") {
		t.Errorf("output should contain 'Actual' (matches filter)")
	}
	if !strings.Contains(captured.Stdout, "ActualVsForecast") {
		t.Errorf("output should contain 'ActualVsForecast' (matches filter)")
	}
	if strings.Contains(captured.Stdout, "Plan") {
		t.Errorf("output should NOT contain 'Plan' (doesn't match 'actual' filter)")
	}
}

func TestRunViews_ServerSideFilterSuccess(t *testing.T) {
	resetCmdFlags(t)
	viewsFilter = "actual"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		// Server supports filter — return filtered results
		w.Header().Set("Content-Type", "application/json")
		w.Write(viewsJSON("Actual", "ActualVsBudget"))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"SalesCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// No warning — server-side filter worked
	if strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("stderr should NOT contain '[warn]' when server-side filter succeeds")
	}
	if !strings.Contains(captured.Stdout, "Actual") {
		t.Errorf("output missing 'Actual'")
	}
}

func TestRunViews_NotFound(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"Cube not found"}`))
	})

	captured := captureAll(t, func() {
		err := runViews(viewsCmd, []string{"NonExistentCube"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Not found") {
		t.Errorf("stderr should contain 'Not found' for 404, got:\n%s", captured.Stderr)
	}
}

// ============================================================
// Helpers
// ============================================================

func viewNames(views []model.View) []string {
	if views == nil {
		return nil
	}
	names := make([]string, len(views))
	for i, v := range views {
		names[i] = v.Name
	}
	return names
}
