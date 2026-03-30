package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// ============================================================
// filterSubsetsByName
// ============================================================

func TestFilterSubsetsByName(t *testing.T) {
	subsets := []model.Subset{
		{Name: "AllRegions"},
		{Name: "EastRegion"},
		{Name: "WestRegion"},
		{Name: "NORTH_REGION"},
		{Name: "SouthAmerica"},
	}

	tests := []struct {
		name      string
		input     []model.Subset
		filter    string
		wantNames []string
	}{
		{
			name:      "case-insensitive partial match",
			input:     subsets,
			filter:    "region",
			wantNames: []string{"AllRegions", "EastRegion", "WestRegion", "NORTH_REGION"},
		},
		{
			name:      "uppercase filter matches lowercase names",
			input:     subsets,
			filter:    "EAST",
			wantNames: []string{"EastRegion"},
		},
		{
			name:      "mixed case filter",
			input:     subsets,
			filter:    "wEsT",
			wantNames: []string{"WestRegion"},
		},
		{
			name:      "no match returns empty",
			input:     subsets,
			filter:    "nonexistent",
			wantNames: nil,
		},
		{
			name:      "empty filter matches all",
			input:     subsets,
			filter:    "",
			wantNames: []string{"AllRegions", "EastRegion", "WestRegion", "NORTH_REGION", "SouthAmerica"},
		},
		{
			name:      "nil input returns nil",
			input:     nil,
			filter:    "region",
			wantNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSubsetsByName(tt.input, tt.filter)

			gotNames := subsetNames(result)
			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterSubsetsByName(%q) = %v, want %v", tt.filter, gotNames, tt.wantNames)
			}
		})
	}
}

func TestFilterSubsetsByName_EmptyInput(t *testing.T) {
	result := filterSubsetsByName(nil, "anything")
	if len(result) != 0 {
		t.Errorf("filterSubsetsByName(nil) = %v, want empty", result)
	}

	result = filterSubsetsByName([]model.Subset{}, "anything")
	if len(result) != 0 {
		t.Errorf("filterSubsetsByName([]) = %v, want empty", result)
	}
}

// ============================================================
// displaySubsets
// ============================================================

func TestDisplaySubsets_TableOutput(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	subsets := []model.Subset{
		{Name: "AllRegions"},
		{Name: "EastRegion"},
		{Name: "WestRegion"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, len(subsets), 0, false)
	})

	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header, got:\n%s", out)
	}
	for _, s := range subsets {
		if !strings.Contains(out, s.Name) {
			t.Errorf("table output missing subset %q, got:\n%s", s.Name, out)
		}
	}
}

func TestDisplaySubsets_JSONOutput(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	subsets := []model.Subset{
		{Name: "AllRegions"},
		{Name: "EastRegion"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, len(subsets), 0, true)
	})

	var parsed []model.Subset
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON output: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d subsets, want 2", len(parsed))
	}
	if parsed[0].Name != "AllRegions" {
		t.Errorf("JSON first subset = %q, want AllRegions", parsed[0].Name)
	}
	if parsed[1].Name != "EastRegion" {
		t.Errorf("JSON second subset = %q, want EastRegion", parsed[1].Name)
	}
}

func TestDisplaySubsets_CountTable(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = true

	subsets := []model.Subset{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, 3, 0, false)
	})

	expected := "3 subsets\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplaySubsets_CountJSON(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = true

	subsets := []model.Subset{
		{Name: "A"},
		{Name: "B"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, 9, 0, true)
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\nOutput:\n%s", err, out)
	}
	if parsed["count"] != 9 {
		t.Errorf("count = %d, want 9", parsed["count"])
	}
}

func TestDisplaySubsets_CountUsesTotalNotSliceLength(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = true

	subsets := []model.Subset{
		{Name: "A"},
	}

	out := captureStdout(t, func() {
		// total=12, even though only 1 subset in slice
		displaySubsets(subsets, 12, 0, false)
	})

	expected := "12 subsets\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplaySubsets_LimitTruncation(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	subsets := []model.Subset{
		{Name: "Alpha"},
		{Name: "Bravo"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	captured := captureAll(t, func() {
		displaySubsets(subsets, 5, 3, false)
	})

	if !strings.Contains(captured.Stdout, "Alpha") {
		t.Error("output missing subset Alpha")
	}
	if !strings.Contains(captured.Stdout, "Charlie") {
		t.Error("output missing subset Charlie")
	}
	if strings.Contains(captured.Stdout, "Delta") {
		t.Error("output should NOT contain subset Delta (truncated)")
	}
	if strings.Contains(captured.Stdout, "Echo") {
		t.Error("output should NOT contain subset Echo (truncated)")
	}

	if !strings.Contains(captured.Stderr, "Showing 3 of 5") {
		t.Errorf("stderr should contain summary, got: %q", captured.Stderr)
	}
}

func TestDisplaySubsets_LimitZeroShowsAll(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	subsets := []model.Subset{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, len(subsets), 0, false)
	})

	for _, s := range subsets {
		if !strings.Contains(out, s.Name) {
			t.Errorf("output missing subset %q with limit=0", s.Name)
		}
	}
}

func TestDisplaySubsets_JSONLimitTruncation(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	subsets := []model.Subset{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
		{Name: "D"},
	}

	out := captureStdout(t, func() {
		displaySubsets(subsets, 4, 2, true)
	})

	var parsed []model.Subset
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d subsets, want 2 (limit=2)", len(parsed))
	}
}

func TestDisplaySubsets_EmptySubsets(t *testing.T) {
	origCount := subsetsCount
	defer func() { subsetsCount = origCount }()
	subsetsCount = false

	out := captureStdout(t, func() {
		displaySubsets([]model.Subset{}, 0, 0, false)
	})

	// Should still print header
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header for empty list, got:\n%s", out)
	}
}

// ============================================================
// Integration tests — runSubsets with httptest
// ============================================================

func TestRunSubsets_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("AllRegions", "EastRegion", "WestRegion"))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output missing header 'NAME'")
	}
	for _, name := range []string{"AllRegions", "EastRegion", "WestRegion"} {
		if !strings.Contains(captured.Stdout, name) {
			t.Errorf("output missing subset %q", name)
		}
	}
}

func TestRunSubsets_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("AllRegions", "EastRegion"))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result []model.Subset
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 subsets, got %d", len(result))
	}
}

func TestRunSubsets_CountFlag(t *testing.T) {
	resetCmdFlags(t)
	subsetsCount = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("AllRegions", "EastRegion", "WestRegion"))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "3 subsets") {
		t.Errorf("expected '3 subsets', got:\n%s", captured.Stdout)
	}
}

func TestRunSubsets_FilterFallbackWarning(t *testing.T) {
	resetCmdFlags(t)
	subsetsFilter = "east"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.RawQuery
		if strings.Contains(query, "$filter") {
			// Simulate server-side filter not supported
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"filter not supported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("EastRegion", "NorthEast", "WestRegion"))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
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
	if !strings.Contains(captured.Stdout, "EastRegion") {
		t.Errorf("output should contain 'EastRegion' (matches filter)")
	}
	if !strings.Contains(captured.Stdout, "NorthEast") {
		t.Errorf("output should contain 'NorthEast' (matches filter)")
	}
	if strings.Contains(captured.Stdout, "WestRegion") {
		t.Errorf("output should NOT contain 'WestRegion' (doesn't match 'east' filter)")
	}
}

func TestRunSubsets_ServerSideFilterSuccess(t *testing.T) {
	resetCmdFlags(t)
	subsetsFilter = "east"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		// Server supports filter — return filtered results
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("EastRegion", "NorthEast"))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// No warning — server-side filter worked
	if strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("stderr should NOT contain '[warn]' when server-side filter succeeds")
	}
	if !strings.Contains(captured.Stdout, "EastRegion") {
		t.Errorf("output missing 'EastRegion'")
	}
}

func TestRunSubsets_CustomHierarchy(t *testing.T) {
	resetCmdFlags(t)
	subsetsHierarchy = "AlternateHierarchy"

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("SubsetA", "SubsetB"))
	})

	captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Endpoint should include the custom hierarchy name
	if !strings.Contains(capturedPath, "AlternateHierarchy") {
		t.Errorf("request path should contain custom hierarchy 'AlternateHierarchy', got: %s", capturedPath)
	}
	// Default hierarchy (dimension name) should NOT be in the hierarchy portion
	if strings.Contains(capturedPath, "Hierarchies('Region')") {
		t.Errorf("request path should use custom hierarchy, not dimension name, got: %s", capturedPath)
	}
}

func TestRunSubsets_DefaultHierarchyMatchesDimName(t *testing.T) {
	resetCmdFlags(t)
	// subsetsHierarchy is empty — should default to dimension name

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(subsetsJSON("AllRegions"))
	})

	captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Default hierarchy should match dimension name "Region"
	if !strings.Contains(capturedPath, "Hierarchies('Region')") {
		t.Errorf("request path should use dimension name as default hierarchy, got: %s", capturedPath)
	}
}

func TestRunSubsets_NotFound(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"Dimension not found"}`))
	})

	captured := captureAll(t, func() {
		err := runSubsets(subsetsCmd, []string{"NonExistent"})
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

func subsetNames(subsets []model.Subset) []string {
	if subsets == nil {
		return nil
	}
	names := make([]string, len(subsets))
	for i, s := range subsets {
		names[i] = s.Name
	}
	return names
}
