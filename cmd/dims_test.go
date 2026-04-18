package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// ============================================================
// filterSystemDims
// ============================================================

func TestFilterSystemDims(t *testing.T) {
	tests := []struct {
		name       string
		dims       []model.Dimension
		showSystem bool
		wantNames  []string
	}{
		{
			name: "showSystem false hides system dims",
			dims: []model.Dimension{
				{Name: "Region"},
				{Name: "}Cubes"},
				{Name: "Period"},
				{Name: "}Dimensions"},
			},
			showSystem: false,
			wantNames:  []string{"Region", "Period"},
		},
		{
			name: "showSystem true returns all dims",
			dims: []model.Dimension{
				{Name: "Region"},
				{Name: "}Cubes"},
				{Name: "Period"},
			},
			showSystem: true,
			wantNames:  []string{"Region", "}Cubes", "Period"},
		},
		{
			name:       "empty input returns empty",
			dims:       []model.Dimension{},
			showSystem: false,
			wantNames:  nil,
		},
		{
			name:       "nil input returns empty",
			dims:       nil,
			showSystem: false,
			wantNames:  nil,
		},
		{
			name: "all system dims filtered out",
			dims: []model.Dimension{
				{Name: "}Cubes"},
				{Name: "}Dimensions"},
				{Name: "}Processes"},
			},
			showSystem: false,
			wantNames:  nil,
		},
		{
			name: "no system dims — nothing filtered",
			dims: []model.Dimension{
				{Name: "Region"},
				{Name: "Period"},
			},
			showSystem: false,
			wantNames:  []string{"Region", "Period"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSystemDims(tt.dims, tt.showSystem)
			gotNames := dimNames(result)

			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterSystemDims() names = %v, want %v", gotNames, tt.wantNames)
			}
		})
	}
}

// ============================================================
// filterDimsByName
// ============================================================

func TestFilterDimsByName(t *testing.T) {
	dims := []model.Dimension{
		{Name: "Region"},
		{Name: "Period"},
		{Name: "Account"},
		{Name: "Product"},
		{Name: "RegionalSales"},
	}

	tests := []struct {
		name      string
		filter    string
		wantNames []string
	}{
		{
			name:      "case-insensitive partial match",
			filter:    "region",
			wantNames: []string{"Region", "RegionalSales"},
		},
		{
			name:      "uppercase filter matches lowercase content",
			filter:    "PERIOD",
			wantNames: []string{"Period"},
		},
		{
			name:      "mixed case filter",
			filter:    "AccOunT",
			wantNames: []string{"Account"},
		},
		{
			name:      "no match returns empty",
			filter:    "xyz",
			wantNames: nil,
		},
		{
			name:      "empty filter matches all",
			filter:    "",
			wantNames: []string{"Region", "Period", "Account", "Product", "RegionalSales"},
		},
		{
			name:      "single character match",
			filter:    "p",
			wantNames: []string{"Period", "Product"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterDimsByName(dims, tt.filter)
			gotNames := dimNames(result)

			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterDimsByName(%q) names = %v, want %v", tt.filter, gotNames, tt.wantNames)
			}
		})
	}
}

func TestFilterDimsByName_EmptyInput(t *testing.T) {
	result := filterDimsByName(nil, "anything")
	if len(result) != 0 {
		t.Errorf("filterDimsByName(nil) = %v, want empty", result)
	}

	result = filterDimsByName([]model.Dimension{}, "anything")
	if len(result) != 0 {
		t.Errorf("filterDimsByName([]) = %v, want empty", result)
	}
}

// ============================================================
// filterElementsByName
// ============================================================

func TestFilterElementsByName(t *testing.T) {
	elements := []model.Element{
		{Name: "2024", Type: "Numeric"},
		{Name: "Q1", Type: "Consolidated"},
		{Name: "Q2", Type: "Consolidated"},
		{Name: "Jan", Type: "Numeric"},
		{Name: "Feb", Type: "Numeric"},
		{Name: "Quarter1", Type: "String"},
	}

	tests := []struct {
		name      string
		filter    string
		wantNames []string
	}{
		{
			name:      "partial match",
			filter:    "Q",
			wantNames: []string{"Q1", "Q2", "Quarter1"},
		},
		{
			name:      "case-insensitive match",
			filter:    "jan",
			wantNames: []string{"Jan"},
		},
		{
			name:      "uppercase filter matches",
			filter:    "FEB",
			wantNames: []string{"Feb"},
		},
		{
			name:      "no match returns empty",
			filter:    "xyz",
			wantNames: nil,
		},
		{
			name:      "numeric string filter",
			filter:    "2024",
			wantNames: []string{"2024"},
		},
		{
			name:      "empty filter matches all",
			filter:    "",
			wantNames: []string{"2024", "Q1", "Q2", "Jan", "Feb", "Quarter1"},
		},
		{
			name:      "quarter partial match",
			filter:    "quarter",
			wantNames: []string{"Quarter1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterElementsByName(elements, tt.filter)
			gotNames := elementNames(result)

			if !stringSliceEqual(gotNames, tt.wantNames) {
				t.Errorf("filterElementsByName(%q) names = %v, want %v", tt.filter, gotNames, tt.wantNames)
			}
		})
	}
}

func TestFilterElementsByName_EmptyInput(t *testing.T) {
	result := filterElementsByName(nil, "anything")
	if len(result) != 0 {
		t.Errorf("filterElementsByName(nil) = %v, want empty", result)
	}

	result = filterElementsByName([]model.Element{}, "anything")
	if len(result) != 0 {
		t.Errorf("filterElementsByName([]) = %v, want empty", result)
	}
}

func TestFilterElementsByName_PreservesType(t *testing.T) {
	elements := []model.Element{
		{Name: "Jan", Type: "Numeric"},
		{Name: "Q1", Type: "Consolidated"},
	}

	result := filterElementsByName(elements, "jan")
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].Name != "Jan" || result[0].Type != "Numeric" {
		t.Errorf("expected {Jan, Numeric}, got {%s, %s}", result[0].Name, result[0].Type)
	}
}

// ============================================================
// displayDims
// ============================================================

func TestDisplayDims_TableOutput(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "Region"},
		{Name: "Period"},
		{Name: "Account"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 3, 0, false)
	})

	// Check header
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header, got:\n%s", out)
	}
	// Check all dim names
	for _, d := range dims {
		if !strings.Contains(out, d.Name) {
			t.Errorf("table output missing dimension %q, got:\n%s", d.Name, out)
		}
	}
}

func TestDisplayDims_JSONOutput(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "Region"},
		{Name: "Period"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 2, 0, true)
	})

	var parsed []model.Dimension
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON output: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d dims, want 2", len(parsed))
	}
	if parsed[0].Name != "Region" {
		t.Errorf("JSON first dim = %q, want Region", parsed[0].Name)
	}
	if parsed[1].Name != "Period" {
		t.Errorf("JSON second dim = %q, want Period", parsed[1].Name)
	}
}

func TestDisplayDims_CountTable(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = true

	dims := []model.Dimension{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 3, 0, false)
	})

	expected := "3 dimensions\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayDims_CountJSON(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = true

	dims := []model.Dimension{
		{Name: "A"},
		{Name: "B"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 5, 0, true)
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\nOutput:\n%s", err, out)
	}
	if parsed["count"] != 5 {
		t.Errorf("count = %d, want 5", parsed["count"])
	}
}

func TestDisplayDims_LimitTruncation(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "Alpha"},
		{Name: "Bravo"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	captured := captureAll(t, func() {
		displayDims(dims, 5, 3, false)
	})

	// Should show only first 3 dims
	if !strings.Contains(captured.Stdout, "Alpha") {
		t.Error("output missing dim Alpha")
	}
	if !strings.Contains(captured.Stdout, "Charlie") {
		t.Error("output missing dim Charlie")
	}
	if strings.Contains(captured.Stdout, "Delta") {
		t.Error("output should NOT contain dim Delta (truncated)")
	}
	if strings.Contains(captured.Stdout, "Echo") {
		t.Error("output should NOT contain dim Echo (truncated)")
	}

	// PrintSummary writes to stderr when shown < total
	if !strings.Contains(captured.Stderr, "Showing 3 of 5") {
		t.Errorf("stderr should contain summary, got: %q", captured.Stderr)
	}
}

func TestDisplayDims_NoSummaryWhenAllShown(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "A"},
		{Name: "B"},
	}

	captured := captureAll(t, func() {
		displayDims(dims, 2, 0, false)
	})

	// No truncation summary when all items shown
	if strings.Contains(captured.Stderr, "Showing") {
		t.Errorf("stderr should be empty when no truncation, got: %q", captured.Stderr)
	}
}

func TestDisplayDims_LimitZeroShowsAll(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 3, 0, false)
	})

	for _, d := range dims {
		if !strings.Contains(out, d.Name) {
			t.Errorf("output missing dim %q with limit=0", d.Name)
		}
	}
}

func TestDisplayDims_JSONLimitTruncation(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	dims := []model.Dimension{
		{Name: "A"},
		{Name: "B"},
		{Name: "C"},
		{Name: "D"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 4, 2, true)
	})

	var parsed []model.Dimension
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d dims, want 2 (limit=2)", len(parsed))
	}
}

func TestDisplayDims_EmptyDims(t *testing.T) {
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = false

	out := captureStdout(t, func() {
		displayDims([]model.Dimension{}, 0, 0, false)
	})

	// Should still print header
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header for empty list, got:\n%s", out)
	}
}

// ============================================================
// displayMembers
// ============================================================

func TestDisplayMembers_TableOutput(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "Jan", Type: "Numeric"},
		{Name: "Q1", Type: "Consolidated"},
		{Name: "Year", Type: "String"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 3, 0, false, false)
	})

	// Check headers
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header, got:\n%s", out)
	}
	if !strings.Contains(out, "TYPE") {
		t.Errorf("table output missing TYPE header, got:\n%s", out)
	}

	// Check element data
	for _, e := range elements {
		if !strings.Contains(out, e.Name) {
			t.Errorf("table output missing element %q, got:\n%s", e.Name, out)
		}
		if !strings.Contains(out, e.Type) {
			t.Errorf("table output missing type %q, got:\n%s", e.Type, out)
		}
	}
}

func TestDisplayMembers_JSONOutput(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "Jan", Type: "Numeric"},
		{Name: "Q1", Type: "Consolidated"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 2, 0, true, false)
	})

	var parsed []model.Element
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON output: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d elements, want 2", len(parsed))
	}
	if parsed[0].Name != "Jan" || parsed[0].Type != "Numeric" {
		t.Errorf("first element = {%s, %s}, want {Jan, Numeric}", parsed[0].Name, parsed[0].Type)
	}
	if parsed[1].Name != "Q1" || parsed[1].Type != "Consolidated" {
		t.Errorf("second element = {%s, %s}, want {Q1, Consolidated}", parsed[1].Name, parsed[1].Type)
	}
}

func TestDisplayMembers_CountTable(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = true

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "Numeric"},
		{Name: "C", Type: "Numeric"},
		{Name: "D", Type: "Numeric"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 4, 0, false, false)
	})

	expected := "4 members\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayMembers_CountJSON(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = true

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 10, 0, true, false)
	})

	var parsed map[string]int
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON count: %v\nOutput:\n%s", err, out)
	}
	if parsed["count"] != 10 {
		t.Errorf("count = %d, want 10", parsed["count"])
	}
}

func TestDisplayMembers_LimitTruncation(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "Jan", Type: "Numeric"},
		{Name: "Feb", Type: "Numeric"},
		{Name: "Mar", Type: "Numeric"},
		{Name: "Apr", Type: "Numeric"},
		{Name: "May", Type: "Numeric"},
	}

	captured := captureAll(t, func() {
		displayMembers(elements, 5, 2, false, false)
	})

	// Should show first 2 only
	if !strings.Contains(captured.Stdout, "Jan") {
		t.Error("output missing Jan")
	}
	if !strings.Contains(captured.Stdout, "Feb") {
		t.Error("output missing Feb")
	}
	if strings.Contains(captured.Stdout, "Mar") {
		t.Error("output should NOT contain Mar (truncated)")
	}

	// Summary on stderr
	if !strings.Contains(captured.Stderr, "Showing 2 of 5") {
		t.Errorf("stderr should contain summary, got: %q", captured.Stderr)
	}
}

func TestDisplayMembers_NoSummaryWhenAllShown(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "String"},
	}

	captured := captureAll(t, func() {
		displayMembers(elements, 2, 0, false, false)
	})

	if strings.Contains(captured.Stderr, "Showing") {
		t.Errorf("stderr should be empty when no truncation, got: %q", captured.Stderr)
	}
}

func TestDisplayMembers_LimitZeroShowsAll(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "String"},
		{Name: "C", Type: "Consolidated"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 3, 0, false, false)
	})

	for _, e := range elements {
		if !strings.Contains(out, e.Name) {
			t.Errorf("output missing element %q with limit=0", e.Name)
		}
	}
}

func TestDisplayMembers_JSONLimitTruncation(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "String"},
		{Name: "C", Type: "Consolidated"},
		{Name: "D", Type: "Numeric"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 4, 2, true, false)
	})

	var parsed []model.Element
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("cannot parse JSON: %v\nOutput:\n%s", err, out)
	}
	if len(parsed) != 2 {
		t.Errorf("JSON output has %d elements, want 2 (limit=2)", len(parsed))
	}
}

func TestDisplayMembers_EmptyElements(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	out := captureStdout(t, func() {
		displayMembers([]model.Element{}, 0, 0, false, false)
	})

	// Should still print headers
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing NAME header for empty list, got:\n%s", out)
	}
	if !strings.Contains(out, "TYPE") {
		t.Errorf("table output missing TYPE header for empty list, got:\n%s", out)
	}
}

func TestDisplayMembers_CountUsesTotalNotLen(t *testing.T) {
	// Verify that count mode uses the total parameter, not len(elements)
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = true

	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
	}

	out := captureStdout(t, func() {
		displayMembers(elements, 42, 0, false, false)
	})

	expected := "42 members\n"
	if out != expected {
		t.Errorf("count output = %q, want %q (should use total parameter)", out, expected)
	}
}

func TestDisplayDims_CountUsesTotalNotLen(t *testing.T) {
	// Verify that count mode uses the total parameter, not len(dims)
	origCount := dimsCount
	defer func() { dimsCount = origCount }()
	dimsCount = true

	dims := []model.Dimension{
		{Name: "A"},
	}

	out := captureStdout(t, func() {
		displayDims(dims, 99, 0, false)
	})

	expected := "99 dimensions\n"
	if out != expected {
		t.Errorf("count output = %q, want %q (should use total parameter)", out, expected)
	}
}

// ============================================================
// buildTree / flattenTree
// ============================================================

func flatNamesDepths(rows []indentedRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%s@%d", r.name, r.depth)
	}
	return out
}

func TestBuildTree_SimpleHierarchy(t *testing.T) {
	elements := []model.Element{
		{Name: "Year", Type: "Consolidated", Components: []model.Component{{Name: "Q1"}, {Name: "Q2"}}},
		{Name: "Q1", Type: "Consolidated", Components: []model.Component{{Name: "Jan"}}},
		{Name: "Q2", Type: "Consolidated", Components: []model.Component{{Name: "Feb"}}},
		{Name: "Jan", Type: "Numeric"},
		{Name: "Feb", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"Year@0", "Q1@1", "Jan@2", "Q2@1", "Feb@2"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_AllLeaves(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "Numeric"},
		{Name: "C", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"A@0", "B@0", "C@0"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_Diamond(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Consolidated", Components: []model.Component{{Name: "B"}, {Name: "C"}}},
		{Name: "B", Type: "Consolidated", Components: []model.Component{{Name: "D"}}},
		{Name: "C", Type: "Consolidated", Components: []model.Component{{Name: "D"}}},
		{Name: "D", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"A@0", "B@1", "D@2", "C@1", "D@2"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("diamond flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_CycleIsGuarded(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Consolidated", Components: []model.Component{{Name: "B"}}},
		{Name: "B", Type: "Consolidated", Components: []model.Component{{Name: "A"}}},
		{Name: "X", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	names := make(map[string]int)
	for _, r := range flat {
		names[r.name]++
	}
	if names["X"] != 1 {
		t.Errorf("X should appear exactly once, got %d: %v", names["X"], flatNamesDepths(flat))
	}
	if names["A"] == 0 || names["B"] == 0 {
		t.Errorf("A and B should both appear, got: %v", flatNamesDepths(flat))
	}
	if len(flat) > 10 {
		t.Errorf("output should be finite and small for a cycle, got %d rows: %v", len(flat), flatNamesDepths(flat))
	}
}

func TestBuildTree_PureCycleRendersAllElements(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Consolidated", Components: []model.Component{{Name: "B"}}},
		{Name: "B", Type: "Consolidated", Components: []model.Component{{Name: "A"}}},
	}

	flat := flattenTree(buildTree(elements))

	seen := make(map[string]bool)
	for _, r := range flat {
		seen[r.name] = true
	}
	if !seen["A"] || !seen["B"] {
		t.Errorf("pure cycle should still render every element; got %v", flatNamesDepths(flat))
	}
}

func TestBuildTree_UnknownComponentIgnored(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Consolidated", Components: []model.Component{{Name: "Ghost"}}},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"A@0"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("unknown component should be ignored; flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_RootOrderPreserved(t *testing.T) {
	elements := []model.Element{
		{Name: "C", Type: "Numeric"},
		{Name: "A", Type: "Numeric"},
		{Name: "B", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"C@0", "A@0", "B@0"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("root order should follow input; flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_PreservesComponentOrder(t *testing.T) {
	elements := []model.Element{
		{Name: "A", Type: "Consolidated", Components: []model.Component{{Name: "C"}, {Name: "B"}}},
		{Name: "B", Type: "Numeric"},
		{Name: "C", Type: "Numeric"},
	}

	flat := flattenTree(buildTree(elements))

	want := []string{"A@0", "C@1", "B@1"}
	if !stringSliceEqual(flatNamesDepths(flat), want) {
		t.Errorf("component order should follow Components slice; flatten = %v, want %v", flatNamesDepths(flat), want)
	}
}

func TestBuildTree_EmptyInput(t *testing.T) {
	if got := buildTree(nil); len(got) != 0 {
		t.Errorf("buildTree(nil) = %v, want empty", got)
	}
	if got := buildTree([]model.Element{}); len(got) != 0 {
		t.Errorf("buildTree([]) = %v, want empty", got)
	}
	if got := flattenTree(nil); len(got) != 0 {
		t.Errorf("flattenTree(nil) = %v, want empty", got)
	}
}

// ============================================================
// displayMembers — tree mode
// ============================================================

func treeFixtureElements() []model.Element {
	return []model.Element{
		{Name: "Year", Type: "Consolidated", Components: []model.Component{{Name: "Q1"}, {Name: "Q2"}}},
		{Name: "Q1", Type: "Consolidated", Components: []model.Component{{Name: "Jan"}}},
		{Name: "Q2", Type: "Consolidated", Components: []model.Component{{Name: "Feb"}}},
		{Name: "Jan", Type: "Numeric"},
		{Name: "Feb", Type: "Numeric"},
	}
}

func TestDisplayMembers_TreeIndentation(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := treeFixtureElements()

	out := captureStdout(t, func() {
		displayMembers(elements, len(elements), 0, false, true)
	})

	wantSubstrings := []string{"Year", "  Q1", "    Jan", "  Q2", "    Feb"}
	for _, s := range wantSubstrings {
		if !strings.Contains(out, s) {
			t.Errorf("tree output missing %q; got:\n%s", s, out)
		}
	}
}

func TestDisplayMembers_TreeLimitTruncation(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	elements := treeFixtureElements()

	captured := captureAll(t, func() {
		displayMembers(elements, len(elements), 3, false, true)
	})

	if !strings.Contains(captured.Stdout, "Year") {
		t.Error("output missing Year")
	}
	if !strings.Contains(captured.Stdout, "Q1") {
		t.Error("output missing Q1")
	}
	if !strings.Contains(captured.Stdout, "Jan") {
		t.Error("output missing Jan")
	}
	if strings.Contains(captured.Stdout, "Q2") {
		t.Error("output should NOT contain Q2 (truncated)")
	}
	if strings.Contains(captured.Stdout, "Feb") {
		t.Error("output should NOT contain Feb (truncated)")
	}
	if !strings.Contains(captured.Stderr, "Showing 3 of 5") {
		t.Errorf("stderr should contain truncation summary, got: %q", captured.Stderr)
	}
}

func TestDisplayMembers_TreeEmptyElements(t *testing.T) {
	origCount := membersCount
	defer func() { membersCount = origCount }()
	membersCount = false

	out := captureStdout(t, func() {
		displayMembers([]model.Element{}, 0, 0, false, true)
	})

	if !strings.Contains(out, "NAME") || !strings.Contains(out, "TYPE") {
		t.Errorf("tree output should still print headers for empty input, got:\n%s", out)
	}
}

// ============================================================
// runDimsMembers — tree mode integration
// ============================================================

func TestRunDimsMembers_TreeOutputEndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsWithComponentsJSON(
			[]string{"Year", "Q1", "Jan"},
			[]string{"Consolidated", "Consolidated", "Numeric"},
			map[string][]string{"Year": {"Q1"}, "Q1": {"Jan"}},
		))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, want := range []string{"Year", "  Q1", "    Jan"} {
		if !strings.Contains(captured.Stdout, want) {
			t.Errorf("tree stdout missing %q; got:\n%s", want, captured.Stdout)
		}
	}
}

func TestRunDimsMembers_FlatFlagSkipsExpand(t *testing.T) {
	resetCmdFlags(t)
	membersFlat = true

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON(
			[]string{"Year", "Q1", "Jan"},
			[]string{"Consolidated", "Consolidated", "Numeric"},
		))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "expand") {
		t.Errorf("--flat should not send $expand, got query: %s", capturedQuery)
	}
	if strings.Contains(captured.Stdout, "  Q1") {
		t.Errorf("--flat output should not be indented, got:\n%s", captured.Stdout)
	}
}

func TestRunDimsMembers_FilterSkipsExpand(t *testing.T) {
	resetCmdFlags(t)
	membersFilter = "q"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON([]string{"Q1"}, []string{"Consolidated"}))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "expand") {
		t.Errorf("--filter should not send $expand, got query: %s", capturedQuery)
	}
}

func TestRunDimsMembers_JSONSkipsExpand(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON([]string{"Year"}, []string{"Consolidated"}))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "expand") {
		t.Errorf("--output json should not send $expand, got query: %s", capturedQuery)
	}
}

func TestRunDimsMembers_CountSkipsExpand(t *testing.T) {
	resetCmdFlags(t)
	membersCount = true

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON([]string{"Year"}, []string{"Consolidated"}))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "expand") {
		t.Errorf("--count should not send $expand, got query: %s", capturedQuery)
	}
}

func TestRunDimsMembers_TreeModeWarnsOnLargeFetch(t *testing.T) {
	resetCmdFlags(t)

	names := make([]string, treeWarnThreshold+1)
	types := make([]string, treeWarnThreshold+1)
	for i := range names {
		names[i] = fmt.Sprintf("Elem%d", i)
		types[i] = "Numeric"
	}

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsWithComponentsJSON(names, types, nil))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Big"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("expected warning for oversize tree fetch, got stderr: %q", captured.Stderr)
	}
	if !strings.Contains(captured.Stderr, "--flat") {
		t.Errorf("warning should mention --flat, got stderr: %q", captured.Stderr)
	}
	wantCount := fmt.Sprintf("%d", treeWarnThreshold+1)
	if !strings.Contains(captured.Stderr, wantCount) {
		t.Errorf("warning should include element count %s, got stderr: %q", wantCount, captured.Stderr)
	}
}

func TestRunDimsMembers_TreeModeQuietBelowThreshold(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsWithComponentsJSON(
			[]string{"Year", "Q1"},
			[]string{"Consolidated", "Numeric"},
			map[string][]string{"Year": {"Q1"}},
		))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Small"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(captured.Stderr, "Fetched") {
		t.Errorf("small dimensions should not emit fetch-size warning, got stderr: %q", captured.Stderr)
	}
}

func TestRunDimsMembers_FlatModeNeverWarnsOnSize(t *testing.T) {
	resetCmdFlags(t)
	membersFlat = true

	names := make([]string, treeWarnThreshold+1)
	types := make([]string, treeWarnThreshold+1)
	for i := range names {
		names[i] = fmt.Sprintf("Elem%d", i)
		types[i] = "Numeric"
	}

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON(names, types))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Big"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(captured.Stderr, "Fetched") {
		t.Errorf("--flat should never emit fetch-size warning, got stderr: %q", captured.Stderr)
	}
}

func TestRunDimsMembers_TreeModeOmitsTop(t *testing.T) {
	resetCmdFlags(t)
	membersLimit = 10

	var capturedQuery string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsWithComponentsJSON(
			[]string{"Year"},
			[]string{"Consolidated"},
			nil,
		))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if strings.Contains(capturedQuery, "top=") {
		t.Errorf("tree mode must not send $top (would break hierarchy); got query: %s", capturedQuery)
	}
	if !strings.Contains(capturedQuery, "expand") {
		t.Errorf("tree mode should send $expand; got query: %s", capturedQuery)
	}
}

// ============================================================
// Helpers
// ============================================================

func dimNames(dims []model.Dimension) []string {
	if dims == nil {
		return nil
	}
	names := make([]string, len(dims))
	for i, d := range dims {
		names[i] = d.Name
	}
	return names
}

func elementNames(elements []model.Element) []string {
	if elements == nil {
		return nil
	}
	names := make([]string, len(elements))
	for i, e := range elements {
		names[i] = e.Name
	}
	return names
}

func stringSliceEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ============================================================
// Integration tests — runDims / runDimsMembers with httptest
// ============================================================

func TestRunDims_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(dimsJSON("Region", "Period", "Account"))
	})

	captured := captureAll(t, func() {
		err := runDims(dimsCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output missing header 'NAME'")
	}
	for _, name := range []string{"Region", "Period", "Account"} {
		if !strings.Contains(captured.Stdout, name) {
			t.Errorf("output missing dimension %q", name)
		}
	}
}

func TestRunDims_SystemDimsHiddenByDefault(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(dimsJSON("Region", "}Cubes", "Period", "}Dimensions"))
	})

	captured := captureAll(t, func() {
		err := runDims(dimsCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "Region") {
		t.Error("output should contain 'Region'")
	}
	if strings.Contains(captured.Stdout, "}Cubes") {
		t.Error("output should NOT contain system dim '}Cubes'")
	}
}

func TestRunDimsMembers_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON(
			[]string{"Jan", "Feb", "Mar"},
			[]string{"Numeric", "Numeric", "Numeric"},
		))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output missing header 'NAME'")
	}
	if !strings.Contains(captured.Stdout, "TYPE") {
		t.Errorf("output missing header 'TYPE'")
	}
	if !strings.Contains(captured.Stdout, "Jan") {
		t.Errorf("output missing 'Jan'")
	}
	if !strings.Contains(captured.Stdout, "Numeric") {
		t.Errorf("output missing type 'Numeric'")
	}
}

func TestRunDimsMembers_CustomHierarchy(t *testing.T) {
	resetCmdFlags(t)
	membersHierarchy = "AlternateHierarchy"

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON([]string{"East", "West"}, []string{"Numeric", "Numeric"}))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Region"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Endpoint should include the custom hierarchy name
	if !strings.Contains(capturedPath, "AlternateHierarchy") {
		t.Errorf("request path should contain custom hierarchy 'AlternateHierarchy', got: %s", capturedPath)
	}
	// Default hierarchy (dimension name) should NOT be in the path
	if strings.Contains(capturedPath, "Hierarchies('Region')") {
		t.Errorf("request path should use custom hierarchy, not dimension name, got: %s", capturedPath)
	}
}

func TestRunDimsMembers_DefaultHierarchyMatchesDimName(t *testing.T) {
	resetCmdFlags(t)
	// membersHierarchy is empty — should default to dimension name

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON([]string{"Q1"}, []string{"Consolidated"}))
	})

	captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Default hierarchy should match dimension name "Period"
	if !strings.Contains(capturedPath, "Hierarchies('Period')") {
		t.Errorf("request path should use dimension name as default hierarchy, got: %s", capturedPath)
	}
}

func TestRunDimsMembers_NotFound(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"Dimension not found"}`))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"NonExistent"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Not found") {
		t.Errorf("stderr should contain 'Not found' for 404, got:\n%s", captured.Stderr)
	}
}

func TestRunDimsMembers_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(elementsJSON(
			[]string{"Jan", "Feb"},
			[]string{"Numeric", "Numeric"},
		))
	})

	captured := captureAll(t, func() {
		err := runDimsMembers(dimsMembersCmd, []string{"Period"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result []model.Element
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 elements, got %d", len(result))
	}
}

func TestRunDims_FilterFallbackWarning(t *testing.T) {
	resetCmdFlags(t)
	dimsFilter = "region"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.RawQuery
		if strings.Contains(query, "$filter") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"not supported"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(dimsJSON("Region", "RegionalSales", "Period"))
	})

	captured := captureAll(t, func() {
		err := runDims(dimsCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "[warn]") {
		t.Errorf("should show filter fallback warning")
	}
	if !strings.Contains(captured.Stdout, "Region") {
		t.Errorf("output should contain 'Region' (matches filter)")
	}
	if strings.Contains(captured.Stdout, "Period") {
		t.Errorf("output should NOT contain 'Period' (doesn't match 'region' filter)")
	}
}
