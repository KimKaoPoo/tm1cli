package cmd

import (
	"encoding/json"
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
		displayMembers(elements, 3, 0, false)
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
		displayMembers(elements, 2, 0, true)
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
		displayMembers(elements, 4, 0, false)
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
		displayMembers(elements, 10, 0, true)
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
		displayMembers(elements, 5, 2, false)
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
		displayMembers(elements, 2, 0, false)
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
		displayMembers(elements, 3, 0, false)
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
		displayMembers(elements, 4, 2, true)
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
		displayMembers([]model.Element{}, 0, 0, false)
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
		displayMembers(elements, 42, 0, false)
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
