package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"tm1cli/internal/model"
)

// --- filterSystemCubes tests ---

func TestFilterSystemCubes(t *testing.T) {
	tests := []struct {
		name       string
		cubes      []model.Cube
		showSystem bool
		wantNames  []string
	}{
		{
			name: "showSystem true returns all cubes including system",
			cubes: []model.Cube{
				{Name: "Sales"},
				{Name: "}ClientGroups"},
				{Name: "Budget"},
				{Name: "}SecurityProperties"},
			},
			showSystem: true,
			wantNames:  []string{"Sales", "}ClientGroups", "Budget", "}SecurityProperties"},
		},
		{
			name: "showSystem false filters out system cubes",
			cubes: []model.Cube{
				{Name: "Sales"},
				{Name: "}ClientGroups"},
				{Name: "Budget"},
				{Name: "}SecurityProperties"},
			},
			showSystem: false,
			wantNames:  []string{"Sales", "Budget"},
		},
		{
			name:       "empty input returns empty",
			cubes:      []model.Cube{},
			showSystem: false,
			wantNames:  nil,
		},
		{
			name:       "nil input returns nil",
			cubes:      nil,
			showSystem: false,
			wantNames:  nil,
		},
		{
			name: "all system cubes with showSystem false returns empty",
			cubes: []model.Cube{
				{Name: "}ClientGroups"},
				{Name: "}SecurityProperties"},
				{Name: "}CubeProperties"},
			},
			showSystem: false,
			wantNames:  nil,
		},
		{
			name: "all user cubes with showSystem false returns all",
			cubes: []model.Cube{
				{Name: "Sales"},
				{Name: "Budget"},
				{Name: "Forecast"},
			},
			showSystem: false,
			wantNames:  []string{"Sales", "Budget", "Forecast"},
		},
		{
			name: "showSystem true with empty input returns empty",
			cubes: []model.Cube{},
			showSystem: true,
			wantNames:  []string{},
		},
		{
			name: "single system cube with showSystem false",
			cubes: []model.Cube{
				{Name: "}CubeProperties"},
			},
			showSystem: false,
			wantNames:  nil,
		},
		{
			name: "single user cube with showSystem false",
			cubes: []model.Cube{
				{Name: "Sales"},
			},
			showSystem: false,
			wantNames:  []string{"Sales"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSystemCubes(tt.cubes, tt.showSystem)

			if len(result) != len(tt.wantNames) {
				t.Fatalf("got %d cubes, want %d", len(result), len(tt.wantNames))
			}
			for i, cube := range result {
				if cube.Name != tt.wantNames[i] {
					t.Errorf("cube[%d].Name = %q, want %q", i, cube.Name, tt.wantNames[i])
				}
			}
		})
	}
}

// --- filterCubesByName tests ---

func TestFilterCubesByName(t *testing.T) {
	allCubes := []model.Cube{
		{Name: "Sales"},
		{Name: "Budget"},
		{Name: "SalesForecast"},
		{Name: "SALES_PLAN"},
		{Name: "Inventory"},
	}

	tests := []struct {
		name      string
		cubes     []model.Cube
		filter    string
		wantNames []string
	}{
		{
			name:      "case-insensitive partial match",
			cubes:     allCubes,
			filter:    "sales",
			wantNames: []string{"Sales", "SalesForecast", "SALES_PLAN"},
		},
		{
			name:      "uppercase filter matches lowercase names",
			cubes:     allCubes,
			filter:    "BUDGET",
			wantNames: []string{"Budget"},
		},
		{
			name:      "mixed case filter",
			cubes:     allCubes,
			filter:    "SaLeS",
			wantNames: []string{"Sales", "SalesForecast", "SALES_PLAN"},
		},
		{
			name:      "exact match returns single result",
			cubes:     allCubes,
			filter:    "Inventory",
			wantNames: []string{"Inventory"},
		},
		{
			name:      "no match returns empty",
			cubes:     allCubes,
			filter:    "nonexistent",
			wantNames: nil,
		},
		{
			name:      "empty filter returns all cubes",
			cubes:     allCubes,
			filter:    "",
			wantNames: []string{"Sales", "Budget", "SalesForecast", "SALES_PLAN", "Inventory"},
		},
		{
			name:      "empty input returns empty",
			cubes:     []model.Cube{},
			filter:    "sales",
			wantNames: nil,
		},
		{
			name:      "nil input returns nil",
			cubes:     nil,
			filter:    "sales",
			wantNames: nil,
		},
		{
			name:      "partial match in middle of name",
			cubes:     allCubes,
			filter:    "Fore",
			wantNames: []string{"SalesForecast"},
		},
		{
			name:      "single character filter",
			cubes:     allCubes,
			filter:    "v",
			wantNames: []string{"Inventory"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterCubesByName(tt.cubes, tt.filter)

			if len(result) != len(tt.wantNames) {
				t.Fatalf("got %d cubes, want %d", len(result), len(tt.wantNames))
			}
			for i, cube := range result {
				if cube.Name != tt.wantNames[i] {
					t.Errorf("cube[%d].Name = %q, want %q", i, cube.Name, tt.wantNames[i])
				}
			}
		})
	}
}

// --- displayCubes tests ---

func TestDisplayCubesTableOutput(t *testing.T) {
	// Save and restore global state
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: "2024-01-15T10:30:00"},
		{Name: "Budget", LastDataUpdate: "2024-02-20T14:00:00"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 0, false)
	})

	// Verify headers
	if !strings.Contains(out, "NAME") {
		t.Errorf("output missing header 'NAME', got:\n%s", out)
	}
	if !strings.Contains(out, "LAST UPDATED") {
		t.Errorf("output missing header 'LAST UPDATED', got:\n%s", out)
	}

	// Verify data rows
	if !strings.Contains(out, "Sales") {
		t.Errorf("output missing 'Sales', got:\n%s", out)
	}
	if !strings.Contains(out, "Budget") {
		t.Errorf("output missing 'Budget', got:\n%s", out)
	}
	if !strings.Contains(out, "2024-01-15T10:30:00") {
		t.Errorf("output missing last update timestamp, got:\n%s", out)
	}
}

func TestDisplayCubesEmptyLastDataUpdate(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: ""},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 0, false)
	})

	// Empty LastDataUpdate should show "-"
	if !strings.Contains(out, "-") {
		t.Errorf("empty LastDataUpdate should show '-', got:\n%s", out)
	}
}

func TestDisplayCubesJSONOutput(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: "2024-01-15T10:30:00"},
		{Name: "Budget", LastDataUpdate: "2024-02-20T14:00:00"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 0, true)
	})

	// Verify valid JSON output
	var result []model.Cube
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 cubes in JSON output, got %d", len(result))
	}
	if result[0].Name != "Sales" {
		t.Errorf("first cube name = %q, want %q", result[0].Name, "Sales")
	}
	if result[1].Name != "Budget" {
		t.Errorf("second cube name = %q, want %q", result[1].Name, "Budget")
	}
}

func TestDisplayCubesJSONEmptyList(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{}

	out := captureStdout(t, func() {
		displayCubes(cubes, 0, 0, true)
	})

	// Should produce valid JSON (empty array)
	var result []model.Cube
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(result) != 0 {
		t.Errorf("expected empty array, got %d items", len(result))
	}
}

func TestDisplayCubesCountTableMode(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = true

	cubes := []model.Cube{
		{Name: "Sales"},
		{Name: "Budget"},
		{Name: "Forecast"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, 3, 0, false)
	})

	expected := "3 cubes\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayCubesCountJSONMode(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = true

	cubes := []model.Cube{
		{Name: "Sales"},
		{Name: "Budget"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, 2, 0, true)
	})

	var result map[string]int
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if result["count"] != 2 {
		t.Errorf("count = %d, want 2", result["count"])
	}
}

func TestDisplayCubesCountUsesTotalNotSliceLength(t *testing.T) {
	// The count should use the total parameter, not len(cubes)
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = true

	cubes := []model.Cube{
		{Name: "Sales"},
	}

	out := captureStdout(t, func() {
		// total=5, even though only 1 cube in slice
		displayCubes(cubes, 5, 0, false)
	})

	expected := "5 cubes\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayCubesLimitTruncation(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	var out string
	captureStderr(t, func() {
		out = captureStdout(t, func() {
			displayCubes(cubes, len(cubes), 3, false)
		})
	})

	// Should show only the first 3 cubes
	if !strings.Contains(out, "Alpha") {
		t.Errorf("output missing 'Alpha', got:\n%s", out)
	}
	if !strings.Contains(out, "Charlie") {
		t.Errorf("output missing 'Charlie', got:\n%s", out)
	}
	// Delta and Echo should not appear because limit=3
	if strings.Contains(out, "Delta") {
		t.Errorf("output should not contain 'Delta' (limit=3), got:\n%s", out)
	}
	if strings.Contains(out, "Echo") {
		t.Errorf("output should not contain 'Echo' (limit=3), got:\n%s", out)
	}
}

func TestDisplayCubesLimitTruncationSummary(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	stderr := captureStderr(t, func() {
		// Redirect stdout to /dev/null so only stderr is captured
		origStdout := os.Stdout
		devNull, _ := os.Open(os.DevNull)
		os.Stdout = devNull
		defer func() {
			os.Stdout = origStdout
			devNull.Close()
		}()
		displayCubes(cubes, len(cubes), 3, false)
	})

	// Summary should indicate truncation
	if !strings.Contains(stderr, "Showing 3 of 5") {
		t.Errorf("stderr should contain truncation summary 'Showing 3 of 5', got:\n%s", stderr)
	}
}

func TestDisplayCubesNoSummaryWhenNotTruncated(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha"},
		{Name: "Beta"},
	}

	stderr := captureStderr(t, func() {
		origStdout := os.Stdout
		devNull, _ := os.Open(os.DevNull)
		os.Stdout = devNull
		defer func() {
			os.Stdout = origStdout
			devNull.Close()
		}()
		displayCubes(cubes, len(cubes), 0, false)
	})

	// No truncation, so no summary should be printed
	if stderr != "" {
		t.Errorf("expected no stderr output when not truncated, got:\n%s", stderr)
	}
}

func TestDisplayCubesLimitZeroShowsAll(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 0, false)
	})

	// limit=0 means no limit, all cubes should show
	if !strings.Contains(out, "Alpha") {
		t.Errorf("output missing 'Alpha', got:\n%s", out)
	}
	if !strings.Contains(out, "Beta") {
		t.Errorf("output missing 'Beta', got:\n%s", out)
	}
	if !strings.Contains(out, "Charlie") {
		t.Errorf("output missing 'Charlie', got:\n%s", out)
	}
}

func TestDisplayCubesJSONLimitTruncation(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha", LastDataUpdate: "2024-01-01"},
		{Name: "Beta", LastDataUpdate: "2024-01-02"},
		{Name: "Charlie", LastDataUpdate: "2024-01-03"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 2, true)
	})

	var result []model.Cube
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	// Should only contain the first 2 cubes
	if len(result) != 2 {
		t.Errorf("expected 2 cubes, got %d", len(result))
	}
	if result[0].Name != "Alpha" {
		t.Errorf("first cube = %q, want %q", result[0].Name, "Alpha")
	}
	if result[1].Name != "Beta" {
		t.Errorf("second cube = %q, want %q", result[1].Name, "Beta")
	}
}

func TestDisplayCubesLimitExceedsTotal(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Alpha"},
		{Name: "Beta"},
	}

	out := captureStdout(t, func() {
		// limit=100 but only 2 cubes -> no truncation
		displayCubes(cubes, len(cubes), 100, false)
	})

	if !strings.Contains(out, "Alpha") {
		t.Errorf("output missing 'Alpha', got:\n%s", out)
	}
	if !strings.Contains(out, "Beta") {
		t.Errorf("output missing 'Beta', got:\n%s", out)
	}
}

func TestDisplayCubesTableColumnCount(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = false

	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: "2024-01-15"},
	}

	out := captureStdout(t, func() {
		displayCubes(cubes, len(cubes), 0, false)
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + 1 row), got %d:\n%s", len(lines), out)
	}

	// Header line should contain both column names
	header := lines[0]
	if !strings.Contains(header, "NAME") || !strings.Contains(header, "LAST UPDATED") {
		t.Errorf("header = %q, want it to contain NAME and LAST UPDATED", header)
	}
}

// --- Integration: filterSystemCubes + filterCubesByName combined ---

func TestFilterPipeline(t *testing.T) {
	// Simulates the filtering pipeline in runCubes:
	// 1. filterSystemCubes to remove system objects
	// 2. filterCubesByName for client-side filter
	cubes := []model.Cube{
		{Name: "Sales"},
		{Name: "}ClientGroups"},
		{Name: "SalesForecast"},
		{Name: "Budget"},
		{Name: "}SecurityProperties"},
		{Name: "SalesPlan"},
	}

	// Step 1: Filter system cubes (4 user cubes remain)
	filtered := filterSystemCubes(cubes, false)
	if len(filtered) != 4 {
		t.Fatalf("after filterSystemCubes: got %d cubes, want 4", len(filtered))
	}

	// Step 2: Filter by name "sales" matches 3 of the 4 user cubes
	result := filterCubesByName(filtered, "sales")
	if len(result) != 3 {
		t.Fatalf("after filterCubesByName: got %d cubes, want 3", len(result))
	}

	expectedNames := []string{"Sales", "SalesForecast", "SalesPlan"}
	for i, cube := range result {
		if cube.Name != expectedNames[i] {
			t.Errorf("cube[%d].Name = %q, want %q", i, cube.Name, expectedNames[i])
		}
	}
}

func TestFilterPipelineShowSystemWithNameFilter(t *testing.T) {
	cubes := []model.Cube{
		{Name: "Sales"},
		{Name: "}ClientGroups"},
		{Name: "}ClientSecurity"},
		{Name: "Budget"},
	}

	// showSystem=true keeps all cubes
	filtered := filterSystemCubes(cubes, true)
	if len(filtered) != 4 {
		t.Fatalf("after filterSystemCubes: got %d cubes, want 4", len(filtered))
	}

	// Filter by "client" should match the system cubes
	result := filterCubesByName(filtered, "client")
	if len(result) != 2 {
		t.Fatalf("after filterCubesByName: got %d cubes, want 2", len(result))
	}

	expectedNames := []string{"}ClientGroups", "}ClientSecurity"}
	for i, cube := range result {
		if cube.Name != expectedNames[i] {
			t.Errorf("cube[%d].Name = %q, want %q", i, cube.Name, expectedNames[i])
		}
	}
}

// --- Edge cases ---

func TestFilterSystemCubesPreservesOrder(t *testing.T) {
	cubes := []model.Cube{
		{Name: "Zebra"},
		{Name: "Alpha"},
		{Name: "}System"},
		{Name: "Middle"},
	}

	result := filterSystemCubes(cubes, false)
	expected := []string{"Zebra", "Alpha", "Middle"}
	if len(result) != len(expected) {
		t.Fatalf("got %d cubes, want %d", len(result), len(expected))
	}
	for i, cube := range result {
		if cube.Name != expected[i] {
			t.Errorf("cube[%d].Name = %q, want %q (order must be preserved)", i, cube.Name, expected[i])
		}
	}
}

func TestFilterCubesByNamePreservesMetadata(t *testing.T) {
	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: "2024-01-15T10:30:00"},
		{Name: "Budget", LastDataUpdate: "2024-02-20T14:00:00"},
	}

	result := filterCubesByName(cubes, "sales")
	if len(result) != 1 {
		t.Fatalf("got %d cubes, want 1", len(result))
	}
	if result[0].LastDataUpdate != "2024-01-15T10:30:00" {
		t.Errorf("LastDataUpdate = %q, want %q", result[0].LastDataUpdate, "2024-01-15T10:30:00")
	}
}

func TestFilterSystemCubesPreservesMetadata(t *testing.T) {
	cubes := []model.Cube{
		{Name: "Sales", LastDataUpdate: "2024-01-15T10:30:00"},
		{Name: "}System", LastDataUpdate: "2024-03-01T08:00:00"},
	}

	result := filterSystemCubes(cubes, false)
	if len(result) != 1 {
		t.Fatalf("got %d cubes, want 1", len(result))
	}
	if result[0].Name != "Sales" {
		t.Errorf("Name = %q, want %q", result[0].Name, "Sales")
	}
	if result[0].LastDataUpdate != "2024-01-15T10:30:00" {
		t.Errorf("LastDataUpdate = %q, want %q", result[0].LastDataUpdate, "2024-01-15T10:30:00")
	}
}

func TestDisplayCubesCountZero(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = true

	out := captureStdout(t, func() {
		displayCubes([]model.Cube{}, 0, 0, false)
	})

	expected := "0 cubes\n"
	if out != expected {
		t.Errorf("count output = %q, want %q", out, expected)
	}
}

func TestDisplayCubesCountZeroJSON(t *testing.T) {
	origCount := cubesCount
	defer func() { cubesCount = origCount }()
	cubesCount = true

	out := captureStdout(t, func() {
		displayCubes([]model.Cube{}, 0, 0, true)
	})

	var result map[string]int
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if result["count"] != 0 {
		t.Errorf("count = %d, want 0", result["count"])
	}
}
