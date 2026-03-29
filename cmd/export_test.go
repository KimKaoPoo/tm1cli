package cmd

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"tm1cli/internal/model"
	"tm1cli/internal/output"
)

// ---------------------------------------------------------------------------
// TestPrintCellsetTable — unit tests for the printCellsetTable function
// ---------------------------------------------------------------------------

func TestPrintCellsetTable(t *testing.T) {
	tests := []struct {
		name     string
		resp     model.CellsetResponse
		wantSubs []string // substrings that must appear in output
	}{
		{
			name: "normal 2-axis table with 2 cols and 3 rows",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{ // column axis
						Ordinal: 0,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
						},
					},
					{ // row axis
						Ordinal: 1,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost"}}},
							{Ordinal: 2, Members: []model.CellsetMember{{Name: "Profit"}}},
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 100.0},
					{Ordinal: 1, Value: 200.0},
					{Ordinal: 2, Value: 50.0},
					{Ordinal: 3, Value: 80.0},
					{Ordinal: 4, Value: 50.0},
					{Ordinal: 5, Value: 120.0},
				},
			},
			wantSubs: []string{
				"DIM1", "Jan", "Feb",
				"Revenue", "100", "200",
				"Cost", "50", "80",
				"Profit", "50", "120",
			},
		},
		{
			name: "multi-member column headers joined with /",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{
						Ordinal: 0,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}, {Name: "Actual"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}, {Name: "Actual"}}},
						},
					},
					{
						Ordinal: 1,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 100.0},
					{Ordinal: 1, Value: 200.0},
				},
			},
			wantSubs: []string{
				"Jan / Actual",
				"Feb / Actual",
				"Revenue",
			},
		},
		{
			name: "multi-member row headers produce DIM1 DIM2 columns",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{
						Ordinal: 0,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Q1"}}},
						},
					},
					{
						Ordinal: 1,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "US"}, {Name: "Revenue"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "EU"}, {Name: "Cost"}}},
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 500.0},
					{Ordinal: 1, Value: 300.0},
				},
			},
			wantSubs: []string{
				"DIM1", "DIM2", "Q1",
				"US", "Revenue", "500",
				"EU", "Cost", "300",
			},
		},
		{
			name: "fewer than 2 axes prints no data",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{}},
				},
				Cells: nil,
			},
			wantSubs: []string{"No data returned."},
		},
		{
			name: "0 column tuples prints no data",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Row1"}}},
					}},
				},
				Cells: nil,
			},
			wantSubs: []string{"No data returned."},
		},
		{
			name: "null cell values rendered as empty string",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{
						Ordinal: 0,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
						},
					},
					{
						Ordinal: 1,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Sales"}}},
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: nil},
					{Ordinal: 1, Value: nil},
				},
			},
			wantSubs: []string{"DIM1", "Jan", "Feb", "Sales"},
		},
		{
			name: "sparse cells with missing ordinals",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{
						Ordinal: 0,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
						},
					},
					{
						Ordinal: 1,
						Tuples: []model.CellsetTuple{
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
							{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost"}}},
						},
					},
				},
				// Only ordinals 0 and 3 present; 1 and 2 are missing
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 100.0},
					{Ordinal: 3, Value: 400.0},
				},
			},
			wantSubs: []string{
				"Revenue", "100",
				"Cost", "400",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := captureStdout(t, func() {
				printCellsetTable(tt.resp)
			})

			for _, sub := range tt.wantSubs {
				if !strings.Contains(got, sub) {
					t.Errorf("output missing %q\ngot:\n%s", sub, got)
				}
			}
		})
	}
}

func TestPrintCellsetTableNullCellsNotShowValue(t *testing.T) {
	// When cells have nil Value, the cell position should not contain
	// a literal "nil" or "<nil>" string.
	// Use "Vanilla" as member name — it contains "nil", proving
	// the assertion targets cell values, not member names.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				},
			},
			{
				Ordinal: 1,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Vanilla"}}},
				},
			},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: nil},
		},
	}

	got := captureStdout(t, func() {
		printCellsetTable(resp)
	})

	// <nil> should never appear in output
	if strings.Contains(got, "<nil>") {
		t.Errorf("output should not contain <nil>, got:\n%s", got)
	}

	// The member name "Vanilla" contains "nil" — ensure it still renders correctly
	if !strings.Contains(got, "Vanilla") {
		t.Errorf("output should contain member name 'Vanilla', got:\n%s", got)
	}

	// Find data rows and check that cell values don't show "nil".
	// Data rows follow the header. Split by whitespace to isolate
	// cell values from member names (which may legitimately contain "nil").
	for _, line := range strings.Split(got, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "DIM") {
			continue
		}
		// Row format: "Vanilla   <cell_value>"
		// Check cell value columns (everything after the member name)
		parts := strings.Fields(trimmed)
		if len(parts) > 1 {
			for _, cellVal := range parts[1:] {
				if strings.EqualFold(cellVal, "nil") || cellVal == "<nil>" {
					t.Errorf("cell value should not be 'nil', got %q in line: %q", cellVal, trimmed)
				}
			}
		}
	}
}

func TestPrintCellsetTableSparseCellsEmpty(t *testing.T) {
	// Missing ordinals should produce empty cells, not cause a panic.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
					{Ordinal: 2, Members: []model.CellsetMember{{Name: "Mar"}}},
				},
			},
			{
				Ordinal: 1,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
				},
			},
		},
		// Only ordinal 1 is present; 0 and 2 are missing
		Cells: []model.CellsetCell{
			{Ordinal: 1, Value: 999.0},
		},
	}

	got := captureStdout(t, func() {
		printCellsetTable(resp)
	})

	if !strings.Contains(got, "999") {
		t.Errorf("expected output to contain 999, got:\n%s", got)
	}
}

// ---------------------------------------------------------------------------
// TestRunExportStubs — stub messages for unimplemented features
// ---------------------------------------------------------------------------

func TestRunExportStubs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "mdx flag returns v0.2.0 message",
			args:    []string{"export", "Sales", "--mdx", "SELECT {[Measures].Members} ON COLUMNS FROM [Sales]"},
			wantErr: "coming in v0.2.0",
		},
		{
			name:    "out csv returns v0.1.1 message",
			args:    []string{"export", "Sales", "--view", "Default", "--out", "report.csv"},
			wantErr: "coming in v0.1.1",
		},
		{
			name:    "out json file returns v0.1.1 message",
			args:    []string{"export", "Sales", "--view", "Default", "--out", "report.json"},
			wantErr: "coming in v0.1.1",
		},
		{
			name:    "out xlsx returns v0.2.0 message",
			args:    []string{"export", "Sales", "--view", "Default", "--out", "report.xlsx"},
			wantErr: "coming in v0.2.0",
		},
		{
			name:    "out txt returns unsupported format error",
			args:    []string{"export", "Sales", "--view", "Default", "--out", "report.txt"},
			wantErr: "Unsupported file format",
		},
		{
			name:    "no view and no mdx returns error with cube name",
			args:    []string{"export", "MyCube"},
			wantErr: "MyCube",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save and restore global flag state
			origView := exportView
			origMDX := exportMDX
			origOut := exportOut
			origSilenceUsage := rootCmd.SilenceUsage
			defer func() {
				exportView = origView
				exportMDX = origMDX
				exportOut = origOut
				rootCmd.SilenceUsage = origSilenceUsage
			}()

			// Reset flags before each test
			exportView = ""
			exportMDX = ""
			exportOut = ""
			rootCmd.SilenceUsage = true

			cmd := rootCmd
			cmd.SetArgs(tt.args)

			err := cmd.Execute()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestRunExportValidation — validation error messages
// ---------------------------------------------------------------------------

func TestRunExportValidation(t *testing.T) {
	tests := []struct {
		name     string
		cubeName string
		wantSubs []string
	}{
		{
			name:     "error includes cube name and usage hint",
			cubeName: "SalesCube",
			wantSubs: []string{"SalesCube", "--view", "--mdx"},
		},
		{
			name:     "error includes cube name with spaces",
			cubeName: "My Cube",
			wantSubs: []string{"My Cube", "--view"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origView := exportView
			origMDX := exportMDX
			origOut := exportOut
			origSilenceUsage := rootCmd.SilenceUsage
			defer func() {
				exportView = origView
				exportMDX = origMDX
				exportOut = origOut
				rootCmd.SilenceUsage = origSilenceUsage
			}()

			exportView = ""
			exportMDX = ""
			exportOut = ""
			rootCmd.SilenceUsage = true

			cmd := rootCmd
			cmd.SetArgs([]string{"export", tt.cubeName})

			err := cmd.Execute()
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			errMsg := err.Error()
			for _, sub := range tt.wantSubs {
				if !strings.Contains(errMsg, sub) {
					t.Errorf("error %q missing substring %q", errMsg, sub)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestPrintCellsetTableJSONOutput — JSON output via output.PrintJSON
// ---------------------------------------------------------------------------

func TestPrintCellsetTableJSONOutput(t *testing.T) {
	// When --output json is set, export prints the raw CellsetResponse as JSON
	// rather than a formatted table. Since this path goes through output.PrintJSON,
	// we verify the behavior by calling PrintJSON directly with a CellsetResponse.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				},
			},
			{
				Ordinal: 1,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
				},
			},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 100.0},
		},
	}

	got := captureStdout(t, func() {
		output.PrintJSON(resp)
	})

	// Should be valid JSON containing expected keys
	if !strings.Contains(got, "\"Axes\"") {
		t.Errorf("JSON output should contain 'Axes' key, got:\n%s", got)
	}
	if !strings.Contains(got, "\"Cells\"") {
		t.Errorf("JSON output should contain 'Cells' key, got:\n%s", got)
	}
	if !strings.Contains(got, "\"Value\"") {
		t.Errorf("JSON output should contain 'Value' key, got:\n%s", got)
	}
	// Should NOT contain table formatting
	if strings.Contains(got, "DIM1") {
		t.Errorf("JSON output should not contain table header 'DIM1', got:\n%s", got)
	}
}

// ============================================================
// Integration tests — runExport with httptest.NewServer
// ============================================================

// cellsetResponseJSON returns a mock TM1 cellset response for testing.
func cellsetResponseJSON() []byte {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
				},
			},
			{
				Ordinal: 1,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost"}}},
				},
			},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 1000.0},
			{Ordinal: 1, Value: 2000.0},
			{Ordinal: 2, Value: 500.0},
			{Ordinal: 3, Value: 800.0},
		},
	}
	data, _ := json.Marshal(resp)
	return data
}

func TestRunExport_ViewToScreen(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should contain table with headers and data
	if !strings.Contains(captured.Stdout, "Jan") {
		t.Errorf("output missing column 'Jan', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Feb") {
		t.Errorf("output missing column 'Feb', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Revenue") {
		t.Errorf("output missing row 'Revenue', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "1000") {
		t.Errorf("output missing cell value '1000', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "800") {
		t.Errorf("output missing cell value '800', got:\n%s", captured.Stdout)
	}
}

func TestRunExport_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should be valid JSON containing Axes and Cells
	var resp model.CellsetResponse
	if err := json.Unmarshal([]byte(captured.Stdout), &resp); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if len(resp.Axes) != 2 {
		t.Errorf("expected 2 axes, got %d", len(resp.Axes))
	}
	if len(resp.Cells) != 4 {
		t.Errorf("expected 4 cells, got %d", len(resp.Cells))
	}
	// Should NOT contain table formatting
	if strings.Contains(captured.Stdout, "DIM1") {
		t.Error("JSON output should not contain table header 'DIM1'")
	}
}

func TestRunExport_NoViewOrMDX(t *testing.T) {
	resetCmdFlags(t)
	// Neither view nor mdx set

	err := runExport(exportCmd, []string{"MyCube"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "--view") {
		t.Errorf("error should mention --view, got: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "MyCube") {
		t.Errorf("error should contain cube name, got: %s", err.Error())
	}
}

func TestRunExport_ServerError(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`View not found`))
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Not found") {
		t.Errorf("stderr should contain 'Not found', got:\n%s", captured.Stderr)
	}
}

func TestRunExport_ViewEndpointContainsCubeAndView(t *testing.T) {
	resetCmdFlags(t)
	exportView = "MyView"

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{"MyCube"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(capturedPath, "Cubes('MyCube')") {
		t.Errorf("request path should contain cube name, got: %s", capturedPath)
	}
	if !strings.Contains(capturedPath, "Views('MyView')") {
		t.Errorf("request path should contain view name, got: %s", capturedPath)
	}
}
