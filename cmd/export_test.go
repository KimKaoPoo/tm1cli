package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

// ===========================================================================
// TestCellsetToRecords — unit tests for the cellsetToRecords function
// ===========================================================================

func TestCellsetToRecords(t *testing.T) {
	tests := []struct {
		name       string
		resp       model.CellsetResponse
		wantLen    int
		wantKeys   []string               // keys that must exist in first record
		wantValues map[string]interface{} // expected values in first record
	}{
		{
			name: "normal 2-axis with 2 cols and 2 rows",
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
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 1000.0},
					{Ordinal: 1, Value: 2000.0},
					{Ordinal: 2, Value: 500.0},
					{Ordinal: 3, Value: 800.0},
				},
			},
			wantLen:  2,
			wantKeys: []string{"DIM1", "Jan", "Feb"},
			wantValues: map[string]interface{}{
				"DIM1": "Revenue",
				"Jan":  1000.0,
				"Feb":  2000.0,
			},
		},
		{
			name: "multi-member row headers produce DIM1 and DIM2",
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
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 500.0},
				},
			},
			wantLen:  1,
			wantKeys: []string{"DIM1", "DIM2", "Q1"},
			wantValues: map[string]interface{}{
				"DIM1": "US",
				"DIM2": "Revenue",
				"Q1":   500.0,
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
			},
			wantLen:  1,
			wantKeys: []string{"DIM1", "Jan / Actual"},
			wantValues: map[string]interface{}{
				"DIM1":         "Revenue",
				"Jan / Actual": 100.0,
			},
		},
		{
			name: "fewer than 2 axes returns empty slice",
			resp: model.CellsetResponse{
				Axes:  []model.CellsetAxis{{Ordinal: 0}},
				Cells: nil,
			},
			wantLen: 0,
		},
		{
			name: "0 column tuples returns empty slice",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Row1"}}},
					}},
				},
				Cells: nil,
			},
			wantLen: 0,
		},
		{
			name: "null cell values preserved as nil",
			resp: model.CellsetResponse{
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
							{Ordinal: 0, Members: []model.CellsetMember{{Name: "Sales"}}},
						},
					},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: nil},
				},
			},
			wantLen:  1,
			wantKeys: []string{"DIM1", "Jan"},
			wantValues: map[string]interface{}{
				"DIM1": "Sales",
				"Jan":  nil,
			},
		},
		{
			name: "sparse cells with missing ordinals produce nil",
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
						},
					},
				},
				// Only ordinal 1 present; ordinal 0 is missing
				Cells: []model.CellsetCell{
					{Ordinal: 1, Value: 999.0},
				},
			},
			wantLen:  1,
			wantKeys: []string{"DIM1", "Jan", "Feb"},
			wantValues: map[string]interface{}{
				"DIM1": "Revenue",
				"Jan":  nil,
				"Feb":  999.0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := cellsetToRecords(tt.resp)

			if len(records) != tt.wantLen {
				t.Fatalf("got %d records, want %d", len(records), tt.wantLen)
			}

			if tt.wantLen == 0 {
				return
			}

			first := records[0]
			for _, key := range tt.wantKeys {
				if _, ok := first[key]; !ok {
					t.Errorf("record missing key %q, got keys: %v", key, mapKeys(first))
				}
			}

			for key, wantVal := range tt.wantValues {
				gotVal, ok := first[key]
				if !ok {
					t.Errorf("record missing key %q", key)
					continue
				}
				if wantVal == nil {
					if gotVal != nil {
						t.Errorf("key %q = %v, want nil", key, gotVal)
					}
					continue
				}
				// Compare as strings for simplicity (handles float formatting)
				gotStr := fmt.Sprintf("%v", gotVal)
				wantStr := fmt.Sprintf("%v", wantVal)
				if gotStr != wantStr {
					t.Errorf("key %q = %v, want %v", key, gotVal, wantVal)
				}
			}
		})
	}
}

func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// ---------------------------------------------------------------------------
// TestBuildCellsetRows — unit tests for the buildCellsetRows function
// ---------------------------------------------------------------------------

func TestBuildCellsetRows(t *testing.T) {
	tests := []struct {
		name        string
		resp        model.CellsetResponse
		wantHeaders []string
		wantRows    [][]string
		wantNil     bool
	}{
		{
			name: "normal 2-axis data",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
						{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
					}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
						{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost"}}},
					}},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 100.0},
					{Ordinal: 1, Value: 200.0},
					{Ordinal: 2, Value: 50.0},
					{Ordinal: 3, Value: 80.0},
				},
			},
			wantHeaders: []string{"DIM1", "Jan", "Feb"},
			wantRows: [][]string{
				{"Revenue", "100", "200"},
				{"Cost", "50", "80"},
			},
		},
		{
			name: "multi-member column headers",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}, {Name: "Actual"}}},
					}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
					}},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 100.0},
				},
			},
			wantHeaders: []string{"DIM1", "Jan / Actual"},
			wantRows:    [][]string{{"Revenue", "100"}},
		},
		{
			name: "multi-member row headers",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Q1"}}},
					}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "US"}, {Name: "Revenue"}}},
					}},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: 500.0},
				},
			},
			wantHeaders: []string{"DIM1", "DIM2", "Q1"},
			wantRows:    [][]string{{"US", "Revenue", "500"}},
		},
		{
			name: "null cell values as empty strings",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Sales"}}},
					}},
				},
				Cells: []model.CellsetCell{
					{Ordinal: 0, Value: nil},
				},
			},
			wantHeaders: []string{"DIM1", "Jan"},
			wantRows:    [][]string{{"Sales", ""}},
		},
		{
			name: "fewer than 2 axes returns nil",
			resp: model.CellsetResponse{
				Axes:  []model.CellsetAxis{{Ordinal: 0}},
				Cells: nil,
			},
			wantNil: true,
		},
		{
			name: "0 column tuples returns nil",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Row1"}}},
					}},
				},
				Cells: nil,
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers, rows := buildCellsetRows(tt.resp)

			if tt.wantNil {
				if headers != nil || rows != nil {
					t.Errorf("expected nil, got headers=%v rows=%v", headers, rows)
				}
				return
			}

			if len(headers) != len(tt.wantHeaders) {
				t.Fatalf("headers length = %d, want %d", len(headers), len(tt.wantHeaders))
			}
			for i, h := range headers {
				if h != tt.wantHeaders[i] {
					t.Errorf("headers[%d] = %q, want %q", i, h, tt.wantHeaders[i])
				}
			}

			if len(rows) != len(tt.wantRows) {
				t.Fatalf("rows length = %d, want %d", len(rows), len(tt.wantRows))
			}
			for i, row := range rows {
				if len(row) != len(tt.wantRows[i]) {
					t.Fatalf("rows[%d] length = %d, want %d", i, len(row), len(tt.wantRows[i]))
				}
				for j, v := range row {
					if v != tt.wantRows[i][j] {
						t.Errorf("rows[%d][%d] = %q, want %q", i, j, v, tt.wantRows[i][j])
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestWriteCSV — unit tests for the writeCSV function
// ---------------------------------------------------------------------------

func TestWriteCSV(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 1000.0},
			{Ordinal: 1, Value: 2000.0},
			{Ordinal: 2, Value: 500.0},
			{Ordinal: 3, Value: 800.0},
		},
	}

	t.Run("writes CSV with headers", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "out.csv")

		captured := captureAll(t, func() {
			err := writeCSV(resp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		// Verify success message on stderr
		if !strings.Contains(captured.Stderr, "Exported 2 rows") {
			t.Errorf("stderr should contain export summary, got: %s", captured.Stderr)
		}

		// Read and parse CSV
		f, err := os.Open(outFile)
		if err != nil {
			t.Fatalf("cannot open output file: %v", err)
		}
		defer f.Close()

		records, err := csv.NewReader(f).ReadAll()
		if err != nil {
			t.Fatalf("cannot parse CSV: %v", err)
		}

		// 1 header + 2 data rows = 3 total
		if len(records) != 3 {
			t.Fatalf("expected 3 CSV records, got %d", len(records))
		}

		// Check header
		wantHeader := []string{"DIM1", "Jan", "Feb"}
		for i, h := range records[0] {
			if h != wantHeader[i] {
				t.Errorf("header[%d] = %q, want %q", i, h, wantHeader[i])
			}
		}

		// Check data
		if records[1][0] != "Revenue" || records[1][1] != "1000" || records[1][2] != "2000" {
			t.Errorf("row 1 = %v, want [Revenue 1000 2000]", records[1])
		}
		if records[2][0] != "Cost" || records[2][1] != "500" || records[2][2] != "800" {
			t.Errorf("row 2 = %v, want [Cost 500 800]", records[2])
		}
	})

	t.Run("writes CSV without headers when noHeader is true", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "out.csv")

		captureAll(t, func() {
			err := writeCSV(resp, outFile, true)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := os.Open(outFile)
		if err != nil {
			t.Fatalf("cannot open output file: %v", err)
		}
		defer f.Close()

		records, err := csv.NewReader(f).ReadAll()
		if err != nil {
			t.Fatalf("cannot parse CSV: %v", err)
		}

		// 2 data rows, no header
		if len(records) != 2 {
			t.Fatalf("expected 2 CSV records (no header), got %d", len(records))
		}
		if records[0][0] != "Revenue" {
			t.Errorf("first row should be data, got: %v", records[0])
		}
	})

	t.Run("empty cellset prints message and creates no file", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "out.csv")
		emptyResp := model.CellsetResponse{
			Axes:  []model.CellsetAxis{{Ordinal: 0}},
			Cells: nil,
		}

		captured := captureAll(t, func() {
			err := writeCSV(emptyResp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(captured.Stderr, "No data to export") {
			t.Errorf("stderr should contain 'No data to export', got: %s", captured.Stderr)
		}

		if _, err := os.Stat(outFile); err == nil {
			t.Error("file should not be created for empty cellset")
		}
	})

	t.Run("returns error for invalid path", func(t *testing.T) {
		badPath := filepath.Join(t.TempDir(), "nonexistent", "subdir", "out.csv")

		captureAll(t, func() {
			err := writeCSV(resp, badPath, false)
			if err == nil {
				t.Fatal("expected error for invalid path, got nil")
			}
			if !strings.Contains(err.Error(), "Cannot create file") {
				t.Errorf("error should mention file creation, got: %s", err.Error())
			}
		})
	})
}

// ===========================================================================
// TestRunExport_JSONFile — integration tests for JSON file output
// ===========================================================================

func TestRunExport_JSONFile(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "export.json")
	exportOut = outFile

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

	// File should exist
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}

	// Should be valid JSON array
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("output file is not valid JSON array: %v\ncontents: %s", err, string(data))
	}

	// Should have 2 records (Revenue and Cost rows)
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// First record should be Revenue row
	first := records[0]
	if first["DIM1"] != "Revenue" {
		t.Errorf("first record DIM1 = %v, want Revenue", first["DIM1"])
	}
	// JSON numbers unmarshal as float64
	if first["Jan"] != 1000.0 {
		t.Errorf("first record Jan = %v, want 1000", first["Jan"])
	}
	if first["Feb"] != 2000.0 {
		t.Errorf("first record Feb = %v, want 2000", first["Feb"])
	}

	// Second record should be Cost row
	second := records[1]
	if second["DIM1"] != "Cost" {
		t.Errorf("second record DIM1 = %v, want Cost", second["DIM1"])
	}
	if second["Jan"] != 500.0 {
		t.Errorf("second record Jan = %v, want 500", second["Jan"])
	}

	// Stderr should contain success message
	if !strings.Contains(captured.Stderr, "Wrote 2 records") {
		t.Errorf("stderr should contain success message, got:\n%s", captured.Stderr)
	}
	if !strings.Contains(captured.Stderr, outFile) {
		t.Errorf("stderr should contain file path, got:\n%s", captured.Stderr)
	}

	// Stdout should be empty (output goes to file, not stdout)
	if strings.TrimSpace(captured.Stdout) != "" {
		t.Errorf("stdout should be empty when writing to file, got:\n%s", captured.Stdout)
	}
}

func TestRunExport_JSONFileInvalidPath(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportOut = "/nonexistent/dir/export.json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Cannot write file") {
		t.Errorf("stderr should contain 'Cannot write file', got:\n%s", captured.Stderr)
	}
}

func TestRunExport_JSONFileOverwrite(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "export.json")
	exportOut = outFile

	// Write existing content to the file
	os.WriteFile(outFile, []byte("old content"), 0644)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// File should be overwritten with valid JSON
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}

	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("overwritten file is not valid JSON: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records after overwrite, got %d", len(records))
	}
}

func TestRunExport_JSONFileEmptyCellset(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "empty.json")
	exportOut = outFile

	// Return a cellset with fewer than 2 axes
	emptyResp := model.CellsetResponse{
		Axes:  []model.CellsetAxis{{Ordinal: 0}},
		Cells: nil,
	}
	emptyData, _ := json.Marshal(emptyResp)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(emptyData)
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}

	// Should be an empty JSON array
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("output is not valid JSON: %v\ncontents: %s", err, string(data))
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records for empty cellset, got %d", len(records))
	}

	// Stderr should say 0 records
	if !strings.Contains(captured.Stderr, "Wrote 0 records") {
		t.Errorf("stderr should contain 'Wrote 0 records', got:\n%s", captured.Stderr)
	}
}

// ---------------------------------------------------------------------------
// Integration tests — CSV file export via runExport
// ---------------------------------------------------------------------------

func TestRunExport_CSVFile(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.csv")
	exportOut = outFile

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

	// Verify success message
	if !strings.Contains(captured.Stderr, "Exported") {
		t.Errorf("stderr should contain export message, got: %s", captured.Stderr)
	}

	// Nothing on stdout (file output, not screen)
	if strings.TrimSpace(captured.Stdout) != "" {
		t.Errorf("stdout should be empty for file output, got: %s", captured.Stdout)
	}

	// Read and verify CSV
	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("cannot open output file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV: %v", err)
	}

	if len(records) != 3 { // 1 header + 2 data
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0][0] != "DIM1" {
		t.Errorf("header[0] = %q, want DIM1", records[0][0])
	}
	if records[1][1] != "1000" {
		t.Errorf("Revenue/Jan = %q, want 1000", records[1][1])
	}
}

func TestRunExport_CSVNoHeader(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.csv")
	exportOut = outFile
	exportNoHeader = true

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("cannot open output file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV: %v", err)
	}

	// 2 data rows only, no header
	if len(records) != 2 {
		t.Fatalf("expected 2 records (no header), got %d", len(records))
	}
	if records[0][0] != "Revenue" {
		t.Errorf("first row should be data, got: %v", records[0])
	}
}

func TestRunExport_UsesPOSTNotGET(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	var capturedMethod string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if capturedMethod != "POST" {
		t.Errorf("export should use POST for tm1.Execute, got %q", capturedMethod)
	}
}

func TestRunExport_CSVUsesPOST(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportOut = filepath.Join(t.TempDir(), "out.csv")

	var capturedMethod string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{"Sales"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if capturedMethod != "POST" {
		t.Errorf("CSV export should use POST for tm1.Execute, got %q", capturedMethod)
	}
}

func TestRunExport_CSVServerError(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.csv")
	exportOut = outFile

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
		t.Errorf("stderr should contain error, got: %s", captured.Stderr)
	}

	// File should not be created
	if _, err := os.Stat(outFile); err == nil {
		t.Error("CSV file should not be created on server error")
	}
}

// ===========================================================================
// MDX Export Tests — helpers
// ===========================================================================

// mdxResponseJSON returns a mock ExecuteMDX response with ID and axes (no cells).
func mdxResponseJSON(id string) []byte {
	resp := model.CellsetResponse{
		ID: id,
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
	}
	data, _ := json.Marshal(resp)
	return data
}

// cellsCollectionJSON returns a mock TM1 cells collection response.
func cellsCollectionJSON(cells []model.CellsetCell) []byte {
	resp := struct {
		Value []model.CellsetCell `json:"value"`
	}{Value: cells}
	data, _ := json.Marshal(resp)
	return data
}

var testMDXCells = []model.CellsetCell{
	{Ordinal: 0, Value: 1000.0},
	{Ordinal: 1, Value: 2000.0},
	{Ordinal: 2, Value: 500.0},
	{Ordinal: 3, Value: 800.0},
}

// ===========================================================================
// MDX Export Tests — integration tests
// ===========================================================================

func TestRunExport_MDXToScreen(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT {[Period].[Jan],[Period].[Feb]} ON COLUMNS, {[Measure].Members} ON ROWS FROM [Sales]"

	deleteCalled := false
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("test-cellset-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE" && strings.Contains(r.URL.Path, "Cellsets"):
			deleteCalled = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should contain table output
	for _, want := range []string{"Jan", "Feb", "Revenue", "Cost", "1000", "2000", "500", "800"} {
		if !strings.Contains(captured.Stdout, want) {
			t.Errorf("output missing %q, got:\n%s", want, captured.Stdout)
		}
	}

	// Cellset should be cleaned up
	if !deleteCalled {
		t.Error("DELETE should be called to clean up cellset")
	}
}

func TestRunExport_MDXJSONOutput(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

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
}

func TestRunExport_MDXCSVFile(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"

	outFile := filepath.Join(t.TempDir(), "mdx_export.csv")
	exportOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Exported 2 rows") {
		t.Errorf("stderr should contain export summary, got: %s", captured.Stderr)
	}

	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("cannot open output file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV: %v", err)
	}

	if len(records) != 3 { // 1 header + 2 data
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0][0] != "DIM1" {
		t.Errorf("header[0] = %q, want DIM1", records[0][0])
	}
	if records[1][1] != "1000" {
		t.Errorf("Revenue/Jan = %q, want 1000", records[1][1])
	}
}

func TestRunExport_MDXJSONFile(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"

	outFile := filepath.Join(t.TempDir(), "mdx_export.json")
	exportOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "Wrote 2 records") {
		t.Errorf("stderr should contain write summary, got: %s", captured.Stderr)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}

	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
	if records[0]["DIM1"] != "Revenue" {
		t.Errorf("first record DIM1 = %v, want Revenue", records[0]["DIM1"])
	}
}

func TestRunExport_MDXServerError(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT INVALID FROM [Sales]"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error": {"message": "Invalid MDX"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "HTTP 400") {
		t.Errorf("stderr should contain HTTP 400 error, got:\n%s", captured.Stderr)
	}
}

func TestRunExport_MDXCellsetCleanupOnError(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"

	deleteCalled := false
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("cleanup-test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		case r.Method == "DELETE" && strings.Contains(r.URL.Path, "Cellsets"):
			deleteCalled = true
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !deleteCalled {
		t.Error("DELETE should be called even when cell fetch fails")
	}
}

func TestRunExport_MDXEndpoints(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT {[Period].[Jan]} ON 0, {[Measure].[Revenue]} ON 1 FROM [Sales]"

	var postPath, postBody, getPath, deletePath string
	var postMethod, getMethod, deleteMethod string

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			postMethod = r.Method
			postPath = r.URL.Path
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			postBody = string(body)
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("endpoint-test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			getMethod = r.Method
			getPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE":
			deleteMethod = r.Method
			deletePath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Verify POST to ExecuteMDX
	if postMethod != "POST" {
		t.Errorf("ExecuteMDX should use POST, got %q", postMethod)
	}
	if !strings.Contains(postPath, "ExecuteMDX") {
		t.Errorf("POST path should contain ExecuteMDX, got: %s", postPath)
	}
	if !strings.Contains(postBody, "MDX") {
		t.Errorf("POST body should contain MDX key, got: %s", postBody)
	}
	if !strings.Contains(postBody, "SELECT") {
		t.Errorf("POST body should contain the MDX query, got: %s", postBody)
	}

	// Verify GET cells
	if getMethod != "GET" {
		t.Errorf("Cells fetch should use GET, got %q", getMethod)
	}
	if !strings.Contains(getPath, "Cellsets('endpoint-test-id')") {
		t.Errorf("GET path should contain cellset ID, got: %s", getPath)
	}
	if !strings.Contains(getPath, "Cells") {
		t.Errorf("GET path should contain Cells, got: %s", getPath)
	}

	// Verify DELETE cleanup
	if deleteMethod != "DELETE" {
		t.Errorf("Cleanup should use DELETE, got %q", deleteMethod)
	}
	if !strings.Contains(deletePath, "Cellsets('endpoint-test-id')") {
		t.Errorf("DELETE path should contain cellset ID, got: %s", deletePath)
	}
}

func TestRunExport_MDXPagination(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [LargeCube]"

	// Use CSV file output to avoid pipe buffer deadlock with large datasets
	outFile := filepath.Join(t.TempDir(), "paginated.csv")
	exportOut = outFile

	// Generate axes that imply 12000 cells (2 cols * 6000 rows)
	rowTuples := make([]model.CellsetTuple, 6000)
	for i := range rowTuples {
		rowTuples[i] = model.CellsetTuple{
			Ordinal: i,
			Members: []model.CellsetMember{{Name: fmt.Sprintf("Row%d", i)}},
		}
	}
	mdxResp := model.CellsetResponse{
		ID: "pagination-test-id",
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
				},
			},
			{Ordinal: 1, Tuples: rowTuples},
		},
	}
	mdxData, _ := json.Marshal(mdxResp)

	// Generate cells for two pages
	page1 := make([]model.CellsetCell, mdxCellPageSize)
	for i := range page1 {
		page1[i] = model.CellsetCell{Ordinal: i, Value: float64(i)}
	}
	page2 := make([]model.CellsetCell, 2000)
	for i := range page2 {
		page2[i] = model.CellsetCell{Ordinal: mdxCellPageSize + i, Value: float64(mdxCellPageSize + i)}
	}

	getCount := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxData)
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			getCount++
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.URL.RawQuery, "skip=0") || !strings.Contains(r.URL.RawQuery, "skip=") {
				w.Write(cellsCollectionJSON(page1))
			} else {
				w.Write(cellsCollectionJSON(page2))
			}
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should have made 2 GET requests for cells
	if getCount != 2 {
		t.Errorf("expected 2 GET requests for cells, got %d", getCount)
	}

	// Progress should appear on stderr (totalCells=12000 > mdxCellPageSize)
	if !strings.Contains(captured.Stderr, "Fetching cells:") {
		t.Errorf("stderr should contain progress output, got:\n%s", captured.Stderr)
	}

	// CSV file should contain data from both pages
	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("cannot open output file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV: %v", err)
	}

	// 1 header + 6000 data rows
	if len(records) != 6001 {
		t.Fatalf("expected 6001 CSV records, got %d", len(records))
	}
	if records[1][0] != "Row0" {
		t.Errorf("first data row should be Row0, got: %s", records[1][0])
	}
}

func TestRunExport_MDXNoCellsetID(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"

	// Return a response without an ID field
	noIDResp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
			}},
		},
	}
	noIDData, _ := json.Marshal(noIDResp)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(noIDData)
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "cellset ID") {
		t.Errorf("stderr should mention cellset ID, got:\n%s", captured.Stderr)
	}
}

func TestRunExport_MDXExactPageSize(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [Sales]"

	// Use CSV output to avoid pipe buffer issues
	outFile := filepath.Join(t.TempDir(), "exact.csv")
	exportOut = outFile

	// Axes imply exactly mdxCellPageSize cells (no progress display expected)
	rowTuples := make([]model.CellsetTuple, mdxCellPageSize)
	for i := range rowTuples {
		rowTuples[i] = model.CellsetTuple{
			Ordinal: i,
			Members: []model.CellsetMember{{Name: fmt.Sprintf("R%d", i)}},
		}
	}
	mdxResp := model.CellsetResponse{
		ID: "exact-page-id",
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Val"}}},
			}},
			{Ordinal: 1, Tuples: rowTuples},
		},
	}
	mdxData, _ := json.Marshal(mdxResp)

	// Return exactly mdxCellPageSize cells — triggers a second GET that returns 0
	fullPage := make([]model.CellsetCell, mdxCellPageSize)
	for i := range fullPage {
		fullPage[i] = model.CellsetCell{Ordinal: i, Value: float64(i)}
	}

	getCount := 0
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxData)
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			getCount++
			w.Header().Set("Content-Type", "application/json")
			if getCount == 1 {
				w.Write(cellsCollectionJSON(fullPage))
			} else {
				// Second request returns empty — pagination stops
				w.Write(cellsCollectionJSON(nil))
			}
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Should make 2 GET requests (first returns full page, second returns empty)
	if getCount != 2 {
		t.Errorf("expected 2 GET requests, got %d", getCount)
	}

	// No progress output because totalCells == mdxCellPageSize (not >)
	if strings.Contains(captured.Stderr, "Fetching cells:") {
		t.Errorf("should not show progress for single-page results, got:\n%s", captured.Stderr)
	}

	// Verify CSV was written
	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("cannot open output file: %v", err)
	}
	defer f.Close()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("cannot parse CSV: %v", err)
	}

	// 1 header + mdxCellPageSize data rows
	if len(records) != mdxCellPageSize+1 {
		t.Fatalf("expected %d CSV records, got %d", mdxCellPageSize+1, len(records))
	}
}

func TestRunExport_MDXProgress(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT ... FROM [LargeCube]"

	// Use CSV file output to avoid pipe buffer deadlock with large datasets
	outFile := filepath.Join(t.TempDir(), "progress.csv")
	exportOut = outFile

	// Axes that imply > mdxCellPageSize total cells for progress display
	rowTuples := make([]model.CellsetTuple, mdxCellPageSize+1)
	for i := range rowTuples {
		rowTuples[i] = model.CellsetTuple{
			Ordinal: i,
			Members: []model.CellsetMember{{Name: fmt.Sprintf("R%d", i)}},
		}
	}
	mdxResp := model.CellsetResponse{
		ID: "progress-test-id",
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Val"}}},
			}},
			{Ordinal: 1, Tuples: rowTuples},
		},
	}
	mdxData, _ := json.Marshal(mdxResp)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && strings.Contains(r.URL.Path, "ExecuteMDX"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxData)
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			// Return small set of cells (single page)
			w.Write(cellsCollectionJSON([]model.CellsetCell{
				{Ordinal: 0, Value: 42.0},
			}))
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captured := captureAll(t, func() {
		err := runExport(exportCmd, []string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Progress should appear because totalCells > mdxCellPageSize
	if !strings.Contains(captured.Stderr, "Fetching cells:") {
		t.Errorf("stderr should contain progress, got:\n%s", captured.Stderr)
	}
	expectedTotal := fmt.Sprintf("/ %d", mdxCellPageSize+1)
	if !strings.Contains(captured.Stderr, expectedTotal) {
		t.Errorf("progress should show total %d, got:\n%s", mdxCellPageSize+1, captured.Stderr)
	}
}

// ===========================================================================
// MDX Export Tests — validation
// ===========================================================================

func TestRunExport_ViewAndMDXBoth(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportMDX = "SELECT ... FROM [Sales]"

	err := runExport(exportCmd, []string{"Sales"})
	if err == nil {
		t.Fatal("expected error when both --view and --mdx specified, got nil")
	}
	if !strings.Contains(err.Error(), "not both") {
		t.Errorf("error should mention 'not both', got: %s", err.Error())
	}
}

func TestRunExport_ViewNoCube(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	err := runExport(exportCmd, []string{})
	if err == nil {
		t.Fatal("expected error when --view without cube, got nil")
	}
	if !strings.Contains(err.Error(), "Cube name is required") {
		t.Errorf("error should mention cube requirement, got: %s", err.Error())
	}
}

func TestRunExport_NoArgsNoFlags(t *testing.T) {
	resetCmdFlags(t)

	err := runExport(exportCmd, []string{})
	if err == nil {
		t.Fatal("expected error with no args and no flags, got nil")
	}
	if !strings.Contains(err.Error(), "--view") || !strings.Contains(err.Error(), "--mdx") {
		t.Errorf("error should mention --view and --mdx, got: %s", err.Error())
	}
}
