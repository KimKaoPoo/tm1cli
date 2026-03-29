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

	"github.com/xuri/excelize/v2"
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
// TestWriteXLSX — unit tests for the writeXLSX function
// ===========================================================================

func TestWriteXLSX(t *testing.T) {
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

	t.Run("writes XLSX with headers and correct values", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "out.xlsx")

		captured := captureAll(t, func() {
			err := writeXLSX(resp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if !strings.Contains(captured.Stderr, "Exported 2 rows") {
			t.Errorf("stderr should contain export summary, got: %s", captured.Stderr)
		}

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// Row 1: headers
		h1, _ := f.GetCellValue("Sheet1", "A1")
		h2, _ := f.GetCellValue("Sheet1", "B1")
		h3, _ := f.GetCellValue("Sheet1", "C1")
		if h1 != "DIM1" || h2 != "Jan" || h3 != "Feb" {
			t.Errorf("headers = [%s, %s, %s], want [DIM1, Jan, Feb]", h1, h2, h3)
		}

		// Row 2: Revenue 1000 2000
		a2, _ := f.GetCellValue("Sheet1", "A2")
		b2, _ := f.GetCellValue("Sheet1", "B2")
		c2, _ := f.GetCellValue("Sheet1", "C2")
		if a2 != "Revenue" {
			t.Errorf("A2 = %q, want Revenue", a2)
		}
		if b2 != "1000" {
			t.Errorf("B2 = %q, want 1000", b2)
		}
		if c2 != "2000" {
			t.Errorf("C2 = %q, want 2000", c2)
		}

		// Row 3: Cost 500 800
		a3, _ := f.GetCellValue("Sheet1", "A3")
		b3, _ := f.GetCellValue("Sheet1", "B3")
		c3, _ := f.GetCellValue("Sheet1", "C3")
		if a3 != "Cost" || b3 != "500" || c3 != "800" {
			t.Errorf("row 3 = [%s, %s, %s], want [Cost, 500, 800]", a3, b3, c3)
		}
	})

	t.Run("numeric values are stored as numbers not strings", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "num.xlsx")

		captureAll(t, func() {
			err := writeXLSX(resp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// B2 should contain "1000" not "1000.000000" or similar string formatting
		val, _ := f.GetCellValue("Sheet1", "B2")
		if val != "1000" {
			t.Errorf("B2 = %q, want 1000", val)
		}
		// A2 (member name) should be a string
		a2, _ := f.GetCellValue("Sheet1", "A2")
		if a2 != "Revenue" {
			t.Errorf("A2 = %q, want Revenue", a2)
		}
	})

	t.Run("writes XLSX without headers when noHeader is true", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "nohead.xlsx")

		captureAll(t, func() {
			err := writeXLSX(resp, outFile, true)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// Row 1 should be data, not headers
		a1, _ := f.GetCellValue("Sheet1", "A1")
		if a1 != "Revenue" {
			t.Errorf("A1 = %q, want Revenue (first data row)", a1)
		}

		// Row 3 should be empty (only 2 data rows)
		a3, _ := f.GetCellValue("Sheet1", "A3")
		if a3 != "" {
			t.Errorf("A3 should be empty, got %q", a3)
		}
	})

	t.Run("empty cellset prints message and creates no file", func(t *testing.T) {
		outFile := filepath.Join(t.TempDir(), "empty.xlsx")
		emptyResp := model.CellsetResponse{
			Axes:  []model.CellsetAxis{{Ordinal: 0}},
			Cells: nil,
		}

		captured := captureAll(t, func() {
			err := writeXLSX(emptyResp, outFile, false)
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
		badPath := filepath.Join(t.TempDir(), "nonexistent", "subdir", "out.xlsx")

		captureAll(t, func() {
			err := writeXLSX(resp, badPath, false)
			if err == nil {
				t.Fatal("expected error for invalid path, got nil")
			}
			if !strings.Contains(err.Error(), "Cannot write file") {
				t.Errorf("error should mention file creation, got: %s", err.Error())
			}
		})
	})

	t.Run("numeric-looking member names preserved as text", func(t *testing.T) {
		numResp := model.CellsetResponse{
			Axes: []model.CellsetAxis{
				{Ordinal: 0, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				}},
				{Ordinal: 1, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "00123"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "2024"}}},
				}},
			},
			Cells: []model.CellsetCell{
				{Ordinal: 0, Value: 100.0},
				{Ordinal: 1, Value: 200.0},
			},
		}

		outFile := filepath.Join(t.TempDir(), "members.xlsx")
		captureAll(t, func() {
			err := writeXLSX(numResp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// Member "00123" must keep leading zeros (text, not number)
		a2, _ := f.GetCellValue("Sheet1", "A2")
		if a2 != "00123" {
			t.Errorf("A2 = %q, want 00123 (leading zeros preserved)", a2)
		}

		// Member "2024" must remain string "2024"
		a3, _ := f.GetCellValue("Sheet1", "A3")
		if a3 != "2024" {
			t.Errorf("A3 = %q, want 2024", a3)
		}

		// Data column (Jan) should still be numeric
		b2, _ := f.GetCellValue("Sheet1", "B2")
		if b2 != "100" {
			t.Errorf("B2 = %q, want 100", b2)
		}
	})

	t.Run("sparse cells render as empty cells", func(t *testing.T) {
		sparseResp := model.CellsetResponse{
			Axes: []model.CellsetAxis{
				{Ordinal: 0, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
				}},
				{Ordinal: 1, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
				}},
			},
			Cells: []model.CellsetCell{
				{Ordinal: 1, Value: 999.0},
			},
		}

		outFile := filepath.Join(t.TempDir(), "sparse.xlsx")
		captureAll(t, func() {
			err := writeXLSX(sparseResp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// B2 (Jan/Revenue) should be empty (ordinal 0 missing)
		b2, _ := f.GetCellValue("Sheet1", "B2")
		if b2 != "" {
			t.Errorf("B2 should be empty for missing ordinal, got %q", b2)
		}

		// C2 (Feb/Revenue) should be 999
		c2, _ := f.GetCellValue("Sheet1", "C2")
		if c2 != "999" {
			t.Errorf("C2 = %q, want 999", c2)
		}
	})
}

// ===========================================================================
// Integration tests — XLSX file export via runExport
// ===========================================================================

func TestRunExport_XLSXFile(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.xlsx")
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
	if !strings.Contains(captured.Stderr, "Exported 2 rows") {
		t.Errorf("stderr should contain export message, got: %s", captured.Stderr)
	}

	// Nothing on stdout
	if strings.TrimSpace(captured.Stdout) != "" {
		t.Errorf("stdout should be empty for file output, got: %s", captured.Stdout)
	}

	// Open and verify
	f, err := excelize.OpenFile(outFile)
	if err != nil {
		t.Fatalf("cannot open xlsx: %v", err)
	}
	defer f.Close()

	a1, _ := f.GetCellValue("Sheet1", "A1")
	if a1 != "DIM1" {
		t.Errorf("A1 = %q, want DIM1", a1)
	}
	b2, _ := f.GetCellValue("Sheet1", "B2")
	if b2 != "1000" {
		t.Errorf("B2 = %q, want 1000", b2)
	}
}

func TestRunExport_XLSXServerError(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.xlsx")
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

	if _, err := os.Stat(outFile); err == nil {
		t.Error("XLSX file should not be created on server error")
	}
}

func TestRunExport_XLSXUsesPOST(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportOut = filepath.Join(t.TempDir(), "out.xlsx")

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
		t.Errorf("XLSX export should use POST for tm1.Execute, got %q", capturedMethod)
	}
}

func TestRunExport_XLSXNoHeader(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	outFile := filepath.Join(t.TempDir(), "report.xlsx")
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

	f, err := excelize.OpenFile(outFile)
	if err != nil {
		t.Fatalf("cannot open xlsx: %v", err)
	}
	defer f.Close()

	a1, _ := f.GetCellValue("Sheet1", "A1")
	if a1 != "Revenue" {
		t.Errorf("A1 = %q, want Revenue (first data row, no header)", a1)
	}
}
