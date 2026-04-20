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
// TestLayoutToStringRows — exercises buildCellsetLayout + layoutToStringRows
// together for the legacy 2-axis cases previously covered by
// buildCellsetRows. New cases (UniqueName labels, N-axis, scalar/single-axis)
// live in their own tests below.
// ---------------------------------------------------------------------------

func TestLayoutToStringRows(t *testing.T) {
	tests := []struct {
		name        string
		resp        model.CellsetResponse
		wantHeaders []string
		wantRows    [][]string
		wantNilLay  bool
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
			name: "0 column tuples returns nil layout",
			resp: model.CellsetResponse{
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "Row1"}}},
					}},
				},
				Cells: nil,
			},
			wantNilLay: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := buildCellsetLayout(tt.resp)

			if tt.wantNilLay {
				if layout != nil {
					t.Errorf("expected nil layout, got %+v", layout)
				}
				return
			}
			if layout == nil {
				t.Fatalf("expected non-nil layout")
			}

			headers, rows := layoutToStringRows(layout)
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

func TestRunExport_CSVInvalidPath(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportOut = "/nonexistent/dir/report.csv"

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

	if !strings.Contains(captured.Stderr, "Cannot create file") {
		t.Errorf("stderr should contain 'Cannot create file', got:\n%s", captured.Stderr)
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

	t.Run("string cell values preserved without numeric conversion", func(t *testing.T) {
		strResp := model.CellsetResponse{
			Axes: []model.CellsetAxis{
				{Ordinal: 0, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Comment"}}},
				}},
				{Ordinal: 1, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Item1"}}},
				}},
			},
			Cells: []model.CellsetCell{
				{Ordinal: 0, Value: "00123"},
			},
		}

		outFile := filepath.Join(t.TempDir(), "strval.xlsx")
		captureAll(t, func() {
			err := writeXLSX(strResp, outFile, false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		f, err := excelize.OpenFile(outFile)
		if err != nil {
			t.Fatalf("cannot open xlsx: %v", err)
		}
		defer f.Close()

		// String cell value "00123" must be preserved as-is, not converted to 123
		b2, _ := f.GetCellValue("Sheet1", "B2")
		if b2 != "00123" {
			t.Errorf("B2 = %q, want 00123 (string value preserved)", b2)
		}
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

// ===========================================================================
// MDX Export Tests
// ===========================================================================

func mdxResponseJSON(id string) []byte {
	resp := model.CellsetResponse{
		ID: id,
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
	}
	data, _ := json.Marshal(resp)
	return data
}

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

	for _, want := range []string{"Jan", "Feb", "Revenue", "Cost", "1000", "2000", "500", "800"} {
		if !strings.Contains(captured.Stdout, want) {
			t.Errorf("output missing %q, got:\n%s", want, captured.Stdout)
		}
	}

	if !deleteCalled {
		t.Error("DELETE should be called to clean up cellset")
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

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[1][1] != "1000" {
		t.Errorf("Revenue/Jan = %q, want 1000", records[1][1])
	}
}

func TestRunExport_MDXServerError(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT INVALID FROM [Sales]"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": {"message": "Invalid MDX"}}`))
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
		runExport(exportCmd, []string{})
	})

	if !deleteCalled {
		t.Error("DELETE should be called even when cell fetch fails")
	}
}

func TestRunExport_MDXEndpointUsesPOST(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT {[Period].[Jan]} ON 0 FROM [Sales]"

	var capturedMethod string
	var capturedBody string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "ExecuteMDX"):
			capturedMethod = r.Method
			body := make([]byte, r.ContentLength)
			r.Body.Read(body)
			capturedBody = string(body)
			w.Header().Set("Content-Type", "application/json")
			w.Write(mdxResponseJSON("test-id"))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/Cells"):
			w.Header().Set("Content-Type", "application/json")
			w.Write(cellsCollectionJSON(testMDXCells))
		case r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		}
	})

	captureAll(t, func() {
		runExport(exportCmd, []string{})
	})

	if capturedMethod != "POST" {
		t.Errorf("ExecuteMDX should use POST, got %q", capturedMethod)
	}
	if !strings.Contains(capturedBody, "MDX") {
		t.Errorf("POST body should contain MDX key, got: %s", capturedBody)
	}
}

func TestRunExport_ViewAndMDXBoth(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	exportMDX = "SELECT ... FROM [Sales]"

	err := runExport(exportCmd, []string{"Sales"})
	if err == nil {
		t.Fatal("expected error when both --view and --mdx specified")
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
		t.Fatal("expected error when --view without cube")
	}
	if !strings.Contains(err.Error(), "Cube name is required") {
		t.Errorf("error should mention cube requirement, got: %s", err.Error())
	}
}

// ===========================================================================
// Advanced cellset parsing (#25) — helpers, N-axis, scalar, single-axis,
// collision handling, endpoint query shape.
// ===========================================================================

func TestParseBracketSegments(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"[A].[B].[C]", []string{"A", "B", "C"}},
		{"[A]", []string{"A"}},
		{"[A].[B].[C].[D]", []string{"A", "B", "C", "D"}},
		{"[foo]]bar].[baz]", []string{"foo]bar", "baz"}},
		{"[].[A]", []string{"", "A"}},
		{"", nil},
		{"NoBrackets", nil},
		{"[Unterminated", nil},
		{"[A]missingdot[B]", nil},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := parseBracketSegments(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("length = %d, want %d (got=%v)", len(got), len(tt.want), got)
			}
			for i, seg := range got {
				if seg != tt.want[i] {
					t.Errorf("seg[%d] = %q, want %q", i, seg, tt.want[i])
				}
			}
		})
	}
}

func TestDeriveDimensionLabel(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"[Period].[Period].[Jan]", "[Period]"},
		{"[Period].[FY].[2024]", "[Period:FY]"},
		{"[Version].[Actual]", "[Version]"},
		{"", ""},
		{"Garbage", ""},
		{"[Unterminated", ""},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := deriveDimensionLabel(tt.in); got != tt.want {
				t.Errorf("deriveDimensionLabel(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSortedAxesReordersByOrdinal(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 2, Tuples: []model.CellsetTuple{{Ordinal: 0}}},
			{Ordinal: 0, Tuples: []model.CellsetTuple{{Ordinal: 0}}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{{Ordinal: 0}}},
		},
	}
	got := sortedAxes(resp)
	if len(got) != 3 {
		t.Fatalf("got %d axes, want 3", len(got))
	}
	for i, a := range got {
		if a.Ordinal != i {
			t.Errorf("got[%d].Ordinal = %d, want %d", i, a.Ordinal, i)
		}
	}
}

func TestSortedAxesDropsAndWarnsDuplicates(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0},
			{Ordinal: 1},
			{Ordinal: 1},
			{Ordinal: 2},
		},
	}
	stderr := captureStderr(t, func() {
		got := sortedAxes(resp)
		if len(got) != 3 {
			t.Errorf("got %d axes, want 3 (duplicate 1 should be dropped)", len(got))
		}
	})
	if !strings.Contains(stderr, "duplicate axis ordinal 1") {
		t.Errorf("stderr missing duplicate warning, got: %s", stderr)
	}
}

func TestDisambiguateLabels(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"no dups", []string{"A", "B", "C"}, []string{"A", "B", "C"}},
		{"simple dup", []string{"A", "B", "A"}, []string{"A", "B", "A(2)"}},
		{"multiple dups", []string{"X", "X", "X", "Y", "X"}, []string{"X", "X(2)", "X(3)", "Y", "X(4)"}},
		{"empty", []string{}, []string{}},
		{"single", []string{"foo"}, []string{"foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := disambiguateLabels(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i, v := range got {
				if v != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, v, tt.want[i])
				}
			}
		})
	}
}

// cellsetWithTitle builds a 3-axis cellset: axis 0 (Jan, Feb), axis 1
// (Revenue, Cost with UniqueName on Account dimension), axis 2 single-tuple
// Version=Actual. Four cells at ordinals 0..3.
func cellsetWithTitle() model.CellsetResponse {
	return model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{
				Ordinal: 0,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan", UniqueName: "[Period].[Period].[Jan]"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb", UniqueName: "[Period].[Period].[Feb]"}}},
				},
			},
			{
				Ordinal: 1,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue", UniqueName: "[Account].[Account].[Revenue]"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost", UniqueName: "[Account].[Account].[Cost]"}}},
				},
			},
			{
				Ordinal: 2,
				Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Actual", UniqueName: "[Version].[Version].[Actual]"}}},
				},
			},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 100.0},
			{Ordinal: 1, Value: 200.0},
			{Ordinal: 2, Value: 50.0},
			{Ordinal: 3, Value: 80.0},
		},
	}
}

func TestPrintCellsetTableWithTitleAxis(t *testing.T) {
	got := captureStdout(t, func() {
		printCellsetTable(cellsetWithTitle())
	})
	wantSubs := []string{
		"Slicer: [Version] = Actual",
		"[Account]", "Jan", "Feb",
		"Revenue", "100", "200",
		"Cost", "50", "80",
	}
	for _, s := range wantSubs {
		if !strings.Contains(got, s) {
			t.Errorf("output missing %q\ngot:\n%s", s, got)
		}
	}
}

func TestPrintCellsetTableAlternateHierarchy(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan", UniqueName: "[Period].[FY].[Jan]"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue", UniqueName: "[Account].[Account].[Revenue]"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 100.0}},
	}
	got := captureStdout(t, func() { printCellsetTable(resp) })
	// The column-axis alternate-hierarchy member stays joined via " / " logic
	// (single member → just "Jan"); the row-axis label should be "[Account]".
	if !strings.Contains(got, "[Account]") {
		t.Errorf("output should contain [Account] row label, got:\n%s", got)
	}
	// Exercise the alternate-hierarchy label derivation by using it on a row axis.
	resp2 := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan", UniqueName: "[Period].[Period].[Jan]"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "2024", UniqueName: "[Period].[FY].[2024]"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 1.0}},
	}
	got2 := captureStdout(t, func() { printCellsetTable(resp2) })
	if !strings.Contains(got2, "[Period:FY]") {
		t.Errorf("output should contain [Period:FY] label for alt hierarchy, got:\n%s", got2)
	}
}

func TestPrintCellsetTableScalar(t *testing.T) {
	resp := model.CellsetResponse{
		Axes:  nil,
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 42.5}},
	}
	got := captureStdout(t, func() { printCellsetTable(resp) })
	if !strings.Contains(got, "Result: 42.5") {
		t.Errorf("expected 'Result: 42.5', got:\n%s", got)
	}
	if strings.Contains(got, "DIM1") {
		t.Errorf("scalar output should not contain DIM1, got:\n%s", got)
	}
}

func TestPrintCellsetTableSingleAxis(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
				{Ordinal: 2, Members: []model.CellsetMember{{Name: "Mar"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 10.0},
			{Ordinal: 1, Value: 20.0},
			{Ordinal: 2, Value: 30.0},
		},
	}
	got := captureStdout(t, func() { printCellsetTable(resp) })
	for _, s := range []string{"Jan", "Feb", "Mar", "10", "20", "30"} {
		if !strings.Contains(got, s) {
			t.Errorf("output missing %q, got:\n%s", s, got)
		}
	}
}

func TestPrintCellsetTableUniqueNameRowLabels(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Q1"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{
					{Name: "US", UniqueName: "[Region].[Region].[US]"},
					{Name: "Revenue", UniqueName: "[Account].[Account].[Revenue]"},
				}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 500.0}},
	}
	got := captureStdout(t, func() { printCellsetTable(resp) })
	for _, s := range []string{"[Region]", "[Account]"} {
		if !strings.Contains(got, s) {
			t.Errorf("output should contain %q, got:\n%s", s, got)
		}
	}
	if strings.Contains(got, "DIM1") || strings.Contains(got, "DIM2") {
		t.Errorf("output should not fall back to DIMN when UniqueName is present, got:\n%s", got)
	}
}

func TestPrintCellsetTableFallsBackToDimN(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 100.0}},
	}
	got := captureStdout(t, func() { printCellsetTable(resp) })
	if !strings.Contains(got, "DIM1") {
		t.Errorf("output should fall back to DIM1 when UniqueName missing, got:\n%s", got)
	}
}

func TestPrintCellsetTableMultiTupleHigherAxisFlattens(t *testing.T) {
	// Axis 0: 1 col (Jan). Axis 1: 2 rows (Revenue, Cost). Axis 2: 2 tuples
	// (Actual, Budget). Expected: 2*2 = 4 virtual rows; cells at ordinals
	// 0,1 (Actual/Rev, Actual/Cost), 2,3 (Budget/Rev, Budget/Cost).
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue", UniqueName: "[Account].[Account].[Revenue]"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost", UniqueName: "[Account].[Account].[Cost]"}}},
			}},
			{Ordinal: 2, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Actual", UniqueName: "[Version].[Version].[Actual]"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Budget", UniqueName: "[Version].[Version].[Budget]"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 100.0},
			{Ordinal: 1, Value: 50.0},
			{Ordinal: 2, Value: 110.0},
			{Ordinal: 3, Value: 55.0},
		},
	}
	layout := buildCellsetLayout(resp)
	if layout == nil {
		t.Fatal("expected non-nil layout")
	}
	if len(layout.RowCells) != 4 {
		t.Fatalf("expected 4 rows (2x2 flatten), got %d", len(layout.RowCells))
	}
	// Verify each row's (Account, Version) members + cell value.
	// Flat row index r decomposes innermost-first: i_1 = r % 2 (Account),
	// i_2 = r / 2 (Version). Expected sequence:
	//   r=0: (Revenue, Actual)  -> 100
	//   r=1: (Cost,    Actual)  -> 50
	//   r=2: (Revenue, Budget)  -> 110
	//   r=3: (Cost,    Budget)  -> 55
	wantMembers := [][]string{
		{"Revenue", "Actual"},
		{"Cost", "Actual"},
		{"Revenue", "Budget"},
		{"Cost", "Budget"},
	}
	wantCells := []float64{100, 50, 110, 55}
	for r := 0; r < 4; r++ {
		if layout.RowMembers[r][0] != wantMembers[r][0] || layout.RowMembers[r][1] != wantMembers[r][1] {
			t.Errorf("row %d members = %v, want %v", r, layout.RowMembers[r], wantMembers[r])
		}
		got, ok := layout.RowCells[r][0].(float64)
		if !ok || got != wantCells[r] {
			t.Errorf("row %d cell = %v, want %v", r, layout.RowCells[r][0], wantCells[r])
		}
	}
}

func TestWriteCSVWithTitleAxis(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.csv")
	captureAll(t, func() {
		if err := writeCSV(cellsetWithTitle(), outFile, false); err != nil {
			t.Fatalf("writeCSV error: %v", err)
		}
	})
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read CSV: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	// Axis 1 (Revenue, Cost) × axis 2 single-tuple (Actual) = 2 data rows.
	if len(records) != 3 {
		t.Fatalf("expected 3 rows (header + 2 data), got %d", len(records))
	}
	// Header: [Account], [Version], Jan, Feb
	wantHeaders := []string{"[Account]", "[Version]", "Jan", "Feb"}
	for i, h := range wantHeaders {
		if records[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
		}
	}
	// Every data row should have "Actual" in the [Version] column (index 1).
	for r := 1; r < len(records); r++ {
		if records[r][1] != "Actual" {
			t.Errorf("row %d col [Version] = %q, want Actual", r, records[r][1])
		}
	}
}

func TestWriteCSVScalar(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.csv")
	resp := model.CellsetResponse{
		Axes:  nil,
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 42.5}},
	}
	captureAll(t, func() {
		if err := writeCSV(resp, outFile, false); err != nil {
			t.Fatalf("writeCSV error: %v", err)
		}
	})
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read CSV: %v", err)
	}
	records, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 rows (header + 1 data), got %d", len(records))
	}
	if records[0][0] != "Value" {
		t.Errorf("header = %q, want Value", records[0][0])
	}
	if records[1][0] != "42.5" {
		t.Errorf("value = %q, want 42.5", records[1][0])
	}
}

func TestWriteCSVSingleAxis(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.csv")
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 10.0},
			{Ordinal: 1, Value: 20.0},
		},
	}
	captureAll(t, func() {
		if err := writeCSV(resp, outFile, false); err != nil {
			t.Fatalf("writeCSV error: %v", err)
		}
	})
	data, _ := os.ReadFile(outFile)
	records, _ := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if len(records) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(records))
	}
	wantHeaders := []string{"Jan", "Feb"}
	for i, h := range wantHeaders {
		if records[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, records[0][i], h)
		}
	}
	wantVals := []string{"10", "20"}
	for i, v := range wantVals {
		if records[1][i] != v {
			t.Errorf("val[%d] = %q, want %q", i, records[1][i], v)
		}
	}
}

func TestWriteCSVMultiTupleHigherAxis(t *testing.T) {
	// Reuse TestPrintCellsetTableMultiTupleHigherAxisFlattens fixture inline.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue", UniqueName: "[Account].[Account].[Revenue]"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Cost", UniqueName: "[Account].[Account].[Cost]"}}},
			}},
			{Ordinal: 2, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Actual", UniqueName: "[Version].[Version].[Actual]"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Budget", UniqueName: "[Version].[Version].[Budget]"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 100.0},
			{Ordinal: 1, Value: 50.0},
			{Ordinal: 2, Value: 110.0},
			{Ordinal: 3, Value: 55.0},
		},
	}
	outFile := filepath.Join(t.TempDir(), "out.csv")
	captureAll(t, func() {
		if err := writeCSV(resp, outFile, false); err != nil {
			t.Fatalf("writeCSV error: %v", err)
		}
	})
	data, _ := os.ReadFile(outFile)
	records, _ := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if len(records) != 5 { // 1 header + 4 data rows (2x2 flatten)
		t.Fatalf("expected 5 rows, got %d\n%s", len(records), string(data))
	}
}

func TestWriteCSVNoHeaderStillEmitsSlicerCols(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.csv")
	captureAll(t, func() {
		if err := writeCSV(cellsetWithTitle(), outFile, true); err != nil {
			t.Fatalf("writeCSV error: %v", err)
		}
	})
	data, _ := os.ReadFile(outFile)
	records, _ := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if len(records) != 2 { // 2 data rows (Revenue, Cost), no header
		t.Fatalf("expected 2 data rows (no header), got %d", len(records))
	}
	// Second column ([Version] slicer) must still hold "Actual" on every row.
	for r, row := range records {
		if row[1] != "Actual" {
			t.Errorf("row %d slicer col = %q, want Actual (noHeader must still emit slicer data)", r, row[1])
		}
	}
}

func TestCellsetToRecordsWithTitleAxis(t *testing.T) {
	records := cellsetToRecords(cellsetWithTitle())
	// Axis 1 (Revenue, Cost) × axis 2 single-tuple (Actual) = 2 records.
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	first := records[0]
	if first["[Version]"] != "Actual" {
		t.Errorf("record[0][[Version]] = %v, want Actual", first["[Version]"])
	}
	if first["[Account]"] != "Revenue" {
		t.Errorf("record[0][[Account]] = %v, want Revenue", first["[Account]"])
	}
	if first["Jan"].(float64) != 100.0 {
		t.Errorf("record[0][Jan] = %v, want 100.0", first["Jan"])
	}
}

func TestCellsetToRecordsScalar(t *testing.T) {
	resp := model.CellsetResponse{
		Axes:  nil,
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 42.5}},
	}
	records := cellsetToRecords(resp)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0]["Value"].(float64) != 42.5 {
		t.Errorf("Value = %v, want 42.5", records[0]["Value"])
	}
}

func TestCellsetToRecordsSingleAxis(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 10.0},
			{Ordinal: 1, Value: 20.0},
		},
	}
	records := cellsetToRecords(resp)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if records[0]["Jan"].(float64) != 10.0 {
		t.Errorf("Jan = %v, want 10.0", records[0]["Jan"])
	}
	if records[0]["Feb"].(float64) != 20.0 {
		t.Errorf("Feb = %v, want 20.0", records[0]["Feb"])
	}
}

func TestCellsetToRecordsDuplicateColumnHeadersDoNotOverwrite(t *testing.T) {
	// Two column tuples render to the same joined header "Jan / Actual".
	// Disambiguation should give "Jan / Actual" and "Jan / Actual(2)"; both
	// cell values must be retained in the record.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}, {Name: "Actual"}}},
				{Ordinal: 1, Members: []model.CellsetMember{{Name: "Jan"}, {Name: "Actual"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
			}},
		},
		Cells: []model.CellsetCell{
			{Ordinal: 0, Value: 100.0},
			{Ordinal: 1, Value: 200.0},
		},
	}
	records := cellsetToRecords(resp)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	rec := records[0]
	if rec["Jan / Actual"].(float64) != 100.0 {
		t.Errorf("first dup key value = %v, want 100.0", rec["Jan / Actual"])
	}
	if rec["Jan / Actual(2)"].(float64) != 200.0 {
		t.Errorf("second dup key value = %v, want 200.0 (ensure no overwrite)", rec["Jan / Actual(2)"])
	}
}

func TestCellsetToRecordsRowColCollisionDisambiguated(t *testing.T) {
	// Row-dim label [Period] (from UniqueName) collides with a column
	// header also "[Period]" (a tuple of a single member named "[Period]").
	// After global disambiguation they must be distinct keys.
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "[Period]"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan", UniqueName: "[Period].[Period].[Jan]"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 7.0}},
	}
	records := cellsetToRecords(resp)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	rec := records[0]
	// Row-dim label comes first in concat order, so it keeps "[Period]";
	// column header becomes "[Period](2)".
	if _, ok := rec["[Period]"]; !ok {
		t.Errorf("expected key [Period] (row-dim), got keys: %v", keys(rec))
	}
	if _, ok := rec["[Period](2)"]; !ok {
		t.Errorf("expected key [Period](2) (disambiguated col), got keys: %v", keys(rec))
	}
}

func keys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestWriteXLSXPreservesNumericTypes(t *testing.T) {
	// excelize GetCellType returns CellTypeUnset (the default) for plain
	// numeric cells — numeric storage in XLSX XML has no explicit type
	// attribute. The distinguishing test is: a SharedString-typed cell
	// would contain an index reference, while a numeric cell contains
	// the value directly. We verify by contrast: set one cell as
	// SetCellStr and confirm its type differs from a numeric cell.
	outFile := filepath.Join(t.TempDir(), "out.xlsx")
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
			}},
			{Ordinal: 1, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Revenue"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 123.45}},
	}
	captureAll(t, func() {
		if err := writeXLSX(resp, outFile, false); err != nil {
			t.Fatalf("writeXLSX error: %v", err)
		}
	})
	f, err := excelize.OpenFile(outFile)
	if err != nil {
		t.Fatalf("open xlsx: %v", err)
	}
	defer f.Close()
	// Value must round-trip exactly as numeric string (no string quoting).
	b2, err := f.GetCellValue("Sheet1", "B2")
	if err != nil {
		t.Fatalf("GetCellValue B2: %v", err)
	}
	if b2 != "123.45" {
		t.Errorf("B2 = %q, want 123.45", b2)
	}
	// Numeric cells must NOT be stored as SharedString (which would indicate
	// stringification). CellTypeUnset or CellTypeNumber are both acceptable
	// numeric storage indicators.
	t2, _ := f.GetCellType("Sheet1", "B2")
	if t2 == excelize.CellTypeSharedString || t2 == excelize.CellTypeInlineString {
		t.Errorf("B2 stored as string type %v, want numeric", t2)
	}
}

func TestWriteXLSXWithTitleAxis(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.xlsx")
	captureAll(t, func() {
		if err := writeXLSX(cellsetWithTitle(), outFile, false); err != nil {
			t.Fatalf("writeXLSX error: %v", err)
		}
	})
	f, err := excelize.OpenFile(outFile)
	if err != nil {
		t.Fatalf("open xlsx: %v", err)
	}
	defer f.Close()
	// Header row: [Account], [Version], Jan, Feb
	gotA, _ := f.GetCellValue("Sheet1", "A1")
	gotB, _ := f.GetCellValue("Sheet1", "B1")
	if gotA != "[Account]" {
		t.Errorf("A1 = %q, want [Account]", gotA)
	}
	if gotB != "[Version]" {
		t.Errorf("B1 = %q, want [Version]", gotB)
	}
	// Data row 2: Revenue, Actual, 100, 200
	gotB2, _ := f.GetCellValue("Sheet1", "B2")
	if gotB2 != "Actual" {
		t.Errorf("B2 = %q, want Actual (slicer constant)", gotB2)
	}
}

func TestExportViewEndpointIncludesUniqueName(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"

	var capturedPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.RawQuery + "?" + r.URL.Path
		// keep previous format for captured comparison:
		capturedPath = r.URL.Path + "?" + r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write(cellsetResponseJSON())
	})

	captureAll(t, func() {
		if err := runExport(exportCmd, []string{"MyCube"}); err != nil {
			t.Fatalf("runExport error: %v", err)
		}
	})

	if !strings.Contains(capturedPath, "Members($select=Name,UniqueName)") {
		t.Errorf("view endpoint should request UniqueName, got: %s", capturedPath)
	}
}

func TestExportMDXEndpointIncludesUniqueName(t *testing.T) {
	resetCmdFlags(t)
	exportMDX = "SELECT {[X]} ON 0 FROM [C]"

	// Capture the FIRST request (ExecuteMDX); later requests (cells page
	// fetch, cellset cleanup DELETE) should not overwrite.
	var firstPath string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if firstPath == "" {
			firstPath = r.URL.Path + "?" + r.URL.RawQuery
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "ExecuteMDX") {
			resp := model.CellsetResponse{
				ID: "cs1",
				Axes: []model.CellsetAxis{
					{Ordinal: 0, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "X"}}},
					}},
					{Ordinal: 1, Tuples: []model.CellsetTuple{
						{Ordinal: 0, Members: []model.CellsetMember{{Name: "R"}}},
					}},
				},
				Cells: []model.CellsetCell{{Ordinal: 0, Value: 1.0}},
			}
			data, _ := json.Marshal(resp)
			w.Write(data)
			return
		}
		// Cells page fetch: return an empty value array so paging terminates.
		w.Write([]byte(`{"value":[]}`))
	})

	captureAll(t, func() {
		_ = runExport(exportCmd, []string{})
	})

	if !strings.Contains(firstPath, "Members($select=Name,UniqueName)") {
		t.Errorf("MDX endpoint should request UniqueName, got: %s", firstPath)
	}
}

func TestRawJSONOutputPreservesUniqueName(t *testing.T) {
	resp := model.CellsetResponse{
		Axes: []model.CellsetAxis{
			{Ordinal: 0, Tuples: []model.CellsetTuple{
				{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan", UniqueName: "[Period].[Period].[Jan]"}}},
			}},
		},
		Cells: []model.CellsetCell{{Ordinal: 0, Value: 1.0}},
	}
	got := captureStdout(t, func() { output.PrintJSON(resp) })
	// output.PrintJSON uses indent with space after colon.
	if !strings.Contains(got, `"UniqueName": "[Period].[Period].[Jan]"`) {
		t.Errorf("raw JSON should include UniqueName, got:\n%s", got)
	}
}

func TestRunExportScalarCSVFile(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	outFile := filepath.Join(t.TempDir(), "out.csv")
	exportOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		resp := model.CellsetResponse{
			Axes:  nil,
			Cells: []model.CellsetCell{{Ordinal: 0, Value: 42.5}},
		}
		data, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	captureAll(t, func() {
		if err := runExport(exportCmd, []string{"C"}); err != nil {
			t.Fatalf("runExport error: %v", err)
		}
	})
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	records, _ := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if len(records) != 2 || records[0][0] != "Value" || records[1][0] != "42.5" {
		t.Errorf("unexpected csv:\n%s", string(data))
	}
}

func TestRunExportSingleAxisCSVFile(t *testing.T) {
	resetCmdFlags(t)
	exportView = "Default"
	outFile := filepath.Join(t.TempDir(), "out.csv")
	exportOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		resp := model.CellsetResponse{
			Axes: []model.CellsetAxis{
				{Ordinal: 0, Tuples: []model.CellsetTuple{
					{Ordinal: 0, Members: []model.CellsetMember{{Name: "Jan"}}},
					{Ordinal: 1, Members: []model.CellsetMember{{Name: "Feb"}}},
				}},
			},
			Cells: []model.CellsetCell{
				{Ordinal: 0, Value: 10.0},
				{Ordinal: 1, Value: 20.0},
			},
		}
		data, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})

	captureAll(t, func() {
		if err := runExport(exportCmd, []string{"C"}); err != nil {
			t.Fatalf("runExport error: %v", err)
		}
	})
	data, _ := os.ReadFile(outFile)
	records, _ := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if len(records) != 2 {
		t.Fatalf("expected 2 rows, got %d:\n%s", len(records), string(data))
	}
	if records[0][0] != "Jan" || records[0][1] != "Feb" {
		t.Errorf("header = %v, want [Jan Feb]", records[0])
	}
	if records[1][0] != "10" || records[1][1] != "20" {
		t.Errorf("values = %v, want [10 20]", records[1])
	}
}
