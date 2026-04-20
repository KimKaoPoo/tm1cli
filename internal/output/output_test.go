package output

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// captureStdout captures os.Stdout output during the execution of fn.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStdout := os.Stdout
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = origStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

// captureStderr captures os.Stderr output during the execution of fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStderr := os.Stderr
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = origStderr

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

func TestPrintTable(t *testing.T) {
	tests := []struct {
		name     string
		headers  []string
		rows     [][]string
		contains []string
	}{
		{
			name:    "basic table with headers and rows",
			headers: []string{"NAME", "TYPE"},
			rows: [][]string{
				{"Sales", "Cube"},
				{"Budget", "Cube"},
			},
			contains: []string{"NAME", "TYPE", "Sales", "Cube", "Budget"},
		},
		{
			name:     "table with single column",
			headers:  []string{"NAME"},
			rows:     [][]string{{"Alpha"}, {"Beta"}},
			contains: []string{"NAME", "Alpha", "Beta"},
		},
		{
			name:     "empty rows prints only headers",
			headers:  []string{"NAME", "TYPE"},
			rows:     [][]string{},
			contains: []string{"NAME", "TYPE"},
		},
		{
			name:    "table with varying column widths",
			headers: []string{"SHORT", "A MUCH LONGER HEADER"},
			rows: [][]string{
				{"a", "b"},
				{"longvalue", "x"},
			},
			contains: []string{"SHORT", "A MUCH LONGER HEADER", "a", "longvalue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStdout(t, func() {
				PrintTable(tt.headers, tt.rows)
			})

			for _, want := range tt.contains {
				if !strings.Contains(output, want) {
					t.Errorf("output missing %q, got:\n%s", want, output)
				}
			}
		})
	}
}

func TestPrintTableAlignment(t *testing.T) {
	// Verify that tabwriter produces aligned output (columns separated by spaces)
	output := captureStdout(t, func() {
		PrintTable(
			[]string{"NAME", "COUNT"},
			[][]string{
				{"Short", "1"},
				{"VeryLongName", "999"},
			},
		)
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d:\n%s", len(lines), output)
	}

	// All lines should have at least 3 spaces between columns (tabwriter padding=3)
	for _, line := range lines {
		if !strings.Contains(line, "   ") {
			t.Errorf("line %q missing tabwriter padding (at least 3 spaces)", line)
		}
	}
}

func TestPrintJSON(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		validate func(t *testing.T, output string)
	}{
		{
			name: "simple object",
			data: map[string]string{"name": "Sales"},
			validate: func(t *testing.T, output string) {
				var result map[string]string
				if err := json.Unmarshal([]byte(output), &result); err != nil {
					t.Fatalf("output is not valid JSON: %v\noutput: %s", err, output)
				}
				if result["name"] != "Sales" {
					t.Errorf("name = %q, want %q", result["name"], "Sales")
				}
			},
		},
		{
			name: "array of objects",
			data: []map[string]string{{"Name": "A"}, {"Name": "B"}},
			validate: func(t *testing.T, output string) {
				var result []map[string]string
				if err := json.Unmarshal([]byte(output), &result); err != nil {
					t.Fatalf("output is not valid JSON: %v\noutput: %s", err, output)
				}
				if len(result) != 2 {
					t.Errorf("length = %d, want 2", len(result))
				}
			},
		},
		{
			name: "uses 2-space indent",
			data: map[string]int{"count": 42},
			validate: func(t *testing.T, output string) {
				// Should contain 2-space indentation
				if !strings.Contains(output, "  \"count\"") {
					t.Errorf("output should use 2-space indent, got:\n%s", output)
				}
			},
		},
		{
			name: "nested object",
			data: map[string]interface{}{
				"cube": map[string]string{"name": "Sales"},
			},
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "    \"name\"") {
					t.Errorf("nested key should have 4-space indent (2 levels), got:\n%s", output)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStdout(t, func() {
				PrintJSON(tt.data)
			})
			tt.validate(t, output)
		})
	}
}

func TestPrintSummary(t *testing.T) {
	tests := []struct {
		name       string
		shown      int
		total      int
		wantOutput bool
		contains   string
	}{
		{
			name:       "prints when shown < total",
			shown:      50,
			total:      283,
			wantOutput: true,
			contains:   "Showing 50 of 283",
		},
		{
			name:       "no output when shown equals total",
			shown:      10,
			total:      10,
			wantOutput: false,
		},
		{
			name:       "no output when shown greater than total",
			shown:      20,
			total:      10,
			wantOutput: false,
		},
		{
			name:       "includes hint about --filter and --all",
			shown:      5,
			total:      100,
			wantOutput: true,
			contains:   "--all",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStderr(t, func() {
				PrintSummary(tt.shown, tt.total)
			})

			if tt.wantOutput {
				if output == "" {
					t.Error("expected output, got empty string")
				}
				if tt.contains != "" && !strings.Contains(output, tt.contains) {
					t.Errorf("output missing %q, got: %s", tt.contains, output)
				}
			} else {
				if output != "" {
					t.Errorf("expected no output, got: %s", output)
				}
			}
		})
	}
}

func TestPrintTreeSummary(t *testing.T) {
	tests := []struct {
		name      string
		shown     int
		total     int
		unique    int
		wantEmpty bool
		contains  []string
	}{
		{
			name:      "no output when shown >= total",
			shown:     5,
			total:     5,
			unique:    5,
			wantEmpty: true,
		},
		{
			name:     "says rows and omits unique when unique == total",
			shown:    3,
			total:    10,
			unique:   10,
			contains: []string{"Showing 3 of 10 rows", "--filter", "--all"},
		},
		{
			name:     "includes unique-element count when diamonds present",
			shown:    3,
			total:    10,
			unique:   7,
			contains: []string{"Showing 3 of 10 rows", "7 unique elements"},
		},
		{
			name:     "UniqueElementsUnknown sentinel omits unique clause",
			shown:    3,
			total:    10,
			unique:   UniqueElementsUnknown,
			contains: []string{"Showing 3 of 10 rows"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := captureStderr(t, func() {
				PrintTreeSummary(tt.shown, tt.total, tt.unique)
			})

			if tt.wantEmpty {
				if got != "" {
					t.Errorf("expected empty output, got: %q", got)
				}
				return
			}
			for _, want := range tt.contains {
				if !strings.Contains(got, want) {
					t.Errorf("output missing %q, got: %s", want, got)
				}
			}
			if tt.unique == tt.total && strings.Contains(got, "unique") {
				t.Errorf("should not mention 'unique' when unique == total, got: %s", got)
			}
		})
	}
}

func TestPrintError(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		jsonMode bool
		validate func(t *testing.T, output string)
	}{
		{
			name:     "table mode prints Error prefix",
			msg:      "something went wrong",
			jsonMode: false,
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "Error: something went wrong") {
					t.Errorf("output = %q, want 'Error: something went wrong'", output)
				}
			},
		},
		{
			name:     "JSON mode prints structured JSON",
			msg:      "not found",
			jsonMode: true,
			validate: func(t *testing.T, output string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(output), &result); err != nil {
					t.Fatalf("output is not valid JSON: %v\noutput: %s", err, output)
				}
				errMsg, ok := result["error"].(string)
				if !ok {
					t.Fatal("missing 'error' key in JSON output")
				}
				if errMsg != "not found" {
					t.Errorf("error = %q, want %q", errMsg, "not found")
				}
			},
		},
		{
			name:     "JSON mode uses 2-space indent",
			msg:      "test error",
			jsonMode: true,
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "  \"error\"") {
					t.Errorf("output should use 2-space indent, got:\n%s", output)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStderr(t, func() {
				PrintError(tt.msg, tt.jsonMode)
			})
			tt.validate(t, output)
		})
	}
}

func TestPrintErrorWithStatus(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		status   int
		jsonMode bool
		validate func(t *testing.T, output string)
	}{
		{
			name:     "table mode prints Error prefix with HTTP status",
			msg:      "not found",
			status:   404,
			jsonMode: false,
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "Error (HTTP 404): not found") {
					t.Errorf("output = %q, want 'Error (HTTP 404): not found'", output)
				}
			},
		},
		{
			name:     "JSON mode includes error and status fields",
			msg:      "authentication failed",
			status:   401,
			jsonMode: true,
			validate: func(t *testing.T, output string) {
				var result map[string]interface{}
				if err := json.Unmarshal([]byte(output), &result); err != nil {
					t.Fatalf("output is not valid JSON: %v\noutput: %s", err, output)
				}
				errMsg, ok := result["error"].(string)
				if !ok {
					t.Fatal("missing 'error' key in JSON output")
				}
				if errMsg != "authentication failed" {
					t.Errorf("error = %q, want %q", errMsg, "authentication failed")
				}
				statusVal, ok := result["status"].(float64)
				if !ok {
					t.Fatal("missing 'status' key in JSON output")
				}
				if int(statusVal) != 401 {
					t.Errorf("status = %v, want 401", statusVal)
				}
			},
		},
		{
			name:     "JSON mode uses 2-space indent",
			msg:      "server error",
			status:   500,
			jsonMode: true,
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "  \"error\"") {
					t.Errorf("output should use 2-space indent, got:\n%s", output)
				}
			},
		},
		{
			name:     "table mode with 500 status",
			msg:      "internal server error",
			status:   500,
			jsonMode: false,
			validate: func(t *testing.T, output string) {
				if !strings.Contains(output, "Error (HTTP 500)") {
					t.Errorf("output should contain 'Error (HTTP 500)', got: %q", output)
				}
				if !strings.Contains(output, "internal server error") {
					t.Errorf("output should contain message, got: %q", output)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStderr(t, func() {
				PrintErrorWithStatus(tt.msg, tt.status, tt.jsonMode)
			})
			tt.validate(t, output)
		})
	}
}

func TestPrintWarning(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		contains string
	}{
		{
			name:     "warning with prefix",
			msg:      "filter not supported server-side",
			contains: "[warn] filter not supported server-side",
		},
		{
			name:     "empty message still has prefix",
			msg:      "",
			contains: "[warn]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStderr(t, func() {
				PrintWarning(tt.msg)
			})

			if !strings.Contains(output, tt.contains) {
				t.Errorf("output = %q, want it to contain %q", output, tt.contains)
			}
		})
	}
}
