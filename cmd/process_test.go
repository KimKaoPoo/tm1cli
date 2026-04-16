package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"tm1cli/internal/model"

	"gopkg.in/yaml.v3"
)

// --- filterSystemProcesses tests ---

func TestFilterSystemProcesses(t *testing.T) {
	tests := []struct {
		name       string
		procs      []model.Process
		showSystem bool
		wantNames  []string
	}{
		{
			name: "showSystem true returns all processes",
			procs: []model.Process{
				{Name: "LoadData"},
				{Name: "}SecurityRefresh"},
				{Name: "RunReport"},
			},
			showSystem: true,
			wantNames:  []string{"LoadData", "}SecurityRefresh", "RunReport"},
		},
		{
			name: "showSystem false filters out system processes",
			procs: []model.Process{
				{Name: "LoadData"},
				{Name: "}SecurityRefresh"},
				{Name: "RunReport"},
				{Name: "}CubeStats"},
			},
			showSystem: false,
			wantNames:  []string{"LoadData", "RunReport"},
		},
		{
			name:       "empty input returns empty",
			procs:      []model.Process{},
			showSystem: false,
			wantNames:  []string{},
		},
		{
			name:       "nil input returns empty",
			procs:      nil,
			showSystem: false,
			wantNames:  []string{},
		},
		{
			name: "all system processes filtered out",
			procs: []model.Process{
				{Name: "}SecurityRefresh"},
				{Name: "}CubeStats"},
				{Name: "}DimensionStats"},
			},
			showSystem: false,
			wantNames:  []string{},
		},
		{
			name: "no system processes returns all",
			procs: []model.Process{
				{Name: "LoadData"},
				{Name: "RunReport"},
			},
			showSystem: false,
			wantNames:  []string{"LoadData", "RunReport"},
		},
		{
			name: "showSystem true with no system processes",
			procs: []model.Process{
				{Name: "LoadData"},
			},
			showSystem: true,
			wantNames:  []string{"LoadData"},
		},
		{
			name: "process name containing } in middle is not filtered",
			procs: []model.Process{
				{Name: "Load}Data"},
				{Name: "}SystemProc"},
			},
			showSystem: false,
			wantNames:  []string{"Load}Data"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterSystemProcesses(tt.procs, tt.showSystem)

			// For nil/empty input with showSystem=false, result may be nil
			if len(tt.wantNames) == 0 {
				if len(result) != 0 {
					t.Errorf("got %d processes, want 0", len(result))
				}
				return
			}

			if len(result) != len(tt.wantNames) {
				t.Fatalf("got %d processes, want %d", len(result), len(tt.wantNames))
			}
			for i, want := range tt.wantNames {
				if result[i].Name != want {
					t.Errorf("result[%d].Name = %q, want %q", i, result[i].Name, want)
				}
			}
		})
	}
}

// --- filterProcessesByName tests ---

func TestFilterProcessesByName(t *testing.T) {
	allProcs := []model.Process{
		{Name: "LoadData"},
		{Name: "RunReport"},
		{Name: "LoadBudget"},
		{Name: "ExportSales"},
		{Name: "LOAD_DAILY"},
	}

	tests := []struct {
		name      string
		procs     []model.Process
		filter    string
		wantNames []string
	}{
		{
			name:      "case-insensitive partial match",
			procs:     allProcs,
			filter:    "load",
			wantNames: []string{"LoadData", "LoadBudget", "LOAD_DAILY"},
		},
		{
			name:      "uppercase filter matches lowercase",
			procs:     allProcs,
			filter:    "LOAD",
			wantNames: []string{"LoadData", "LoadBudget", "LOAD_DAILY"},
		},
		{
			name:      "mixed case filter",
			procs:     allProcs,
			filter:    "LoAd",
			wantNames: []string{"LoadData", "LoadBudget", "LOAD_DAILY"},
		},
		{
			name:      "exact name match",
			procs:     allProcs,
			filter:    "RunReport",
			wantNames: []string{"RunReport"},
		},
		{
			name:      "partial match in middle of name",
			procs:     allProcs,
			filter:    "port",
			wantNames: []string{"RunReport", "ExportSales"},
		},
		{
			name:      "no match returns empty",
			procs:     allProcs,
			filter:    "nonexistent",
			wantNames: []string{},
		},
		{
			name:      "empty filter matches everything",
			procs:     allProcs,
			filter:    "",
			wantNames: []string{"LoadData", "RunReport", "LoadBudget", "ExportSales", "LOAD_DAILY"},
		},
		{
			name:      "empty input returns empty",
			procs:     []model.Process{},
			filter:    "load",
			wantNames: []string{},
		},
		{
			name:      "nil input returns empty",
			procs:     nil,
			filter:    "load",
			wantNames: []string{},
		},
		{
			name:      "single character filter",
			procs:     allProcs,
			filter:    "x",
			wantNames: []string{"ExportSales"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterProcessesByName(tt.procs, tt.filter)

			if len(tt.wantNames) == 0 {
				if len(result) != 0 {
					t.Errorf("got %d processes, want 0", len(result))
				}
				return
			}

			if len(result) != len(tt.wantNames) {
				t.Fatalf("got %d processes, want %d", len(result), len(tt.wantNames))
			}
			for i, want := range tt.wantNames {
				if result[i].Name != want {
					t.Errorf("result[%d].Name = %q, want %q", i, result[i].Name, want)
				}
			}
		})
	}
}

// --- displayProcesses tests ---

func TestDisplayProcesses_TableOutput(t *testing.T) {
	procs := []model.Process{
		{Name: "LoadData"},
		{Name: "RunReport"},
		{Name: "ExportSales"},
	}

	// Save and restore global state
	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses(procs, 3, 0, false)
	})

	// Table output should contain the header and all process names
	if !strings.Contains(out, "NAME") {
		t.Errorf("table output missing header 'NAME', got:\n%s", out)
	}
	for _, p := range procs {
		if !strings.Contains(out, p.Name) {
			t.Errorf("table output missing process %q, got:\n%s", p.Name, out)
		}
	}
}

func TestDisplayProcesses_JSONOutput(t *testing.T) {
	procs := []model.Process{
		{Name: "LoadData"},
		{Name: "RunReport"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses(procs, 2, 0, true)
	})

	var result []model.Process
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(result) != 2 {
		t.Fatalf("got %d processes, want 2", len(result))
	}
	if result[0].Name != "LoadData" {
		t.Errorf("result[0].Name = %q, want %q", result[0].Name, "LoadData")
	}
	if result[1].Name != "RunReport" {
		t.Errorf("result[1].Name = %q, want %q", result[1].Name, "RunReport")
	}
}

func TestDisplayProcesses_CountTable(t *testing.T) {
	procs := []model.Process{
		{Name: "LoadData"},
		{Name: "RunReport"},
		{Name: "ExportSales"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = true

	out := captureStdout(t, func() {
		displayProcesses(procs, 3, 0, false)
	})

	if !strings.Contains(out, "3 processes") {
		t.Errorf("count output = %q, want it to contain '3 processes'", out)
	}
}

func TestDisplayProcesses_CountJSON(t *testing.T) {
	procs := []model.Process{
		{Name: "LoadData"},
		{Name: "RunReport"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = true

	out := captureStdout(t, func() {
		displayProcesses(procs, 5, 0, true)
	})

	var result map[string]int
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	// Count should use the total parameter, not len(procs)
	if result["count"] != 5 {
		t.Errorf("count = %d, want 5", result["count"])
	}
}

func TestDisplayProcesses_LimitTruncation(t *testing.T) {
	procs := []model.Process{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	var out string
	// Capture stderr too — displayProcesses calls PrintSummary which writes to stderr
	captureStderr(t, func() {
		out = captureStdout(t, func() {
			displayProcesses(procs, 5, 3, false)
		})
	})

	// Should show Alpha, Beta, Charlie but not Delta, Echo
	if !strings.Contains(out, "Alpha") {
		t.Errorf("output missing 'Alpha', got:\n%s", out)
	}
	if !strings.Contains(out, "Beta") {
		t.Errorf("output missing 'Beta', got:\n%s", out)
	}
	if !strings.Contains(out, "Charlie") {
		t.Errorf("output missing 'Charlie', got:\n%s", out)
	}
	if strings.Contains(out, "Delta") {
		t.Errorf("output should not contain 'Delta' (limit=3), got:\n%s", out)
	}
	if strings.Contains(out, "Echo") {
		t.Errorf("output should not contain 'Echo' (limit=3), got:\n%s", out)
	}
}

func TestDisplayProcesses_LimitJSON(t *testing.T) {
	procs := []model.Process{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses(procs, 3, 2, true)
	})

	var result []model.Process
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(result) != 2 {
		t.Errorf("got %d processes, want 2 (limited)", len(result))
	}
}

func TestDisplayProcesses_Summary(t *testing.T) {
	procs := []model.Process{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
		{Name: "Delta"},
		{Name: "Echo"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	stderr := captureStderr(t, func() {
		// Capture stdout too, but we don't check it here
		captureStdout(t, func() {
			displayProcesses(procs, 5, 3, false)
		})
	})

	// Summary should appear because 3 shown < 5 total
	if !strings.Contains(stderr, "Showing 3 of 5") {
		t.Errorf("stderr = %q, want it to contain 'Showing 3 of 5'", stderr)
	}
}

func TestDisplayProcesses_NoSummaryWhenAllShown(t *testing.T) {
	procs := []model.Process{
		{Name: "Alpha"},
		{Name: "Beta"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	stderr := captureStderr(t, func() {
		captureStdout(t, func() {
			displayProcesses(procs, 2, 0, false)
		})
	})

	// No summary when all processes are shown
	if stderr != "" {
		t.Errorf("expected no stderr output when all shown, got: %q", stderr)
	}
}

func TestDisplayProcesses_EmptyTable(t *testing.T) {
	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses([]model.Process{}, 0, 0, false)
	})

	// Should still print the header
	if !strings.Contains(out, "NAME") {
		t.Errorf("empty table should still have header 'NAME', got:\n%s", out)
	}
}

func TestDisplayProcesses_EmptyJSON(t *testing.T) {
	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses([]model.Process{}, 0, 0, true)
	})

	var result []model.Process
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, out)
	}
	if len(result) != 0 {
		t.Errorf("got %d processes, want 0", len(result))
	}
}

func TestDisplayProcesses_LimitZeroShowsAll(t *testing.T) {
	procs := []model.Process{
		{Name: "Alpha"},
		{Name: "Beta"},
		{Name: "Charlie"},
	}

	origCount := procListCount
	defer func() { procListCount = origCount }()
	procListCount = false

	out := captureStdout(t, func() {
		displayProcesses(procs, 3, 0, false)
	})

	// limit=0 means no limit, all should be shown
	for _, p := range procs {
		if !strings.Contains(out, p.Name) {
			t.Errorf("output missing %q when limit=0, got:\n%s", p.Name, out)
		}
	}
}

// --- process run param parsing tests ---

func TestProcessRunParamParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantName  string
		wantValue string
		wantErr   bool
	}{
		{
			name:      "standard Key=Value",
			input:     "pYear=2024",
			wantName:  "pYear",
			wantValue: "2024",
		},
		{
			name:      "value containing equals signs",
			input:     "pFilter=Region=US=East",
			wantName:  "pFilter",
			wantValue: "Region=US=East",
		},
		{
			name:      "empty value after equals",
			input:     "pSource=",
			wantName:  "pSource",
			wantValue: "",
		},
		{
			name:      "value with spaces",
			input:     "pPath=/some/file path/data.csv",
			wantName:  "pPath",
			wantValue: "/some/file path/data.csv",
		},
		{
			name:      "value with special characters",
			input:     "pQuery=SELECT * FROM table WHERE id=1",
			wantName:  "pQuery",
			wantValue: "SELECT * FROM table WHERE id=1",
		},
		{
			name:    "missing equals sign is invalid",
			input:   "pYearNoValue",
			wantErr: true,
		},
		{
			name:      "equals at start gives empty key",
			input:     "=value",
			wantName:  "",
			wantValue: "value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, value, err := parseProcessParam(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if name != tt.wantName {
				t.Errorf("name = %q, want %q", name, tt.wantName)
			}
			if value != tt.wantValue {
				t.Errorf("value = %q, want %q", value, tt.wantValue)
			}
		})
	}
}

func TestProcessRunParamToExecuteBody(t *testing.T) {
	tests := []struct {
		name     string
		params   []string
		wantJSON string
	}{
		{
			name:     "no params produces empty object",
			params:   nil,
			wantJSON: `{}`,
		},
		{
			name:     "single param",
			params:   []string{"pYear=2024"},
			wantJSON: `{"Parameters":[{"Name":"pYear","Value":"2024"}]}`,
		},
		{
			name:     "multiple params",
			params:   []string{"pYear=2024", "pRegion=US"},
			wantJSON: `{"Parameters":[{"Name":"pYear","Value":"2024"},{"Name":"pRegion","Value":"US"}]}`,
		},
		{
			name:     "param with equals in value",
			params:   []string{"pFilter=A=B=C"},
			wantJSON: `{"Parameters":[{"Name":"pFilter","Value":"A=B=C"}]}`,
		},
		{
			name:     "param with empty value",
			params:   []string{"pSource="},
			wantJSON: `{"Parameters":[{"Name":"pSource","Value":""}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body interface{}

			if len(tt.params) > 0 {
				params := make([]model.ProcessParameter, 0, len(tt.params))
				for _, p := range tt.params {
					name, value, err := parseProcessParam(p)
					if err != nil {
						t.Fatalf("parseProcessParam error: %v", err)
					}
					params = append(params, model.ProcessParameter{
						Name:  name,
						Value: value,
					})
				}
				body = model.ProcessExecuteBody{Parameters: params}
			} else {
				body = map[string]interface{}{}
			}

			data, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal error: %v", err)
			}

			if string(data) != tt.wantJSON {
				t.Errorf("JSON = %s, want %s", string(data), tt.wantJSON)
			}
		})
	}
}

// --- URL encoding tests ---

func TestProcessRunURLEncoding(t *testing.T) {
	// Verify that url.PathEscape produces the expected endpoint path.
	// This mirrors the exact logic in runProcessRun.
	tests := []struct {
		name         string
		processName  string
		wantEndpoint string
	}{
		{
			name:         "simple name",
			processName:  "LoadData",
			wantEndpoint: "Processes('LoadData')/tm1.Execute",
		},
		{
			name:         "name with spaces",
			processName:  "Load Data",
			wantEndpoint: "Processes('Load%20Data')/tm1.Execute",
		},
		{
			name:         "name with special characters",
			processName:  "Load/Data",
			wantEndpoint: "Processes('Load%2FData')/tm1.Execute",
		},
		{
			name:         "name with single quotes",
			processName:  "Load'Data",
			wantEndpoint: "Processes('Load%27Data')/tm1.Execute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := fmt.Sprintf("Processes('%s')/tm1.Execute", url.PathEscape(tt.processName))
			if endpoint != tt.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, tt.wantEndpoint)
			}
		})
	}
}

// ============================================================
// Integration tests — runProcessList / runProcessRun with httptest
// ============================================================

func TestRunProcessList_EndToEnd(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(processesJSON("LoadData", "RunReport", "ExportSales"))
	})

	captured := captureAll(t, func() {
		err := runProcessList(processListCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "NAME") {
		t.Errorf("output missing header 'NAME'")
	}
	for _, name := range []string{"LoadData", "RunReport", "ExportSales"} {
		if !strings.Contains(captured.Stdout, name) {
			t.Errorf("output missing process %q", name)
		}
	}
}

func TestRunProcessList_SystemProcessesHidden(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(processesJSON("LoadData", "}SecurityRefresh", "RunReport"))
	})

	captured := captureAll(t, func() {
		err := runProcessList(processListCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "LoadData") {
		t.Error("output should contain 'LoadData'")
	}
	if strings.Contains(captured.Stdout, "}SecurityRefresh") {
		t.Error("output should NOT contain system process '}SecurityRefresh'")
	}
}

func TestRunProcessRun_Success(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.Contains(r.URL.Path, "tm1.Execute") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	captured := captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "executed successfully") {
		t.Errorf("output should contain success message, got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Completed") {
		t.Errorf("output should contain 'Completed' status, got:\n%s", captured.Stdout)
	}
}

func TestRunProcessRun_WithParams(t *testing.T) {
	resetCmdFlags(t)
	procRunParams = []string{"pYear=2024", "pRegion=US"}

	var capturedBody string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			body, _ := io.ReadAll(r.Body)
			capturedBody = string(body)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Verify the POST body contains the parameters
	if !strings.Contains(capturedBody, "pYear") {
		t.Errorf("POST body should contain 'pYear', got: %s", capturedBody)
	}
	if !strings.Contains(capturedBody, "2024") {
		t.Errorf("POST body should contain '2024', got: %s", capturedBody)
	}
	if !strings.Contains(capturedBody, "pRegion") {
		t.Errorf("POST body should contain 'pRegion', got: %s", capturedBody)
	}

	// Verify JSON structure
	var parsed model.ProcessExecuteBody
	if err := json.Unmarshal([]byte(capturedBody), &parsed); err != nil {
		t.Fatalf("POST body is not valid JSON: %v", err)
	}
	if len(parsed.Parameters) != 2 {
		t.Errorf("expected 2 parameters, got %d", len(parsed.Parameters))
	}
}

func TestRunProcessRun_ParamWithEquals(t *testing.T) {
	resetCmdFlags(t)
	procRunParams = []string{"pFilter=Region=US=East"}

	var capturedBody string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			body, _ := io.ReadAll(r.Body)
			capturedBody = string(body)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Value should be "Region=US=East" (split on first = only)
	var parsed model.ProcessExecuteBody
	if err := json.Unmarshal([]byte(capturedBody), &parsed); err != nil {
		t.Fatalf("POST body is not valid JSON: %v", err)
	}
	if len(parsed.Parameters) != 1 {
		t.Fatalf("expected 1 parameter, got %d", len(parsed.Parameters))
	}
	if parsed.Parameters[0].Name != "pFilter" {
		t.Errorf("param name = %q, want 'pFilter'", parsed.Parameters[0].Name)
	}
	if parsed.Parameters[0].Value != "Region=US=East" {
		t.Errorf("param value = %q, want 'Region=US=East'", parsed.Parameters[0].Value)
	}
}

func TestRunProcessRun_ServerError(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`Internal server error`))
	})

	captured := captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"FailProcess"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "failed") {
		t.Errorf("output should contain 'failed', got:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "Error") {
		t.Errorf("output should contain 'Error' status, got:\n%s", captured.Stdout)
	}
}

func TestRunProcessRun_NotFound(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`Process not found`))
	})

	captured := captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"NonExistent"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	if !strings.Contains(captured.Stdout, "failed") {
		t.Errorf("output should contain 'failed', got:\n%s", captured.Stdout)
	}
}

func TestRunProcessRun_JSONOutput(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	captured := captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result model.ProcessRunResult
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if result.Process != "LoadData" {
		t.Errorf("process = %q, want 'LoadData'", result.Process)
	}
	if result.Status != "completed" {
		t.Errorf("status = %q, want 'completed'", result.Status)
	}
}

func TestRunProcessRun_JSONOutputError(t *testing.T) {
	resetCmdFlags(t)
	flagOutput = "json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`Server Error`))
	})

	captured := captureAll(t, func() {
		err := runProcessRun(processRunCmd, []string{"FailProcess"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	var result model.ProcessRunResult
	if err := json.Unmarshal([]byte(captured.Stdout), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, captured.Stdout)
	}
	if result.Status != "error" {
		t.Errorf("status = %q, want 'error'", result.Status)
	}
	if result.Process != "FailProcess" {
		t.Errorf("process = %q, want 'FailProcess'", result.Process)
	}
}

func TestRunProcessList_FilterFallbackWarning(t *testing.T) {
	resetCmdFlags(t)
	procListFilter = "load"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.RawQuery
		if strings.Contains(query, "$filter") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(processesJSON("LoadData", "LoadBudget", "RunReport"))
	})

	captured := captureAll(t, func() {
		err := runProcessList(processListCmd, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(captured.Stderr, "[warn]") {
		t.Error("should show filter fallback warning")
	}
	if !strings.Contains(captured.Stdout, "LoadData") {
		t.Error("output should contain 'LoadData' (matches filter)")
	}
	if strings.Contains(captured.Stdout, "RunReport") {
		t.Error("output should NOT contain 'RunReport' (doesn't match 'load' filter)")
	}
}

// ============================================================
// Helpers for dump/load tests
// ============================================================

func processDetailJSON() []byte {
	return []byte(`{
		"@odata.context": "...",
		"Name": "LoadData",
		"PrologProcedure": "#Region Prolog\nIF(1=1);\nEndIf;",
		"MetadataProcedure": "",
		"DataProcedure": "CellPutN(1, 'Sales', 'Jan', 'Actual');",
		"EpilogProcedure": "",
		"HasSecurityAccess": false,
		"Parameters": [
			{"Name": "pYear", "Prompt": "Year", "Value": "2024", "Type": "String"}
		],
		"DataSource": {
			"Type": "None"
		},
		"Variables": []
	}`)
}

// ============================================================
// Integration tests — runProcessDump
// ============================================================

func TestRunProcessDump_JSONStdout(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(processDetailJSON())
	})

	captured := captureAll(t, func() {
		err := runProcessDump(processDumpCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !json.Valid([]byte(captured.Stdout)) {
		t.Fatalf("stdout is not valid JSON:\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "LoadData") {
		t.Errorf("stdout missing 'LoadData':\n%s", captured.Stdout)
	}
	if !strings.Contains(captured.Stdout, "PrologProcedure") {
		t.Errorf("stdout missing 'PrologProcedure':\n%s", captured.Stdout)
	}
}

func TestRunProcessDump_JSONFile(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "loaddata.json")
	procDumpOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(processDetailJSON())
	})

	captureAll(t, func() {
		err := runProcessDump(processDumpCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	fileData, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}
	if !json.Valid(fileData) {
		t.Fatalf("file content is not valid JSON:\n%s", fileData)
	}
	var detail model.ProcessDetail
	if err := json.Unmarshal(fileData, &detail); err != nil {
		t.Fatalf("cannot parse file as ProcessDetail: %v", err)
	}
	if detail.Name != "LoadData" {
		t.Errorf("detail.Name = %q, want 'LoadData'", detail.Name)
	}
}

func TestRunProcessDump_YAMLFile(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "loaddata.yaml")
	procDumpOut = outFile

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(processDetailJSON())
	})

	captureAll(t, func() {
		err := runProcessDump(processDumpCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	fileData, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("cannot read output file: %v", err)
	}
	var detail model.ProcessDetail
	if err := yaml.Unmarshal(fileData, &detail); err != nil {
		t.Fatalf("cannot parse file as YAML ProcessDetail: %v", err)
	}
	if detail.Name != "LoadData" {
		t.Errorf("detail.Name = %q, want 'LoadData'", detail.Name)
	}
	// YAML tags use "prolog" not "PrologProcedure"
	if !strings.Contains(string(fileData), "prolog:") {
		t.Errorf("YAML output missing 'prolog:' key:\n%s", fileData)
	}
}

func TestRunProcessDump_NotFound(t *testing.T) {
	resetCmdFlags(t)

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`Not found`))
	})

	captureAll(t, func() {
		err := runProcessDump(processDumpCmd, []string{"NonExistent"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
}

func TestRunProcessDump_InvalidExtension(t *testing.T) {
	resetCmdFlags(t)
	procDumpOut = "file.txt"

	// No mock server — error should occur before any network call
	err := runProcessDump(processDumpCmd, []string{"LoadData"})
	if err == nil {
		t.Fatal("expected error for unsupported format, got nil")
	}
	if !strings.Contains(err.Error(), "Unsupported format") {
		t.Errorf("error = %q, want it to mention 'Unsupported format'", err.Error())
	}
}

// ============================================================
// Integration tests — runProcessLoad
// ============================================================

func writeProcessJSONFile(t *testing.T, dir string, detail model.ProcessDetail) string {
	t.Helper()
	data, err := json.MarshalIndent(detail, "", "  ")
	if err != nil {
		t.Fatalf("cannot marshal detail: %v", err)
	}
	path := filepath.Join(dir, "process.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("cannot write file: %v", err)
	}
	return path
}

func writeProcessYAMLFile(t *testing.T, dir string, detail model.ProcessDetail) string {
	t.Helper()
	data, err := yaml.Marshal(detail)
	if err != nil {
		t.Fatalf("cannot marshal detail: %v", err)
	}
	path := filepath.Join(dir, "process.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("cannot write file: %v", err)
	}
	return path
}

func TestRunProcessLoad_PatchSuccess(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:            "LoadData",
		PrologProcedure: "IF(1=1);",
		DataSource:      model.ProcessDataSource{Type: "None"},
	}
	procLoadFile = writeProcessJSONFile(t, tmpDir, detail)

	var capturedMethod string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	})

	captured := captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if capturedMethod != "PATCH" {
		t.Errorf("method = %q, want PATCH", capturedMethod)
	}
	if !strings.Contains(captured.Stdout, "updated successfully") {
		t.Errorf("stdout missing success message:\n%s", captured.Stdout)
	}
}

func TestRunProcessLoad_PatchFallbackToPost(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:       "NewProcess",
		DataSource: model.ProcessDataSource{Type: "None"},
	}
	procLoadFile = writeProcessJSONFile(t, tmpDir, detail)

	var capturedMethods []string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethods = append(capturedMethods, r.Method)
		if r.Method == "PATCH" {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`Not found`))
			return
		}
		// POST
		w.WriteHeader(http.StatusCreated)
	})

	captured := captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"NewProcess"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if len(capturedMethods) < 2 {
		t.Fatalf("expected at least 2 requests, got %d", len(capturedMethods))
	}
	if capturedMethods[0] != "PATCH" {
		t.Errorf("first method = %q, want PATCH", capturedMethods[0])
	}
	if capturedMethods[1] != "POST" {
		t.Errorf("second method = %q, want POST", capturedMethods[1])
	}
	if !strings.Contains(captured.Stdout, "created successfully") {
		t.Errorf("stdout missing created message:\n%s", captured.Stdout)
	}
}

func TestRunProcessLoad_CreateOnly(t *testing.T) {
	resetCmdFlags(t)
	procLoadCreateOnly = true

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:       "NewProcess",
		DataSource: model.ProcessDataSource{Type: "None"},
	}
	procLoadFile = writeProcessJSONFile(t, tmpDir, detail)

	var capturedMethods []string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethods = append(capturedMethods, r.Method)
		w.WriteHeader(http.StatusCreated)
	})

	captured := captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"NewProcess"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, m := range capturedMethods {
		if m == "PATCH" {
			t.Error("--create-only should not send PATCH request")
		}
	}
	if len(capturedMethods) == 0 || capturedMethods[0] != "POST" {
		t.Errorf("expected POST as first request, got: %v", capturedMethods)
	}
	if !strings.Contains(captured.Stdout, "created successfully") {
		t.Errorf("stdout missing created message:\n%s", captured.Stdout)
	}
}

func TestRunProcessLoad_UpdateOnly404(t *testing.T) {
	resetCmdFlags(t)
	procLoadUpdateOnly = true

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:       "NonExistent",
		DataSource: model.ProcessDataSource{Type: "None"},
	}
	procLoadFile = writeProcessJSONFile(t, tmpDir, detail)

	var capturedMethods []string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethods = append(capturedMethods, r.Method)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`Not found`))
	})

	captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"NonExistent"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	// Should not fall back to POST when --update-only is set
	for _, m := range capturedMethods {
		if m == "POST" {
			t.Error("--update-only should not fall back to POST on 404")
		}
	}
}

func TestRunProcessLoad_YAMLFile(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:            "LoadData",
		PrologProcedure: "IF(1=1);",
		DataSource:      model.ProcessDataSource{Type: "None"},
		Parameters:      []model.ProcessParamDef{{Name: "pYear", Type: "String"}},
	}
	procLoadFile = writeProcessYAMLFile(t, tmpDir, detail)

	var capturedBody []byte
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})

	captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"LoadData"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// PATCH body should be valid JSON containing the process fields
	if !json.Valid(capturedBody) {
		t.Fatalf("PATCH body is not valid JSON:\n%s", capturedBody)
	}
	if !strings.Contains(string(capturedBody), "LoadData") {
		t.Errorf("PATCH body missing 'LoadData':\n%s", capturedBody)
	}
}

func TestRunProcessLoad_MissingFileFlag(t *testing.T) {
	resetCmdFlags(t)
	// procLoadFile intentionally left empty

	err := runProcessLoad(processLoadCmd, []string{"LoadData"})
	if err == nil {
		t.Fatal("expected error for missing --file, got nil")
	}
	if !strings.Contains(err.Error(), "--file") && !strings.Contains(err.Error(), "-f") {
		t.Errorf("error = %q, want it to mention --file or -f", err.Error())
	}
}

func TestRunProcessLoad_CreateOnlyUpdateOnlyMutuallyExclusive(t *testing.T) {
	resetCmdFlags(t)
	procLoadCreateOnly = true
	procLoadUpdateOnly = true
	procLoadFile = "some.json" // won't be read; error should occur first

	err := runProcessLoad(processLoadCmd, []string{"LoadData"})
	if err == nil {
		t.Fatal("expected error for mutually exclusive flags, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error = %q, want it to mention 'mutually exclusive'", err.Error())
	}
}

func TestRunProcessLoad_FileNotFound(t *testing.T) {
	resetCmdFlags(t)
	procLoadFile = "/nonexistent/path/process.json"

	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		// Should not be reached
		t.Error("no network call should be made when file is missing")
		w.WriteHeader(http.StatusOK)
	})

	captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"LoadData"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})
}

func TestRunProcessLoad_NonNotFoundErrorNoFallback(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	detail := model.ProcessDetail{
		Name:       "LoadData",
		DataSource: model.ProcessDataSource{Type: "None"},
	}
	procLoadFile = writeProcessJSONFile(t, tmpDir, detail)

	var capturedMethods []string
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		capturedMethods = append(capturedMethods, r.Method)
		// Return 500 for PATCH — should NOT fall back to POST
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`Internal Server Error`))
	})

	captureAll(t, func() {
		err := runProcessLoad(processLoadCmd, []string{"LoadData"})
		if err != errSilent {
			t.Fatalf("expected errSilent, got: %v", err)
		}
	})

	for _, m := range capturedMethods {
		if m == "POST" {
			t.Error("500 error should not fall back to POST")
		}
	}
}

func TestProcessDumpLoadRoundtrip(t *testing.T) {
	resetCmdFlags(t)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "roundtrip.json")
	procDumpOut = outFile

	// Step 1: dump to file
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(processDetailJSON())
			return
		}
		t.Errorf("unexpected method %s in dump phase", r.Method)
	})

	captureAll(t, func() {
		if err := runProcessDump(processDumpCmd, []string{"LoadData"}); err != nil {
			t.Fatalf("dump failed: %v", err)
		}
	})

	// Reset flags for load phase
	zeroAllFlags()
	procLoadFile = outFile

	// Step 2: load from file, capture the PATCH body
	var capturedBody []byte
	setupMockTM1(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PATCH" {
			capturedBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("unexpected method %s in load phase", r.Method)
	})

	captureAll(t, func() {
		if err := runProcessLoad(processLoadCmd, []string{"LoadData"}); err != nil {
			t.Fatalf("load failed: %v", err)
		}
	})

	// The PATCH body should be valid JSON and match the original detail
	if !json.Valid(capturedBody) {
		t.Fatalf("PATCH body is not valid JSON:\n%s", capturedBody)
	}

	var loaded model.ProcessDetail
	if err := json.Unmarshal(capturedBody, &loaded); err != nil {
		t.Fatalf("cannot parse PATCH body: %v", err)
	}
	if loaded.Name != "LoadData" {
		t.Errorf("loaded.Name = %q, want 'LoadData'", loaded.Name)
	}
	if loaded.PrologProcedure != "#Region Prolog\nIF(1=1);\nEndIf;" {
		t.Errorf("loaded.PrologProcedure = %q, want original value", loaded.PrologProcedure)
	}
	if len(loaded.Parameters) != 1 || loaded.Parameters[0].Name != "pYear" {
		t.Errorf("loaded.Parameters = %+v, want one param 'pYear'", loaded.Parameters)
	}
}

