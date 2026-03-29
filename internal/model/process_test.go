package model

import (
	"encoding/json"
	"testing"
)

func TestProcessJSON(t *testing.T) {
	p := Process{Name: "RunDaily"}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"Name":"RunDaily"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got Process
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != p {
		t.Errorf("Round-trip = %+v, want %+v", got, p)
	}
}

func TestProcessResponseJSON(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantCount int
		wantFirst string
	}{
		{
			name:      "multiple processes",
			jsonInput: `{"value":[{"Name":"RunDaily"},{"Name":"LoadData"}]}`,
			wantCount: 2,
			wantFirst: "RunDaily",
		},
		{
			name:      "empty value array",
			jsonInput: `{"value":[]}`,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp ProcessResponse
			if err := json.Unmarshal([]byte(tt.jsonInput), &resp); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if len(resp.Value) != tt.wantCount {
				t.Fatalf("got %d processes, want %d", len(resp.Value), tt.wantCount)
			}
			if tt.wantCount > 0 && resp.Value[0].Name != tt.wantFirst {
				t.Errorf("first process = %s, want %s", resp.Value[0].Name, tt.wantFirst)
			}
		})
	}
}

func TestProcessParameterJSON(t *testing.T) {
	p := ProcessParameter{Name: "pYear", Value: "2024"}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"Name":"pYear","Value":"2024"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got ProcessParameter
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != p {
		t.Errorf("Round-trip = %+v, want %+v", got, p)
	}
}

func TestProcessExecuteBodyJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    ProcessExecuteBody
		wantJSON string
	}{
		{
			name: "with parameters",
			input: ProcessExecuteBody{
				Parameters: []ProcessParameter{
					{Name: "pYear", Value: "2024"},
					{Name: "pRegion", Value: "US"},
				},
			},
			wantJSON: `{"Parameters":[{"Name":"pYear","Value":"2024"},{"Name":"pRegion","Value":"US"}]}`,
		},
		{
			name:     "nil parameters omitted",
			input:    ProcessExecuteBody{},
			wantJSON: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}
			if string(data) != tt.wantJSON {
				t.Errorf("Marshal = %s, want %s", data, tt.wantJSON)
			}
		})
	}
}

func TestProcessRunResultJSON(t *testing.T) {
	r := ProcessRunResult{
		Process:    "RunDaily",
		Status:     "CompletedSuccessfully",
		DurationMs: 1250,
		Message:    "Process completed.",
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got ProcessRunResult
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != r {
		t.Errorf("Round-trip = %+v, want %+v", got, r)
	}
}
