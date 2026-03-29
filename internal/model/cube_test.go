package model

import (
	"encoding/json"
	"testing"
)

func TestCubeJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    Cube
		wantJSON string
	}{
		{
			name:     "all fields",
			input:    Cube{Name: "Sales", LastDataUpdate: "2024-01-15T10:30:00Z"},
			wantJSON: `{"Name":"Sales","LastDataUpdate":"2024-01-15T10:30:00Z"}`,
		},
		{
			name:     "omitempty LastDataUpdate",
			input:    Cube{Name: "Budget"},
			wantJSON: `{"Name":"Budget"}`,
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

			var got Cube
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got != tt.input {
				t.Errorf("Round-trip = %+v, want %+v", got, tt.input)
			}
		})
	}
}

func TestCubeResponseJSON(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantCount int
		wantFirst string
	}{
		{
			name:      "multiple cubes",
			jsonInput: `{"value":[{"Name":"Sales","LastDataUpdate":"2024-01-15T10:30:00Z"},{"Name":"Budget"}]}`,
			wantCount: 2,
			wantFirst: "Sales",
		},
		{
			name:      "empty value array",
			jsonInput: `{"value":[]}`,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp CubeResponse
			if err := json.Unmarshal([]byte(tt.jsonInput), &resp); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if len(resp.Value) != tt.wantCount {
				t.Fatalf("got %d cubes, want %d", len(resp.Value), tt.wantCount)
			}
			if tt.wantCount > 0 && resp.Value[0].Name != tt.wantFirst {
				t.Errorf("first cube = %s, want %s", resp.Value[0].Name, tt.wantFirst)
			}
		})
	}
}
