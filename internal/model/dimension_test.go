package model

import (
	"encoding/json"
	"testing"
)

func TestDimensionJSON(t *testing.T) {
	d := Dimension{Name: "Region"}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	want := `{"Name":"Region"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got Dimension
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != d {
		t.Errorf("Round-trip = %+v, want %+v", got, d)
	}
}

func TestDimensionResponseJSON(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantCount int
		wantFirst string
	}{
		{
			name:      "multiple dimensions",
			jsonInput: `{"value":[{"Name":"Region"},{"Name":"Period"}]}`,
			wantCount: 2,
			wantFirst: "Region",
		},
		{
			name:      "empty value array",
			jsonInput: `{"value":[]}`,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp DimensionResponse
			if err := json.Unmarshal([]byte(tt.jsonInput), &resp); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if len(resp.Value) != tt.wantCount {
				t.Fatalf("got %d dims, want %d", len(resp.Value), tt.wantCount)
			}
			if tt.wantCount > 0 && resp.Value[0].Name != tt.wantFirst {
				t.Errorf("first dim = %s, want %s", resp.Value[0].Name, tt.wantFirst)
			}
		})
	}
}

func TestElementJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    Element
		wantJSON string
	}{
		{
			name:     "numeric element",
			input:    Element{Name: "Q1", Type: "Numeric"},
			wantJSON: `{"Name":"Q1","Type":"Numeric"}`,
		},
		{
			name:     "string element",
			input:    Element{Name: "USA", Type: "String"},
			wantJSON: `{"Name":"USA","Type":"String"}`,
		},
		{
			name:     "consolidated element",
			input:    Element{Name: "Total", Type: "Consolidated"},
			wantJSON: `{"Name":"Total","Type":"Consolidated"}`,
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

			var got Element
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got.Name != tt.input.Name || got.Type != tt.input.Type || len(got.Components) != len(tt.input.Components) {
				t.Errorf("Round-trip = %+v, want %+v", got, tt.input)
			}
		})
	}
}

func TestElementJSON_WithComponents(t *testing.T) {
	e := Element{
		Name:       "Year",
		Type:       "Consolidated",
		Components: []Component{{Name: "Q1"}, {Name: "Q2"}},
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	want := `{"Name":"Year","Type":"Consolidated","Components":[{"Name":"Q1"},{"Name":"Q2"}]}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var got Element
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got.Name != e.Name || got.Type != e.Type || len(got.Components) != 2 {
		t.Errorf("Round-trip lost fields: %+v", got)
	}
	if got.Components[0].Name != "Q1" || got.Components[1].Name != "Q2" {
		t.Errorf("Components order not preserved: %+v", got.Components)
	}
}

func TestElementJSON_OmitsComponentsWhenNil(t *testing.T) {
	e := Element{Name: "Jan", Type: "Numeric"}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	got := string(data)
	if got != `{"Name":"Jan","Type":"Numeric"}` {
		t.Errorf("Marshal = %s, want %s", got, `{"Name":"Jan","Type":"Numeric"}`)
	}
}

func TestElementResponseJSON(t *testing.T) {
	jsonInput := `{"value":[{"Name":"Q1","Type":"Numeric"},{"Name":"Total","Type":"Consolidated"}]}`

	var resp ElementResponse
	if err := json.Unmarshal([]byte(jsonInput), &resp); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if len(resp.Value) != 2 {
		t.Fatalf("got %d elements, want 2", len(resp.Value))
	}
	if resp.Value[0].Name != "Q1" || resp.Value[0].Type != "Numeric" {
		t.Errorf("first element = %+v, want {Q1, Numeric}", resp.Value[0])
	}
	if resp.Value[1].Name != "Total" || resp.Value[1].Type != "Consolidated" {
		t.Errorf("second element = %+v, want {Total, Consolidated}", resp.Value[1])
	}
}
