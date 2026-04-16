package model

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func intPtr(v int) *int { return &v }

func sampleProcessDetail() ProcessDetail {
	return ProcessDetail{
		Name:              "LoadData",
		PrologProcedure:   "IF(1=1);\nASCIIOutput('/tmp/test.csv', 'hello');\nENDIF;",
		MetadataProcedure: "",
		DataProcedure:     "CellPutN(Value, 'Cube', 'e1', 'e2');",
		EpilogProcedure:   "",
		Parameters: []ProcessParamDef{
			{Name: "pSource", Prompt: "Source file", Value: "data.csv", Type: "String"},
			{Name: "pYear", Prompt: "Year", Value: float64(2024), Type: "Numeric"},
		},
		DataSource: ProcessDataSource{
			Type:                    "ASCII",
			AsciiDecimalSeparator:   ".",
			AsciiDelimiterChar:      ",",
			AsciiDelimiterType:      "Character",
			AsciiHeaderRecords:      intPtr(1),
			DataSourceNameForServer: "/data/input.csv",
		},
		Variables: []ProcessVariable{
			{Name: "V1", Type: "String", StartByte: 1, EndByte: 20},
			{Name: "V2", Type: "Numeric", StartByte: 21, EndByte: 30},
		},
	}
}

func TestProcessDetailJSON(t *testing.T) {
	original := sampleProcessDetail()

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got ProcessDetail
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name = %q, want %q", got.Name, original.Name)
	}
	if got.PrologProcedure != original.PrologProcedure {
		t.Errorf("PrologProcedure mismatch")
	}
	if got.DataProcedure != original.DataProcedure {
		t.Errorf("DataProcedure mismatch")
	}
	if len(got.Parameters) != 2 {
		t.Fatalf("Parameters count = %d, want 2", len(got.Parameters))
	}
	if got.Parameters[0].Name != "pSource" {
		t.Errorf("Parameters[0].Name = %q, want %q", got.Parameters[0].Name, "pSource")
	}
	if got.DataSource.Type != "ASCII" {
		t.Errorf("DataSource.Type = %q, want %q", got.DataSource.Type, "ASCII")
	}
	if len(got.Variables) != 2 {
		t.Fatalf("Variables count = %d, want 2", len(got.Variables))
	}
	if got.Variables[0].Name != "V1" {
		t.Errorf("Variables[0].Name = %q, want %q", got.Variables[0].Name, "V1")
	}
}

func TestProcessDetailYAML(t *testing.T) {
	original := sampleProcessDetail()

	data, err := yaml.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	yamlStr := string(data)
	// Verify YAML uses snake_case keys
	if !strings.Contains(yamlStr, "prolog:") {
		t.Error("YAML should use 'prolog' key (not 'PrologProcedure')")
	}
	if !strings.Contains(yamlStr, "datasource:") {
		t.Error("YAML should use 'datasource' key")
	}
	if !strings.Contains(yamlStr, "start_byte:") {
		t.Error("YAML should use 'start_byte' key")
	}

	var got ProcessDetail
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name = %q, want %q", got.Name, original.Name)
	}
	if got.PrologProcedure != original.PrologProcedure {
		t.Errorf("PrologProcedure mismatch")
	}
	if len(got.Parameters) != 2 {
		t.Fatalf("Parameters count = %d, want 2", len(got.Parameters))
	}
	if got.DataSource.Type != "ASCII" {
		t.Errorf("DataSource.Type = %q, want %q", got.DataSource.Type, "ASCII")
	}
}

func TestProcessDetailJSON_EmptyProcedures(t *testing.T) {
	original := ProcessDetail{
		Name:              "EmptyProcs",
		PrologProcedure:   "",
		MetadataProcedure: "",
		DataProcedure:     "",
		EpilogProcedure:   "",
		DataSource:        ProcessDataSource{Type: "None"},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	jsonStr := string(data)
	// Empty strings must NOT be omitted
	if !strings.Contains(jsonStr, `"PrologProcedure":""`) {
		t.Errorf("PrologProcedure should be present as empty string in JSON")
	}
	if !strings.Contains(jsonStr, `"MetadataProcedure":""`) {
		t.Errorf("MetadataProcedure should be present as empty string in JSON")
	}

	var got ProcessDetail
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got.PrologProcedure != "" {
		t.Errorf("PrologProcedure = %q, want empty", got.PrologProcedure)
	}
}

func TestProcessDetailJSON_StripOData(t *testing.T) {
	input := `{
		"@odata.context": "$metadata#Processes/$entity",
		"@odata.etag": "W/\"abc123\"",
		"Name": "LoadData",
		"PrologProcedure": "# prolog",
		"MetadataProcedure": "",
		"DataProcedure": "",
		"EpilogProcedure": "",
		"Parameters": [],
		"DataSource": {"Type": "None"},
		"Variables": [],
		"Attributes": {"Caption": "Load Data"}
	}`

	var got ProcessDetail
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.Name != "LoadData" {
		t.Errorf("Name = %q, want 'LoadData'", got.Name)
	}
	if got.PrologProcedure != "# prolog" {
		t.Errorf("PrologProcedure = %q, want '# prolog'", got.PrologProcedure)
	}
	if got.DataSource.Type != "None" {
		t.Errorf("DataSource.Type = %q, want 'None'", got.DataSource.Type)
	}
}

func TestProcessParamDefJSON(t *testing.T) {
	tests := []struct {
		name      string
		param     ProcessParamDef
		wantValue interface{}
	}{
		{
			name:      "string value",
			param:     ProcessParamDef{Name: "pSource", Prompt: "File", Value: "data.csv", Type: "String"},
			wantValue: "data.csv",
		},
		{
			name:      "numeric value",
			param:     ProcessParamDef{Name: "pYear", Prompt: "Year", Value: float64(2024), Type: "Numeric"},
			wantValue: float64(2024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.param)
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}
			var got ProcessParamDef
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got.Name != tt.param.Name {
				t.Errorf("Name = %q, want %q", got.Name, tt.param.Name)
			}
			if got.Value != tt.wantValue {
				t.Errorf("Value = %v (%T), want %v (%T)", got.Value, got.Value, tt.wantValue, tt.wantValue)
			}
		})
	}
}

func TestProcessDataSourceJSON_Omitempty(t *testing.T) {
	t.Run("None type omits optional fields", func(t *testing.T) {
		ds := ProcessDataSource{Type: "None"}
		data, err := json.Marshal(ds)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}
		want := `{"Type":"None"}`
		if string(data) != want {
			t.Errorf("Marshal = %s, want %s", data, want)
		}
	})

	t.Run("ASCII type includes populated fields", func(t *testing.T) {
		ds := ProcessDataSource{
			Type:               "ASCII",
			AsciiDelimiterChar: ",",
			AsciiHeaderRecords: intPtr(1),
		}
		data, err := json.Marshal(ds)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}
		if !strings.Contains(string(data), `"asciiDelimiterChar":","`) {
			t.Error("should include asciiDelimiterChar")
		}
		if !strings.Contains(string(data), `"asciiHeaderRecords":1`) {
			t.Error("should include asciiHeaderRecords")
		}
	})

	t.Run("zero AsciiHeaderRecords preserved with pointer type", func(t *testing.T) {
		ds := ProcessDataSource{
			Type:               "ASCII",
			AsciiDelimiterChar: ",",
			AsciiHeaderRecords: intPtr(0),
		}
		data, err := json.Marshal(ds)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}
		if !strings.Contains(string(data), `"asciiHeaderRecords":0`) {
			t.Errorf("zero AsciiHeaderRecords should be present in JSON, got: %s", data)
		}

		var got ProcessDataSource
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if got.AsciiHeaderRecords == nil || *got.AsciiHeaderRecords != 0 {
			t.Errorf("AsciiHeaderRecords roundtrip failed, got: %v", got.AsciiHeaderRecords)
		}
	})
}

func TestProcessVariableJSON(t *testing.T) {
	v := ProcessVariable{Name: "V1", Type: "String", StartByte: 1, EndByte: 20}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got ProcessVariable
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if got != v {
		t.Errorf("Round-trip = %+v, want %+v", got, v)
	}
}
