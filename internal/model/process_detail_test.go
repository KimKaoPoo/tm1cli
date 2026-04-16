package model

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestProcessDetailJSON(t *testing.T) {
	original := ProcessDetail{
		Name:            "LoadData",
		PrologProcedure: "#Region Prolog\nIF(1=1);\nEndIf;",
		DataProcedure:   "CellPutN(1, 'Sales', 'Jan', 'Actual');",
		Parameters: []ProcessParamDef{
			{Name: "pYear", Prompt: "Year", Value: "2024", Type: "String"},
		},
		DataSource: ProcessDataSource{
			Type:                    "ASCII",
			DataSourceNameForServer: "/data/file.csv",
			ASCIIDelimiterChar:      ",",
			ASCIIHeaderRecords:      1,
		},
		Variables: []ProcessVariable{
			{Name: "vYear", Type: "String", Position: 1, StartByte: 0, EndByte: 4},
		},
	}

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
		t.Errorf("PrologProcedure = %q, want %q", got.PrologProcedure, original.PrologProcedure)
	}
	if got.DataProcedure != original.DataProcedure {
		t.Errorf("DataProcedure = %q, want %q", got.DataProcedure, original.DataProcedure)
	}
	if len(got.Parameters) != 1 {
		t.Fatalf("Parameters count = %d, want 1", len(got.Parameters))
	}
	if got.Parameters[0].Name != "pYear" {
		t.Errorf("Parameters[0].Name = %q, want %q", got.Parameters[0].Name, "pYear")
	}
	if got.DataSource.Type != "ASCII" {
		t.Errorf("DataSource.Type = %q, want %q", got.DataSource.Type, "ASCII")
	}
	if got.DataSource.ASCIIHeaderRecords != 1 {
		t.Errorf("DataSource.ASCIIHeaderRecords = %d, want 1", got.DataSource.ASCIIHeaderRecords)
	}
	if len(got.Variables) != 1 {
		t.Fatalf("Variables count = %d, want 1", len(got.Variables))
	}
	if got.Variables[0].Name != "vYear" {
		t.Errorf("Variables[0].Name = %q, want %q", got.Variables[0].Name, "vYear")
	}
}

func TestProcessDetailYAML(t *testing.T) {
	original := ProcessDetail{
		Name:            "LoadData",
		PrologProcedure: "#Region Prolog\nIF(1=1);\nEndIf;",
		DataProcedure:   "CellPutN(1, 'Sales', 'Jan', 'Actual');",
		Parameters: []ProcessParamDef{
			{Name: "pYear", Prompt: "Year", Value: "2024", Type: "String"},
		},
		DataSource: ProcessDataSource{
			Type:               "None",
			ASCIIDelimiterChar: ",",
		},
		Variables: []ProcessVariable{
			{Name: "vYear", Type: "String", Position: 1, StartByte: 0, EndByte: 4},
		},
	}

	data, err := yaml.Marshal(original)
	if err != nil {
		t.Fatalf("yaml.Marshal error: %v", err)
	}

	var got ProcessDetail
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatalf("yaml.Unmarshal error: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name = %q, want %q", got.Name, original.Name)
	}
	if got.PrologProcedure != original.PrologProcedure {
		t.Errorf("PrologProcedure = %q, want %q", got.PrologProcedure, original.PrologProcedure)
	}
	if len(got.Parameters) != 1 {
		t.Fatalf("Parameters count = %d, want 1", len(got.Parameters))
	}
	if got.Parameters[0].Name != "pYear" {
		t.Errorf("Parameters[0].Name = %q, want %q", got.Parameters[0].Name, "pYear")
	}
	if got.DataSource.Type != "None" {
		t.Errorf("DataSource.Type = %q, want %q", got.DataSource.Type, "None")
	}
	if len(got.Variables) != 1 {
		t.Fatalf("Variables count = %d, want 1", len(got.Variables))
	}
}

func TestProcessDetailJSONStripsOData(t *testing.T) {
	raw := `{
		"@odata.context": "some-context-url",
		"Name": "LoadData",
		"PrologProcedure": "IF(1=1);",
		"MetadataProcedure": "",
		"DataProcedure": "",
		"EpilogProcedure": "",
		"HasSecurityAccess": false,
		"UIData": "<some-xml/>",
		"Parameters": [],
		"DataSource": {"Type": "None"},
		"Variables": []
	}`

	var got ProcessDetail
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.Name != "LoadData" {
		t.Errorf("Name = %q, want %q", got.Name, "LoadData")
	}
	if got.PrologProcedure != "IF(1=1);" {
		t.Errorf("PrologProcedure = %q, want %q", got.PrologProcedure, "IF(1=1);")
	}
	if got.DataSource.Type != "None" {
		t.Errorf("DataSource.Type = %q, want %q", got.DataSource.Type, "None")
	}
	// OData fields should be silently ignored
	if len(got.Parameters) != 0 {
		t.Errorf("Parameters count = %d, want 0", len(got.Parameters))
	}
	if len(got.Variables) != 0 {
		t.Errorf("Variables count = %d, want 0", len(got.Variables))
	}
}

func TestProcessParamDefValueTypes(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		wantValue interface{}
	}{
		{
			name:      "string value preserved",
			jsonInput: `{"Name":"pYear","Prompt":"Year","Value":"2024","Type":"String"}`,
			wantValue: "2024",
		},
		{
			name:      "numeric value preserved",
			jsonInput: `{"Name":"pCount","Prompt":"Count","Value":100,"Type":"Numeric"}`,
			wantValue: float64(100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p ProcessParamDef
			if err := json.Unmarshal([]byte(tt.jsonInput), &p); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}

			// Round-trip through JSON
			data, err := json.Marshal(p)
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}

			var got ProcessParamDef
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Second Unmarshal error: %v", err)
			}

			// JSON numbers unmarshal as float64 for interface{}
			switch v := tt.wantValue.(type) {
			case string:
				s, ok := got.Value.(string)
				if !ok {
					t.Fatalf("Value type = %T, want string", got.Value)
				}
				if s != v {
					t.Errorf("Value = %q, want %q", s, v)
				}
			case float64:
				f, ok := got.Value.(float64)
				if !ok {
					t.Fatalf("Value type = %T, want float64", got.Value)
				}
				if f != v {
					t.Errorf("Value = %v, want %v", f, v)
				}
			}
		})
	}
}
