package model

import (
	"encoding/json"
	"testing"
)

func TestActiveUserJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    ActiveUser
		wantJSON string
	}{
		{
			name:     "populated name",
			input:    ActiveUser{Name: "admin"},
			wantJSON: `{"Name":"admin"}`,
		},
		{
			name:     "empty name",
			input:    ActiveUser{Name: ""},
			wantJSON: `{"Name":""}`,
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

			var got ActiveUser
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got != tt.input {
				t.Errorf("Round-trip = %+v, want %+v", got, tt.input)
			}
		})
	}
}

func TestServerConfigurationJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    ServerConfiguration
		wantJSON string
	}{
		{
			name: "all fields populated",
			input: ServerConfiguration{
				ServerName:     "MyTM1",
				ProductVersion: "2.0.9.4",
				AdminHost:      "tm1-server.example.com",
				HTTPPortNumber: 8080,
			},
			wantJSON: `{"ServerName":"MyTM1","ProductVersion":"2.0.9.4","AdminHost":"tm1-server.example.com","HTTPPortNumber":8080}`,
		},
		{
			name: "zero HTTPPortNumber",
			input: ServerConfiguration{
				ServerName:     "Dev",
				ProductVersion: "1.0.0",
				AdminHost:      "localhost",
				HTTPPortNumber: 0,
			},
			wantJSON: `{"ServerName":"Dev","ProductVersion":"1.0.0","AdminHost":"localhost","HTTPPortNumber":0}`,
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

			var got ServerConfiguration
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got != tt.input {
				t.Errorf("Round-trip = %+v, want %+v", got, tt.input)
			}
		})
	}
}

func TestServerConfigurationIgnoresExtraFields(t *testing.T) {
	// TM1 API may return additional fields not in our struct — they should be silently ignored.
	jsonInput := `{
		"ServerName": "ProdTM1",
		"ProductVersion": "2.0.9.4",
		"AdminHost": "prod.example.com",
		"HTTPPortNumber": 9000,
		"UnknownField": "should be ignored",
		"AnotherExtra": 42
	}`

	var got ServerConfiguration
	if err := json.Unmarshal([]byte(jsonInput), &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.ServerName != "ProdTM1" {
		t.Errorf("ServerName = %q, want %q", got.ServerName, "ProdTM1")
	}
	if got.ProductVersion != "2.0.9.4" {
		t.Errorf("ProductVersion = %q, want %q", got.ProductVersion, "2.0.9.4")
	}
	if got.AdminHost != "prod.example.com" {
		t.Errorf("AdminHost = %q, want %q", got.AdminHost, "prod.example.com")
	}
	if got.HTTPPortNumber != 9000 {
		t.Errorf("HTTPPortNumber = %d, want %d", got.HTTPPortNumber, 9000)
	}
}
