package model

import (
	"encoding/json"
	"testing"
)

func TestMessageLogEntryUnmarshalID(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantID  string
	}{
		{
			name:    "OData Int64 number",
			payload: `{"ID":123,"TimeStamp":"2026-06-20T00:00:00Z","Level":"Info","Message":"ready"}`,
			wantID:  "123",
		},
		{
			name:    "IEEE754-compatible string",
			payload: `{"ID":"456","TimeStamp":"2026-06-20T00:00:00Z","Level":"Info","Message":"ready"}`,
			wantID:  "456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry MessageLogEntry
			if err := json.Unmarshal([]byte(tt.payload), &entry); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if entry.ID != tt.wantID {
				t.Errorf("ID = %q, want %q", entry.ID, tt.wantID)
			}
		})
	}
}
