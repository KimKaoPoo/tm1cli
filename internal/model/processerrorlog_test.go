package model

import (
	"encoding/json"
	"testing"
)

func TestProcessErrorLog_LogFile(t *testing.T) {
	tests := []struct {
		name string
		log  ProcessErrorLog
		want string
	}{
		{name: "filename only", log: ProcessErrorLog{Filename: "b.log"}, want: "b.log"},
		{name: "file only", log: ProcessErrorLog{File: "a.log"}, want: "a.log"},
		{name: "both set prefers filename", log: ProcessErrorLog{Filename: "b.log", File: "a.log"}, want: "b.log"},
		{name: "neither set", log: ProcessErrorLog{}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.log.LogFile(); got != tt.want {
				t.Errorf("LogFile() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestProcessErrorLog_UnmarshalCaseInsensitiveTimestamp verifies that the
// "TimeStamp" spelling (capital S, used by some TM1 versions) is absorbed by
// the "Timestamp" json tag via encoding/json's case-insensitive matching.
func TestProcessErrorLog_UnmarshalCaseInsensitiveTimestamp(t *testing.T) {
	raw := []byte(`{"TimeStamp":"2026-05-28T09:14:02Z","File":"x.log"}`)

	var log ProcessErrorLog
	if err := json.Unmarshal(raw, &log); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if log.Timestamp != "2026-05-28T09:14:02Z" {
		t.Errorf("Timestamp = %q, want %q", log.Timestamp, "2026-05-28T09:14:02Z")
	}
	if got := log.LogFile(); got != "x.log" {
		t.Errorf("LogFile() = %q, want %q", got, "x.log")
	}
}

func TestProcessErrorLogResponse_Unmarshal(t *testing.T) {
	raw := []byte(`{"value":[{"Timestamp":"2026-05-28T09:14:02Z","ProcessName":"Load","Filename":"e1.log"}]}`)

	var resp ProcessErrorLogResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if len(resp.Value) != 1 {
		t.Fatalf("len(Value) = %d, want 1", len(resp.Value))
	}
	if resp.Value[0].ProcessName != "Load" || resp.Value[0].LogFile() != "e1.log" {
		t.Errorf("unexpected entry: %+v", resp.Value[0])
	}
}
